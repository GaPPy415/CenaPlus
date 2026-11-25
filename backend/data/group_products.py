"""Non-LLM cross-market product grouping (Layers 1-5).

Algorithm:
  Layer 1 (Blocking & Safety):
    - Repair Zur backtick-for-ж leaks, recover words glued to ERP codes, apply
      reviewed `product_name_token_aliases`, strip leaked own-brand tokens.
    - Filter `variant` to physical keys + short/alphanumeric model codes + raw
      safety specs (battery AA/AAA, SPF, fat %, age 45+/65+).
    - Recover missing quantities from `price / singular_price` (+/-4% snap).
    - Block by `(manufacturer_id, qty_kind, base_qty)` folding g=ml and dropping
      `is_fluid` from the block key; run `build_components()` inside each block.
  Layers 2-4 (Pure-IR Matcher inside each `(manufacturer_id, base_qty)` block):
    - Self-supervised same-market differentiator discovery.
    - Block/brand IDF-weighted matching with Cyrillic consonant-skeleton, stem,
      and prefix-abbreviation alignment.
    - Hard vetoes (numeric, form words, protected modifiers, raw safety, price
      ratio) + Mutual-Nearest-Neighbor margin with non-transitive clustering.
  Layer 5 (Optional Local Embeddings via Ollama `bge-m3` cache):
    - Strictly inside multi-market Core Grocery `(manufacturer_id, base_qty)`
      blocks, requiring same `sub_category`, all Layer 1-4 hard vetoes, MNN
      margin, and `cos >= --bge-threshold` (default 0.72, `price_ratio <= 1.8`).
    - Skips gracefully when Ollama is unavailable or `--no-embeddings` is set.

Usage:
    python -m backend.data.group_products --dry-run
    python -m backend.data.group_products --dry-run --no-embeddings
    python -m backend.data.group_products --dry-run --warm-cache
    python -m backend.data.group_products --apply        # truncate groups, rebuild
"""

from __future__ import annotations

import argparse
import itertools
import math
import random
import re
import sys
import time
import uuid
from collections import Counter, defaultdict

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

from backend.data.db_utils import connect_to_db
from backend.data.grouping_rules import (
    CORE_GROCERY_CATEGORIES,
    NON_FOOD_CATEGORIES,
    clean_name_tokens,
    clean_variant_allowlist,
    extract_raw_safety_sig,
    guarded_fuzzy_merge,
    recover_qty_from_singular_price,
    smart_merge_block,
    unified_base_qty,
)
from backend.data.local_embeddings import (
    DEFAULT_BGE_MARGIN,
    DEFAULT_BGE_MAX_PRICE_RATIO,
    DEFAULT_BGE_THRESHOLD,
    fetch_or_compute_embeddings,
    layer5_merge_block,
)

FEATURE_ROWS_SQL = """
    SELECT id, market, manufacturer_id, product_name, product_name_key, product_name_sorted,
           quantity_value, quantity_unit, is_fluid, variant, price, singular_price, in_stock,
           main_category, sub_category, name
    FROM products
    WHERE features_extracted_at IS NOT NULL
"""

UNEXTRACTED_SQL = """
    SELECT id, market, name, main_category, sub_category
    FROM products
    WHERE features_extracted_at IS NULL
"""

INSERT_GROUPS_SQL = """
    INSERT INTO groups (id, name, main_category, sub_category, manufacturer_id,
                        product_name, product_name_sorted, quantity_value, quantity_unit,
                        is_fluid, variant_key, clean_name)
    VALUES %s
"""

UPDATE_PRODUCTS_SQL = """
    UPDATE products AS p
    SET group_id = v.group_id::uuid,
        main_category = v.main_category,
        sub_category = v.sub_category
    FROM (VALUES %s) AS v(id, group_id, main_category, sub_category)
    WHERE p.id = v.id::uuid
"""


def load_name_aliases(conn) -> dict[int, dict[str, str]]:
    """Token alias map from `product_name_token_aliases`, or {} when absent."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.product_name_token_aliases')")
        if cur.fetchone()[0] is None:
            return {}
        cur.execute("""
            SELECT manufacturer_id, alias_token, canonical_token
            FROM product_name_token_aliases
        """)
        aliases: dict[int, dict[str, str]] = defaultdict(dict)
        for manufacturer_id, alias_token, canonical_token in cur.fetchall():
            aliases[manufacturer_id][alias_token] = canonical_token
        return dict(aliases)


def load_manufacturer_info(conn) -> tuple[dict[int, set[str]], dict[int, str]]:
    """Return (own_tokens_map, display_name_map) from manufacturers + manufacturer_aliases."""
    own: dict[int, set[str]] = defaultdict(set)
    display_names: dict[int, str] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, canonical_name, display_name FROM manufacturers")
        for mfr_id, cname, dname in cur.fetchall():
            for tok in (cname or "").split():
                if len(tok) >= 4:
                    own[mfr_id].add(tok)
            label = (dname or cname or "").strip()
            if label:
                # Preserve existing mixed casing (e.g. "Bimilk", "Nivea"); title-case if all-lower or all-upper
                if label.islower() or label.isupper():
                    label = label.title()
                display_names[mfr_id] = label
        cur.execute("SELECT manufacturer_id, alias FROM manufacturer_aliases")
        for mfr_id, alias in cur.fetchall():
            for tok in (alias or "").split():
                if len(tok) >= 4:
                    own[mfr_id].add(tok)
    return dict(own), display_names


_FALLBACK_MARKET_RANK = {
    "reptil": 0,
    "bigshop": 1,
    "elida": 2,
    "ramstore": 3,
    "vero": 4,
    "zito": 5,
    "stokomak": 6,
    "spar": 7,
    "kipper": 8,
    "kam": 9,
    "dim": 10,
    "zur": 11,
}
_ELIDA_TRAILING_NOISE_RE = re.compile(r"\s{2,}\d+#?\s*$")
_ERP_TRAILING_RE = re.compile(r"\b\d{5,8}\b")


def _format_latin_qty(qty_value, qty_unit: str | None) -> str:
    if qty_value is None or not qty_unit:
        return ""
    val = float(qty_value)
    if val <= 0:
        return ""
    # Promote >=1000g/ml cleanly when synthesized from recovered base qty
    unit = qty_unit
    if unit == "g" and val >= 1000 and val % 100 == 0:
        val, unit = val / 1000.0, "kg"
    elif unit == "ml" and val >= 1000 and val % 100 == 0:
        val, unit = val / 1000.0, "l"
    num_str = f"{val:g}"
    return f"{num_str}{unit}"


def _sentence_case(text: str) -> str:
    cleaned = " ".join(text.strip(" -–—.,;/").split())
    if not cleaned:
        return ""
    if cleaned.isupper():
        cleaned = cleaned.lower()
    for i, ch in enumerate(cleaned):
        if ch.isalpha():
            return cleaned[:i] + ch.upper() + cleaned[i + 1 :]
    return cleaned


def _clean_raw_fallback(name: str, market: str | None) -> str:
    text = (name or "").strip()
    if market == "elida":
        text = _ELIDA_TRAILING_NOISE_RE.sub("", text)
    return " ".join(text.split())


def _raw_noise_score(name: str) -> int:
    score = 0
    if any(ch in name for ch in ("?", "!", "`")):
        score += 3
    if "краток рок" in name.lower():
        score += 2
    if " - " in name:
        score += 2
    if _ERP_TRAILING_RE.search(name) or name.rstrip().endswith("#"):
        score += 1
    return score


def choose_display_name(
    rows: list[dict],
    manufacturer_id: int | None = None,
    mfr_display_names: dict[int, str] | None = None,
) -> str:
    """4-tier display title selector for a product group:
      1. Reptil raw name (prefer in-stock)
      2. BigShop raw name (prefer in-stock)
      3. Synthesized '{Brand} – {Product name} {qty}' with Latin units
      4. Cleanest raw name fallback (penalising noisy punctuation / ERP codes)
    """
    ordered = sorted(rows, key=lambda r: (not bool(r.get("in_stock")), str(r.get("id", ""))))

    # Tier 1 & Tier 2: curated mixed-case e-commerce markets
    for preferred_mkt in ("reptil", "bigshop"):
        for r in ordered:
            if r.get("market") == preferred_mkt and (r.get("name") or "").strip():
                return " ".join(r["name"].split())[:200]

    # Tier 3: Synthesized "Brand – Product name qty" (Latin units)
    brand = (mfr_display_names or {}).get(manufacturer_id) if manufacturer_id is not None else None
    if brand:
        prod_candidates = [
            " ".join((r.get("product_name") or "").split())
            for r in ordered
            if (r.get("product_name") or "").strip()
        ]
        if prod_candidates:
            # Pick most common extracted product_name (ties broken by ordered in-stock / id)
            counts = Counter(p.lower() for p in prod_candidates)
            top_cnt = max(counts.values())
            chosen_prod = next(p for p in prod_candidates if counts[p.lower()] == top_cnt)
            prod_title = _sentence_case(chosen_prod)
            if len(prod_title) >= 3:
                # Pick most common explicit (quantity_value, quantity_unit), else recovered base qty
                qty_pairs = [
                    (r.get("quantity_value"), r.get("quantity_unit"))
                    for r in ordered
                    if r.get("quantity_value") is not None and r.get("quantity_unit")
                ]
                qty_str = ""
                if qty_pairs:
                    qv, qu = Counter(qty_pairs).most_common(1)[0][0]
                    qty_str = _format_latin_qty(qv, qu)
                else:
                    rec = next(
                        ((r.get("_base_qty"), r.get("_qty_kind"), r.get("is_fluid"))
                         for r in ordered if r.get("_base_qty") is not None),
                        None,
                    )
                    if rec:
                        bq, kind, is_fl = rec
                        unit = "pcs" if kind == "pcs" else ("ml" if is_fl else "g")
                        qty_str = _format_latin_qty(bq, unit)

                title = f"{brand} – {prod_title} {qty_str}".strip() if qty_str else f"{brand} – {prod_title}"
                return title[:200]

    # Tier 4: Cleanest raw name fallback
    valid = [r for r in ordered if (r.get("name") or "").strip()]
    if not valid:
        return ""
    best = min(
        valid,
        key=lambda r: (
            not bool(r.get("in_stock")),
            _raw_noise_score(r["name"]),
            _FALLBACK_MARKET_RANK.get(r.get("market") or "", 99),
            str(r.get("id", "")),
        ),
    )
    return _clean_raw_fallback(best["name"], best.get("market"))[:200]


def build_components(rows: list[dict]) -> list[list[dict]]:
    """Cluster one block into groups (Layer 1 deterministic + guarded fuzzy)."""
    generic_block = rows[0]["manufacturer_id"] is None and rows[0].get("_base_qty", rows[0].get("quantity_value")) is None

    by_name: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_name[row["product_name_sorted"]].append(row)

    # --- fuzzy merge of names ------------------------------------------------
    name_parent = {name: name for name in by_name}

    def find_name(name: str) -> str:
        while name_parent[name] != name:
            name_parent[name] = name_parent[name_parent[name]]
            name = name_parent[name]
        return name

    def union_name(a: str, b: str) -> None:
        ra, rb = find_name(a), find_name(b)
        if ra != rb:
            name_parent[rb] = ra

    summaries: dict[str, dict[str, set]] = {}
    for name, name_rows in by_name.items():
        summary: dict[str, set] = {}
        for row in name_rows:
            for key, value in (row["variant"] or {}).items():
                summary.setdefault(key, set()).add(value)
        summaries[name] = summary

    def summaries_compatible(a: dict, b: dict) -> bool:
        return all(a[key] == b[key] for key in a.keys() & b.keys())

    if len(by_name) > 1:
        token_index: dict[str, list[str]] = defaultdict(list)
        for name in by_name:
            for token in set(name.split()):
                if len(token) >= 3:
                    token_index[token].append(name)

        seen = set()
        for owners in token_index.values():
            if not 2 <= len(owners) <= 200:
                continue
            for name_a, name_b in itertools.combinations(sorted(owners), 2):
                if (name_a, name_b) in seen:
                    continue
                seen.add((name_a, name_b))
                if generic_block and not (summaries[name_a] and summaries[name_b]):
                    continue
                if not summaries_compatible(summaries[name_a], summaries[name_b]):
                    continue
                if guarded_fuzzy_merge(name_a, name_b):
                    union_name(name_a, name_b)

    merged: dict[str, list[dict]] = defaultdict(list)
    for name, name_rows in by_name.items():
        merged[find_name(name)].extend(name_rows)

    # --- greedy variant clustering inside each name component ---------------
    groups: list[list[dict]] = []
    for name_rows in merged.values():
        clusters: list[tuple[dict, list[dict]]] = []
        for row in sorted(name_rows, key=lambda item: str(item["id"])):
            variant = row["variant"] or {}
            if generic_block and not variant:
                clusters.append(({}, [row]))  # unbranded + unsized: stays alone
                continue
            placed = False
            for summary, members in clusters:
                if all(summary.get(key) == value for key, value in variant.items()):
                    for key, value in variant.items():
                        summary.setdefault(key, value)
                    members.append(row)
                    placed = True
                    break
            if not placed:
                clusters.append((dict(variant), [row]))
        groups.extend(members for _, members in clusters)
    return groups


def representative(
    rows: list[dict],
    manufacturer_id: int | None = None,
    mfr_display_names: dict[int, str] | None = None,
) -> dict:
    """Majority category and 4-tier display name; ties resolved deterministically by id."""
    ordered = sorted(rows, key=lambda row: str(row["id"]))
    categories = Counter((row["main_category"], row["sub_category"]) for row in rows)
    top = max(categories.values())
    category = next(
        (row["main_category"], row["sub_category"])
        for row in ordered
        if categories[(row["main_category"], row["sub_category"])] == top
    )
    name = choose_display_name(rows, manufacturer_id=manufacturer_id, mfr_display_names=mfr_display_names)
    return {"category": category, "name": name or "", "seed": ordered[0]}


def build_group_rows(
    components: list[list[dict]],
    manufacturer_id,
    mfr_display_names: dict[int, str] | None = None,
):
    """Turn components into (group row, member updates)."""
    out = []
    for members in components:
        info = representative(members, manufacturer_id=manufacturer_id, mfr_display_names=mfr_display_names)
        seed = info["seed"]
        group_id = str(uuid.uuid4())
        # Keep non-internal variant keys for display
        variant = {k: v for k, v in (seed["variant"] or {}).items() if not k.startswith("_raw_")}
        out.append((
            {
                "id": group_id,
                "name": info["name"][:200],
                "main_category": info["category"][0],
                "sub_category": info["category"][1],
                "manufacturer_id": manufacturer_id,
                "product_name": seed["product_name_sorted"],
                "product_name_sorted": seed["product_name_sorted"],
                "quantity_value": seed.get("quantity_value"),
                "quantity_unit": seed.get("quantity_unit"),
                "is_fluid": seed.get("is_fluid"),
                "variant_key": str(sorted(variant.items()))[:400] if variant else None,
                "clean_name": info["name"][:200],
            },
            [
                (str(row["id"]), group_id, info["category"][0], info["category"][1])
                for row in members
            ],
        ))
    return out


def prepare_rows_layer1(
    featured: list[dict],
    alias_map: dict[int, dict[str, str]],
    mfr_own_tokens: dict[int, set[str]],
) -> tuple[list[dict], list[dict], int]:
    """Apply Layer 1 cleaning, safety variant allowlist, and singular_price qty recovery."""
    rows: list[dict] = []
    singletons: list[dict] = []
    recovered_sp = 0

    for row in featured:
        if not row["product_name_sorted"]:
            singletons.append(row)
            continue
        d = dict(row)
        raw_sig = extract_raw_safety_sig(d["name"])
        d["_raw_sig"] = raw_sig

        toks = clean_name_tokens(
            d["product_name_key"], d["name"], d["manufacturer_id"], alias_map, mfr_own_tokens
        )
        if not toks:
            singletons.append(row)
            continue
        d["product_name_key"] = " ".join(toks)
        d["product_name_sorted"] = " ".join(sorted(toks))
        d["variant"] = clean_variant_allowlist(d["variant"], raw_sig)

        kind, base_q = unified_base_qty(d["quantity_value"], d["quantity_unit"])
        if base_q is None:
            sp_q, sp_kind = recover_qty_from_singular_price(d["price"], d["singular_price"])
            if sp_q is not None:
                kind, base_q = sp_kind, sp_q
                recovered_sp += 1
        d["_qty_kind"] = kind
        d["_base_qty"] = base_q
        d["_has_qty"] = base_q is not None
        rows.append(d)

    return rows, singletons, recovered_sp


def compute_brand_idf(block_comps: dict[tuple, list[list[dict]]]) -> dict[int, dict[str, float]]:
    brand_df: dict[int, Counter[str]] = defaultdict(Counter)
    brand_n: Counter[int] = Counter()
    for (mfr_id, _, _), clist in block_comps.items():
        if mfr_id is None:
            continue
        for comp in clist:
            brand_n[mfr_id] += 1
            for tok in set(comp[0]["product_name_sorted"].split()):
                brand_df[mfr_id][tok] += 1
    return {
        mfr_id: {tok: math.log((n_c + 1.0) / (cnt + 1.0)) + 1.0 for tok, cnt in brand_df[mfr_id].items()}
        for mfr_id, n_c in brand_n.items()
    }


def _collect_layer5_texts(block_comps: dict[tuple, list[list[dict]]]) -> list[str]:
    """Collect unique original-script embed texts ONLY for multi-market Core Grocery blocks."""
    texts: set[str] = set()
    for (mfr_id, _, base_q), clist in block_comps.items():
        if mfr_id is None or base_q is None or len(clist) < 2 or len(clist) > 250:
            continue
        all_rows = [r for comp in clist for r in comp]
        top_cat = Counter(r.get("main_category") for r in all_rows).most_common(1)[0][0]
        if top_cat not in CORE_GROCERY_CATEGORIES:
            continue
        mkts = {r["market"] for r in all_rows}
        if len(mkts) < 2:
            continue
        for comp in clist:
            txt = (comp[0].get("product_name") or comp[0].get("product_name_key") or comp[0].get("name") or "").strip()
            if txt:
                texts.add(txt)
    return sorted(texts)


def print_coverage_summary(all_components: list[list[dict]], label: str) -> None:
    in_stock_total = in_stock_multi = multi_mkt_groups = 0
    core_total = core_multi = core_bq_total = core_bq_multi = 0
    nonfood_total = nonfood_multi = 0
    max_group_size = max_group_mkts = mkts_sum = 0

    for comp in all_components:
        if len(comp) > max_group_size:
            max_group_size = len(comp)
        mkts = {r["market"] for r in comp if r["in_stock"]}
        n_mkts = len(mkts)
        if n_mkts > max_group_mkts:
            max_group_mkts = n_mkts
        is_multi = n_mkts >= 2
        if is_multi:
            multi_mkt_groups += 1
            mkts_sum += n_mkts

        for r in comp:
            if not r["in_stock"]:
                continue
            in_stock_total += 1
            if is_multi:
                in_stock_multi += 1
            cat = r["main_category"]
            if cat in CORE_GROCERY_CATEGORIES:
                core_total += 1
                if is_multi:
                    core_multi += 1
                if r["manufacturer_id"] is not None and r.get("_has_qty"):
                    core_bq_total += 1
                    if is_multi:
                        core_bq_multi += 1
            elif cat in NON_FOOD_CATEGORIES:
                nonfood_total += 1
                if is_multi:
                    nonfood_multi += 1

    avg_m = mkts_sum / multi_mkt_groups if multi_mkt_groups else 0.0
    print(f"\n[{label}]")
    print(f"  featured groups       : {len(all_components):,} | multi-market (in-stock >=2 mkts): {multi_mkt_groups:,} (avg {avg_m:.2f} mkts)")
    print(f"  all in-stock featured : {in_stock_multi:,} / {in_stock_total:,} ({100*in_stock_multi/max(1,in_stock_total):.1f}%)")
    print(f"  core groceries (all)  : {core_multi:,} / {core_total:,} ({100*core_multi/max(1,core_total):.1f}%)")
    print(f"  core groceries (b+qty): {core_bq_multi:,} / {core_bq_total:,} ({100*core_bq_multi/max(1,core_bq_total):.1f}%)")
    print(f"  non-food hypermarket  : {nonfood_multi:,} / {nonfood_total:,} ({100*nonfood_multi/max(1,nonfood_total):.1f}%)")
    print(f"  max group size        : {max_group_size} rows | max markets in one group: {max_group_mkts}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Non-LLM product grouping (Layers 1-5).")
    parser.add_argument("--dry-run", action="store_true", help="Compute and print, write nothing to groups/products")
    parser.add_argument("--apply", action="store_true", help="Truncate groups and write the result")
    parser.add_argument("--sample", type=int, default=6, help="How many sample merges/groups to print")
    parser.add_argument("--no-embeddings", action="store_true", help="Skip Layer 5 Ollama bge-m3 semantic merge")
    parser.add_argument("--bge-threshold", type=float, default=DEFAULT_BGE_THRESHOLD, help="Layer 5 cosine threshold (default 0.72)")
    parser.add_argument("--bge-margin", type=float, default=DEFAULT_BGE_MARGIN, help="Layer 5 MNN margin (default 0.04)")
    parser.add_argument("--bge-max-price-ratio", type=float, default=DEFAULT_BGE_MAX_PRICE_RATIO, help="Max in-stock price ratio (default 1.8)")
    parser.add_argument("--warm-cache", action="store_true", help="Persist newly computed bge-m3 embeddings into bge_text_embeddings even on --dry-run")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("choose --dry-run or --apply")

    load_dotenv(find_dotenv())
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(FEATURE_ROWS_SQL)
    featured = [dict(row) for row in cur.fetchall()]
    cur.execute(UNEXTRACTED_SQL)
    unextracted = [dict(row) for row in cur.fetchall()]

    aliases = load_name_aliases(conn)
    mfr_own_tokens, mfr_display_names = load_manufacturer_info(conn)

    started = time.perf_counter()
    rows, extra_singletons, recovered_sp = prepare_rows_layer1(featured, aliases, mfr_own_tokens)
    unextracted.extend(extra_singletons)

    print(
        f"featured rows: {len(rows):,} | singleton rows: {len(unextracted):,} | "
        f"aliases: {sum(len(t) for t in aliases.values()):,} | singular_price recovered: {recovered_sp:,}"
    )

    # Layer 1: Unified (manufacturer_id, qty_kind, base_qty) blocking + build_components
    blocks1: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        blocks1[(row["manufacturer_id"], row["_qty_kind"], row["_base_qty"])].append(row)

    block_comps1: dict[tuple, list[list[dict]]] = {
        bk: build_components(b_rows) for bk, b_rows in blocks1.items()
    }

    # Layers 2-4: Smart pure-IR merge inside each (manufacturer_id, base_qty) block
    brand_idf = compute_brand_idf(block_comps1)
    block_comps2: dict[tuple, list[list[dict]]] = {}
    l24_merges: list[dict] = []

    for bk, clist in block_comps1.items():
        mfr_id, _, base_q = bk
        if mfr_id is None or base_q is None:
            block_comps2[bk] = clist
            continue
        merged_c, acc = smart_merge_block(
            clist,
            brand_idf.get(mfr_id, {}),
            max_price_ratio=args.bge_max_price_ratio,
            mnn_max_price_ratio=min(1.40, args.bge_max_price_ratio),
        )
        block_comps2[bk] = merged_c
        l24_merges.extend(acc)

    l24_by_rule = dict(Counter(m["rule"] for m in l24_merges))
    print(f"Layers 2-4 merges: {len(l24_merges):,} {l24_by_rule}")

    # Layer 5: Optional local Ollama bge-m3 semantic merge on Core Grocery blocks
    block_comps_final: dict[tuple, list[list[dict]]] = {}
    l5_merges: list[dict] = []

    if args.no_embeddings:
        block_comps_final = block_comps2
        print("Layer 5 skipped (--no-embeddings)")
    else:
        needed_texts = _collect_layer5_texts(block_comps2)
        write_cache = args.apply or args.warm_cache
        vec_lookup = fetch_or_compute_embeddings(conn, needed_texts, write_cache=write_cache)
        if not vec_lookup:
            block_comps_final = block_comps2
            print("Layer 5 skipped (no bge-m3 vectors available)")
        else:
            for bk, clist in block_comps2.items():
                mfr_id, _, base_q = bk
                if mfr_id is None or base_q is None:
                    block_comps_final[bk] = clist
                    continue
                merged_c, acc = layer5_merge_block(
                    clist,
                    vec_lookup,
                    cos_thresh=args.bge_threshold,
                    margin_min=args.bge_margin,
                    max_price_ratio=args.bge_max_price_ratio,
                )
                block_comps_final[bk] = merged_c
                l5_merges.extend(acc)
            print(f"Layer 5 merges (bge-m3 cos>={args.bge_threshold:.2f}, pr<={args.bge_max_price_ratio:.2f}): {len(l5_merges):,}")

    all_featured_components: list[list[dict]] = []
    group_rows: list[dict] = []
    member_updates: list[tuple] = []

    for (mfr_id, _, _), components in block_comps_final.items():
        all_featured_components.extend(components)
        for group_row, members in build_group_rows(components, mfr_id, mfr_display_names=mfr_display_names):
            group_rows.append(group_row)
            member_updates.extend(members)

    # unextracted rows: one singleton group per (market, raw name)
    singleton_buckets: dict[tuple, list[dict]] = defaultdict(list)
    for row in unextracted:
        singleton_buckets[(row["market"], row["name"])].append(row)
    for (_market, name), bucket in singleton_buckets.items():
        info = representative(bucket)
        group_id = str(uuid.uuid4())
        group_rows.append({
            "id": group_id,
            "name": (name or "")[:200],
            "main_category": info["category"][0],
            "sub_category": info["category"][1],
            "manufacturer_id": None,
            "product_name": None,
            "product_name_sorted": None,
            "quantity_value": None,
            "quantity_unit": None,
            "is_fluid": None,
            "variant_key": None,
            "clean_name": (name or "")[:200],
        })
        member_updates.extend(
            (str(r["id"]), group_id, info["category"][0], info["category"][1])
            for r in bucket
        )

    elapsed = time.perf_counter() - started
    print(f"computed in {elapsed:.1f}s | total groups: {len(group_rows):,} | products to assign: {len(member_updates):,}")
    print_coverage_summary(all_featured_components, "FINAL COVERAGE")

    rng = random.Random(42)
    if args.sample > 0 and l5_merges:
        print(f"\n=== {min(args.sample, len(l5_merges))} sample Layer 5 (bge-m3) merges ===")
        for m in rng.sample(l5_merges, min(args.sample, len(l5_merges))):
            print(f"  [cos={m['cos']:.3f} pr={m['pr']:.2f} | {m['sub_cat']}]")
            print(f"    A [{m['a_mkt']:8s} {m['a_price']}]: {m['a_raw']}")
            print(f"    B [{m['b_mkt']:8s} {m['b_price']}]: {m['b_raw']}")

    if args.dry_run:
        print("\nDRY RUN - groups/products untouched.")
        conn.close()
        return 0

    print("\nrebuilding groups table...")
    cur.execute("TRUNCATE TABLE groups")
    cur.execute("UPDATE products SET group_id = NULL")
    conn.commit()

    with conn.cursor() as write_cur:
        columns = (
            "id", "name", "main_category", "sub_category", "manufacturer_id",
            "product_name", "product_name_sorted", "quantity_value", "quantity_unit",
            "is_fluid", "variant_key", "clean_name",
        )
        payload = [tuple(row[column] for column in columns) for row in group_rows]
        execute_values(write_cur, INSERT_GROUPS_SQL, payload, page_size=1000)
        execute_values(write_cur, UPDATE_PRODUCTS_SQL, member_updates, page_size=1000)
    conn.commit()

    with conn.cursor(cursor_factory=RealDictCursor) as check:
        check.execute("""
            SELECT (SELECT count(*) FROM groups) AS groups,
                   (SELECT count(*) FROM products WHERE group_id IS NOT NULL) AS assigned,
                   (SELECT count(*) FROM products) AS total,
                   (SELECT count(*) FROM grouped_products) AS visible
        """)
        print("[saved]", dict(check.fetchone()))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
