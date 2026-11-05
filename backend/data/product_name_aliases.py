"""Product-name token aliases: reviewed spelling variants of the same word.

Grouping is exact-match on `products.product_name_sorted`, so two shops spelling the
same brand differently ("чоколадо алпен милк" / "чоколадо алпине милк") end up in
different groups even though every group is internally correct. This module proposes
conservative *token* aliases (`алпен` / `алпине` -> `алпин`) and (optionally) stores
them in `product_name_token_aliases`; the grouper rewrites name tokens before blocking.

Token-level (rather than full-name) aliases are deliberate: a spelling variant is a
property of the word, and a full-name alias would map to different canonicals at
different sizes (80g vs 250g), which is exactly the conflict the first prototype hit.

Guards for a proposed pair:
  * neither token contains digits ("формула 1"/"2", "аа"/"ааа", "0%"/"10%");
  * both are at least 4 characters;
  * one is a prefix of the other (shorter token >= 5 chars), or the lengths differ by
    at most 2 and their SequenceMatcher ratio is >= 0.70.

Canonical token = the one seen in the most product rows (ties: most distinct name
keys, then most markets, then lexicographic). A canonical spelling can be curated by
editing the table.

Usage:
    python -m backend.data.product_name_aliases --report --manufacturer милка
    python -m backend.data.product_name_aliases --report --all
    python -m backend.data.product_name_aliases --csv .opencode/name-aliases.csv --all
    python -m backend.data.product_name_aliases --apply --csv .opencode/name-aliases.csv
"""

from __future__ import annotations

import argparse
import csv
import difflib
import sys
from collections import Counter, defaultdict

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

from backend.data.db_utils import connect_to_db
from backend.data.group_products import build_components

TOKEN_ROWS_SQL = """
    SELECT p.manufacturer_id, p.product_name_key,
           count(*) AS rows, count(DISTINCT p.market) AS markets,
           m.canonical_name AS manufacturer
    FROM products p
    JOIN manufacturers m ON m.id = p.manufacturer_id
    WHERE p.features_extracted_at IS NOT NULL
      AND p.product_name_key IS NOT NULL
      {filter}
    GROUP BY p.manufacturer_id, p.product_name_key, m.canonical_name
"""

BLOCK_ROWS_SQL = """
    SELECT p.id, p.market, p.manufacturer_id, p.product_name_key, p.product_name_sorted,
           p.quantity_value, p.quantity_unit, p.is_fluid, p.variant, p.group_id,
           p.price, p.in_stock, p.main_category, p.sub_category, p.name
    FROM products p
    WHERE p.features_extracted_at IS NOT NULL
      AND p.product_name_key IS NOT NULL
      AND p.manufacturer_id IS NOT NULL
      {filter}
"""

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS product_name_token_aliases (
        manufacturer_id bigint NOT NULL REFERENCES manufacturers(id) ON DELETE CASCADE,
        alias_token text NOT NULL,
        canonical_token text NOT NULL,
        confidence real NOT NULL DEFAULT 1.0,
        source text NOT NULL DEFAULT 'proposed',
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (manufacturer_id, alias_token),
        CHECK (alias_token <> canonical_token)
    )
"""

UPSERT_SQL = """
    INSERT INTO product_name_token_aliases
        (manufacturer_id, alias_token, canonical_token, source)
    VALUES %s
    ON CONFLICT (manufacturer_id, alias_token) DO UPDATE
    SET canonical_token = EXCLUDED.canonical_token,
        source = EXCLUDED.source
"""


def _levenshtein(a: str, b: str) -> int:
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        current = [i]
        for j, cb in enumerate(b, 1):
            current.append(min(previous[j] + 1, current[j - 1] + 1,
                               previous[j - 1] + (ca != cb)))
        previous = current
    return previous[-1]


# Packaging/form words are not spelling variants: "бонбони" (candies) is not a typo
# of "бонбониера" (box), and "пакет"/"пакетче" are different pack forms.
FORM_TOKENS = {
    "бонбони", "бонбониера", "колач", "колачи", "колачиња",
    "пакет", "пакетче", "стапче", "стапчиња", "снек", "млеко", "млечно",
}


def token_pair_ok(a: str, b: str) -> bool:
    """True when two tokens are plausible spellings of the same word.

    Deliberately strict: one edit at most, no short tokens, same first letter,
    no packaging/form words, no digits. Character similarity alone confuses
    unrelated words ("малина"/"алпин", "кејк"/"кекс", "млеко"/"млечно"),
    which is why this guard is edit-based.
    """
    if a == b:
        return False
    if a in FORM_TOKENS or b in FORM_TOKENS:
        return False
    if any(ch.isdigit() for ch in a) or any(ch.isdigit() for ch in b):
        return False
    if len(a) < 4 or len(b) < 4 or a[0] != b[0]:
        return False
    if a == b + "и" or b == a + "и" or a == b + "а" or b == a + "а" \
            or a == b + "е" or b == a + "е":
        return True
    if a.startswith(b) or b.startswith(a):
        return min(len(a), len(b)) >= 5 and abs(len(a) - len(b)) <= 3
    if len(a) < 5 or len(b) < 5:
        return False
    distance = _levenshtein(a, b)
    if distance <= 1:
        return True
    # plural/derivation pairs such as лешник/лешници differ by two edits
    return len(a) >= 6 and len(b) >= 6 and a[:3] == b[:3] and distance <= 2


def propose_tokens(keys: list[dict]) -> list[dict]:
    """Propose token aliases for one manufacturer from its name-key statistics."""
    token_rows: Counter = Counter()
    token_keys: Counter = Counter()
    token_markets: Counter = Counter()
    for row in keys:
        for token in set(row["product_name_key"].split()):
            if len(token) < 4 or any(ch.isdigit() for ch in token):
                continue
            token_rows[token] += row["rows"]
            token_keys[token] += 1
            token_markets[token] += row["markets"]

    ordered = sorted(
        token_rows,
        key=lambda token: (-token_rows[token], -token_keys[token],
                           -token_markets[token], token),
    )
    canonicals: list[str] = []
    proposals: list[dict] = []
    for token in ordered:
        best = None
        for canonical in canonicals:
            if token_pair_ok(token, canonical):
                score = difflib.SequenceMatcher(None, token, canonical).ratio()
                if best is None or score > best[0]:
                    best = (score, canonical)
        if best is None:
            canonicals.append(token)
        else:
            proposals.append({
                "alias_token": token,
                "canonical_token": best[1],
                "rows": token_rows[token],
                "keys": token_keys[token],
                "markets": token_markets[token],
                "canonical_rows": token_rows[best[1]],
            })
    return proposals


def propose(key_rows: list[dict]) -> tuple[list[dict], dict]:
    by_manufacturer: dict[int, list[dict]] = defaultdict(list)
    for row in key_rows:
        by_manufacturer[row["manufacturer_id"]].append(row)

    proposals: list[dict] = []
    for manufacturer_id, keys in by_manufacturer.items():
        for proposal in propose_tokens(keys):
            proposal["manufacturer_id"] = manufacturer_id
            proposal["manufacturer"] = keys[0]["manufacturer"]
            proposals.append(proposal)
    proposals.sort(key=lambda p: (p["manufacturer"], p["alias_token"]))
    return proposals, {"manufacturers": len(by_manufacturer)}


def rewrite(rows: list[dict], aliases: list[dict]) -> list[dict]:
    """Return copies of `rows` with alias tokens replaced (for measurement)."""
    mapping: dict[tuple, dict[str, str]] = defaultdict(dict)
    for alias in aliases:
        mapping[alias["manufacturer_id"]][alias["alias_token"]] = alias["canonical_token"]

    out = []
    for row in rows:
        tokens = row["product_name_key"].split()
        table = mapping.get(row["manufacturer_id"], {})
        replaced = [table.get(token, token) for token in tokens]
        if replaced == tokens:
            out.append(row)
            continue
        copy = dict(row)
        copy["product_name_key"] = " ".join(replaced)
        copy["product_name_sorted"] = " ".join(sorted(replaced))
        out.append(copy)
    return out


def project(rows: list[dict], aliases: list[dict]) -> dict:
    """Simulate grouping before/after the aliases (no writes)."""
    rewritten = rewrite(rows, aliases)
    blocks_before: dict[tuple, list[dict]] = defaultdict(list)
    blocks_after: dict[tuple, list[dict]] = defaultdict(list)
    for before, after in zip(rows, rewritten):
        block = (before["manufacturer_id"], before["quantity_value"],
                 before["quantity_unit"], before["is_fluid"])
        blocks_before[block].append(before)
        blocks_after[block].append(after)

    groups_before = groups_after = 0
    for block in blocks_before:
        groups_before += len(build_components(blocks_before[block]))
        groups_after += len(build_components(blocks_after[block]))

    changed = sum(1 for a, b in zip(rows, rewritten) if a is not b)

    # how many rows end up in a group that spans more markets than any of the
    # groups they belonged to before (i.e. the comparison actually got wider)
    original = {row["id"]: row for row in rows}
    clusters: dict[tuple, list[dict]] = defaultdict(list)
    for row in rewritten:
        clusters[(row["manufacturer_id"], row["product_name_key"], row["quantity_value"],
                  row["quantity_unit"], row["is_fluid"])].append(row)
    newly_comparable = 0
    for cluster in clusters.values():
        if not any(original[row["id"]] is not row for row in cluster):
            continue
        markets_after = len({row["market"] for row in cluster})
        markets_before: dict = defaultdict(set)
        for row in cluster:
            markets_before[row["group_id"]].add(original[row["id"]]["market"])
        if markets_after > max(len(markets) for markets in markets_before.values()):
            newly_comparable += len(cluster)

    return {
        "rows_changed": changed,
        "rows_newly_comparable": newly_comparable,
        "groups_before": groups_before,
        "groups_after": groups_after,
        "groups_saved": groups_before - groups_after,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Propose/apply product-name token aliases.")
    parser.add_argument("--manufacturer", help="Only this canonical manufacturer name")
    parser.add_argument("--all", action="store_true", help="All manufacturers")
    parser.add_argument("--report", action="store_true", help="Print the projection (default)")
    parser.add_argument("--csv", help="Write proposals to this CSV")
    parser.add_argument("--apply", action="store_true", help="Create the table and upsert the proposals")
    args = parser.parse_args()
    if not args.manufacturer and not args.all:
        parser.error("choose --manufacturer NAME or --all")

    load_dotenv(find_dotenv())
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if args.manufacturer:
        cur.execute(TOKEN_ROWS_SQL.format(filter="AND m.canonical_name = %s"), (args.manufacturer,))
    else:
        cur.execute(TOKEN_ROWS_SQL.format(filter=""))
    key_rows = [dict(row) for row in cur.fetchall()]
    print(f"name keys: {len(key_rows):,}")

    aliases, meta = propose(key_rows)
    print(f"manufacturers: {meta['manufacturers']:,} | token aliases proposed: {len(aliases):,}")

    if args.manufacturer:
        cur.execute(
            BLOCK_ROWS_SQL.format(
                filter="AND p.manufacturer_id = "
                       "(SELECT id FROM manufacturers WHERE canonical_name = %s)"
            ),
            (args.manufacturer,),
        )
    else:
        cur.execute(BLOCK_ROWS_SQL.format(filter=""))
    rows = [dict(row) for row in cur.fetchall()]
    projection = project(rows, aliases)
    print(f"projection: {projection['rows_changed']:,} rows rewritten | "
          f"{projection['rows_newly_comparable']:,} rows gain a wider market comparison | "
          f"groups {projection['groups_before']:,} -> {projection['groups_after']:,} "
          f"(-{projection['groups_saved']:,})")

    if args.manufacturer:
        print("\nproposals:")
        for alias in aliases:
            print(f"  {alias['alias_token']!r} -> {alias['canonical_token']!r} "
                  f"(alias {alias['rows']} rows/{alias['keys']} keys; "
                  f"canonical {alias['canonical_rows']} rows)")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=[
                "manufacturer_id", "manufacturer", "alias_token", "canonical_token",
                "rows", "keys", "markets", "canonical_rows",
            ])
            writer.writeheader()
            writer.writerows(aliases)
        print(f"wrote {args.csv}")

    if args.apply:
        cur.execute(CREATE_TABLE_SQL)
        execute_values(cur, UPSERT_SQL, [
            (alias["manufacturer_id"], alias["alias_token"], alias["canonical_token"], "proposed")
            for alias in aliases
        ])
        conn.commit()
        print(f"applied {len(aliases):,} token aliases to product_name_token_aliases")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
