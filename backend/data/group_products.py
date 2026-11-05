"""Deterministic cross-market grouping (replaces the semantic/cosine grouper).

Algorithm (see `.opencode/todo.md` S2.x for the decisions behind it):

1. Block products by (manufacturer_id, quantity_value, quantity_unit, is_fluid).
2. Inside a block, merge rows whose `variant` dicts are *compatible* — a missing
   key means unknown, only a key present in both with different values splits.
3. Merge names that pass `guarded_fuzzy_merge` (typo/soft-word only; any numeric
   token blocks) when their variant summaries are compatible.
4. In the unbranded/no-size block (no manufacturer, no quantity) rows merge only
   when variant information exists — otherwise generic nouns ("чинија" x67) would
   collapse into one group.
5. Same-market duplicates stay in the group: the API's `WHERE in_stock = true`
   and price ordering decide what the customer sees. vero "краток рок" rows are
   therefore allowed to coexist by construction.
6. Every product ends up in exactly one group. Rows without features get one
   singleton group per (market, raw name) so they stay visible in listings.
7. Group category = majority (main_category, sub_category) among members, written
   back onto every member; a tie takes the tuple of the member with the smallest id.

Usage:
    python -m backend.data.group_products --dry-run
    python -m backend.data.group_products --apply        # truncate groups, rebuild
"""

from __future__ import annotations

import argparse
import itertools
import os
import sys
import time
import uuid
from collections import Counter, defaultdict

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

from backend.data.db_utils import connect_to_db
from backend.data.grouping_rules import guarded_fuzzy_merge, variants_compatible

FEATURE_ROWS_SQL = """
    SELECT id, market, manufacturer_id, product_name_sorted, quantity_value, quantity_unit,
           is_fluid, variant, price, in_stock, main_category, sub_category, name
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


def build_components(rows: list[dict]) -> list[list[dict]]:
    """Cluster one block into groups.

    Compatibility is NOT transitive (A~B and B~C does not imply A~C), so
    union-find over pairwise edges would chain conflicting variants into one
    group. Instead: merge *names* fuzzily first, then greedily cluster the rows
    of each name group so that no cluster ever holds conflicting variant values.
    """
    generic_block = rows[0]["manufacturer_id"] is None and rows[0]["quantity_value"] is None

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


def representative(rows: list[dict]) -> dict:
    """Majority category and name; ties resolved by smallest id (deterministic)."""
    ordered = sorted(rows, key=lambda row: str(row["id"]))
    categories = Counter((row["main_category"], row["sub_category"]) for row in rows)
    top = max(categories.values())
    category = next((row["main_category"], row["sub_category"]) for row in ordered
                    if categories[(row["main_category"], row["sub_category"])] == top)
    names = Counter(row["name"] or "" for row in rows)
    top_name = max(names.values())
    name = next(row["name"] for row in ordered if names[row["name"] or ""] == top_name)
    return {"category": category, "name": name or "", "seed": ordered[0]}


def build_group_rows(components: list[list[dict]], block_key: tuple):
    """Turn components into (group row, member updates)."""
    manufacturer_id, quantity_value, quantity_unit, is_fluid = block_key
    out = []
    for members in components:
        info = representative(members)
        seed = info["seed"]
        group_id = str(uuid.uuid4())
        variant = seed["variant"] or {}
        out.append((
            {
                "id": group_id,
                "name": info["name"][:200],
                "main_category": info["category"][0],
                "sub_category": info["category"][1],
                "manufacturer_id": manufacturer_id,
                "product_name": seed["product_name_sorted"],
                "product_name_sorted": seed["product_name_sorted"],
                "quantity_value": quantity_value,
                "quantity_unit": quantity_unit,
                "is_fluid": is_fluid,
                "variant_key": str(sorted(variant.items()))[:400] if variant else None,
                "clean_name": info["name"][:200],
            },
            [
                (str(row["id"]), group_id, info["category"][0], info["category"][1])
                for row in members
            ],
        ))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Deterministic product grouping.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and print, write nothing")
    parser.add_argument("--apply", action="store_true", help="Truncate groups and write the result")
    parser.add_argument("--sample", type=int, default=6, help="How many groups to print")
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

    # rows with features but no usable name cannot be keyed -> singleton groups
    rows = [row for row in featured if row["product_name_sorted"]]
    unextracted += [row for row in featured if not row["product_name_sorted"]]
    print(f"featured rows: {len(rows):,} | singleton rows: {len(unextracted):,}")

    started = time.perf_counter()
    blocks: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        blocks[(row["manufacturer_id"], row["quantity_value"],
                row["quantity_unit"], row["is_fluid"])].append(row)

    group_rows: list[dict] = []
    group_members: list[list[tuple]] = []
    member_updates: list[tuple] = []
    multi_groups = 0
    rows_in_multi = 0
    same_market_groups = 0

    for block_key, block_rows in blocks.items():
        components = build_components(block_rows)
        built = build_group_rows(components, block_key)
        for component, (group_row, members) in zip(components, built):
            group_rows.append(group_row)
            group_members.append(members)
            member_updates.extend(members)
            if len(members) >= 2:
                multi_groups += 1
                rows_in_multi += len(members)
                markets = [row["market"] for row in component]
                if len(set(markets)) < len(markets):
                    same_market_groups += 1

    # unextracted rows: one singleton group per (market, raw name)
    singleton_buckets: dict[tuple, list[dict]] = defaultdict(list)
    for row in unextracted:
        singleton_buckets[(row["market"], row["name"])].append(row)
    for (market, name), bucket in singleton_buckets.items():
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
        member_updates.extend((str(r["id"]), group_id, info["category"][0], info["category"][1])
                              for r in bucket)
        group_members.append([(str(r["id"]), group_id, info["category"][0], info["category"][1])
                              for r in bucket])

    elapsed = time.perf_counter() - started
    print(f"computed in {elapsed:.1f}s")
    print(f"groups: {len(group_rows):,} | multi-product: {multi_groups:,} | "
          f"rows in multi: {rows_in_multi:,} ({rows_in_multi/len(rows)*100:.1f}%) | "
          f"same-market groups: {same_market_groups:,}")
    print(f"products to assign: {len(member_updates):,} "
          f"(featured {len(rows):,} + unextracted {len(unextracted):,})")

    print(f"\n=== {args.sample} sample groups ===")
    for group_row, members in list(zip(group_rows, group_members))[: args.sample]:
        print(f"\n{group_row['name']!r} | {group_row['main_category']} / {group_row['sub_category']} "
              f"| mfr={group_row['manufacturer_id']} | {group_row['quantity_value']} "
              f"{group_row['quantity_unit'] or ''} | {len(members)} rows")

    if args.dry_run:
        print("\nDRY RUN - nothing written.")
        return 0

    print("\nrebuilding groups table...")
    cur.execute("TRUNCATE TABLE groups")
    cur.execute("UPDATE products SET group_id = NULL")
    conn.commit()

    with conn.cursor() as write_cur:
        columns = ("id", "name", "main_category", "sub_category", "manufacturer_id",
                   "product_name", "product_name_sorted", "quantity_value", "quantity_unit",
                   "is_fluid", "variant_key", "clean_name")
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
