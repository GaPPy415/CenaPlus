"""Projection of the intended grouper: rule B (compatible variants) + guarded fuzzy.

Reads the keyed products and clusters them in memory exactly as the production
grouper will, without writing anything:

  block   = (manufacturer_id, quantity_value, quantity_unit, is_fluid)
  within  = rows merge when their variant dicts are compatible
            (missing key = unknown; only differing values split)
  fuzzy   = names may merge when `guarded_fuzzy_merge` accepts them and their
            variant summaries are compatible

Reports resulting groups, multi-product groups, rows in multi groups (coverage)
and how many components still hold two rows from the same market.

Usage: python -m backend.data.grouping_projection
"""

from __future__ import annotations

import itertools
import os
import sys
from collections import defaultdict

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor

from backend.data.db_utils import connect_to_db
from backend.data.grouping_rules import guarded_fuzzy_merge, variants_compatible

SQL = """
    SELECT id, market, manufacturer_id, product_name_sorted, quantity_value,
           quantity_unit, is_fluid, variant, price, in_stock
    FROM products
    WHERE grouping_keys_version IS NOT NULL AND product_name_sorted IS NOT NULL
"""


def main() -> int:
    load_dotenv(find_dotenv())
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(SQL)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    total = len(rows)
    print(f"rows: {total:,}")

    blocks: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        blocks[(row["manufacturer_id"], row["quantity_value"],
                row["quantity_unit"], row["is_fluid"])].append(row)

    groups = multi = rows_in_multi = collision_components = 0

    for block_key, block_rows in blocks.items():
        parent = list(range(len(block_rows)))

        def find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def union(i: int, j: int) -> None:
            ri, rj = find(i), find(j)
            if ri != rj:
                parent[rj] = ri

        # 1) rule B inside identical names
        by_name: dict[str, list[int]] = defaultdict(list)
        for idx, row in enumerate(block_rows):
            by_name[row["product_name_sorted"]].append(idx)

        for indices in by_name.values():
            for i, j in itertools.combinations(indices, 2):
                if find(i) == find(j):
                    continue
                if variants_compatible(block_rows[i]["variant"] or {}, block_rows[j]["variant"] or {}):
                    union(i, j)

        # 2) guarded fuzzy across names sharing a token
        if len(by_name) > 1:
            token_index: dict[str, list[str]] = defaultdict(list)
            for name in by_name:
                for token in set(name.split()):
                    if len(token) >= 3:
                        token_index[token].append(name)

            summaries = {}
            for name, indices in by_name.items():
                summary: dict[str, set] = {}
                for idx in indices:
                    for key, value in (block_rows[idx]["variant"] or {}).items():
                        summary.setdefault(key, set()).add(value)
                summaries[name] = summary

            def summaries_compatible(a: dict, b: dict) -> bool:
                return all(a[key] == b[key] for key in a.keys() & b.keys())

            seen = set()
            for owners in token_index.values():
                if not 2 <= len(owners) <= 200:
                    continue
                for name_a, name_b in itertools.combinations(sorted(owners), 2):
                    if (name_a, name_b) in seen:
                        continue
                    seen.add((name_a, name_b))
                    if not summaries_compatible(summaries[name_a], summaries[name_b]):
                        continue
                    if not guarded_fuzzy_merge(name_a, name_b):
                        continue
                    root_a, root_b = find(by_name[name_a][0]), find(by_name[name_b][0])
                    if root_a != root_b:
                        union(root_a, root_b)

        # 3) collect components
        components: dict[int, list[int]] = defaultdict(list)
        for idx in range(len(block_rows)):
            components[find(idx)].append(idx)

        groups += len(components)
        for indices in components.values():
            size = len(indices)
            if size >= 2:
                multi += 1
                rows_in_multi += size
                markets = [block_rows[i]["market"] for i in indices]
                if len(set(markets)) < len(markets):
                    collision_components += 1

    print(f"\n{'groups':>10}{'multi':>10}{'rows in multi':>16}{'coverage':>10}{'same-market groups':>20}")
    print(f"{groups:>10,}{multi:>10,}{rows_in_multi:>16,}{rows_in_multi/total*100:>9.1f}%"
          f"{collision_components:>20,}")

    print("\nfor reference:")
    print("  rule A (strict variant_key)     114,967 groups |  26,443 rows | 20.3%")
    print("  rule B (compatible variants)    105,706 groups |  38,481 rows | 29.5%")
    print("  variant ignored                 101,220 groups |  44,825 rows | 34.4%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
