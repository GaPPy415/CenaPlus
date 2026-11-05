"""Measure grouping coverage under different variant rules (no writing).

Compares three keys on the same data:
  A strict    : manufacturer + name_sorted + qty + unit + fluid + variant_key (current)
  B compatible: same, but rows whose variant dicts are *compatible* may share a group
                (a missing metadata key means unknown, only differing values split)
  C ignore    : variant_key ignored entirely (upper bound of name+size matching)

Usage: python -m backend.data.variant_impact
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor

from backend.data.db_utils import connect_to_db
from backend.data.grouping_rules import variants_compatible, variant_dict

SQL = """
    SELECT id, manufacturer_id, product_name_sorted, quantity_value, quantity_unit,
           is_fluid, variant_key, metadata
    FROM products
    WHERE grouping_keys_version IS NOT NULL
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
        blocks[(row["manufacturer_id"], row["product_name_sorted"],
                row["quantity_value"], row["quantity_unit"], row["is_fluid"])].append(row)

    # A: strict key (reproduces the DB)
    strict_groups = len({(b, r["variant_key"]) for b, rs in blocks.items() for r in rs})
    strict_rows_multi = 0
    for block_rows in blocks.values():
        by_variant: dict[str, int] = defaultdict(int)
        for row in block_rows:
            by_variant[row["variant_key"]] += 1
        strict_rows_multi += sum(count for count in by_variant.values() if count >= 2)

    # C: variant ignored
    ignore_groups = len(blocks)
    ignore_rows_multi = sum(len(rs) for rs in blocks.values() if len(rs) >= 2)

    # B: variant compatible (union-find inside each name+size block)
    compatible_groups = 0
    compatible_rows_multi = 0
    split_examples: list[tuple[str, int]] = []
    for block_key, block_rows in blocks.items():
        if len(block_rows) == 1:
            compatible_groups += 1
            continue
        parent = list(range(len(block_rows)))

        def find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        variants = [variant_dict(row["metadata"]) for row in block_rows]
        for i in range(len(block_rows)):
            for j in range(i + 1, len(block_rows)):
                if find(i) == find(j):
                    continue
                if variants_compatible(variants[i], variants[j]):
                    parent[find(j)] = find(i)

        components: dict[int, int] = defaultdict(int)
        for i in range(len(block_rows)):
            components[find(i)] += 1
        compatible_groups += len(components)
        compatible_rows_multi += sum(count for count in components.values() if count >= 2)
        if len(components) > 1 and len(split_examples) < 5:
            split_examples.append((block_key[1] or "-", len(block_rows)))

    print(f"\n{'rule':<30}{'groups':>10}{'rows in multi':>16}{'coverage':>10}")
    print(f"{'A strict variant_key':<30}{strict_groups:>10,}{strict_rows_multi:>16,}"
          f"{strict_rows_multi/total*100:>9.1f}%")
    print(f"{'B compatible (missing=unknown)':<30}{compatible_groups:>10,}{compatible_rows_multi:>16,}"
          f"{compatible_rows_multi/total*100:>9.1f}%")
    print(f"{'C variant ignored':<30}{ignore_groups:>10,}{ignore_rows_multi:>16,}"
          f"{ignore_rows_multi/total*100:>9.1f}%")

    print("\nblocks still split under rule B (name + size already identical):")
    for name, size in split_examples:
        print(f"  {name!r} -> {size} rows still in multiple groups")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
