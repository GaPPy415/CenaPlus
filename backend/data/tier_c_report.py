"""Tier C analysis: how much cross-market grouping does fuzzy name matching buy?

Blocking: candidates must share (manufacturer_id, quantity_value, quantity_unit, is_fluid).
Inside a block, names are compared only if they share a token (inverted index), so this
does not explode on 130k rows.

For every threshold it reports what the deterministic grouper would produce if names were
merged when difflib similarity >= threshold:

  strict  = pairs must also share the same variant_key (current rules)
  relaxed = variant_key ignored (shows what the variant rule costs)

Usage:
    python -m backend.data.tier_c_report
    python -m backend.data.tier_c_report --thresholds 0.80 0.90 0.95 --examples 15
    python -m backend.data.tier_c_report --csv merges.csv --csv-threshold 0.90
"""

from __future__ import annotations

import argparse
import csv
import difflib
import itertools
import os
import sys
import time
from collections import defaultdict

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor

from backend.data.db_utils import connect_to_db

ROWS_SQL = """
    SELECT id, market, manufacturer_id, product_name_key, product_name_sorted,
           quantity_value, quantity_unit, is_fluid, variant_key, price, in_stock, name
    FROM products
    WHERE grouping_keys_version IS NOT NULL
      AND product_name_key IS NOT NULL
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate fuzzy-merge gains (Tier C).")
    parser.add_argument("--thresholds", type=float, nargs="+", default=[1.00, 0.95, 0.90, 0.85, 0.80])
    parser.add_argument("--min-token-len", type=int, default=3)
    parser.add_argument("--max-names", type=int, default=5000,
                        help="Skip blocks with more distinct names than this")
    parser.add_argument("--examples", type=int, default=12)
    parser.add_argument("--csv", type=str, default=None)
    parser.add_argument("--csv-threshold", type=float, default=0.90)
    args = parser.parse_args()

    load_dotenv(find_dotenv())
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(ROWS_SQL)
    rows = cur.fetchall()
    conn.close()
    print(f"rows with keys: {len(rows):,}")

    # block by the parts of the key that fuzzy matching must not change
    blocks: dict[tuple, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        block = (row["manufacturer_id"], row["quantity_value"], row["quantity_unit"], row["is_fluid"])
        blocks[block][row["product_name_key"]].append(row)

    skipped_blocks = 0
    skipped_rows = 0
    edges: list[tuple[str, str, float, bool]] = []  # name_a, name_b, similarity, same_variant
    block_of: dict[tuple, str] = {}

    started = time.perf_counter()
    for block_key, names in blocks.items():
        if len(names) < 2:
            continue
        if len(names) > args.max_names:
            skipped_blocks += 1
            skipped_rows += sum(len(v) for v in names.values())
            continue

        token_index: dict[str, list[str]] = defaultdict(list)
        for name in names:
            for token in set(name.split()):
                if len(token) >= args.min_token_len:
                    token_index[token].append(name)

        candidate_pairs = set()
        for token, owners in token_index.items():
            if 2 <= len(owners) <= 200:
                candidate_pairs.update(itertools.combinations(sorted(owners), 2))

        for name_a, name_b in candidate_pairs:
            ratio = difflib.SequenceMatcher(None, name_a, name_b).ratio()
            same_variant = any(
                row_a["variant_key"] == row_b["variant_key"]
                for row_a in names[name_a] for row_b in names[name_b]
            )
            edges.append((name_a, name_b, ratio, same_variant))
            block_of[(name_a, name_b)] = repr(block_key)

    print(f"candidate pairs: {len(edges):,} in {time.perf_counter() - started:.1f}s "
          f"| skipped {skipped_blocks} large blocks ({skipped_rows:,} rows)")

    total_rows = len(rows)
    total_keys = sum(len(names) for names in blocks.values())
    print(f"exact baseline: {total_keys:,} keys")

    def evaluate(threshold: float, require_same_variant: bool):
        parent: dict[str, str] = {}

        def find(x: str) -> str:
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra

        for name_a, name_b, ratio, same_variant in edges:
            if ratio >= threshold and (same_variant or not require_same_variant):
                union(name_a, name_b)

        merged: dict[tuple, list[str]] = defaultdict(list)
        for block_key, names in blocks.items():
            for name in names:
                if name in parent:
                    merged[(block_key, find(name))].append(name)

        merged_components = 0
        rows_in_multi = 0
        names_merged = 0
        for (block_key, _root), names in merged.items():
            if len(names) < 2:
                continue  # single-name component: same as the exact baseline
            merged_components += 1
            names_merged += len(names)
            rows_in_multi += sum(len(blocks[block_key][name]) for name in names)

        # rows that share a name but never merged are still multi-product groups
        for block_key, names in blocks.items():
            for name, name_rows in names.items():
                if name in parent:
                    continue
                if len(name_rows) >= 2:
                    rows_in_multi += len(name_rows)

        groups_after = total_keys - names_merged + merged_components
        return groups_after, merged_components, rows_in_multi

    print(f"\n{'threshold':>9} {'mode':<8}{'groups after':>14}{'multi':>8}{'rows in multi':>15}{'coverage':>10}")
    for threshold in sorted(args.thresholds):
        for mode, strict in (("strict", True), ("relaxed", False)):
            groups_after, multi, rows_in_multi = evaluate(threshold, strict)
            coverage = rows_in_multi / total_rows * 100
            print(f"{threshold:>9.2f} {mode:<8}{groups_after:>14,}{multi:>8,}{rows_in_multi:>15,}{coverage:>9.1f}%")

    if args.thresholds:
        print(f"\n=== sample merges at >= {args.csv_threshold:.2f} (strict) ===")
        shown = 0
        for name_a, name_b, ratio, same_variant in sorted(edges, key=lambda e: -e[2]):
            if ratio < args.csv_threshold or not same_variant:
                continue
            print(f"  {ratio:.3f}  {name_a!r:<46} ~ {name_b!r}")
            shown += 1
            if shown >= args.examples:
                break

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["block", "name_a", "name_b", "similarity", "same_variant"])
            for name_a, name_b, ratio, same_variant in sorted(edges, key=lambda e: -e[2]):
                if ratio >= args.csv_threshold:
                    writer.writerow([block_of.get((name_a, name_b), ""), name_a, name_b,
                                     round(ratio, 4), same_variant])
        print(f"\nwrote candidate merges (>= {args.csv_threshold:.2f}) to {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
