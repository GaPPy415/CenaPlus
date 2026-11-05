"""Classify same-market collisions inside the deterministic identity key.

A "collision" is one identity key (manufacturer + product name + quantity + unit +
is_fluid) holding more than one product from the SAME market. The rules live in
`backend/data/grouping_rules.py` so the grouper and this report cannot drift.

Classes and their meaning:
  kratok_rok      vero short-expiry      -> same group
  duplicate       identical raw name     -> same group
  promo           promo wording only     -> same group
  word_order      same tokens, any order -> same group
  near_identical  >=0.90 + comparable price -> same group
  single_char     <=1 edit apart         -> same group
  variant         variant key differs    -> separate products
  residual        needs human review

Usage:
    python -m backend.data.same_market_collisions
    python -m backend.data.same_market_collisions --sample 15
    python -m backend.data.same_market_collisions --csv leftover.csv          # residual only
    python -m backend.data.same_market_collisions --csv all.csv --include-handled
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import os

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor

from backend.data.db_utils import connect_to_db
from backend.data.extract_features import normalize_string
from backend.data.grouping_rules import MERGE_CLASSES, classify

BUCKETS_SQL = """
    SELECT p.manufacturer_id,
           m.canonical_name,
           p.product_name,
           p.quantity_value,
           p.quantity_unit,
           p.is_fluid,
           p.market,
           COUNT(*) AS products
    FROM products p
    LEFT JOIN manufacturers m ON m.id = p.manufacturer_id
    WHERE p.features_extracted_at IS NOT NULL
    GROUP BY p.manufacturer_id, m.canonical_name, p.product_name,
             p.quantity_value, p.quantity_unit, p.is_fluid, p.market
    HAVING COUNT(*) > 1
"""

COLLISION_ROWS_SQL = """
    WITH buckets AS (
        SELECT p.manufacturer_id, p.product_name, p.quantity_value, p.quantity_unit,
               p.is_fluid, p.market, COUNT(*) AS bucket_size
        FROM products p
        WHERE p.features_extracted_at IS NOT NULL
        GROUP BY p.manufacturer_id, p.product_name, p.quantity_value, p.quantity_unit,
                 p.is_fluid, p.market
        HAVING COUNT(*) > 1
    )
    SELECT dense_rank() OVER (
               ORDER BY b.manufacturer_id, b.product_name, b.quantity_value,
                        b.quantity_unit, b.is_fluid, b.market
           ) AS bucket_id,
           p.id, p.group_id, p.market, p.name AS raw_name, p.price, p.singular_price,
           p.in_stock, p.main_category, p.sub_category, p.link, p.metadata,
           b.product_name, b.quantity_value, b.quantity_unit, b.is_fluid, b.bucket_size
    FROM buckets b
    JOIN products p
      ON p.market = b.market
     AND p.product_name IS NOT DISTINCT FROM b.product_name
     AND p.manufacturer_id IS NOT DISTINCT FROM b.manufacturer_id
     AND p.quantity_value IS NOT DISTINCT FROM b.quantity_value
     AND p.quantity_unit IS NOT DISTINCT FROM b.quantity_unit
     AND p.is_fluid IS NOT DISTINCT FROM b.is_fluid
    WHERE p.features_extracted_at IS NOT NULL
    ORDER BY bucket_id, p.in_stock DESC NULLS LAST, p.price NULLS LAST
"""

VERDICTS = {
    "kratok_rok": "vero short-expiry -> same group",
    "duplicate": "same raw name -> same group",
    "promo": "promo wording only -> same group",
    "word_order": "same tokens, any order -> same group",
    "near_identical": ">=0.90 similar + comparable price -> same group",
    "single_char": "<=1 char apart -> same group",
    "variant": "variant key differs -> separate products",
    "residual": "needs review",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify same-market identity-key collisions.")
    parser.add_argument("--sample", type=int, default=5, help="How many residual buckets to print")
    parser.add_argument("--csv", type=str, default=None, help="Write classified rows to this CSV")
    parser.add_argument("--include-handled", action="store_true",
                        help="Write every class to the CSV instead of only 'residual'")
    args = parser.parse_args()

    load_dotenv(find_dotenv())
    try:
        os.sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(COLLISION_ROWS_SQL)
    all_rows = [dict(row) for row in cur.fetchall()]

    buckets: dict[int, list[dict]] = {}
    for row in all_rows:
        buckets.setdefault(row["bucket_id"], []).append(row)

    classes: dict[str, list[int]] = {}
    for bucket_id, rows in buckets.items():
        classes.setdefault(classify(rows), []).append(bucket_id)

    total_rows = sum(len(rows) for rows in buckets.values())
    print(f"collision buckets: {len(buckets):,} | rows involved: {total_rows:,}\n")
    print(f"{'class':<14}{'buckets':>9}{'rows':>8}  verdict")
    for name in (*MERGE_CLASSES, "variant", "residual"):
        ids = classes.get(name, [])
        rows = sum(len(buckets[bucket_id]) for bucket_id in ids)
        print(f"{name:<14}{len(ids):>9,}{rows:>8,}  {VERDICTS[name]}")

    if args.csv:
        wanted = set(buckets) if args.include_handled else set(classes.get("residual", []))
        out_rows = []
        for bucket_id in sorted(wanted):
            label = classify(buckets[bucket_id])
            names = [normalize_string(r["raw_name"]) or "" for r in buckets[bucket_id]]
            similarity = max(
                (difflib.SequenceMatcher(None, names[i], names[j]).ratio()
                 for i in range(len(names)) for j in range(i + 1, len(names))),
                default=0.0,
            )
            for row in buckets[bucket_id]:
                row = dict(row)
                row["class"] = label
                row["bucket_max_similarity"] = round(similarity, 3)
                row["metadata"] = json.dumps(row["metadata"], ensure_ascii=False) if row["metadata"] else ""
                out_rows.append(row)
        if out_rows:
            with open(args.csv, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(out_rows[0].keys()))
                writer.writeheader()
                writer.writerows(out_rows)
        print(f"\nwrote {len(out_rows):,} rows ({len(wanted):,} buckets) to {args.csv}")

    residual = classes.get("residual", [])
    print(f"\n=== {min(args.sample, len(residual))} of {len(residual)} residual buckets ===")
    for bucket_id in residual[: args.sample]:
        rows = buckets[bucket_id]
        head = rows[0]
        qty = f"{head['quantity_value']} {head['quantity_unit'] or ''}".strip() if head["quantity_value"] else "-"
        print(f"\n--- bucket {bucket_id} | {head['product_name']!r} | {qty} | fluid={head['is_fluid']} "
              f"| [{head['market']}] x{head['bucket_size']}")
        for row in rows:
            stock = "in-stock" if row["in_stock"] else "OUT"
            meta = json.dumps(row["metadata"], ensure_ascii=False) if row["metadata"] else "-"
            print(f"      {str(row['price']):>7} ден {stock:<9} | {row['raw_name'][:70]}")
            print(f"              meta: {meta[:100]}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
