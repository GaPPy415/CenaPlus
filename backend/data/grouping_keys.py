"""S2.1: compute the materialised identity-key columns on `products`.

Fills `product_name_key`, `product_name_sorted` and `variant_key` for every row
that has extracted features, using the rules in `grouping_rules.py`.

Usage:
    python -m backend.data.grouping_keys --dry-run --sample 8
    python -m backend.data.grouping_keys            # fill missing keys
    python -m backend.data.grouping_keys --recompute # rewrite every key
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor, execute_values

from backend.data.db_utils import connect_to_db
from backend.data.grouping_rules import KEY_VERSION, compute_keys, variant_dict

LOAD_SQL = """
    SELECT id, name, product_name, metadata
    FROM products
    WHERE features_extracted_at IS NOT NULL
      {extra_filter}
    ORDER BY id
"""

WRITE_SQL = """
    UPDATE products AS p
    SET product_name_key      = v.product_name_key,
        product_name_sorted   = v.product_name_sorted,
        variant_key           = v.variant_key,
        variant               = v.variant::jsonb,
        grouping_keys_version = {version}
    FROM (VALUES %s) AS v(id, product_name_key, product_name_sorted, variant_key, variant)
    WHERE p.id = v.id::uuid
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute grouping key columns on products.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N rows")
    parser.add_argument("--sample", type=int, default=8, help="How many derived keys to print")
    parser.add_argument("--dry-run", action="store_true", help="Print results, write nothing")
    parser.add_argument("--recompute", action="store_true", help="Rewrite keys that already exist")
    args = parser.parse_args()

    load_dotenv(find_dotenv())
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    extra = "" if args.recompute else "AND grouping_keys_version IS NULL"
    query = LOAD_SQL.format(extra_filter=extra)
    if args.limit:
        query += f" LIMIT {int(args.limit)}"
    cur.execute(query)
    rows = cur.fetchall()
    print(f"rows to key: {len(rows):,} (version {KEY_VERSION})")
    if not rows:
        print("Nothing to do.")
        return 0

    started = time.perf_counter()
    payload = []
    for row in rows:
        name_key, name_sorted, variant_key = compute_keys(
            row["product_name"], row["name"], row["metadata"]
        )
        payload.append((
            str(row["id"]), name_key, name_sorted, variant_key,
            json.dumps(variant_dict(row["metadata"]), ensure_ascii=False, sort_keys=True),
        ))

    elapsed = time.perf_counter() - started
    print(f"derived in {elapsed:.1f}s ({len(payload) / max(elapsed, 1e-9):,.0f} rows/s)")

    print(f"\n{'raw name':<46} {'product_name_key':<32} {'sorted':<28} variant")
    for row, derived in list(zip(rows, payload))[: args.sample]:
        _, name_key, name_sorted, variant_key, _variant = derived
        print(f"{(row['name'] or '')[:46]:<46} {(name_key or '-')[:32]:<32} "
              f"{(name_sorted or '-')[:28]:<28} {variant_key[:8]}")

    nulls = sum(1 for _, name_key, name_sorted, _variant_key, _variant in payload
                if not name_key or not name_sorted)
    print(f"\nrows with an empty name key: {nulls:,}")

    if args.dry_run:
        print("\nDRY RUN - nothing written.")
        return 0

    with conn.cursor() as write_cur:
        execute_values(write_cur, WRITE_SQL.format(version=KEY_VERSION), payload, page_size=1000)
    conn.commit()
    print(f"\n[saved] keyed {len(payload):,} rows (version {KEY_VERSION})")

    write_cur = conn.cursor(cursor_factory=RealDictCursor)
    write_cur.execute("""
        SELECT count(*) AS keyed,
               count(DISTINCT (manufacturer_id, product_name_sorted, quantity_value,
                               quantity_unit, is_fluid, variant_key)) AS keys
        FROM products WHERE grouping_keys_version IS NOT NULL
    """)
    stats = write_cur.fetchone()
    print(f"keyed rows: {stats['keyed']:,} | distinct identity keys: {stats['keys']:,}")
    write_cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
