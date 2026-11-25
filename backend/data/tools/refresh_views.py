"""Refresh API materialized read-model views (`grouped_products`, `manufacturer_stats`).

Usage:
    python -m backend.data.tools.refresh_views
    python -m backend.data.tools.refresh_views --blocking
"""

from __future__ import annotations

import argparse

from backend.data.db_utils import connect_to_db, refresh_read_views


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh API materialized views.")
    parser.add_argument(
        "--blocking",
        action="store_true",
        help="Run blocking REFRESH instead of REFRESH CONCURRENTLY",
    )
    args = parser.parse_args()

    conn = connect_to_db()
    try:
        refreshed = refresh_read_views(conn, concurrently=not args.blocking)
        if not refreshed:
            print("No materialized read views found to refresh.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
