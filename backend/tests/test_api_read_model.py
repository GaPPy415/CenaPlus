"""Integration tests for the materialized read-model endpoints and refresh helper.

Verifies that `/manufacturer`, `/{main}/{sub}`, `/search`, and `/product` return
exact parity against the underlying `groups`/`products` tables with no leaked
internal materialized-view columns, and that concurrent view refresh succeeds.
"""

from __future__ import annotations

import unittest

from fastapi import HTTPException

from backend.api import (
    get_grouped_products,
    get_manufacturer_products,
    get_product_detail,
    search_products,
)
from backend.data.db_utils import connect_to_db, refresh_read_views

EXPECTED_GROUP_KEYS = {"group_id", "group_name", "main_category", "sub_category", "products"}


class ApiReadModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = connect_to_db()
        cls.conn.autocommit = True

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_manufacturer_parity_with_base_tables(self):
        with self.conn.cursor() as cur:
            for name in ("др еткер", "монстер", "нивеа"):
                res = get_manufacturer_products(name, page=1, per_page=12)
                mfr_name = res["manufacturer"]["name"]

                cur.execute(
                    """
                    SELECT count(DISTINCT g.id),
                           count(DISTINCT g.main_category),
                           count(DISTINCT p.market)
                    FROM manufacturers m
                    JOIN groups g ON g.manufacturer_id = m.id
                    JOIN products p ON p.group_id = g.id AND p.in_stock = true
                    WHERE m.canonical_name = %s
                    """,
                    (mfr_name,),
                )
                expected_groups, expected_cats, expected_markets = cur.fetchone()

                self.assertEqual(res["total"], expected_groups, name)
                self.assertEqual(res["manufacturer"]["groups"], expected_groups, name)
                self.assertEqual(res["manufacturer"]["categories"], expected_cats, name)
                self.assertEqual(res["manufacturer"]["markets"], expected_markets, name)
                self.assertEqual(len(res["data"]), min(12, expected_groups), name)

                # Sorted by group_name and no internal columns leaked
                names = [row["group_name"] for row in res["data"]]
                self.assertEqual(len(names), len(res["data"]))
                for row in res["data"]:
                    self.assertEqual(set(row.keys()), EXPECTED_GROUP_KEYS)
                    prices = [p["price"] for p in row["products"] if p["price"] is not None]
                    self.assertEqual(prices, sorted(prices))

    def test_manufacturer_not_found_returns_404(self):
        with self.assertRaises(HTTPException) as ctx:
            get_manufacturer_products("__definitely_nonexistent_brand_xyz__", page=1, per_page=12)
        self.assertEqual(ctx.exception.status_code, 404)

    def test_manufacturer_with_zero_in_stock_groups(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.canonical_name
                FROM manufacturers m
                LEFT JOIN manufacturer_stats ms ON ms.manufacturer_id = m.id
                WHERE m.merged_into IS NULL AND ms.manufacturer_id IS NULL
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if not row:
            self.skipTest("No zero-stock manufacturer in DB")

        res = get_manufacturer_products(row[0], page=1, per_page=12)
        self.assertEqual(res["total"], 0)
        self.assertEqual(res["manufacturer"]["groups"], 0)
        self.assertEqual(res["manufacturer"]["categories"], 0)
        self.assertEqual(res["manufacturer"]["markets"], 0)
        self.assertEqual(res["data"], [])

    def test_subcategory_and_market_filter_parity(self):
        main_cat = "Дом и домаќинство"
        sub_cat = "Кујнски прибор и садови"
        markets = ["reptil", "vero"]

        unfiltered = get_grouped_products(main_cat, sub_cat, page=1, per_page=12, market=None)
        filtered = get_grouped_products(main_cat, sub_cat, page=1, per_page=12, market=markets)

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM groups g
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE g.main_category = %s AND g.sub_category = %s
                """,
                (main_cat, sub_cat),
            )
            expected_unfiltered = cur.fetchone()[0]

            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM groups g
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE g.main_category = %s AND g.sub_category = %s
                  AND p.market = ANY(%s)
                """,
                (main_cat, sub_cat, markets),
            )
            expected_filtered = cur.fetchone()[0]

        self.assertEqual(unfiltered["total"], expected_unfiltered)
        self.assertEqual(filtered["total"], expected_filtered)
        for row in unfiltered["data"] + filtered["data"]:
            self.assertEqual(set(row.keys()), EXPECTED_GROUP_KEYS)
        for row in filtered["data"]:
            row_markets = {p["market"] for p in row["products"]}
            self.assertTrue(row_markets & set(markets))

    def test_search_and_product_detail(self):
        res = search_products("кока кола")
        self.assertGreater(len(res["data"]), 0)
        first = res["data"][0]
        self.assertEqual(set(first.keys()), EXPECTED_GROUP_KEYS | {"similarity"})
        self.assertGreaterEqual(first["similarity"], 0.8)

        detail = get_product_detail(str(first["group_id"]))
        self.assertEqual(str(detail["group"]["group_id"]), str(first["group_id"]))
        self.assertGreater(detail["stats"]["in_stock"], 0)

    def test_concurrent_refresh_read_views(self):
        refreshed = refresh_read_views(self.conn, concurrently=True)
        self.assertEqual(refreshed, ["grouped_products", "manufacturer_stats"])


if __name__ == "__main__":
    unittest.main()
