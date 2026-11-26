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
                           count(DISTINCT p.market),
                           max(p.last_updated)
                    FROM manufacturers m
                    JOIN groups g ON g.manufacturer_id = m.id
                    JOIN products p ON p.group_id = g.id AND p.in_stock = true
                    WHERE m.canonical_name = %s
                    """,
                    (mfr_name,),
                )
                expected_groups, expected_cats, expected_markets, expected_last_updated = cur.fetchone()

                self.assertEqual(res["total"], expected_groups, name)
                self.assertEqual(res["manufacturer"]["groups"], expected_groups, name)
                self.assertEqual(res["manufacturer"]["categories"], expected_cats, name)
                self.assertEqual(res["manufacturer"]["markets"], expected_markets, name)
                self.assertEqual(res["manufacturer"]["last_updated"], expected_last_updated, name)
                self.assertEqual(len(res["data"]), min(12, expected_groups), name)
                self.assertEqual(
                    sum(s["count"] for s in res["subcategories"]),
                    expected_groups,
                    name,
                )

                # Sorted by group_name and no internal columns leaked
                names = [row["group_name"] for row in res["data"]]
                self.assertEqual(len(names), len(res["data"]))
                for row in res["data"]:
                    self.assertEqual(set(row.keys()), EXPECTED_GROUP_KEYS)
                    prices = [p["price"] for p in row["products"] if p["price"] is not None]
                    self.assertEqual(prices, sorted(prices))

    def test_manufacturer_subcategory_filter(self):
        unfiltered = get_manufacturer_products("др еткер", page=1, per_page=12)
        subs = unfiltered["subcategories"]
        self.assertGreater(len(subs), 2)

        first, second = subs[0], subs[1]
        key_first = f"{first['main_category']}::{first['sub_category']}"
        key_second = f"{second['main_category']}::{second['sub_category']}"

        # Single subcategory filter via main::sub key
        one = get_manufacturer_products("др еткер", page=1, per_page=12, sub_category=[key_first])
        self.assertEqual(one["total"], first["count"])
        self.assertEqual(one["manufacturer"]["groups"], unfiltered["manufacturer"]["groups"])
        self.assertEqual(one["subcategories"], subs)
        self.assertEqual(len(one["data"]), min(12, first["count"]))
        for row in one["data"]:
            self.assertEqual(row["main_category"], first["main_category"])
            self.assertEqual(row["sub_category"], first["sub_category"])

        # Multi-subcategory filter
        two = get_manufacturer_products(
            "др еткер", page=1, per_page=36, sub_category=[key_first, key_second]
        )
        self.assertEqual(two["total"], first["count"] + second["count"])
        allowed = {
            (first["main_category"], first["sub_category"]),
            (second["main_category"], second["sub_category"]),
        }
        for row in two["data"]:
            self.assertIn((row["main_category"], row["sub_category"]), allowed)

        # Plain subcategory name (without main:: prefix) also works
        plain = get_manufacturer_products(
            "др еткер", page=1, per_page=12, sub_category=[first["sub_category"]]
        )
        self.assertGreaterEqual(plain["total"], first["count"])

        # Non-matching subcategory yields 0 rows without error
        empty = get_manufacturer_products(
            "др еткер", page=1, per_page=12, sub_category=["Непостоечка::Поткатегорија"]
        )
        self.assertEqual(empty["total"], 0)
        self.assertEqual(empty["data"], [])

    def test_manufacturer_market_filter_parity(self):
        unfiltered = get_manufacturer_products("др еткер", page=1, per_page=12)
        mfr_name = unfiltered["manufacturer"]["name"]
        markets = ["reptil", "vero"]

        filtered = get_manufacturer_products("др еткер", page=1, per_page=36, market=markets)

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM manufacturers m
                JOIN groups g ON g.manufacturer_id = m.id
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE m.canonical_name = %s
                  AND p.market = ANY(%s)
                """,
                (mfr_name, markets),
            )
            expected_market_only = cur.fetchone()[0]

        self.assertEqual(filtered["total"], expected_market_only)
        self.assertEqual(filtered["manufacturer"]["groups"], unfiltered["manufacturer"]["groups"])
        for row in filtered["data"]:
            self.assertEqual(set(row.keys()), EXPECTED_GROUP_KEYS)
            row_markets = {p["market"] for p in row["products"]}
            self.assertTrue(row_markets & set(markets))

        # Combined subcategory + market filter
        first_sub = unfiltered["subcategories"][0]
        sub_key = f"{first_sub['main_category']}::{first_sub['sub_category']}"
        combined = get_manufacturer_products(
            "др еткер", page=1, per_page=36, sub_category=[sub_key], market=markets
        )
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM manufacturers m
                JOIN groups g ON g.manufacturer_id = m.id
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE m.canonical_name = %s
                  AND g.main_category = %s
                  AND g.sub_category = %s
                  AND p.market = ANY(%s)
                """,
                (mfr_name, first_sub["main_category"], first_sub["sub_category"], markets),
            )
            expected_combined = cur.fetchone()[0]

        self.assertEqual(combined["total"], expected_combined)
        for row in combined["data"]:
            self.assertEqual(row["main_category"], first_sub["main_category"])
            self.assertEqual(row["sub_category"], first_sub["sub_category"])
            row_markets = {p["market"] for p in row["products"]}
            self.assertTrue(row_markets & set(markets))

        # Non-matching market yields 0 rows without error
        empty = get_manufacturer_products("др еткер", page=1, per_page=12, market=["__no_such_market__"])
        self.assertEqual(empty["total"], 0)
        self.assertEqual(empty["data"], [])

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
        self.assertGreater(res["total"], 0)
        self.assertLessEqual(res["total"], 500)
        self.assertEqual(res["page"], 1)
        self.assertEqual(res["per_page"], 24)
        self.assertGreater(len(res["data"]), 0)
        self.assertLessEqual(len(res["data"]), 24)
        first = res["data"][0]
        self.assertEqual(set(first.keys()), EXPECTED_GROUP_KEYS | {"similarity"})
        self.assertGreaterEqual(first["similarity"], 0.8)

        # Pagination + sorting + market filter on /search
        paged = search_products("кока кола", page=1, per_page=12, sort="price", order="asc")
        self.assertEqual(paged["total"], res["total"])
        self.assertEqual(paged["per_page"], 12)
        mins = [min(p["price"] for p in row["products"]) for row in paged["data"]]
        self.assertEqual(mins, sorted(mins))

        filtered = search_products("кока кола", page=1, per_page=12, market=["vero"])
        self.assertLessEqual(filtered["total"], res["total"])
        for row in filtered["data"]:
            self.assertIn("vero", {p["market"] for p in row["products"]})

        detail = get_product_detail(str(first["group_id"]))
        self.assertEqual(str(detail["group"]["group_id"]), str(first["group_id"]))
        self.assertGreater(detail["stats"]["in_stock"], 0)
        for offer in detail["offers"]:
            self.assertIn("regular_price", offer)
            self.assertIn("market_last_updated", offer)
            self.assertIsNotNone(offer["market_last_updated"])

    def test_on_sale_filter_parity(self):
        main_cat = "Дом и домаќинство"
        sub_cat = "Кујнски прибор и садови"
        markets = ["reptil", "vero"]

        sale_sub = get_grouped_products(main_cat, sub_cat, page=1, per_page=12, on_sale=True)
        sale_sub_mkt = get_grouped_products(
            main_cat, sub_cat, page=1, per_page=12, market=markets, on_sale=True
        )
        sale_mfr = get_manufacturer_products("др еткер", page=1, per_page=12, on_sale=True)
        sale_mfr_mkt = get_manufacturer_products(
            "др еткер", page=1, per_page=12, market=markets, on_sale=True
        )

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM groups g
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE g.main_category = %s AND g.sub_category = %s
                  AND p.regular_price IS NOT NULL
                """,
                (main_cat, sub_cat),
            )
            self.assertEqual(sale_sub["total"], cur.fetchone()[0])

            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM groups g
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE g.main_category = %s AND g.sub_category = %s
                  AND p.market = ANY(%s)
                  AND p.regular_price IS NOT NULL
                """,
                (main_cat, sub_cat, markets),
            )
            self.assertEqual(sale_sub_mkt["total"], cur.fetchone()[0])

            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM manufacturers m
                JOIN groups g ON g.manufacturer_id = m.id
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE m.canonical_name = %s
                  AND p.regular_price IS NOT NULL
                """,
                (sale_mfr["manufacturer"]["name"],),
            )
            self.assertEqual(sale_mfr["total"], cur.fetchone()[0])

            cur.execute(
                """
                SELECT count(DISTINCT g.id)
                FROM manufacturers m
                JOIN groups g ON g.manufacturer_id = m.id
                JOIN products p ON p.group_id = g.id AND p.in_stock = true
                WHERE m.canonical_name = %s
                  AND p.market = ANY(%s)
                  AND p.regular_price IS NOT NULL
                """,
                (sale_mfr_mkt["manufacturer"]["name"], markets),
            )
            self.assertEqual(sale_mfr_mkt["total"], cur.fetchone()[0])

        for row in sale_sub["data"] + sale_mfr["data"]:
            self.assertEqual(set(row.keys()), EXPECTED_GROUP_KEYS)
            self.assertTrue(any(p.get("regular_price") is not None for p in row["products"]))
        for row in sale_sub_mkt["data"] + sale_mfr_mkt["data"]:
            self.assertTrue(
                any(
                    p["market"] in markets and p.get("regular_price") is not None
                    for p in row["products"]
                )
            )

    def test_concurrent_refresh_read_views(self):
        refreshed = refresh_read_views(self.conn, concurrently=True)
        self.assertEqual(refreshed, ["grouped_products", "manufacturer_stats"])

    def test_sorting_options(self):
        main_cat = "Дом и домаќинство"
        sub_cat = "Кујнски прибор и садови"

        price_asc = get_grouped_products(main_cat, sub_cat, page=1, per_page=24, sort="price", order="asc")
        mins_asc = [min(p["price"] for p in row["products"]) for row in price_asc["data"]]
        self.assertEqual(mins_asc, sorted(mins_asc))

        price_desc = get_grouped_products(main_cat, sub_cat, page=1, per_page=24, sort="price", order="desc")
        mins_desc = [min(p["price"] for p in row["products"]) for row in price_desc["data"]]
        self.assertEqual(mins_desc, sorted(mins_desc, reverse=True))

        mkts_desc = get_manufacturer_products("др еткер", page=1, per_page=24, sort="markets", order="desc")
        counts_desc = [len({p["market"] for p in row["products"]}) for row in mkts_desc["data"]]
        self.assertEqual(counts_desc, sorted(counts_desc, reverse=True))

        mkts_asc = get_manufacturer_products("др еткер", page=1, per_page=24, sort="markets", order="asc")
        counts_asc = [len({p["market"] for p in row["products"]}) for row in mkts_asc["data"]]
        self.assertEqual(counts_asc, sorted(counts_asc))

        # Discount % and discount amount (denars) sorts
        disc_desc = get_grouped_products(main_cat, sub_cat, page=1, per_page=12, sort="discount", order="desc")
        self.assertEqual(disc_desc["total"], price_asc["total"])

        amt_desc = get_grouped_products(
            main_cat, sub_cat, page=1, per_page=24, on_sale=True, sort="discount_amount", order="desc"
        )
        amts = [
            max(
                (p["regular_price"] - p["price"])
                for p in row["products"]
                if p.get("regular_price") is not None and p["price"] is not None and p["regular_price"] > p["price"]
            )
            for row in amt_desc["data"]
        ]
        self.assertEqual(amts, sorted(amts, reverse=True))

        search_amt = search_products("кока кола", page=1, per_page=12, sort="discount_amount", order="desc")
        self.assertGreaterEqual(search_amt["total"], 0)


if __name__ == "__main__":
    unittest.main()
