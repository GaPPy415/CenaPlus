"""Tests for the scraper strategy layer.

Run from the repo root::

    python -m unittest discover -s backend/tests -t .
"""

import unittest
from datetime import datetime

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct


class _FakeScraper(MarketScraper):
    market = "fake"

    def __init__(self, products, **attrs):
        self._products = products
        for key, value in attrs.items():
            setattr(self, key, value)

    def fetch(self):
        return list(self._products)


class CollectTests(unittest.TestCase):
    def test_duplicates_collapse_and_keep_stock_up(self):
        scraper = _FakeScraper(
            [
                ScrapedProduct(name="Млеко 1л", price=50, in_stock=False),
                ScrapedProduct(name="Млеко 1л", price=55, in_stock=True),
            ]
        )
        products = scraper.collect()

        self.assertEqual(len(products), 1)
        self.assertEqual(products[0].price, 55)  # later record wins on price
        self.assertTrue(products[0].in_stock)    # stock is sticky by default

    def test_woocommerce_merge_lets_last_record_win(self):
        scraper = _FakeScraper(
            [
                ScrapedProduct(name="Сир", price=100, in_stock=True),
                ScrapedProduct(name="Сир", price=100, in_stock=False),
            ],
            keep_in_stock_on_duplicate=False,
        )
        self.assertFalse(scraper.collect()[0].in_stock)

    def test_optional_fields_backfill_from_earlier_record(self):
        scraper = _FakeScraper(
            [
                ScrapedProduct(name="Леб", price=30, singular_price="30 ден/кг", link="a"),
                ScrapedProduct(name="Леб", price=32),
            ]
        )
        product = scraper.collect()[0]
        self.assertEqual(product.price, 32)
        self.assertEqual(product.singular_price, "30 ден/кг")
        self.assertEqual(product.link, "a")

    def test_records_without_name_or_price_are_skipped(self):
        scraper = _FakeScraper(
            [ScrapedProduct(name="", price=10), ScrapedProduct(name="Ориз", price=None)]
        )
        self.assertEqual(scraper.collect(), [])

    def test_names_are_html_unescaped_and_stripped(self):
        scraper = _FakeScraper([ScrapedProduct(name="  Coca &amp; Cola  ", price=70)])
        self.assertEqual(scraper.collect()[0].name, "Coca & Cola")

    def test_price_is_coerced_to_int(self):
        self.assertEqual(ScrapedProduct(name="X", price="120").price, 120)


class BuildRowsTests(unittest.TestCase):
    def test_reuses_existing_id_and_stamps_both_timestamps(self):
        scraper = _FakeScraper([])
        now = datetime(2026, 11, 3, 12, 0, 0)
        rows = scraper.build_rows(
            [ScrapedProduct(name="Шеќер", price=90)],
            {("Шеќер", "fake"): "11111111-1111-1111-1111-111111111111"},
            now,
        )

        self.assertEqual(rows[0]["id"], "11111111-1111-1111-1111-111111111111")
        self.assertEqual(rows[0]["market"], "fake")
        self.assertEqual(rows[0]["ETL_loadtime"], now)
        self.assertEqual(rows[0]["last_updated"], now)
        # The payload must contain every column the upsert expects.
        self.assertEqual(
            set(rows[0]),
            {
                "id", "name", "price", "singular_price", "description",
                "in_stock", "image", "link", "market", "ETL_loadtime",
                "last_updated",
            },
        )

    def test_new_product_gets_a_uuid(self):
        rows = _FakeScraper([]).build_rows(
            [ScrapedProduct(name="Нов", price=10)], {}, datetime.now()
        )
        self.assertEqual(len(rows[0]["id"]), 36)


class EmptyRunTests(unittest.TestCase):
    def test_empty_run_aborts_without_touching_the_database(self):
        scraper = _FakeScraper([])
        # connect_to_db would raise without credentials; reaching it is the bug.
        self.assertEqual(scraper.run(), 1)

    def test_market_is_required(self):
        class NoMarket(MarketScraper):
            def fetch(self):
                return []

        with self.assertRaises(ValueError):
            NoMarket().run()


if __name__ == "__main__":
    unittest.main()
