"""Tests for the scraper strategy layer.

Run from the repo root::

    python -m unittest discover -s backend/tests -t .
"""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

from backend.data.run_scrapers import run_script
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

    def test_regular_price_is_only_kept_when_strictly_higher_than_price(self):
        self.assertEqual(ScrapedProduct(name="A", price=89, regular_price="119").regular_price, 119)
        self.assertIsNone(ScrapedProduct(name="B", price=119, regular_price=119).regular_price)
        self.assertIsNone(ScrapedProduct(name="C", price=119, regular_price=99).regular_price)
        self.assertIsNone(ScrapedProduct(name="D", price=119, regular_price=None).regular_price)

    def test_merge_does_not_backfill_regular_price_from_earlier_store(self):
        scraper = _FakeScraper(
            [
                ScrapedProduct(name="Млеко", price=89, regular_price=119),
                ScrapedProduct(name="Млеко", price=119, regular_price=None),
            ]
        )
        product = scraper.collect()[0]
        self.assertEqual(product.price, 119)
        self.assertIsNone(product.regular_price)


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
                "id", "name", "price", "regular_price", "singular_price", "description",
                "in_stock", "image", "link", "market", "ETL_loadtime",
                "last_updated",
            },
        )
        self.assertIsNone(rows[0]["regular_price"])

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


class ReachabilityAndWatchdogTests(unittest.TestCase):
    def test_unreachable_probe_skips_fetch_and_returns_1(self):
        fetch_called = False

        class DownScraper(MarketScraper):
            market = "down"
            probe_url = "https://example.invalid/"

            def fetch(self):
                nonlocal fetch_called
                fetch_called = True
                return [ScrapedProduct(name="X", price=10)]

        with patch("backend.data.scrapers.scraper_base.requests.get", side_effect=requests.Timeout("hang")):
            self.assertEqual(DownScraper().run(), 1)
        self.assertFalse(fetch_called)

    def test_5xx_probe_skips_fetch_and_returns_1(self):
        fetch_called = False

        class ServerErrorScraper(MarketScraper):
            market = "err503"
            probe_url = "https://example.invalid/"

            def fetch(self):
                nonlocal fetch_called
                fetch_called = True
                return [ScrapedProduct(name="X", price=10)]

        fake_resp = MagicMock()
        fake_resp.status_code = 503
        fake_resp.__enter__.return_value = fake_resp
        with patch("backend.data.scrapers.scraper_base.requests.get", return_value=fake_resp):
            self.assertEqual(ServerErrorScraper().run(), 1)
        self.assertFalse(fetch_called)

    def test_run_script_kills_hanging_scraper_on_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            script = tmp_path / "hang_scraper.py"
            script.write_text("import time\ntime.sleep(30)\n", encoding="utf-8")
            result = run_script(script, tmp_path / "logs", stream=False, timeout_seconds=1)
            self.assertEqual(result["status"], "timeout")
            self.assertEqual(result["exit_code"], 124)


if __name__ == "__main__":
    unittest.main()
