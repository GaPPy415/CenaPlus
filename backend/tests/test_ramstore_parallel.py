"""Offline tests for the parallel Ramstore fetch.

The store pages are ~7 MB and the server is fragile, so the merge logic is tested
with a stubbed `scrape_store` instead of hitting the network.

    python -m unittest backend.tests.test_ramstore_parallel
"""

import time
import unittest

from unittest.mock import MagicMock, patch

from backend.data.scrapers import Ramstore_scraper as ramstore
from backend.data.scrapers.scraper_base import ScrapedProduct


class ParallelFetchTests(unittest.TestCase):
    def setUp(self):
        self._original = ramstore.scrape_store

    def tearDown(self):
        ramstore.scrape_store = self._original

    def test_merges_in_store_order_regardless_of_completion_order(self):
        delays = {store: (len(ramstore.stores) - index) * 0.01 for index, store in enumerate(ramstore.stores)}

        def fake_scrape(store):
            time.sleep(delays[store])  # later stores finish first
            return [ScrapedProduct(name=f"product-{store}", price=1)]

        ramstore.scrape_store = fake_scrape
        products = ramstore.RamstoreScraper().fetch()

        expected = [f"product-{store}" for store in ramstore.stores]
        self.assertEqual([p.name for p in products], expected)

    def test_one_failing_store_does_not_drop_the_others(self):
        broken = ramstore.stores[0]

        def fake_scrape(store):
            if store == broken:
                raise RuntimeError("store page unavailable")
            return [ScrapedProduct(name=f"product-{store}", price=1)]

        ramstore.scrape_store = fake_scrape
        products = ramstore.RamstoreScraper().fetch()

        self.assertEqual(len(products), len(ramstore.stores) - 1)
        self.assertNotIn(f"product-{broken}", [p.name for p in products])


class PriceAndTableParsingTests(unittest.TestCase):
    def test_parse_int_price_handles_macedonian_and_decimal_formats(self):
        self.assertEqual(ramstore._parse_int_price(120), 120)
        self.assertEqual(ramstore._parse_int_price(120.0), 120)
        self.assertEqual(ramstore._parse_int_price("120.00 ден."), 120)
        self.assertEqual(ramstore._parse_int_price("120,00 ден."), 120)
        self.assertEqual(ramstore._parse_int_price("1.250,00 ден."), 1250)
        self.assertEqual(ramstore._parse_int_price("1.250 ден."), 1250)
        self.assertIsNone(ramstore._parse_int_price("0"))
        self.assertIsNone(ramstore._parse_int_price(""))
        self.assertIsNone(ramstore._parse_int_price(None))

    def test_scrape_store_skips_bad_price_rows_and_ignores_missing_promo_dates_col(self):
        html_table = """
        <table>
          <thead>
            <tr>
              <th>НАЗИВ</th>
              <th>ПРОДАЖНА ЦЕНА</th>
              <th>РЕДОВНА ЦЕНА</th>
              <th>ЦЕНА СО ПОПУСТ</th>
              <th>ЕДИНЕЧНА ЦЕНА</th>
              <th>ОПИС НА ПРОИЗВОД</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Млеко 1л</td>
              <td>79</td>
              <td>99.00 ден.</td>
              <td>69.00 ден.</td>
              <td>69 ден/л</td>
              <td>Млечни</td>
            </tr>
            <tr>
              <td>Лош ред</td>
              <td>н/а</td>
              <td></td>
              <td></td>
              <td></td>
              <td></td>
            </tr>
          </tbody>
        </table>
        """
        fake_resp = MagicMock(text=html_table)
        fake_resp.raise_for_status.return_value = None
        with patch("backend.data.scrapers.Ramstore_scraper.requests.get", return_value=fake_resp):
            products = ramstore.scrape_store("https://ramstore.com.mk/marketi/ramstore-vardar/")

        self.assertEqual(len(products), 1)
        self.assertEqual(products[0].name, "Млеко 1л")
        self.assertEqual(products[0].price, 69)
        self.assertEqual(products[0].regular_price, 99)


if __name__ == "__main__":
    unittest.main()
