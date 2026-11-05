"""Offline tests for the parallel Ramstore fetch.

The store pages are ~7 MB and the server is fragile, so the merge logic is tested
with a stubbed `scrape_store` instead of hitting the network.

    python -m unittest backend.tests.test_ramstore_parallel
"""

import time
import unittest

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


if __name__ == "__main__":
    unittest.main()
