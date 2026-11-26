"""Offline unit tests for the shared WooCommerce platform client and scrapers.

    python -m unittest backend.tests.test_woocommerce_platform
"""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from backend.data.scrapers.Bigshop_scraper_maintenance import (
    BigshopScraper,
    parse_singular_price as parse_bigshop_singular,
)
from backend.data.scrapers.Reptil_scraper import (
    ReptilScraper,
    parse_singular_price as parse_reptil_singular,
)
from backend.data.scrapers.woocommerce_platform import (
    decode_wc_json,
    parse_optional_price,
    parse_price,
)


def _fake_response(body: str, total_pages: int = 1):
    resp = MagicMock()
    resp.text = body
    resp.headers = {"X-WP-TotalPages": str(total_pages)}
    resp.raise_for_status.return_value = None
    return resp


class WooCommerceJsonTests(unittest.TestCase):
    def test_strips_leading_php_warning_before_json_array(self):
        raw = "<br />\n<b>Warning</b>: deprecated in foo.php<br />\n[{\"name\": \"Milk\"}]"
        self.assertEqual(decode_wc_json(raw), [{"name": "Milk"}])

    def test_rejects_non_json_or_non_list_body(self):
        with self.assertRaises(ValueError):
            decode_wc_json("<html>Maintenance mode</html>")
        with self.assertRaises(ValueError):
            decode_wc_json('{"code": "rest_error"}')

    def test_price_helpers(self):
        self.assertEqual(parse_price("3200", 2), 32)
        self.assertEqual(parse_price("bad", 2), 0)
        self.assertEqual(parse_optional_price("4500", 2), 45)
        self.assertIsNone(parse_optional_price("0", 2))
        self.assertIsNone(parse_optional_price(None, 2))


class WooCommerceFetchTests(unittest.TestCase):
    def test_fetches_page_one_once_and_merges_pages_in_order(self):
        calls: list[str] = []

        def fake_get(url, timeout=None):
            calls.append(url)
            if url.endswith("&page=1"):
                return _fake_response(
                    "Warning: notice\n"
                    + json.dumps([{"name": "P1", "prices": {"price": "5000"}, "is_in_stock": True}]),
                    total_pages=3,
                )
            if url.endswith("&page=2"):
                return _fake_response(
                    json.dumps([{"name": "P2", "prices": {"price": "6000"}, "is_in_stock": True}])
                )
            return _fake_response(
                json.dumps([{"name": "P3", "prices": {"price": "7000"}, "is_in_stock": False}])
            )

        with patch("backend.data.scrapers.woocommerce_platform.requests.get", side_effect=fake_get):
            products = ReptilScraper().fetch()

        self.assertEqual(len(calls), 3)
        self.assertEqual(sum(1 for u in calls if u.endswith("&page=1")), 1)
        self.assertEqual([p.name for p in products], ["P1", "P2", "P3"])
        self.assertEqual([p.price for p in products], [50, 60, 70])
        self.assertIsNone(products[0].image)

    def test_bigshop_empty_image_and_singular_parsers(self):
        item = {
            "name": "Сок 1л",
            "prices": {"price": "8900", "regular_price": "10900", "currency_minor_unit": 2},
            "is_in_stock": True,
            "images": [],
            "categories": [{"name": "Сокови"}],
            "permalink": "https://bigshop.mk/p/1",
        }
        prod = BigshopScraper().parse_product(item)
        self.assertEqual(prod.price, 89)
        self.assertEqual(prod.regular_price, 109)
        self.assertEqual(prod.image, "")

        reptil_html = (
            '<span class="mcmp_recalc_price_row"><bdi>128&nbsp;ден</bdi>'
            '<span class="mcmp-recalc-price-suffix">/kg</span></span>'
        )
        self.assertEqual(parse_reptil_singular(reptil_html), "128 ден/kg")
        self.assertIsNone(parse_bigshop_singular("<span>short</span>"))


if __name__ == "__main__":
    unittest.main()
