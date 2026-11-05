"""Tests for the Kipper product payload mapping.

    python -m unittest backend.tests.test_kipper_parsing
"""

import unittest

from backend.data.scrapers.Kipper_scraper import parse_price, parse_row, unit_price

SAMPLE = {
    "product_image": "000307.webp",
    "product_name": "7ДЕЈС МИНИ КРОАСАН КАКАО 60г",
    "product_name_alt": "7DAYS MINI KROASAN KAKAO 60gr",
    "product_price": "36",
    "product_price_normal": "36",
    "product_price_discount_percentage": None,
    "product_price_weight": "600",
    "product_type_alt": "1КГ=",
    "product_status": "D",
    "product_subgroup": "КИФЛИ И ТОРТИЧКИ",
    "promotion_datetime_from": None,
    "promotion_datetime_to": None,
}


class RowParsingTests(unittest.TestCase):
    def test_maps_the_store_table_row(self):
        product = parse_row(SAMPLE)

        self.assertEqual(product.name, "7ДЕЈС МИНИ КРОАСАН КАКАО 60г")
        self.assertEqual(product.price, 36)
        self.assertEqual(product.singular_price, "1КГ=600 ден")
        self.assertEqual(product.description, "КИФЛИ И ТОРТИЧКИ")
        self.assertTrue(product.in_stock)

    def test_name_stays_cyrillic(self):
        # product_name_alt is the Latin name; it must not replace the Cyrillic one.
        self.assertNotIn("7DAYS", parse_row(SAMPLE).name)

    def test_rows_without_a_price_are_dropped(self):
        self.assertIsNone(parse_row({**SAMPLE, "product_price": None}))
        self.assertIsNone(parse_row({**SAMPLE, "product_price": "0"}))
        self.assertIsNone(parse_row({**SAMPLE, "product_price": ""}))

    def test_rows_without_a_name_are_dropped(self):
        self.assertIsNone(parse_row({**SAMPLE, "product_name": "   "}))

    def test_non_available_status_is_out_of_stock(self):
        self.assertFalse(parse_row({**SAMPLE, "product_status": "N"}).in_stock)

    def test_promo_price_is_the_selling_price(self):
        product = parse_row({**SAMPLE, "product_price": "29", "product_price_normal": "36"})
        self.assertEqual(product.price, 29)


class UnitPriceTests(unittest.TestCase):
    def test_labels(self):
        self.assertEqual(unit_price({"product_type_alt": "1КГ=", "product_price_weight": "600"}), "1КГ=600 ден")
        self.assertEqual(unit_price({"product_type_alt": "1ПАР=", "product_price_weight": "149"}), "1ПАР=149 ден")
        self.assertEqual(unit_price({"product_type_alt": "=1КГ", "product_price_weight": "12"}), "1КГ=12 ден")
        self.assertIsNone(unit_price({"product_type_alt": "1КГ=", "product_price_weight": ""}))

    def test_parse_price(self):
        self.assertEqual(parse_price("36"), 36)
        self.assertEqual(parse_price("36.00"), 36)
        self.assertIsNone(parse_price(None))


if __name__ == "__main__":
    unittest.main()
