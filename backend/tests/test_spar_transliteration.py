"""Tests for the SPAR name transliteration.

SPAR's ERP writes Macedonian in ASCII: the physical Macedonian keyboard supplies
the letters Latin lacks (``[`` = Ш, ``^`` = Ч, ``@`` = Ж, ``]`` = Ќ, ``\\`` = Ѓ,
``W`` = Њ, ``Q`` = Љ, ``X`` = Џ) and the rest is a normal Latin transliteration.

    python -m unittest backend.tests.test_spar_transliteration
"""

import unittest

from backend.data.scrapers.Spar_scraper import parse_price, parse_row, to_cyrillic


class TransliterationTests(unittest.TestCase):
    def test_keyboard_symbols(self):
        self.assertEqual(to_cyrillic("MA[KI"), "МАШКИ")
        self.assertEqual(to_cyrillic("^OKOLADO"), "ЧОКОЛАДО")
        self.assertEqual(to_cyrillic("@ENSKI"), "ЖЕНСКИ")
        self.assertEqual(to_cyrillic("SPANA]"), "СПАНАЌ")
        self.assertEqual(to_cyrillic("\\UBRE"), "ЃУБРЕ")

    def test_lowercase_shifted_symbols(self):
        self.assertEqual(to_cyrillic("Radovi{"), "Радовиш")
        self.assertEqual(to_cyrillic("Peh~evo"), "Пехчево")

    def test_word_letters(self):
        self.assertEqual(to_cyrillic("SKIJAWE"), "СКИЈАЊЕ")
        self.assertEqual(to_cyrillic("SIREWE"), "СИРЕЊЕ")
        self.assertEqual(to_cyrillic("TORTIQA"), "ТОРТИЉА")
        self.assertEqual(to_cyrillic("LOS ANXELES"), "ЛОС АНЏЕЛЕС")

    def test_model_codes_survive(self):
        # W/X/Q only become letters inside words, so codes stay readable.
        self.assertEqual(to_cyrillic("AKSESOAR ZA DEVOJ^E W2-1 C05/24"), "АКСЕСОАР ЗА ДЕВОЈЧЕ W2-1 Ц05/24")

    def test_mixed_name(self):
        self.assertEqual(
            to_cyrillic("VRTENA BANICA SIREWE I SPANA] 800G TVINS"),
            "ВРТЕНА БАНИЦА СИРЕЊЕ И СПАНАЌ 800Г ТВИНС",
        )


class RowParsingTests(unittest.TestCase):
    def test_price_is_the_selling_price(self):
        row = {"name": "VODA 0.5L", "category": "Water", "available": "1", "sale": "39", "regular": "49"}
        product = parse_row(row)
        self.assertEqual(product.price, 39)
        self.assertTrue(product.in_stock)
        self.assertEqual(product.description, "Water")

    def test_zero_price_rows_are_dropped(self):
        self.assertIsNone(parse_row({"name": "VODA", "available": "1", "sale": "0"}))

    def test_unavailable_rows_are_flagged(self):
        product = parse_row({"name": "X", "available": "0", "sale": "10"})
        self.assertFalse(product.in_stock)

    def test_decimal_prices_are_floored_to_whole_denars(self):
        self.assertEqual(parse_price("149.00"), 149)
        self.assertEqual(parse_price("149,50"), 149)
        self.assertIsNone(parse_price(""))
        self.assertIsNone(parse_price(None))


if __name__ == "__main__":
    unittest.main()
