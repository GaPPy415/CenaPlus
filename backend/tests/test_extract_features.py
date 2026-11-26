"""Offline unit tests for extract_features.py deterministic parsing & cache logic.

No database or Gemini API access is needed.

    python -m unittest backend.tests.test_extract_features
"""

from __future__ import annotations

import unittest

from backend.data.extract_features import (
    MetadataEntry,
    ProductFeatures,
    build_row,
    canonical_quantity,
    clean_product_name,
    load_manufacturer_cache,
    parse_multipack_from_name,
)


class CanonicalQuantityTests(unittest.TestCase):
    def test_mass_and_volume_promotion(self):
        self.assertEqual(canonical_quantity(500, "гр", 1, False), (500.0, "g"))
        self.assertEqual(canonical_quantity(1000, "g", 1, False), (1.0, "kg"))
        self.assertEqual(canonical_quantity(1.5, "кг", 1, False), (1.5, "kg"))
        self.assertEqual(canonical_quantity(330, "мл", 6, True), (1.98, "l"))
        self.assertEqual(canonical_quantity(250, "ml", 1, True), (250.0, "ml"))

    def test_fluid_converts_grams_to_millilitres(self):
        self.assertEqual(canonical_quantity(500, "g", 1, True), (500.0, "ml"))
        self.assertEqual(canonical_quantity(1, "kg", 1, True), (1.0, "l"))
        self.assertEqual(canonical_quantity(400, "g", 1, False), (400.0, "g"))

    def test_pcs_and_invalid_inputs(self):
        self.assertEqual(canonical_quantity(6, "ком", 2, False), (12, "pcs"))
        self.assertEqual(canonical_quantity(0, "g", 1, False), (None, None))
        self.assertEqual(canonical_quantity(-5, "g", 1, False), (None, None))
        self.assertEqual(canonical_quantity(None, "g", 1, False), (None, None))
        self.assertEqual(canonical_quantity(100, "unknown", 1, False), (None, None))


class MultipackParsingTests(unittest.TestCase):
    def test_parses_multipack_and_promo_sum(self):
        self.assertEqual(parse_multipack_from_name("КОКА КОЛА 6x330ml"), (330.0, 6))
        self.assertEqual(parse_multipack_from_name("Вода 4 х 1,5л"), (1.5, 4))
        self.assertEqual(parse_multipack_from_name("Сапун 2+1 гратис"), (3, 1))
        self.assertEqual(parse_multipack_from_name("Обично млеко 1л"), (None, None))


class CleanProductNameTests(unittest.TestCase):
    def test_keeps_raw_spelling_and_strips_brand_packaging_and_noise(self):
        # LLM paraphrase "метелен" is not in raw name and must not leak in
        cleaned = clean_product_name(
            llm_name="кисело млеко 2.8% метелен стаклена",
            raw_name="КИСЕЛО МЛЕКО БИТОЛСКО 400гр 2.8% СТАКЛЕНА 123456",
            manufacturer="Битолско",
            metadata={"packaging": "стаклена"},
        )
        self.assertEqual(cleaned, "кисело млеко 2.8%")

    def test_fallback_strips_quantity_brand_and_duplicates(self):
        cleaned = clean_product_name(
            llm_name="",
            raw_name="Милка Чоколадо - Милка Чоколадо 100г",
            manufacturer="Милка",
        )
        self.assertEqual(cleaned, "чоколадо")

    def test_returns_none_for_only_tiny_fragments(self):
        self.assertIsNone(clean_product_name("а б", "а б 100г", None))


class BuildRowTests(unittest.TestCase):
    def _product(self, name: str) -> dict:
        return {"id": "00000000-0000-0000-0000-000000000001", "name": name}

    def test_missing_features_marks_row_failed(self):
        row = build_row(self._product("Млеко 1л"), None)
        self.assertTrue(row["failed"])
        self.assertEqual(row["features_confidence"], 0.0)

    def test_dry_goods_guard_prevents_fluid_conversion(self):
        features = ProductFeatures(
            index=1,
            manufacturer="Алкалоид",
            product_name="чај камилица",
            quantity_value=30,
            quantity_unit="g",
            pack_count=1,
            is_fluid=True,  # model wrongly thinks tea box is a drink
            confidence=0.95,
        )
        row = build_row(self._product("Алкалоид Чај камилица 30г"), features)
        self.assertFalse(row["is_fluid"])
        self.assertEqual(row["quantity_value"], 30.0)
        self.assertEqual(row["quantity_unit"], "g")

    def test_dimension_guard_drops_fake_pcs_and_explicit_count_wins(self):
        dim_features = ProductFeatures(
            index=1,
            manufacturer="Палкома",
            product_name="салфетки 3сл",
            quantity_value=24,
            quantity_unit="pcs",
            pack_count=1,
            is_fluid=False,
            confidence=0.8,
        )
        dim_row = build_row(self._product("Палкома Салфетки 24Х24ЦМ 3СЛ"), dim_features)
        self.assertIsNone(dim_row["quantity_value"])
        self.assertIsNone(dim_row["quantity_unit"])

        count_features = ProductFeatures(
            index=1,
            manufacturer="Фино",
            product_name="ќеси",
            quantity_value=27,
            quantity_unit="pcs",
            pack_count=1,
            is_fluid=False,
            metadata=[MetadataEntry(key="dimensions", value="27x20")],
            confidence=0.85,
        )
        count_row = build_row(self._product("Фино Ќеси 15пар 27х20см"), count_features)
        self.assertEqual(count_row["quantity_value"], 15)
        self.assertEqual(count_row["quantity_unit"], "pcs")
        self.assertEqual(count_row["metadata"]["pack_count"], "15")

    def test_fallback_multipack_when_llm_omits_quantity(self):
        features = ProductFeatures(
            index=1,
            manufacturer="Кока Кола",
            product_name="сок",
            quantity_value=None,
            quantity_unit=None,
            pack_count=1,
            is_fluid=True,
            confidence=0.9,
        )
        row = build_row(self._product("КОКА КОЛА 6x330ml"), features)
        self.assertEqual(row["quantity_value"], 1.98)
        self.assertEqual(row["quantity_unit"], "l")
        self.assertEqual(row["metadata"]["pack_count"], "6")


class ManufacturerCacheTests(unittest.TestCase):
    def test_follows_chains_handles_cycles_and_prefers_alias(self):
        class FakeCursor:
            def __init__(self):
                self._res = []

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, _params=None):
                if "FROM manufacturers" in sql:
                    self._res = [
                        (1, "surviving", None),
                        (2, "mid", 1),
                        (3, "old", 2),
                        (10, "cycle_a", 11),
                        (11, "cycle_b", 10),
                    ]
                elif "FROM manufacturer_aliases" in sql:
                    self._res = [
                        ("stale_alias", 3),
                        ("direct_alias", 1),
                    ]

            def fetchall(self):
                return self._res

        class FakeConn:
            def cursor(self):
                return FakeCursor()

        cache = load_manufacturer_cache(FakeConn())
        self.assertEqual(cache["surviving"], 1)
        self.assertEqual(cache["mid"], 1)
        self.assertEqual(cache["old"], 1)
        self.assertEqual(cache["stale_alias"], 1)
        self.assertEqual(cache["direct_alias"], 1)
        # Cycle terminates cleanly without infinite loop
        self.assertIn(cache["cycle_a"], (10, 11))


if __name__ == "__main__":
    unittest.main()
