"""Unit tests for Layers 1-5 non-LLM product grouping rules and safety vetoes.

Pure in-memory suite (no Postgres or Ollama connection required).
"""

from __future__ import annotations

import unittest

import numpy as np

from backend.data.grouping_rules import (
    clean_name_tokens,
    clean_variant_allowlist,
    consonant_skeleton,
    extract_raw_safety_sig,
    guarded_fuzzy_merge,
    recover_qty_from_singular_price,
    safety_sigs_compatible,
    smart_merge_block,
    tokens_align,
    unified_base_qty,
)
from backend.data.group_products import choose_display_name
from backend.data.local_embeddings import layer5_merge_block


def _row(
    idx: int,
    market: str,
    name: str,
    sorted_key: str,
    price: int = 100,
    in_stock: bool = True,
    main_cat: str = "Слатки и грицки",
    sub_cat: str = "Чоколади и чоколадни барови",
    variant: dict | None = None,
    product_name: str | None = None,
) -> dict:
    raw_sig = extract_raw_safety_sig(name)
    return {
        "id": f"00000000-0000-0000-0000-{idx:012d}",
        "market": market,
        "manufacturer_id": 1,
        "name": name,
        "product_name": product_name or sorted_key,
        "product_name_key": sorted_key,
        "product_name_sorted": " ".join(sorted(sorted_key.split())),
        "price": price,
        "in_stock": in_stock,
        "main_category": main_cat,
        "sub_category": sub_cat,
        "variant": clean_variant_allowlist(variant, raw_sig),
        "_raw_sig": raw_sig,
    }


class Layer1HelpersTests(unittest.TestCase):
    def test_negation_opposites_never_fuzzy_merge(self):
        self.assertFalse(guarded_fuzzy_merge("вода газирана", "вода негазирана"))
        self.assertFalse(guarded_fuzzy_merge("бадем печен", "бадем непечен"))
        self.assertFalse(tokens_align("газирана", "негазирана"))
        self.assertFalse(tokens_align("печен", "непечен"))

    def test_singular_price_quantity_recovery(self):
        self.assertEqual(recover_qty_from_singular_price(665, "665 ден/кг"), (1000.0, "mv"))
        self.assertEqual(recover_qty_from_singular_price(505, "100 ГР: =50.5ДЕН"), (1000.0, "mv"))
        self.assertEqual(recover_qty_from_singular_price(387, "100ml = 387.00 ден."), (100.0, "mv"))
        self.assertEqual(recover_qty_from_singular_price(179, "1 ПАРЧЕ =89.5ДЕН"), (2.0, "pcs"))
        # Far outside +/-4% of any standard size -> None
        self.assertEqual(recover_qty_from_singular_price(137, "100gr = 100.00 ден."), (None, None))

    def test_unified_base_qty_folds_mass_and_volume(self):
        self.assertEqual(unified_base_qty(400, "g"), unified_base_qty(400, "ml"))
        self.assertEqual(unified_base_qty(1, "kg"), unified_base_qty(1000, "ml"))
        self.assertEqual(unified_base_qty(1, "l"), ("mv", 1000.0))
        self.assertEqual(unified_base_qty(6, "pcs"), ("pcs", 6.0))

    def test_zur_backtick_and_glued_erp_code_and_brand_alias_cleanup(self):
        toks = clean_name_tokens(
            "а вода ко мицеларна нормална",
            "НИВЕА МИЦЕЛАРНА ВОДА НОРМАЛНА КО`А 400МЛ",
            manufacturer_id=10,
        )
        self.assertIn("кожа", toks)
        self.assertNotIn("ко", toks)

        toks2 = clean_name_tokens(
            "висаж вода",
            "НИВЕА ВИСАЖ МИЦ.ВОДА 400М СЕНЗИТИВ550166",
            manufacturer_id=10,
        )
        self.assertIn("сензитив", toks2)

        toks3 = clean_name_tokens(
            "битолско јогурт 2.8%",
            "Јогурт Битолско 2.8% 1кг",
            manufacturer_id=42,
            mfr_own_tokens={42: {"бимилк", "битолско"}},
        )
        self.assertEqual(toks3, ["јогурт", "2.8%"])

    def test_variant_allowlist_keeps_model_codes_drops_erp_codes(self):
        erp_sig = extract_raw_safety_sig("НИВЕА ВОДА 550166")
        self.assertNotIn("code", clean_variant_allowlist({"code": "|550166", "note": "|abc"}, erp_sig))

        pasta_sig = extract_raw_safety_sig("Макарони Divella 500гр no.48")
        self.assertEqual(clean_variant_allowlist({"code": "|48"}, pasta_sig).get("code"), "|48")

        shoe_sig = extract_raw_safety_sig("ПАТИКИ С126-6")
        self.assertEqual(clean_variant_allowlist({"code": "|с1266"}, shoe_sig).get("code"), "|с1266")

    def test_raw_safety_signature_blocks_battery_spf_pct_age(self):
        self.assertFalse(safety_sigs_compatible(
            extract_raw_safety_sig("Варта батерија 4/1 АА"),
            extract_raw_safety_sig("Варта батерија 4/1 ААА"),
        ))
        self.assertFalse(safety_sigs_compatible(
            extract_raw_safety_sig("Амбре солер масло Ф30 150мл"),
            extract_raw_safety_sig("Амбре солер масло Ф15 150мл"),
        ))
        self.assertFalse(safety_sigs_compatible(
            extract_raw_safety_sig("Јогурт грчки Кри Кри 150гр 0%автентичен"),
            extract_raw_safety_sig("Јогурт грчки Кри Кри 150гр 2%автентичен"),
        ))
        self.assertFalse(safety_sigs_compatible(
            extract_raw_safety_sig("Крема Нивеа 45+ 50мл"),
            extract_raw_safety_sig("Крема Нивеа 65+ 50мл"),
        ))
        # Comma vs dot fat % is compatible!
        self.assertTrue(safety_sigs_compatible(
            extract_raw_safety_sig("Јогурт Дукат 2.8% 330g"),
            extract_raw_safety_sig("Јогурт Дукат 2,8% 330гр"),
        ))


class Layers2To4MatcherTests(unittest.TestCase):
    def test_phonetic_and_prefix_alignment(self):
        self.assertEqual(consonant_skeleton("алпен"), consonant_skeleton("алпин"))
        self.assertEqual(consonant_skeleton("алпине"), consonant_skeleton("алпин"))
        self.assertTrue(tokens_align("алпен", "алпине"))
        self.assertTrue(tokens_align("брајтенинг", "брајтинг"))
        self.assertTrue(tokens_align("каптаин", "кептан"))
        self.assertTrue(tokens_align("чок", "чоколадо"))
        self.assertTrue(tokens_align("детер", "детергент"))
        self.assertTrue(tokens_align("лешник", "лешници"))
        # Hard negatives never align
        self.assertFalse(tokens_align("аа", "ааа"))
        self.assertFalse(tokens_align("бонбони", "бонбониера"))
        self.assertFalse(tokens_align("бело", "темно"))

    def test_merges_dropped_generic_noun_but_blocks_same_market_protein_variant(self):
        # Vero sells BOTH regular granola (45) and protein granola (66).
        # Spar sells regular granola with extra generic word "мусли" (45).
        r_vero_reg = _row(1, "vero", "ГРАНОЛА ВИТАЛИА КОКОС БАДЕМ 60Г", "бадем гранола кокос", price=45)
        r_vero_pro = _row(2, "vero", "ГРАНОЛА ВИТАЛИА ПРОТЕИН КОКОС БАДЕМ 60Г", "бадем гранола кокос протеин", price=66)
        r_spar_reg = _row(3, "spar", "МУСЛИ ВИТАЛИА 60ГР ГРАНОЛА КОКОС БАДЕМ", "бадем гранола кокос мусли", price=45)

        comps, _ = smart_merge_block([[r_vero_reg], [r_vero_pro], [r_spar_reg]], brand_idf={})
        groups_by_id = [sorted(r["id"] for r in c) for c in comps]
        self.assertIn(sorted([r_vero_reg["id"], r_spar_reg["id"]]), groups_by_id)
        self.assertIn([r_vero_pro["id"]], groups_by_id)

    def test_mnn_merges_bar_and_plochka_at_same_price(self):
        r_ram = _row(1, "ramstore", "ВИТАЛИА БАР СПОРТ АНАНАС БЕЛО ЧОКОЛАДО 60Г", "ананас бар бело спорт чоколадо", price=105)
        r_ver = _row(2, "vero", "ПЛОЧКА ВИТАЛИА СПОРТ АНАНАС БЕЛО ЧОКОЛАДО 60Г", "ананас бело плочка спорт чоколадо", price=105)
        comps, merges = smart_merge_block([[r_ram], [r_ver]], brand_idf={})
        self.assertEqual(len(comps), 1)
        self.assertEqual(len(merges), 1)

    def test_blocks_all_core_hard_negatives(self):
        pairs = [
            (_row(1, "vero", "Формула 1 400г", "млечна формула 1", price=500),
             _row(2, "zito", "Формула 2 400г", "млечна формула 2", price=500)),
            (_row(3, "vero", "Милка лешник 250г", "чоколадо лешник", price=200),
             _row(4, "zito", "Милка цел лешник 250г", "чоколадо цел лешник", price=200)),
            (_row(5, "vero", "Ариел прашок 2кг", "детергент прашок", price=400),
             _row(6, "zito", "Ариел гел 2л", "детергент гел", price=400)),
            (_row(7, "vero", "Милка бело чоколадо 100г", "бело чоколадо", price=90),
             _row(8, "zito", "Милка темно чоколадо 100г", "темно чоколадо", price=90)),
            (_row(9, "vero", "Бонбони лешник 200г", "бонбони лешник", price=180),
             _row(10, "zito", "Бонбониера лешник 200г", "бонбониера лешник", price=180)),
        ]
        for a, b in pairs:
            with self.subTest(a=a["name"], b=b["name"]):
                comps, merges = smart_merge_block([[a], [b]], brand_idf={})
                self.assertEqual(len(comps), 2)
                self.assertEqual(len(merges), 0)


class Layer5EmbeddingGuardTests(unittest.TestCase):
    def _unit(self, vals: list[float]) -> np.ndarray:
        v = np.asarray(vals, dtype=np.float32)
        return v / np.linalg.norm(v)

    def test_merges_bilingual_pair_in_same_subcategory(self):
        a = _row(1, "bigshop", "Pepsi 330ml Lime", "сок лиме", price=35,
                 main_cat="Безалкохолни пијалоци", sub_cat="Газирани сокови", product_name="pepsi lime")
        b = _row(2, "vero", "Пепси Лимета 0.33л", "пијалок лимета", price=34,
                 main_cat="Безалкохолни пијалоци", sub_cat="Газирани сокови", product_name="пепси лимета")
        vecs = {
            "pepsi lime": self._unit([1.0, 0.0, 0.0]),
            "пепси лимета": self._unit([0.96, 0.28, 0.0]),  # cos = 0.96
        }
        comps, merges = layer5_merge_block([[a], [b]], vecs, cos_thresh=0.72, max_price_ratio=1.80)
        self.assertEqual(len(comps), 1)
        self.assertEqual(len(merges), 1)

    def test_blocks_different_subcategory_even_at_high_cosine(self):
        a = _row(1, "vero", "Кафе Агроплод 100г", "кафе", price=115,
                 main_cat="Топли пијалоци", sub_cat="Кафе", product_name="кафе")
        b = _row(2, "zur", "Какао Агроплод 100г", "какао", price=89,
                 main_cat="Топли пијалоци", sub_cat="Какао", product_name="какао")
        vecs = {"кафе": self._unit([1.0, 0.0]), "какао": self._unit([0.99, 0.14])}
        comps, merges = layer5_merge_block([[a], [b]], vecs, cos_thresh=0.72, max_price_ratio=1.80)
        self.assertEqual(len(comps), 2)
        self.assertEqual(len(merges), 0)

    def test_skips_non_food_categories(self):
        a = _row(1, "ramstore", "Нурди женски хеланки", "женски хеланки", price=269,
                 main_cat="Облека", sub_cat="Чорапи и долна облека", product_name="женски хеланки")
        b = _row(2, "stokomak", "Женски чорапи Нурди", "женски чорапи", price=269,
                 main_cat="Облека", sub_cat="Чорапи и долна облека", product_name="женски чорапи")
        vecs = {"женски хеланки": self._unit([1.0, 0.0]), "женски чорапи": self._unit([0.99, 0.14])}
        comps, merges = layer5_merge_block([[a], [b]], vecs, cos_thresh=0.72, max_price_ratio=1.80)
        self.assertEqual(len(comps), 2)
        self.assertEqual(len(merges), 0)

    def test_respects_hard_vetoes_and_price_ratio(self):
        a = _row(1, "vero", "Милка лешник 250г", "лешник", price=200, product_name="лешник")
        b = _row(2, "zito", "Милка цел лешник 250г", "цел лешник", price=200, product_name="цел лешник")
        vecs = {"лешник": self._unit([1.0, 0.0]), "цел лешник": self._unit([0.99, 0.14])}
        comps, merges = layer5_merge_block([[a], [b]], vecs, cos_thresh=0.72, max_price_ratio=1.80)
        self.assertEqual(len(comps), 2)
        self.assertEqual(len(merges), 0)

        # Price ratio > 1.80 blocked
        c = _row(3, "vero", "Сок портокал 1л", "портокал", price=60,
                 main_cat="Безалкохолни пијалоци", sub_cat="Негазирани сокови", product_name="портокал")
        d = _row(4, "zito", "Сок orange 1л", "оранж", price=130,
                 main_cat="Безалкохолни пијалоци", sub_cat="Негазирани сокови", product_name="оранж")
        vecs2 = {"портокал": self._unit([1.0, 0.0]), "оранж": self._unit([0.99, 0.14])}
        comps2, merges2 = layer5_merge_block([[c], [d]], vecs2, cos_thresh=0.72, max_price_ratio=1.80)
        self.assertEqual(len(comps2), 2)
        self.assertEqual(len(merges2), 0)


class DisplayTitleTests(unittest.TestCase):
    def test_prefers_reptil_then_bigshop(self):
        rows = [
            _row(1, "zur", "ИНТЕСА?!!!ЛАК ЗА КОСА 300МЛ", "лак за коса", product_name="лак за коса"),
            _row(2, "bigshop", "Лак за коса Intesa 300мл", "лак за коса", product_name="лак за коса"),
            _row(3, "reptil", "Intesa Styling – Лак за коса 300ml", "лак за коса", product_name="лак за коса"),
        ]
        self.assertEqual(
            choose_display_name(rows, manufacturer_id=1, mfr_display_names={1: "Intesa"}),
            "Intesa Styling – Лак за коса 300ml",
        )
        # Without reptil -> picks bigshop
        self.assertEqual(
            choose_display_name(rows[:2], manufacturer_id=1, mfr_display_names={1: "Intesa"}),
            "Лак за коса Intesa 300мл",
        )

    def test_synthesizes_latin_unit_title_when_no_reptil_or_bigshop(self):
        r1 = _row(1, "zur", "ПОДРАВКА? СУПА ГОВЕДСКА 65Г", "супа говедска", product_name="супа говедска со тестенини")
        r1["quantity_value"], r1["quantity_unit"] = 65.000, "g"
        r2 = _row(2, "vero", "СУПА ПОДРАВКА ГОВЕДСКА 65Г", "супа говедска", product_name="супа говедска со тестенини")
        r2["quantity_value"], r2["quantity_unit"] = 65.000, "g"
        self.assertEqual(
            choose_display_name([r1, r2], manufacturer_id=5, mfr_display_names={5: "Подравка"}),
            "Подравка – Супа говедска со тестенини 65g",
        )

    def test_fallback_prefers_clean_raw_name_and_strips_elida_code(self):
        r1 = _row(1, "zur", "ЦИФ?! ЛИМОН 500МЛ", "лимон", product_name="")
        r2 = _row(2, "elida", "Циф лимон 500мл   12#", "лимон", product_name="")
        self.assertEqual(choose_display_name([r1, r2]), "Циф лимон 500мл")


if __name__ == "__main__":
    unittest.main()
