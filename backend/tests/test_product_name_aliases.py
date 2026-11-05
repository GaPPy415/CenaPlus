import unittest

from backend.data.product_name_aliases import propose_tokens, token_pair_ok


class TokenPairGuardTests(unittest.TestCase):
    def test_accepts_brand_spelling_variants(self):
        for a, b in [
            ("алпен", "алпин"),
            ("алпине", "алпин"),
            ("чокол", "чоколадо"),
            ("чоколада", "чоколадо"),
            ("ноисетте", "ноисет"),
            ("хазалнут", "хазелнут"),
            ("расбери", "росбери"),
            ("чискејк", "чизкејк"),
            ("лешници", "лешник"),
        ]:
            with self.subTest(a=a, b=b):
                self.assertTrue(token_pair_ok(a, b))

    def test_rejects_unrelated_words(self):
        for a, b in [
            ("малина", "алпин"),
            ("кејк", "кекс"),
            ("млеко", "млечно"),
            ("барни", "брауни"),
            ("бани", "брауни"),
            ("дабл", "бабл"),
            ("снек", "снешко"),
            ("фловер", "лове"),
            ("спаче", "стапче"),
            ("бонбони", "бонбониера"),
        ]:
            with self.subTest(a=a, b=b):
                self.assertFalse(token_pair_ok(a, b))

    def test_rejects_numeric_and_short_tokens(self):
        self.assertFalse(token_pair_ok("формула", "формула"))
        self.assertFalse(token_pair_ok("аа", "ааа"))
        self.assertFalse(token_pair_ok("0%", "10%"))
        self.assertFalse(token_pair_ok("250", "500"))

    def test_never_aliases_a_token_to_itself(self):
        self.assertFalse(token_pair_ok("алпин", "алпин"))


class ProposalTests(unittest.TestCase):
    def test_prefers_the_most_frequent_spelling_as_canonical(self):
        keys = [
            {"product_name_key": "чоколадо алпин милк", "rows": 10, "markets": 4},
            {"product_name_key": "чоколадо алпен милк", "rows": 2, "markets": 1},
            {"product_name_key": "чоколадо алпине милк", "rows": 3, "markets": 1},
        ]
        proposals = {p["alias_token"]: p["canonical_token"] for p in propose_tokens(keys)}
        self.assertEqual(proposals, {"алпен": "алпин", "алпине": "алпин"})

    def test_does_not_merge_different_flavours(self):
        keys = [
            {"product_name_key": "чоколадо алпин јогурт", "rows": 5, "markets": 2},
            {"product_name_key": "чоколадо алпин милк", "rows": 5, "markets": 2},
            {"product_name_key": "чоколадо алпин малина", "rows": 1, "markets": 1},
        ]
        proposals = {p["alias_token"]: p["canonical_token"] for p in propose_tokens(keys)}
        self.assertEqual(proposals, {})


if __name__ == "__main__":
    unittest.main()
