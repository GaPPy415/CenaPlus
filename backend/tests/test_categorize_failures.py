"""Categorization must fail loudly, not fabricate ``Разно``/0.0 rows.

No database or network access is needed.

    python -m unittest backend.tests.test_categorize_failures
"""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from backend.data.RateLimiter import RateLimiter
from backend.data.categorize_products import categorize_batch_main_category
from backend.data.db_utils import save_categorizations_to_db

PRODUCTS = [
    {"name": "Млеко 1л", "description": "", "existing_categories": ""},
    {"name": "Леб 500г", "description": "", "existing_categories": ""},
    {"name": "Сир 250г", "description": "", "existing_categories": ""},
]


class FakeClient:
    def __init__(self, respond):
        self.models = SimpleNamespace(generate_content=respond)


class CategorizeFailureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.limiter = RateLimiter(
            1000,
            1_000_000,
            scope="test-categorize",
            state_file=Path(self.tmp.name) / "r.json",
        )
        patcher = mock.patch("backend.data.categorize_products.rate_limiter", self.limiter)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, client):
        return asyncio.run(
            categorize_batch_main_category(PRODUCTS, client, "fake-model", 100)
        )

    def test_api_error_is_raised_instead_of_fake_zero_confidence(self):
        def boom(**_kwargs):
            raise ValueError("schema mismatch")

        with self.assertRaises(ValueError):
            self._run(FakeClient(boom))

    def test_missing_products_are_none_not_default_category(self):
        def respond(**_kwargs):
            return SimpleNamespace(
                text=json.dumps({"products": [{"main_category": "Млеко", "confidence": 0.9}]})
            )

        result = self._run(FakeClient(respond))

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].main_category, "Млеко")
        self.assertIsNone(result[1])
        self.assertIsNone(result[2])

    def test_failed_rows_are_not_written_to_the_database(self):
        class ExplodingConnection:
            def cursor(self):
                raise AssertionError("a failed categorization must not touch the database")

        written = save_categorizations_to_db(
            ExplodingConnection(),
            [{"id": "00000000-0000-0000-0000-000000000000"}],
        )

        self.assertEqual(written, 0)


if __name__ == "__main__":
    unittest.main()
