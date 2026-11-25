"""Unit & integration tests for pipeline safety guards:

1. `resolve_manufacturer_id` follows `manufacturers.merged_into` chains so new
   products never get tagged with an absorbed manufacturer ID.
2. `prepare_rows_layer1` derives missing `product_name_key`/`product_name_sorted`
   on the fly instead of silently dropping extracted rows to ungrouped singletons.
3. `run_pipeline` continues when `run_scrapers` reports partial scraper failures
   (`PARTIAL_FAILURE_EXIT_CODE`) and still aborts on total failure (`1`).
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from backend.data.db_utils import connect_to_db
from backend.data.extract_features import resolve_manufacturer_id
from backend.data.group_products import prepare_rows_layer1
from backend.data.run_scrapers import PARTIAL_FAILURE_EXIT_CODE
from backend.data import run_pipeline


class MergedManufacturerResolutionTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect_to_db()
        self.conn.autocommit = False

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def test_follows_multi_hop_merged_into_and_heals_alias(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO manufacturers (canonical_name, display_name) VALUES (%s, %s) RETURNING id",
                ("тест преживеан бренд 991", "Surviving Brand"),
            )
            c_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO manufacturers (canonical_name, display_name, merged_into) VALUES (%s, %s, %s) RETURNING id",
                ("тест среден бренд 991", "Mid Brand", c_id),
            )
            b_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO manufacturers (canonical_name, display_name, merged_into) VALUES (%s, %s, %s) RETURNING id",
                ("тест апсорбиран бренд 991", "Absorbed Brand", b_id),
            )
            a_id = cur.fetchone()[0]
            # Stale alias pointing at the mid absorbed row
            cur.execute(
                "INSERT INTO manufacturer_aliases (alias, manufacturer_id) VALUES (%s, %s)",
                ("тест стар алијас 991", b_id),
            )

        # Lookup via absorbed canonical_name (not yet in manufacturer_aliases) must land on C
        resolved_by_name = resolve_manufacturer_id(self.conn, "тест апсорбиран бренд 991")
        self.assertEqual(resolved_by_name, c_id)
        self.assertNotEqual(resolved_by_name, a_id)

        # Lookup via stale alias pointing at B must land on C and self-heal the alias row
        resolved_by_alias = resolve_manufacturer_id(self.conn, "тест стар алијас 991")
        self.assertEqual(resolved_by_alias, c_id)
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT manufacturer_id FROM manufacturer_aliases WHERE alias = %s",
                ("тест стар алијас 991",),
            )
            self.assertEqual(cur.fetchone()[0], c_id)


class Layer1MissingKeyFallbackTests(unittest.TestCase):
    def test_derives_keys_on_the_fly_when_product_name_sorted_is_null(self):
        featured = [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "market": "reptil",
                "manufacturer_id": 42,
                "product_name": "Трајно млеко",
                "product_name_key": None,
                "product_name_sorted": None,
                "quantity_value": 1,
                "quantity_unit": "l",
                "is_fluid": True,
                "variant": None,
                "metadata": {"pack_count": "6"},
                "price": 79,
                "singular_price": None,
                "in_stock": True,
                "main_category": "Млечни производи",
                "sub_category": "Млеко",
                "name": "Бимилк Трајно млеко 3.2% 6x1L",
            }
        ]
        rows, singletons, _ = prepare_rows_layer1(featured, {}, {})
        self.assertEqual(len(singletons), 0)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["product_name_sorted"])
        self.assertEqual(rows[0]["variant"].get("pack_count"), "|6")


class PipelinePartialScraperTests(unittest.TestCase):
    def test_pipeline_continues_on_partial_scraper_failure(self):
        called = []

        def fake_run(cmd, cwd=None, env=None):
            script_name = cmd[1].rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
            called.append(script_name)
            rc = PARTIAL_FAILURE_EXIT_CODE if script_name == "run_scrapers.py" else 0
            return type("Res", (), {"returncode": rc})()

        with patch.object(run_pipeline.subprocess, "run", side_effect=fake_run):
            rc = run_pipeline.main([])

        self.assertEqual(rc, 0)
        self.assertEqual(
            called,
            ["run_scrapers.py", "categorize_products.py", "extract_features.py", "group_products.py"],
        )

    def test_pipeline_aborts_when_all_scrapers_fail(self):
        called = []

        def fake_run(cmd, cwd=None, env=None):
            script_name = cmd[1].rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
            called.append(script_name)
            return type("Res", (), {"returncode": 1})()

        with patch.object(run_pipeline.subprocess, "run", side_effect=fake_run):
            rc = run_pipeline.main([])

        self.assertEqual(rc, 1)
        self.assertEqual(called, ["run_scrapers.py"])


class ScraperFreshnessSkipTests(unittest.TestCase):
    def test_skips_fetch_when_source_marker_matches_stored_state(self):
        from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

        fetch_called = False

        class FakeFreshScraper(MarketScraper):
            market = "__test_fresh_market__"

            def get_source_marker(self):
                return "2026-11-25 10:00:00"

            def fetch(self):
                nonlocal fetch_called
                fetch_called = True
                return [ScrapedProduct(name="Test", price=50)]

        fake_conn = type("Conn", (), {"closed": False, "close": lambda self: None})()
        with (
            patch("backend.data.scrapers.scraper_base.connect_to_db", return_value=fake_conn),
            patch(
                "backend.data.scrapers.scraper_base.get_scraper_state",
                return_value={"last_source_marker": "2026-11-25 10:00:00"},
            ),
        ):
            rc = FakeFreshScraper().run()

        self.assertEqual(rc, 0)
        self.assertFalse(fetch_called)

    def test_force_bypasses_matching_source_marker(self):
        from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

        fetch_called = False

        class FakeForceScraper(MarketScraper):
            market = "__test_force_market__"

            def get_source_marker(self):
                return "2026-11-25 10:00:00"

            def fetch(self):
                nonlocal fetch_called
                fetch_called = True
                return [ScrapedProduct(name="Test", price=50)]

        fake_conn = type("Conn", (), {"closed": False, "close": lambda self: None})()
        with (
            patch("backend.data.scrapers.scraper_base.connect_to_db", return_value=fake_conn),
            patch("backend.data.scrapers.scraper_base.get_products_by_market", return_value={}),
            patch("backend.data.scrapers.scraper_base.save_products_to_products_table") as mock_save,
            patch("backend.data.scrapers.scraper_base.upsert_scraper_state") as mock_state,
        ):
            rc = FakeForceScraper().run(force=True)

        self.assertEqual(rc, 0)
        self.assertTrue(fetch_called)
        mock_save.assert_called_once()
        mock_state.assert_called_once()

    def test_fails_open_when_source_marker_raises(self):
        from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

        fetch_called = False

        class FakeBrokenMarkerScraper(MarketScraper):
            market = "__test_broken_marker__"

            def get_source_marker(self):
                raise RuntimeError("network hiccup")

            def fetch(self):
                nonlocal fetch_called
                fetch_called = True
                return [ScrapedProduct(name="Test", price=50)]

        fake_conn = type("Conn", (), {"closed": False, "close": lambda self: None})()
        with (
            patch("backend.data.scrapers.scraper_base.connect_to_db", return_value=fake_conn),
            patch("backend.data.scrapers.scraper_base.get_products_by_market", return_value={}),
            patch("backend.data.scrapers.scraper_base.save_products_to_products_table"),
            patch("backend.data.scrapers.scraper_base.upsert_scraper_state"),
        ):
            rc = FakeBrokenMarkerScraper().run()

        self.assertEqual(rc, 0)
        self.assertTrue(fetch_called)


if __name__ == "__main__":
    unittest.main()
