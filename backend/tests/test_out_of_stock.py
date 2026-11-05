"""Tests for the out-of-stock sweep.

Requires the Postgres instance from ``.env``; skipped when it is unreachable.

    python -m unittest backend.tests.test_out_of_stock
"""

import unittest
import uuid

from backend.data.db_utils import connect_to_db, mark_out_of_stock_products_table

TABLE = "_tmp_oos_test"
MARKET = "test_market"


class OutOfStockTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.conn = connect_to_db()
        except Exception:
            raise unittest.SkipTest("PostgreSQL is not reachable")
        cls.cursor = cls.conn.cursor()
        cls.cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cls.cursor.execute(
            f"""
            CREATE TEMP TABLE {TABLE} (
                id           uuid PRIMARY KEY,
                name         text,
                market       text,
                in_stock     boolean,
                last_updated timestamp
            )
            """
        )
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "conn", None) is not None:
            cls.cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
            cls.conn.commit()
            cls.conn.close()

    def setUp(self):
        self.cursor.execute(f"DELETE FROM {TABLE}")
        self.conn.commit()
        self.ids = {}
        for name in ("Млеко", "Леб", "Сир"):
            self.ids[name] = str(uuid.uuid4())
            self.cursor.execute(
                f"INSERT INTO {TABLE} (id, name, market, in_stock, last_updated) "
                "VALUES (%s, %s, %s, true, '2026-01-01 08:00:00')",
                (self.ids[name], name, MARKET),
            )
        self.conn.commit()

    def _state(self):
        self.cursor.execute(f"SELECT name, in_stock, last_updated FROM {TABLE} ORDER BY name")
        return {name: (stock, ts) for name, stock, ts in self.cursor.fetchall()}

    def test_unseen_products_are_marked_out_of_stock(self):
        marked = mark_out_of_stock_products_table(self.conn, MARKET, {"Млеко", "Леб"}, table=TABLE)

        state = self._state()
        self.assertEqual(marked, 1)
        self.assertFalse(state["Сир"][0])
        self.assertTrue(state["Млеко"][0])
        self.assertTrue(state["Леб"][0])

    def test_seen_products_are_untouched(self):
        before = self._state()
        marked = mark_out_of_stock_products_table(self.conn, MARKET, {"Млеко", "Леб", "Сир"}, table=TABLE)

        self.assertEqual(marked, 0)
        self.assertEqual(self._state(), before)

    def test_rerun_is_idempotent(self):
        mark_out_of_stock_products_table(self.conn, MARKET, {"Млеко"}, table=TABLE)
        first = self._state()
        marked = mark_out_of_stock_products_table(self.conn, MARKET, {"Млеко"}, table=TABLE)

        self.assertEqual(marked, 0, "already out-of-stock rows must not be rewritten")
        self.assertEqual(self._state(), first)

    def test_other_markets_are_not_touched(self):
        self.cursor.execute(
            f"INSERT INTO {TABLE} (id, name, market, in_stock, last_updated) "
            "VALUES (%s, 'Млеко', 'other_market', true, '2026-01-01 08:00:00')",
            (str(uuid.uuid4()),),
        )
        self.conn.commit()

        mark_out_of_stock_products_table(self.conn, MARKET, set(), table=TABLE)

        self.cursor.execute(f"SELECT market, in_stock FROM {TABLE} WHERE market = 'other_market'")
        self.assertEqual(self.cursor.fetchone(), ("other_market", True))

    def test_invalid_table_name_is_rejected(self):
        with self.assertRaises(ValueError):
            mark_out_of_stock_products_table(self.conn, MARKET, set(), table="products; DROP TABLE x")


if __name__ == "__main__":
    unittest.main()
