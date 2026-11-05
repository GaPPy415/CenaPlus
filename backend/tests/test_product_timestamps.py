"""Regression tests for the products timestamp semantics.

``etl_loadtime`` must be written once (first load) and ``last_updated`` must only
move when a scraped field actually changes. Both used to be overwritten with the
same ``datetime.now()`` on every run.

Requires the Postgres instance from ``.env``; skipped when it is unreachable.

    python -m unittest discover -s backend/tests -t .
"""

import unittest
import uuid
from datetime import datetime

from backend.data.db_utils import bulk_upsert_products_table, connect_to_db

TABLE = "_tmp_products_timestamps_test"
OLD_TS = datetime(2026, 1, 1, 8, 0, 0)
UNCHANGED_TS = datetime(2026, 1, 2, 8, 0, 0)
CHANGED_TS = datetime(2026, 1, 3, 8, 0, 0)

COLUMNS = [
    "id", "name", "price", "singular_price", "description", "in_stock",
    "image", "link", "market", "ETL_loadtime", "last_updated",
]


def _conn_or_none():
    try:
        return connect_to_db()
    except Exception:
        return None


class TimestampTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = _conn_or_none()
        if cls.conn is None:
            raise unittest.SkipTest("PostgreSQL is not reachable")
        cls.cursor = cls.conn.cursor()
        cls.cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cls.cursor.execute(
            f"""
            CREATE TEMP TABLE {TABLE} (
                id             uuid PRIMARY KEY,
                name           text,
                price          integer,
                singular_price text,
                description    text,
                in_stock       boolean,
                image          text,
                link           text,
                market         text,
                etl_loadtime   timestamp,
                last_updated   timestamp
            )
            """
        )
        cls.conn.commit()
        cls.row_id = str(uuid.uuid4())

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "conn", None) is not None:
            cls.cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
            cls.conn.commit()
            cls.conn.close()

    # ---------------------------------------------------------------- helpers
    def _payload(self, **overrides):
        row = {
            "id": self.row_id,
            "name": "Млеко 1л",
            "price": 50,
            "singular_price": "50 ден/л",
            "description": "Млечни производи",
            "in_stock": True,
            "image": None,
            "link": None,
            "market": "test_market",
            "ETL_loadtime": OLD_TS,
            "last_updated": OLD_TS,
        }
        row.update(overrides)
        return row

    def _fetch(self):
        self.cursor.execute(
            f"SELECT etl_loadtime, last_updated, price FROM {TABLE} WHERE id = %s",
            (self.row_id,),
        )
        return self.cursor.fetchone()

    def _reset(self):
        self.cursor.execute(f"DELETE FROM {TABLE}")
        self.conn.commit()

    # ------------------------------------------------------------------ tests
    def test_new_row_starts_with_equal_timestamps(self):
        self._reset()
        bulk_upsert_products_table(self.conn, [self._payload()], table=TABLE)
        etl, updated, _ = self._fetch()
        self.assertEqual(etl, OLD_TS)
        self.assertEqual(updated, OLD_TS)

    def test_rescrape_without_changes_keeps_both_timestamps(self):
        self._reset()
        bulk_upsert_products_table(self.conn, [self._payload()], table=TABLE)
        bulk_upsert_products_table(
            self.conn,
            [
                self._payload(
                    ETL_loadtime=UNCHANGED_TS,
                    last_updated=UNCHANGED_TS,
                )
            ],
            table=TABLE,
        )
        etl, updated, _ = self._fetch()
        self.assertEqual(etl, OLD_TS, "etl_loadtime must never be rewritten")
        self.assertEqual(
            updated, OLD_TS, "last_updated must not move when nothing changed"
        )

    def test_price_change_bumps_last_updated_only(self):
        self._reset()
        bulk_upsert_products_table(self.conn, [self._payload()], table=TABLE)
        bulk_upsert_products_table(
            self.conn,
            [
                self._payload(
                    price=60,
                    ETL_loadtime=CHANGED_TS,
                    last_updated=CHANGED_TS,
                )
            ],
            table=TABLE,
        )
        etl, updated, price = self._fetch()
        self.assertEqual(etl, OLD_TS)
        self.assertEqual(updated, CHANGED_TS)
        self.assertEqual(price, 60)

    def test_stock_flip_counts_as_a_change(self):
        self._reset()
        bulk_upsert_products_table(self.conn, [self._payload()], table=TABLE)
        bulk_upsert_products_table(
            self.conn,
            [
                self._payload(
                    in_stock=False,
                    ETL_loadtime=CHANGED_TS,
                    last_updated=CHANGED_TS,
                )
            ],
            table=TABLE,
        )
        self.assertEqual(self._fetch()[1], CHANGED_TS)

    def test_noop_upsert_does_not_write_a_new_row_version(self):
        """The conflict update is skipped entirely when nothing changed."""
        self._reset()
        bulk_upsert_products_table(self.conn, [self._payload()], table=TABLE)
        self.cursor.execute(f"SELECT xmin FROM {TABLE} WHERE id = %s", (self.row_id,))
        xmin_before = self.cursor.fetchone()[0]

        bulk_upsert_products_table(
            self.conn,
            [self._payload(ETL_loadtime=UNCHANGED_TS, last_updated=UNCHANGED_TS)],
            table=TABLE,
        )
        self.cursor.execute(f"SELECT xmin FROM {TABLE} WHERE id = %s", (self.row_id,))
        self.assertEqual(self.cursor.fetchone()[0], xmin_before)

    def test_changed_upsert_does_write(self):
        self._reset()
        bulk_upsert_products_table(self.conn, [self._payload()], table=TABLE)
        self.cursor.execute(f"SELECT xmin FROM {TABLE} WHERE id = %s", (self.row_id,))
        xmin_before = self.cursor.fetchone()[0]

        bulk_upsert_products_table(
            self.conn,
            [self._payload(price=61, ETL_loadtime=CHANGED_TS, last_updated=CHANGED_TS)],
            table=TABLE,
        )
        self.cursor.execute(f"SELECT xmin, price FROM {TABLE} WHERE id = %s", (self.row_id,))
        xmin_after, price = self.cursor.fetchone()
        self.assertNotEqual(xmin_after, xmin_before)
        self.assertEqual(price, 61)

    def test_invalid_table_name_is_rejected(self):
        with self.assertRaises(ValueError):
            bulk_upsert_products_table(self.conn, [self._payload()], table="products; DROP TABLE x")


class TimestampTriggerTests(unittest.TestCase):
    """The DB trigger must enforce the same rules for direct writes.

    Skipped until ``init/03_product_timestamps.sql`` has been applied.
    """

    @classmethod
    def setUpClass(cls):
        cls.conn = _conn_or_none()
        if cls.conn is None:
            raise unittest.SkipTest("PostgreSQL is not reachable")
        cls.cursor = cls.conn.cursor()
        cls.cursor.execute(
            "SELECT 1 FROM pg_proc WHERE proname = 'products_set_timestamps'"
        )
        if cls.cursor.fetchone() is None:
            cls.conn.close()
            raise unittest.SkipTest("init/03_product_timestamps.sql is not applied")

        cls.table = "_tmp_products_trigger_test"
        cls.cursor.execute(f"DROP TABLE IF EXISTS {cls.table}")
        cls.cursor.execute(
            f"""
            CREATE TEMP TABLE {cls.table} (
                id             uuid PRIMARY KEY,
                name           text,
                price          integer,
                singular_price text,
                description    text,
                in_stock       boolean,
                image          text,
                link           text,
                market         text,
                etl_loadtime   timestamp,
                last_updated   timestamp
            )
            """
        )
        cls.cursor.execute(
            f"""
            CREATE TRIGGER products_set_timestamps_trg
                BEFORE INSERT OR UPDATE ON {cls.table}
                FOR EACH ROW EXECUTE FUNCTION products_set_timestamps()
            """
        )
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "conn", None) is not None:
            cls.cursor.execute(f"DROP TABLE IF EXISTS {cls.table}")
            cls.conn.commit()
            cls.conn.close()

    def setUp(self):
        self.cursor.execute(f"DELETE FROM {self.table}")
        self.conn.commit()
        self.row_id = str(uuid.uuid4())
        self.cursor.execute(
            f"""
            INSERT INTO {self.table}
                (id, name, price, in_stock, market, etl_loadtime, last_updated)
            VALUES (%s, 'Ориз', 40, true, 'test_market', %s, %s)
            """,
            (self.row_id, OLD_TS, OLD_TS),
        )
        self.conn.commit()

    def test_trigger_keeps_first_seen_time_on_update(self):
        self.cursor.execute(
            f"""
            UPDATE {self.table}
            SET price = 45, etl_loadtime = %s, last_updated = %s
            WHERE id = %s
            """,
            (CHANGED_TS, CHANGED_TS, self.row_id),
        )
        self.conn.commit()
        self.cursor.execute(
            f"SELECT etl_loadtime, last_updated FROM {self.table} WHERE id = %s",
            (self.row_id,),
        )
        etl, updated = self.cursor.fetchone()
        self.assertEqual(etl, OLD_TS)
        self.assertGreater(updated, OLD_TS)

    def test_trigger_ignores_noop_updates(self):
        self.cursor.execute(
            f"""
            UPDATE {self.table}
            SET price = 40, etl_loadtime = %s, last_updated = %s
            WHERE id = %s
            """,
            (CHANGED_TS, CHANGED_TS, self.row_id),
        )
        self.conn.commit()
        self.cursor.execute(
            f"SELECT etl_loadtime, last_updated FROM {self.table} WHERE id = %s",
            (self.row_id,),
        )
        etl, updated = self.cursor.fetchone()
        self.assertEqual(etl, OLD_TS)
        self.assertEqual(updated, OLD_TS)

    def test_trigger_invents_a_timestamp_when_writer_forgot_one(self):
        """A change without a new last_updated still gets a fresh timestamp."""
        self.cursor.execute(
            f"UPDATE {self.table} SET price = 41 WHERE id = %s", (self.row_id,)
        )
        self.conn.commit()
        self.cursor.execute(
            f"SELECT etl_loadtime, last_updated FROM {self.table} WHERE id = %s",
            (self.row_id,),
        )
        etl, updated = self.cursor.fetchone()
        self.assertEqual(etl, OLD_TS)
        self.assertGreater(updated, OLD_TS)

    def test_trigger_fills_timestamps_on_insert(self):
        new_id = str(uuid.uuid4())
        self.cursor.execute(
            f"""
            INSERT INTO {self.table} (id, name, price, in_stock, market)
            VALUES (%s, 'Шеќер', 90, true, 'test_market')
            """,
            (new_id,),
        )
        self.conn.commit()
        self.cursor.execute(
            f"SELECT etl_loadtime, last_updated FROM {self.table} WHERE id = %s",
            (new_id,),
        )
        etl, updated = self.cursor.fetchone()
        self.assertIsNotNone(etl)
        self.assertEqual(etl, updated)


if __name__ == "__main__":
    unittest.main()
