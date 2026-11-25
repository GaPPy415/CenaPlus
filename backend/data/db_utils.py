"""Database helpers shared by the pipeline stages and the scrapers.

Timestamp invariants (enforced here *and* by ``init/03_product_timestamps.sql``):

* ``etl_loadtime`` — first time a row entered the warehouse. Never rewritten.
* ``last_updated`` — last time a *scraped* field actually changed.

``bulk_upsert_products_table`` implements both: a conflict update only runs when
at least one scraped column differs, so re-scraping unchanged data writes
nothing (no dead tuples, no timestamp movement).

Indexes that keep the pipeline queries off sequential scans live in
``init/04_pipeline_indexes.sql``.
"""

import html
import os
import re
import time
import uuid
from datetime import datetime
from typing import List

import psycopg2
from dotenv import load_dotenv, find_dotenv
from psycopg2.extras import execute_values, RealDictCursor

from backend.data.constants import CATEGORIES

_ETL_LOADTIME = 'ETL_loadtime'
_LAST_UPDATED = 'last_updated'
_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _require_identifier(name: str) -> str:
    """Guard f-string SQL that interpolates a table name."""
    if not _IDENTIFIER_RE.match(name or ''):
        raise ValueError(f'Invalid SQL identifier: {name!r}')
    return name


def connect_to_db() -> psycopg2.extensions.connection:
    load_dotenv(find_dotenv())
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password")
    )
    print(f"Connected to PostgreSQL at {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}, db '{os.getenv('POSTGRES_DB')}'")
    return conn


# --------------------------------------------------------------------------- #
# Scraper write path
# --------------------------------------------------------------------------- #

def bulk_upsert_products_table(
    conn: psycopg2.extensions.connection,
    products: list,
    table: str = 'products',
):
    """Insert new products and update existing rows only when data changed.

    Columns are taken from the first row. ``id`` and the two timestamp columns
    are never copied blindly:

    * ``ETL_loadtime`` is left out of the ``DO UPDATE`` set, so the stored value
      survives.
    * ``last_updated`` is only assigned when the ``WHERE`` row-comparison finds a
      difference, which also skips the write entirely for a no-op re-scrape.

    ``table`` exists so tests can target a scratch table; it is an identifier,
    never user input, and is validated anyway.
    """
    if not products:
        return
    _require_identifier(table)

    columns = list(products[0].keys())
    updatable = [
        col for col in columns
        if col not in ('id', _ETL_LOADTIME, _LAST_UPDATED)
    ]

    if not updatable:
        conflict_clause = 'DO NOTHING'
    else:
        assignments = [f'{col} = EXCLUDED.{col}' for col in updatable]
        if _LAST_UPDATED in columns:
            assignments.append(f'{_LAST_UPDATED} = EXCLUDED.{_LAST_UPDATED}')

        # Row comparison is NULL-safe and treats a missing optional field the
        # same as an unchanged one.
        target_row = ', '.join(f'target.{col}' for col in updatable)
        excluded_row = ', '.join(f'EXCLUDED.{col}' for col in updatable)
        conflict_clause = (
            f'DO UPDATE SET {", ".join(assignments)} '
            f'WHERE ({target_row}) IS DISTINCT FROM ({excluded_row})'
        )

    insert_sql = f"""
        INSERT INTO {table} AS target ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (id) {conflict_clause}
    """
    data = [tuple(prod.get(col) for col in columns) for prod in products]

    cursor = conn.cursor()
    execute_values(cursor, insert_sql, data, page_size=5000)
    conn.commit()
    cursor.close()


def mark_out_of_stock_products_table(
    conn: psycopg2.extensions.connection,
    market: str,
    product_names: set,
    table: str = 'products',
) -> int:
    """Flip ``in_stock`` for this market's rows that the latest scrape did not see.

    Returning the number of flipped rows lets callers log a real number instead
    of guessing. Rows already out of stock are skipped, so the statement is
    idempotent.
    """
    _require_identifier(table)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT id, name FROM {table} WHERE market = %s AND in_stock = true",
        (market,),
    )
    ids_to_update = [row[0] for row in cursor.fetchall() if row[1] not in product_names]

    if ids_to_update:
        cursor.execute(
            f"UPDATE {table} SET in_stock = false, last_updated = %s "
            f"WHERE id = ANY(%s::uuid[])",
            (datetime.now(), ids_to_update),
        )
    conn.commit()
    cursor.close()
    return len(ids_to_update)


READ_MODEL_MATVIEWS = ("grouped_products", "manufacturer_stats")


def refresh_read_views(
    conn: psycopg2.extensions.connection,
    *,
    concurrently: bool = True,
) -> list[str]:
    """Refresh materialized read-model views used by the API.

    Uses ``CONCURRENTLY`` when a view is already populated so live readers never
    block. Missing views (e.g. minimal test schemas) are skipped cleanly.
    """
    refreshed: list[str] = []
    if not conn.autocommit:
        conn.commit()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT matviewname, ispopulated
            FROM pg_matviews
            WHERE schemaname = 'public' AND matviewname = ANY(%s)
            """,
            (list(READ_MODEL_MATVIEWS),),
        )
        present = dict(cursor.fetchall())

        for view_name in READ_MODEL_MATVIEWS:
            if view_name not in present:
                continue
            start = time.perf_counter()
            mode = "CONCURRENTLY " if (concurrently and present[view_name]) else ""
            cursor.execute(f"REFRESH MATERIALIZED VIEW {mode}public.{view_name}")
            cursor.execute(f"ANALYZE public.{view_name}")
            if not conn.autocommit:
                conn.commit()
            refreshed.append(view_name)
            print(
                f"Refreshed materialized view {view_name} "
                f"({mode.strip().lower() or 'blocking'}) in {time.perf_counter() - start:.2f}s"
            )
    return refreshed


def get_scraper_state(conn: psycopg2.extensions.connection, market: str) -> dict | None:
    """Return ``{"last_source_marker", "last_scraped_at", "products_count"}`` for a market, or None."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT to_regclass('public.scraper_state') AS tbl")
        if cur.fetchone()["tbl"] is None:
            return None
        cur.execute(
            """
            SELECT last_source_marker, last_scraped_at, products_count
            FROM public.scraper_state
            WHERE market = %s
            """,
            (market,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def upsert_scraper_state(
    conn: psycopg2.extensions.connection,
    market: str,
    source_marker: str | None,
    products_count: int,
    scraped_at: datetime | None = None,
) -> None:
    """Record a completed market scrape and its source freshness marker."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.scraper_state')")
        if cur.fetchone()[0] is None:
            return
        cur.execute(
            """
            INSERT INTO public.scraper_state (market, last_source_marker, last_scraped_at, products_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (market) DO UPDATE SET
                last_source_marker = EXCLUDED.last_source_marker,
                last_scraped_at    = EXCLUDED.last_scraped_at,
                products_count     = EXCLUDED.products_count
            """,
            (market, source_marker, scraped_at or datetime.now(), products_count),
        )
    if not conn.autocommit:
        conn.commit()


def save_products_to_products_table(
    conn: psycopg2.extensions.connection,
    market: str,
    products_to_upsert: list,
    all_product_names: set,
    *,
    close: bool = True,
):
    """Upsert a market's products, then mark the leftovers out of stock.

    Closes ``conn`` when ``close=True`` (scrapers run one market per process).
    """
    save_start = time.time()

    # The upsert keys on names, which are stored HTML-unescaped.
    for prod in products_to_upsert:
        prod['name'] = html.unescape(prod['name'])
    names = {html.unescape(name) for name in all_product_names}

    if products_to_upsert:
        bulk_upsert_products_table(conn, products_to_upsert)

    print(f"Saved to PostgreSQL in {round(time.time() - save_start, 2)}s")
    mark_start = time.time()
    marked = mark_out_of_stock_products_table(conn, market, names)
    print(f"Marked {marked} products out of stock in {round(time.time() - mark_start, 2)}s")
    if os.getenv("CENAPLUS_SKIP_VIEW_REFRESH") != "1":
        refresh_read_views(conn)
    if close:
        conn.close()


def get_products_by_market(conn: psycopg2.extensions.connection, market: str) -> dict:
    """``{(name, market): id}`` for every row of a market.

    The scrapers use it to find an existing row's UUID so a re-scrape updates in
    place instead of inserting a duplicate (there is no unique key on
    ``(market, name)``).
    """
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM products WHERE market = %s", (market,))
    result = {(name, market): pk for pk, name in cursor.fetchall()}
    cursor.close()
    return result


# --------------------------------------------------------------------------- #
# Categorization stage
# --------------------------------------------------------------------------- #

def _categorize_row(row: dict) -> dict:
    description = row['description'] or ''
    return {
        'id': row['id'],
        'name': row['name'],
        'description': description,
        'existing_categories': description,
        'market': row['market'],
    }


def load_products_to_categorize(conn: psycopg2.extensions.connection, limit: int = None) -> List[dict]:
    """Products the categorizer should look at.

    Two groups: rows never categorized (or categorized below 0.5 confidence),
    and rows whose stored ``(main_category, sub_category)`` no longer exists in
    the taxonomy. The taxonomy check runs in SQL so the table never has to be
    read into Python just to be validated.
    """
    products = []
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    pending_sql = """
        SELECT id, name, description, market
        FROM products
        WHERE main_category IS NULL
           OR confidence IS NULL
           OR confidence < 0.5
    """
    params: tuple = ()
    if limit:
        pending_sql += " LIMIT %s"
        params = (limit,)
    cursor.execute(pending_sql, params)
    products.extend(_categorize_row(row) for row in cursor.fetchall())

    # Equal-length arrays so unnest() yields (main, sub) pairs.
    pairs = [(main, sub) for main, subs in CATEGORIES.items() for sub in subs]
    main_categories = [main for main, _ in pairs]
    sub_categories = [sub for _, sub in pairs]

    cursor.execute(
        """
        SELECT p.id, p.name, p.description, p.market
        FROM products p
        WHERE p.main_category IS NOT NULL
          AND p.confidence IS NOT NULL
          AND p.confidence >= 0.5
          AND NOT EXISTS (
              SELECT 1
              FROM unnest(%s::text[], %s::text[]) AS valid(main_cat, sub_cat)
              WHERE valid.main_cat = p.main_category
                AND valid.sub_cat = p.sub_category
          )
        """,
        (main_categories, sub_categories),
    )
    products.extend(_categorize_row(row) for row in cursor.fetchall())

    cursor.close()
    print(f"📊 Total products to categorize: {len(products)}")
    return products


_UPDATE_BATCH_SQL = """
    UPDATE products AS p
    SET {set_clause}
    FROM (VALUES %s) AS v({value_columns})
    WHERE p.id = v.id::uuid
"""


def batch_update_products(conn: psycopg2.extensions.connection, products: List[dict], fields_to_update: List[str], batch_size: int = 500):
    """Update a column subset for many ids, committing every ``batch_size`` rows."""
    if not products:
        return

    set_clause = ', '.join(f"{field} = v.{field}" for field in fields_to_update)
    value_columns = ', '.join(fields_to_update + ['id'])
    update_sql = _UPDATE_BATCH_SQL.format(set_clause=set_clause, value_columns=value_columns)

    cursor = conn.cursor()
    batch = []
    for product in products:
        row = [product.get(field) for field in fields_to_update]
        row.append(product['id'])
        batch.append(tuple(row))

        if len(batch) >= batch_size:
            execute_values(cursor, update_sql, batch, page_size=batch_size)
            conn.commit()
            batch.clear()

    if batch:
        execute_values(cursor, update_sql, batch, page_size=batch_size)
        conn.commit()
    cursor.close()


def save_categorizations_to_db(conn: psycopg2.extensions.connection, products: List[dict], batch_size: int = 500):
    # Only rows that completed both stages are written. A missing ``categorization``
    # means the API call failed after retries — storing the old/empty value would
    # hide the failure and keep the product out of the retry queue.
    products = [product for product in products if product.get('categorization')]
    print(f"\nSaving {len(products)} categorizations to database...")
    if not products:
        print("   Updated: 0")
        return 0
    updates = []
    for product in products:
        cat = product['categorization']
        updates.append({
            'id': str(product['id']),
            'main_category': cat.get('main_category'),
            'sub_category': cat.get('sub_category'),
            'confidence': cat.get('sub_confidence'),
            'categorized_at': datetime.now()
        })
    batch_update_products(conn, updates, ['main_category', 'sub_category', 'confidence', 'categorized_at'], batch_size)
    print(f"   Updated: {len(products)}")
    return len(products)

