from contextlib import contextmanager
from enum import Enum
import os
import uuid as uuid_lib

from dotenv import find_dotenv, load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from backend.data.constants import CATEGORIES
from backend.data.text_utils import normalize_string, strip_quantity_tokens

app = FastAPI()

# `/search` uses pg_trgm over the canonical product name; a name is a match when
# similarity clears this floor. Brands live outside product_name, so the query
# matches manufacturer names too.
SEARCH_SIMILARITY_THRESHOLD = 0.8


def _create_pool() -> ThreadedConnectionPool:
    load_dotenv(find_dotenv())
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "postgres")
    pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=int(os.getenv("DB_POOL_MAX", "10")),
        host=host,
        port=port,
        database=database,
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
    )
    print(f"Connected to PostgreSQL pool at {host}:{port}, db '{database}'")
    return pool


db_pool = _create_pool()


@contextmanager
def get_cursor():
    """Borrow an autocommit RealDictCursor from the thread-safe pool.

    Each newly opened connection sets autocommit=True (so read-only endpoints
    never sit 'idle in transaction' holding ACCESS SHARE locks) and aligns
    pg_trgm.similarity_threshold with SEARCH_SIMILARITY_THRESHOLD so GIN index
    scans prune non-matches before touching heap pages.
    """
    conn = db_pool.getconn()
    broken = False
    try:
        if not conn.autocommit:
            conn.autocommit = True
            with conn.cursor() as init_cur:
                init_cur.execute(
                    "SELECT set_config('pg_trgm.similarity_threshold', %s, false)",
                    (str(SEARCH_SIMILARITY_THRESHOLD),),
                )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        broken = True
        raise
    finally:
        db_pool.putconn(conn, close=broken)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "https://cenaplus.net",
        "https://www.cenaplus.net",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


class PerPage(int, Enum):
    twelve = 12
    twenty_four = 24
    thirty_six = 36


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.get("/categories")
def get_categories():
    return {k: v for k, v in CATEGORIES.items()}


SEARCH_SQL = """
    WITH candidates AS MATERIALIZED (
        SELECT g.id, similarity(g.product_name, %(q)s) AS sim
        FROM groups g
        WHERE g.product_name %% %(q)s
          AND similarity(g.product_name, %(q)s) >= %(threshold)s
        UNION ALL
        SELECT g.id, similarity(m.canonical_name, %(q)s) AS sim
        FROM manufacturers m
        JOIN groups g ON g.manufacturer_id = m.id
        WHERE m.canonical_name %% %(q)s
          AND similarity(m.canonical_name, %(q)s) >= %(threshold)s
    ),
    matched AS (
        SELECT id, max(sim) AS similarity FROM candidates GROUP BY id
    )
    SELECT gp.group_id,
           gp.group_name,
           gp.main_category,
           gp.sub_category,
           matched.similarity,
           gp.products
    FROM matched
    JOIN grouped_products gp ON gp.group_id = matched.id
    ORDER BY matched.similarity DESC
"""


@app.get("/search")
def search_products(q: str = Query(..., min_length=1)):
    """Fuzzy product search (pg_trgm).

    Every value is passed as a bound parameter — the query text is never
    assembled from user input, so `%`, quotes and `;` cannot alter it.
    """
    needle = strip_quantity_tokens(normalize_string(q)) or (normalize_string(q) or q.strip().lower())
    with get_cursor() as cur:
        cur.execute(SEARCH_SQL, {"q": needle, "threshold": SEARCH_SIMILARITY_THRESHOLD})
        rows = cur.fetchall()
    return {"data": rows}


MANUFACTURER_GROUPS_SQL = """
    SELECT group_id,
           group_name,
           main_category,
           sub_category,
           products
    FROM grouped_products
    WHERE manufacturer_id = %s
    ORDER BY group_name
    LIMIT %s OFFSET %s
"""

RESOLVE_MANUFACTURER_SQL = """
    WITH RECURSIVE seed AS (
        SELECT m.id, m.canonical_name, m.display_name, m.merged_into, 0 AS priority
        FROM manufacturers m
        WHERE m.canonical_name = %s
        UNION
        SELECT m.id, m.canonical_name, m.display_name, m.merged_into, 1 AS priority
        FROM manufacturers m
        JOIN manufacturer_aliases a ON a.manufacturer_id = m.id
        WHERE a.alias = %s
    ),
    chain AS (
        SELECT id, canonical_name, display_name, merged_into, priority, 0 AS depth
        FROM seed
        UNION ALL
        SELECT m.id, m.canonical_name, m.display_name, m.merged_into,
               c.priority, c.depth + 1
        FROM manufacturers m
        JOIN chain c ON m.id = c.merged_into
        WHERE c.merged_into IS NOT NULL
    ),
    resolved AS (
        SELECT id, canonical_name, display_name
        FROM chain
        WHERE merged_into IS NULL
        ORDER BY priority, depth
        LIMIT 1
    )
    SELECT r.id,
           r.canonical_name,
           r.display_name,
           COALESCE(ms.groups, 0) AS groups,
           COALESCE(ms.categories, 0) AS categories,
           COALESCE(ms.markets, 0) AS markets
    FROM resolved r
    LEFT JOIN manufacturer_stats ms ON ms.manufacturer_id = r.id
"""


@app.get("/manufacturer")
def get_manufacturer_products(
    manufacturer_name: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    per_page: PerPage = Query(PerPage.twelve),
):
    """All grouped products of one manufacturer, across every category."""
    # `per_page` arrives as the enum through FastAPI, but plain ints are accepted
    # too so the function stays callable directly (tests, scripts).
    per_page_value = int(getattr(per_page, "value", per_page))
    needle = normalize_string(manufacturer_name) or manufacturer_name.strip().lower()

    with get_cursor() as cur:
        cur.execute(RESOLVE_MANUFACTURER_SQL, (needle, needle))
        manufacturer = cur.fetchone()
        if not manufacturer:
            raise HTTPException(404, "Manufacturer not found")

        total = manufacturer["groups"]
        if total > 0:
            offset = per_page_value * (page - 1)
            cur.execute(MANUFACTURER_GROUPS_SQL, (manufacturer["id"], per_page_value, offset))
            rows = cur.fetchall()
        else:
            rows = []

    return {
        "manufacturer": {
            "name": manufacturer["canonical_name"],
            "display_name": manufacturer["display_name"] or manufacturer["canonical_name"],
            "groups": total,
            "categories": manufacturer["categories"],
            "markets": manufacturer["markets"],
        },
        "total": total,
        "page": page,
        "per_page": per_page_value,
        "data": rows,
    }


@app.get("/product")
def get_product_detail(g_id: str = Query(..., description="Group id (uuid)")):
    """Everything about one grouped product: metadata, price stats, per-market offers.

    Both in-stock and out-of-stock offers are returned (the listing view hides
    unavailable rows, this page is where they belong).
    """
    try:
        group_id = str(uuid_lib.UUID(g_id))
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(400, "Invalid group id")

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT g.id AS group_id,
                   g.name AS group_name,
                   g.main_category,
                   g.sub_category,
                   g.product_name,
                   g.quantity_value,
                   g.quantity_unit,
                   g.is_fluid,
                   mf.canonical_name AS manufacturer,
                   COALESCE(mf.display_name, mf.canonical_name) AS manufacturer_display_name
            FROM groups g
            LEFT JOIN manufacturers mf ON mf.id = g.manufacturer_id
            WHERE g.id = %s
            """,
            (group_id,),
        )
        group = cur.fetchone()
        if not group:
            raise HTTPException(404, "Product not found")

        cur.execute(
            """
            SELECT p.id AS product_id,
                   p.market,
                   p.name,
                   p.price,
                   p.singular_price,
                   p.in_stock,
                   p.image,
                   p.link,
                   p.last_updated,
                   (p.name ILIKE %s) AS kratok_rok
            FROM products p
            WHERE p.group_id = %s
            ORDER BY p.in_stock DESC NULLS LAST, p.price ASC NULLS LAST
            """,
            ("%краток рок%", group_id),
        )
        offers = cur.fetchall()

    available = [o for o in offers if o["in_stock"]]
    prices = [o["price"] for o in available if o["price"] is not None]
    stats = {
        "offers": len(offers),
        "markets": len({o["market"] for o in offers}),
        "in_stock": len(available),
        "out_of_stock": len(offers) - len(available),
        "cheapest_price": min(prices) if prices else None,
        "priciest_price": max(prices) if prices else None,
        "savings": (max(prices) - min(prices)) if len(prices) > 1 else None,
    }

    return {
        "group": {**group, "kratok_rok": any(o["kratok_rok"] for o in offers)},
        "offers": offers,
        "stats": stats,
    }


SUBCATEGORY_GROUPS_SQL = """
    SELECT group_id, group_name, main_category, sub_category, products
    FROM grouped_products
    WHERE main_category = %s AND sub_category = %s
    ORDER BY group_name
    LIMIT %s OFFSET %s
"""

SUBCATEGORY_COUNT_SQL = """
    SELECT COUNT(*)
    FROM grouped_products
    WHERE main_category = %s AND sub_category = %s
"""

SUBCATEGORY_GROUPS_MARKET_SQL = """
    SELECT group_id, group_name, main_category, sub_category, products
    FROM grouped_products
    WHERE main_category = %s AND sub_category = %s
      AND markets && %s::text[]
    ORDER BY group_name
    LIMIT %s OFFSET %s
"""

SUBCATEGORY_COUNT_MARKET_SQL = """
    SELECT COUNT(*)
    FROM grouped_products
    WHERE main_category = %s AND sub_category = %s
      AND markets && %s::text[]
"""


@app.get("/{main_category}/{sub_category}")
def get_grouped_products(
    main_category: str,
    sub_category: str,
    page: int = Query(1, ge=1),
    per_page: PerPage = Query(PerPage.twelve),
    market: list[str] = Query(None),
):
    if main_category not in CATEGORIES:
        raise HTTPException(404, "Main category not found")
    if sub_category not in CATEGORIES[main_category]:
        raise HTTPException(404, "Sub-category not found")

    per_page_value = int(getattr(per_page, "value", per_page))
    offset = per_page_value * (page - 1)
    with get_cursor() as cur:
        if market:
            cur.execute(
                SUBCATEGORY_GROUPS_MARKET_SQL,
                (main_category, sub_category, market, per_page_value, offset),
            )
            rows = cur.fetchall()
            cur.execute(
                SUBCATEGORY_COUNT_MARKET_SQL,
                (main_category, sub_category, market),
            )
            total = cur.fetchone()["count"]
        else:
            cur.execute(
                SUBCATEGORY_GROUPS_SQL,
                (main_category, sub_category, per_page_value, offset),
            )
            rows = cur.fetchall()
            cur.execute(
                SUBCATEGORY_COUNT_SQL,
                (main_category, sub_category),
            )
            total = cur.fetchone()["count"]

    return {
        "total": total,
        "page": page,
        "per_page": per_page_value,
        "data": rows,
    }
