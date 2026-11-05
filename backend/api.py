from enum import Enum
import uuid as uuid_lib

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor

from backend.data.constants import CATEGORIES
from backend.data.db_utils import connect_to_db
from backend.data.text_utils import (
    get_embeddings_client,
    normalize_embedding,
    normalize_name,
    normalize_string,
    strip_quantity_tokens,
)

app = FastAPI()
conn = connect_to_db()
# Read-only endpoints: commit per statement so the shared connection never sits
# "idle in transaction" (it held ACCESS SHARE locks and blocked DDL indefinitely).
conn.autocommit = True

# `/search` uses pg_trgm over the canonical product name; a name is a match when
# similarity clears this floor. Brands live outside product_name, so the query
# matches manufacturer names too.
SEARCH_SIMILARITY_THRESHOLD = 0.8

# Created lazily: the trigram search needs no API key, so the service must boot
# (and serve /search) without GOOGLE_API_KEY.
_embeddings_client = None


def embeddings_client():
    global _embeddings_client
    if _embeddings_client is None:
        _embeddings_client = get_embeddings_client()
    return _embeddings_client


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
    SELECT g.id AS group_id,
           g.name AS group_name,
           g.main_category,
           g.sub_category,
           matched.similarity,
           json_agg(json_build_object(
               'product_id', p.id, 'name', p.name, 'price', p.price, 'market', p.market,
               'in_stock', p.in_stock, 'image', p.image, 'link', p.link,
               'singular_price', p.singular_price) ORDER BY p.price) AS products
    FROM matched
    JOIN groups g ON g.id = matched.id
    JOIN products p ON p.group_id = g.id AND p.in_stock = true
    GROUP BY g.id, g.name, g.main_category, g.sub_category, matched.similarity
    ORDER BY matched.similarity DESC
"""


@app.get("/search")
def search_products(q: str = Query(..., min_length=1)):
    """Fuzzy product search (pg_trgm).

    Every value is passed as a bound parameter — the query text is never
    assembled from user input, so `%`, quotes and `;` cannot alter it.
    """
    needle = strip_quantity_tokens(normalize_string(q)) or (normalize_string(q) or q.strip().lower())
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SEARCH_SQL, {"q": needle, "threshold": SEARCH_SIMILARITY_THRESHOLD})
        rows = cur.fetchall()
    return {"data": rows}


@app.get("/search/semantic")
def search_products_semantic(q: str = Query(..., min_length=1)):
    """Embedding fallback, kept for comparison (needs GOOGLE_API_KEY)."""
    normalized = normalize_name(q)
    vector = normalize_embedding(np.array(embeddings_client().embed_query(normalized))).tolist()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT group_id, group_name, main_category, sub_category, products,
                   1 - (name_embedding <=> %s::vector) AS similarity
            FROM grouped_products
            WHERE 1 - (name_embedding <=> %s::vector) >= 0.80
            ORDER BY similarity DESC
            """,
            (vector, vector),
        )
        rows = cur.fetchall()
    return {"data": rows}


MANUFACTURER_GROUPS_SQL = """
    SELECT gp.group_id,
           gp.group_name,
           gp.main_category,
           gp.sub_category,
           gp.products
    FROM grouped_products gp
    JOIN groups g ON g.id = gp.group_id
    WHERE g.manufacturer_id = %s
    ORDER BY gp.group_name
    LIMIT %s OFFSET %s
"""

MANUFACTURER_TOTAL_SQL = """
    SELECT count(*) AS total,
           count(DISTINCT g.main_category) AS categories
    FROM grouped_products gp
    JOIN groups g ON g.id = gp.group_id
    WHERE g.manufacturer_id = %s
"""

MANUFACTURER_MARKETS_SQL = """
    SELECT count(DISTINCT p.market) AS markets
    FROM products p
    JOIN groups g ON g.id = p.group_id
    WHERE g.manufacturer_id = %s AND p.in_stock = true
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
    )
    SELECT id, canonical_name, display_name
    FROM chain
    WHERE merged_into IS NULL
    ORDER BY priority, depth
    LIMIT 1
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

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(RESOLVE_MANUFACTURER_SQL, (needle, needle))
        manufacturer = cur.fetchone()
        if not manufacturer:
            raise HTTPException(404, "Manufacturer not found")

        cur.execute(MANUFACTURER_TOTAL_SQL, (manufacturer["id"],))
        totals = cur.fetchone()
        cur.execute(MANUFACTURER_MARKETS_SQL, (manufacturer["id"],))
        markets = cur.fetchone()["markets"]

        offset = per_page_value * (page - 1)
        cur.execute(MANUFACTURER_GROUPS_SQL, (manufacturer["id"], per_page_value, offset))
        rows = cur.fetchall()

    return {
        "manufacturer": {
            "name": manufacturer["canonical_name"],
            "display_name": manufacturer["display_name"] or manufacturer["canonical_name"],
            "groups": totals["total"],
            "categories": totals["categories"],
            "markets": markets,
        },
        "total": totals["total"],
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

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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

    offset = per_page.value * (page - 1)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if market:
            cur.execute(
                """
                SELECT * FROM grouped_products gp
                WHERE main_category = %s AND sub_category = %s
                  AND EXISTS (
                    SELECT 1 FROM json_array_elements(gp.products) AS elem
                    WHERE elem->>'market' = ANY(%s)
                  )
                ORDER BY group_name
                LIMIT %s OFFSET %s
                """,
                (main_category, sub_category, market, per_page.value, offset),
            )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT COUNT(*) FROM grouped_products gp
                WHERE main_category = %s AND sub_category = %s
                  AND EXISTS (
                    SELECT 1 FROM json_array_elements(gp.products) AS elem
                    WHERE elem->>'market' = ANY(%s)
                  )
                """,
                (main_category, sub_category, market),
            )
            total = cur.fetchone()["count"]
        else:
            cur.execute(
                """
                SELECT * FROM grouped_products
                WHERE main_category = %s AND sub_category = %s
                ORDER BY group_name
                LIMIT %s OFFSET %s
                """,
                (main_category, sub_category, per_page.value, offset),
            )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT COUNT(*) FROM grouped_products
                WHERE main_category = %s AND sub_category = %s
                """,
                (main_category, sub_category),
            )
            total = cur.fetchone()["count"]

    return {
        "total": total,
        "page": page,
        "per_page": per_page.value,
        "data": rows,
    }
