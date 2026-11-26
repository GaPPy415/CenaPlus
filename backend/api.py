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


class SortBy(str, Enum):
    name = "name"
    price = "price"
    discount = "discount"
    markets = "markets"


class SortDir(str, Enum):
    asc = "asc"
    desc = "desc"


_DEFAULT_SORT_DIR: dict[SortBy, SortDir] = {
    SortBy.name: SortDir.asc,
    SortBy.price: SortDir.asc,
    SortBy.discount: SortDir.desc,
    SortBy.markets: SortDir.desc,
}

_ORDER_BY_SQL: dict[tuple[SortBy, SortDir], str] = {
    (SortBy.name, SortDir.asc): "group_name ASC",
    (SortBy.name, SortDir.desc): "group_name DESC",
    (SortBy.price, SortDir.asc): "min_price ASC NULLS LAST, group_name ASC",
    (SortBy.price, SortDir.desc): "min_price DESC NULLS LAST, group_name ASC",
    (SortBy.discount, SortDir.desc): "max_discount_pct DESC NULLS LAST, min_price ASC NULLS LAST, group_name ASC",
    (SortBy.discount, SortDir.asc): "max_discount_pct ASC NULLS LAST, min_price ASC NULLS LAST, group_name ASC",
    (SortBy.markets, SortDir.desc): "market_count DESC, min_price ASC NULLS LAST, group_name ASC",
    (SortBy.markets, SortDir.asc): "market_count ASC, min_price ASC NULLS LAST, group_name ASC",
}


def _resolve_order_by(sort: SortBy | str | object, order: SortDir | str | None | object) -> str:
    """Resolve (sort, order) to a static ORDER BY clause, tolerating direct Python calls."""
    sort_raw = getattr(sort, "value", sort)
    try:
        sort_by = SortBy(sort_raw) if isinstance(sort_raw, str) else SortBy.name
    except ValueError:
        sort_by = SortBy.name

    order_raw = getattr(order, "value", order)
    try:
        sort_dir = SortDir(order_raw) if isinstance(order_raw, str) else _DEFAULT_SORT_DIR[sort_by]
    except ValueError:
        sort_dir = _DEFAULT_SORT_DIR[sort_by]

    return _ORDER_BY_SQL[(sort_by, sort_dir)]


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


def _build_listing_filters(
    base_clauses: list[str],
    base_params: list,
    *,
    sub_keys: list[str] | None = None,
    markets: list[str] | None = None,
    on_sale: bool = False,
) -> tuple[str, list]:
    """Assemble static WHERE predicates + bound params for grouped_products listings.

    When both `markets` and `on_sale` are active, `sale_markets && %s::text[]`
    ensures we match only groups that are actually discounted in at least one
    selected market.
    """
    clauses = list(base_clauses)
    params = list(base_params)
    if sub_keys:
        clauses.append("(main_category || '::' || sub_category) = ANY(%s::text[])")
        params.append(sub_keys)
    if markets and on_sale:
        clauses.append("sale_markets && %s::text[]")
        params.append(markets)
    elif markets:
        clauses.append("markets && %s::text[]")
        params.append(markets)
    elif on_sale:
        clauses.append("on_sale = true")
    return " AND ".join(clauses), params

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
           COALESCE(ms.markets, 0) AS markets,
           ms.last_updated,
           COALESCE(ms.subcategories, '[]'::jsonb) AS subcategories
    FROM resolved r
    LEFT JOIN manufacturer_stats ms ON ms.manufacturer_id = r.id
"""


def _match_manufacturer_subcategories(
    available: list[dict],
    requested: list[str],
) -> tuple[list[str], int]:
    """Match requested `sub_category` params (`main::sub` or plain `sub`) against
    precomputed manufacturer subcategories, returning `(exact_keys, filtered_total)`.
    """
    pair_filters: set[tuple[str, str]] = set()
    plain_filters: set[str] = set()
    for raw in requested:
        token = (raw or "").strip()
        if not token:
            continue
        if "::" in token:
            main_cat, sub_cat = token.split("::", 1)
            pair_filters.add((main_cat, sub_cat))
        else:
            plain_filters.add(token)

    matched_keys: list[str] = []
    total = 0
    for item in available:
        mc, sc = item["main_category"], item["sub_category"]
        if (mc, sc) in pair_filters or sc in plain_filters:
            matched_keys.append(f"{mc}::{sc}")
            total += int(item["count"])
    return matched_keys, total


@app.get("/manufacturer")
def get_manufacturer_products(
    manufacturer_name: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    per_page: PerPage = Query(PerPage.twelve),
    sub_category: list[str] = Query(None),
    market: list[str] = Query(None),
    on_sale: bool = Query(False),
    sort: SortBy = Query(SortBy.name),
    order: SortDir | None = Query(None),
):
    """All grouped products of one manufacturer, across every category."""
    # `per_page` arrives as the enum through FastAPI, but plain ints are accepted
    # too so the function stays callable directly (tests, scripts).
    per_page_value = int(getattr(per_page, "value", per_page))
    needle = normalize_string(manufacturer_name) or manufacturer_name.strip().lower()
    raw_subs = sub_category if isinstance(sub_category, (list, tuple)) else []
    active_sub_filters = [s for s in raw_subs if (s or "").strip()]
    raw_markets = market if isinstance(market, (list, tuple)) else []
    active_markets = [m for m in raw_markets if (m or "").strip()]
    only_on_sale = on_sale is True
    order_by_sql = _resolve_order_by(sort, order)

    with get_cursor() as cur:
        cur.execute(RESOLVE_MANUFACTURER_SQL, (needle, needle))
        manufacturer = cur.fetchone()
        if not manufacturer:
            raise HTTPException(404, "Manufacturer not found")

        subcategories = manufacturer["subcategories"] or []
        offset = per_page_value * (page - 1)

        matched_keys: list[str] | None = None
        precomputed_total: int | None = None
        if active_sub_filters:
            matched_keys, sub_total = _match_manufacturer_subcategories(
                subcategories, active_sub_filters
            )
            if sub_total == 0:
                total = 0
                rows = []
            elif not active_markets and not only_on_sale:
                precomputed_total = sub_total
        elif manufacturer["groups"] == 0:
            total = 0
            rows = []
        elif not active_markets and not only_on_sale:
            precomputed_total = manufacturer["groups"]

        if not (
            (active_sub_filters and not matched_keys)
            or (not active_sub_filters and manufacturer["groups"] == 0)
        ):
            where_sql, filter_params = _build_listing_filters(
                ["manufacturer_id = %s"],
                [manufacturer["id"]],
                sub_keys=matched_keys,
                markets=active_markets,
                on_sale=only_on_sale,
            )
            cur.execute(
                f"""
                SELECT group_id,
                       group_name,
                       main_category,
                       sub_category,
                       products
                FROM grouped_products
                WHERE {where_sql}
                ORDER BY {order_by_sql}
                LIMIT %s OFFSET %s
                """,
                (*filter_params, per_page_value, offset),
            )
            rows = cur.fetchall()
            if precomputed_total is not None:
                total = precomputed_total
            else:
                cur.execute(
                    f"SELECT COUNT(*) FROM grouped_products WHERE {where_sql}",
                    filter_params,
                )
                total = cur.fetchone()["count"]

    return {
        "manufacturer": {
            "name": manufacturer["canonical_name"],
            "display_name": manufacturer["display_name"] or manufacturer["canonical_name"],
            "groups": manufacturer["groups"],
            "categories": manufacturer["categories"],
            "markets": manufacturer["markets"],
            "last_updated": manufacturer["last_updated"],
        },
        "subcategories": subcategories,
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
                   p.regular_price,
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

        # Newest scraped change per market = when that store's data was last refreshed.
        markets = sorted({o["market"] for o in offers})
        market_updates: dict = {}
        if markets:
            cur.execute(
                """
                SELECT m.market,
                       (SELECT MAX(p.last_updated) FROM products p WHERE p.market = m.market)
                           AS last_updated
                FROM unnest(%s::text[]) AS m(market)
                """,
                (markets,),
            )
            market_updates = {r["market"]: r["last_updated"] for r in cur.fetchall()}
        for o in offers:
            o["market_last_updated"] = market_updates.get(o["market"])

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
    on_sale: bool = Query(False),
    sort: SortBy = Query(SortBy.name),
    order: SortDir | None = Query(None),
):
    if main_category not in CATEGORIES:
        raise HTTPException(404, "Main category not found")
    if sub_category not in CATEGORIES[main_category]:
        raise HTTPException(404, "Sub-category not found")

    per_page_value = int(getattr(per_page, "value", per_page))
    offset = per_page_value * (page - 1)
    raw_markets = market if isinstance(market, (list, tuple)) else []
    active_markets = [m for m in raw_markets if (m or "").strip()]
    only_on_sale = on_sale is True
    order_by_sql = _resolve_order_by(sort, order)

    where_sql, filter_params = _build_listing_filters(
        ["main_category = %s", "sub_category = %s"],
        [main_category, sub_category],
        markets=active_markets,
        on_sale=only_on_sale,
    )
    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT group_id, group_name, main_category, sub_category, products
            FROM grouped_products
            WHERE {where_sql}
            ORDER BY {order_by_sql}
            LIMIT %s OFFSET %s
            """,
            (*filter_params, per_page_value, offset),
        )
        rows = cur.fetchall()
        cur.execute(
            f"SELECT COUNT(*) FROM grouped_products WHERE {where_sql}",
            filter_params,
        )
        total = cur.fetchone()["count"]

    return {
        "total": total,
        "page": page,
        "per_page": per_page_value,
        "data": rows,
    }
