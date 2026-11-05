"""Kipper — price list fetched straight from the site's AJAX/JSON endpoints.

`kipper.mk/mk/marketet/` is a WordPress store directory with a city filter; each
store is a `post` (e.g. `kipper-150-skopje-zelezara`) whose page renders a table
that is *empty* in the HTML and filled client-side by DataTables from:

    POST https://kipper.mk/wp-admin/admin-ajax.php
         action=get_products_data&post_id=<store post id>
         draw=1&start=0&length=<n>          (DataTables paging)

`length` is not capped — 100000 returns a whole store in one response — so a
store costs one request instead of the ~200 the rendered page would need.

Stores come from the WP REST API, filtered by the same city term the page's
filter uses (563 = Скопје):

    GET https://kipper.mk/mk/wp-json/wp/v2/posts?categories=563&per_page=100

`product_name` is already Cyrillic (the ERP also returns `product_name_alt` in
Latin); `product_price_weight` is the computed unit price shown in the "Единечна
цена" column together with `product_type_alt` ("1КГ=", "1ПАР=" …).
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

MARKET_NAME = "kipper"

#: "Скопје" in the marketet filter (value of the city <select> option).
CITY_TERM = 563

POSTS_URL = "https://kipper.mk/mk/wp-json/wp/v2/posts"
AJAX_URL = "https://kipper.mk/wp-admin/admin-ajax.php"
PRODUCTS_ACTION = "get_products_data"

PER_PAGE = 100  # WP REST maximum
MAX_PRODUCTS = 100000  # requests the whole store; the endpoint does not cap it
MAX_WORKERS = 6

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CenaPlus/1.0",
        "Origin": "https://kipper.mk",
        "Referer": "https://kipper.mk/mk/marketet/",
    }
)

_PRICE_RE = re.compile(r"^\s*(\d+)(?:[.,]\d+)?\s*$")


def parse_price(value) -> int | None:
    match = _PRICE_RE.match(str(value if value is not None else ""))
    if not match:
        return None
    price = int(match.group(1))
    return price if price > 0 else None


def unit_price(row: dict) -> str | None:
    """The "Единечна цена" cell, e.g. "1КГ=600 ден"."""
    label = (row.get("product_type_alt") or "").strip()
    weight = (row.get("product_price_weight") or "").strip()
    if not weight:
        return None
    if label.startswith("="):  # the source occasionally has "=1КГ"
        label = f"{label[1:]}="
    return f"{label}{weight} ден"


def parse_row(row: dict) -> ScrapedProduct | None:
    name = (row.get("product_name") or "").strip()
    price = parse_price(row.get("product_price"))
    if not name or price is None:
        return None

    # The store page only lists what it carries; every row seen so far is "D".
    status = str(row.get("product_status") or "").strip().upper()

    return ScrapedProduct(
        name=name,
        price=price,
        singular_price=unit_price(row),
        description=(row.get("product_subgroup") or "").strip() or None,
        in_stock=status in ("", "D"),
    )


def get_skopje_stores() -> list[dict]:
    """Store posts tagged with the Скопје city term."""
    stores: list[dict] = []
    page = 1
    while True:
        response = SESSION.get(
            POSTS_URL,
            params={
                "categories": CITY_TERM,
                "per_page": PER_PAGE,
                "page": page,
                "_fields": "id,title,link",
            },
            timeout=60,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        stores.extend(batch)
        if len(batch) < PER_PAGE:
            break
        page += 1
    return stores


def fetch_store(store: dict) -> list[ScrapedProduct]:
    """Every product of one store in a single AJAX request."""
    response = SESSION.post(
        AJAX_URL,
        data={
            "action": PRODUCTS_ACTION,
            "post_id": store["id"],
            "draw": 1,
            "start": 0,
            "length": MAX_PRODUCTS,
            "search[value]": "",
        },
        timeout=240,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("data") or []

    products = []
    for row in rows:
        product = parse_row(row)
        if product is not None:
            products.append(product)
    return products


class KipperScraper(MarketScraper):
    market = MARKET_NAME

    def fetch(self) -> list[ScrapedProduct]:
        stores = get_skopje_stores()
        if not stores:
            print(f"[{MARKET_NAME}] no stores found for city term {CITY_TERM}")
            return []
        print(f"[{MARKET_NAME}] {len(stores)} Skopje stores")

        batches: dict[int, list[ScrapedProduct]] = {}
        failed = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_store, store): store for store in stores}
            for future in as_completed(futures):
                store = futures[future]
                try:
                    batches[store["id"]] = future.result()
                except Exception as e:
                    failed += 1
                    print(f"[{MARKET_NAME}] store {store['id']} failed: {e}")

        # Store-id order: the stores share most of the catalogue, but a product
        # can carry a different price, and the merge must not depend on timing.
        products: list[ScrapedProduct] = []
        for store_id in sorted(batches):
            products.extend(batches[store_id])

        if failed:
            print(f"[{MARKET_NAME}] {failed} of {len(stores)} stores failed")
        print(f"[{MARKET_NAME}] {sum(len(v) for v in batches.values())} rows from stores")
        return products


if __name__ == "__main__":
    raise SystemExit(KipperScraper().run())
