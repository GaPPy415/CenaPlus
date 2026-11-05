"""Shared client for the ``ceni.zur.mk`` price-list platform.

ДИМ (``dim.marketceni.mk``) and Зур (``ceni.zur.mk``) are the same WordPress
app: a store selector plus a price table fetched from ``wp-admin/admin-ajax.php``.
Only the AJAX ``action`` and the store list differ, so a market is a subclass
that sets three attributes — see ``Dim_scraper`` and ``Zur_scraper``.

Platform quirks:

* The endpoint ignores the UI's 100/250/500 page sizes and honours any ``limit``,
  so one request per store returns that store's whole catalogue.
* Pagination is **unstable**: the same request twice returns a different order,
  so walking ``page=1..N`` duplicates ~30% of rows and drops others. Keep each
  store in a single page; the loop below is only a fallback.
* Rows are ``[code, name, selling price, unit price, description, availability,
  regular price, promo price + discount, promo type, promo window]``. The selling
  price already reflects an active promotion, the description column is a literal
  ``/`` on every row, and every row is in stock.
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

AJAX_URL = "https://ceni.zur.mk/wp-admin/admin-ajax.php"

#: A store holds ~3-6k rows; asking for more keeps it to one consistent response.
PER_PAGE = 10000
MAX_PAGES = 20
MAX_WORKERS = 6
ROW_CELLS = 10

SESSION = requests.Session()
SESSION.headers.update(
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CenaPlus/1.0"}
)

#: Prices arrive as "139.00" (denars, two decimals). Malformed cells such as
#: ".01" must not become price 0.
_PRICE_RE = re.compile(r"^(\d+)(?:\.(\d+))?$")


def parse_price(text: str) -> int | None:
    """Whole denars from the price cell, or None when the cell is unusable."""
    match = _PRICE_RE.match((text or "").strip().replace(",", "."))
    if not match:
        return None
    value = int(match.group(1))
    return value if value > 0 else None


def parse_row(cells) -> ScrapedProduct | None:
    """Map one price-table row to a ScrapedProduct."""
    if len(cells) != ROW_CELLS:
        return None

    price = parse_price(cells[2].get_text(" ", strip=True))
    if price is None:
        return None

    # "Опис на стока" is a literal "/" on every row, so there is no category or
    # description to carry over.
    return ScrapedProduct(
        name=cells[1].get_text(" ", strip=True),
        price=price,
        singular_price=cells[3].get_text(" ", strip=True) or None,
        in_stock="да" in cells[5].get_text(" ", strip=True).lower(),
    )


def get_org_ids(index_url: str) -> list[str]:
    """Store ids from the market selector on the price-list page."""
    response = SESSION.get(index_url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    select = soup.find("select", id="org-list")
    if not select:
        return []
    return [option["value"] for option in select.find_all("option") if option.get("value")]


class ZurPlatformScraper(MarketScraper):
    """One market on the ZUR price-list platform."""

    #: Page holding the store selector (and the table shell).
    index_url: str = ""
    #: `action` value of the AJAX endpoint, e.g. "load_prices_data".
    ajax_action: str = ""

    def fetch_org(self, org: str) -> list[ScrapedProduct]:
        """All products of one store."""
        products: list[ScrapedProduct] = []
        page = 1
        total_pages = 1

        while page <= total_pages and page <= MAX_PAGES:
            response = SESSION.post(
                AJAX_URL,
                data={
                    "action": self.ajax_action,
                    "org": org,
                    "page": page,
                    "search": "",
                    "limit": PER_PAGE,
                },
                headers={"Referer": self.index_url},
                timeout=120,
            )
            response.raise_for_status()
            payload = response.json()

            if not payload.get("success"):
                print(f"[{self.market}] store {org} page {page} rejected: {payload.get('data')}")
                break

            data = payload.get("data") or {}
            total_pages = int(data.get("total_pages") or 1)
            if total_pages > 1:
                print(f"[{self.market}] store {org} spans {total_pages} pages, paginating")

            soup = BeautifulSoup(data.get("html") or "", "html.parser")
            for row in soup.find_all("tr"):
                product = parse_row(row.find_all("td"))
                if product is not None:
                    products.append(product)
            page += 1

        return products

    def fetch(self) -> list[ScrapedProduct]:
        if not (self.index_url and self.ajax_action):
            raise ValueError(f"{type(self).__name__} needs index_url and ajax_action")

        orgs = get_org_ids(self.index_url)
        if not orgs:
            print(f"[{self.market}] market selector not found")
            return []
        print(f"[{self.market}] {len(orgs)} stores to scrape")

        failed = 0
        batches: dict[str, list[ScrapedProduct]] = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.fetch_org, org): org for org in orgs}
            for future in as_completed(futures):
                org = futures[future]
                try:
                    batches[org] = future.result()
                except Exception as e:
                    failed += 1
                    print(f"[{self.market}] store {org} failed: {e}")

        # Several hundred product names carry a different price in different
        # stores. Merge in store-id order so the surviving price (and therefore
        # products.last_updated) is stable across runs instead of depending on
        # which thread finished last.
        products: list[ScrapedProduct] = []
        for org in sorted(batches, key=lambda value: int(value)):
            products.extend(batches[org])
            print(f"[{self.market}] store {org}: {len(batches[org])} rows")

        if failed:
            print(f"[{self.market}] {failed} of {len(orgs)} stores failed")
        return products
