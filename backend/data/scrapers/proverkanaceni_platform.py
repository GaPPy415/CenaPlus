"""Shared client for the ``*.proverkanaceni.mk`` price-list platform.

Zito, Stokomak and Elida all run the same PHP app: a store selector on the index
page (`select.form-select`, one option per store) and, per store, a product
table paginated with ``?page=N&perPage=&search=&org=<id>``.

Platform quirks:

* ``perPage`` is capped at 100 rows server-side, whatever value is sent, so a
  store with 5k products means 50 requests. That is why pages *and* stores are
  both threaded.
* The product count lives in a ``<p>`` ("Прикажи 1 - 4929 од 4929 артикли");
  the number of pages is ``count // 100 + 1``.
* Price cells are ``"299 ден."``. A handful of voucher rows carry junk prices
  (``"0.1 ден."``, ``"0.01 ден."``) — those rows are skipped, never stored as 0.
* Rows have 6 cells (9 when a promotion is active): name, selling price, unit
  price, description/category, availability, regular price, [promo columns].
  Availability is always "Да"; the store page only lists what is on the shelf.
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

#: The table is capped at 100 rows per request, regardless of `perPage`.
PAGE_SIZE = 100

_PRODUCT_TABLE_CLASS = "table table-bordered table-striped table-hover"
_PRICE_INT = re.compile(r"^(\d+)\s+ден\.?$")
_PRICE_GROUPED = re.compile(r"^(\d{1,3}(?:\.\d{3})+)\s+ден\.?$")
_COUNT_RE = re.compile(r"од\s+([\d.,]+)\s+артикли")


def get_soup(url: str) -> BeautifulSoup:
    response = SESSION.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")


def parse_price(text: str) -> int | None:
    """Whole denars from a price cell; None for junk such as "0.01 ден."."""
    cleaned = " ".join((text or "").split())
    match = _PRICE_INT.match(cleaned)
    if match:
        return int(match.group(1))
    grouped = _PRICE_GROUPED.match(cleaned)
    if grouped:
        return int(grouped.group(1).replace(".", ""))
    return None


def parse_row(cells) -> ScrapedProduct | None:
    """Map one table row to a ScrapedProduct.

    Cells are read with ``.text`` (not ``get_text(" ")``) so names with nested
    markup keep exactly the spacing the pre-merge scrapers wrote to the DB —
    names are the upsert key.
    """
    if len(cells) < 5:
        return None

    price = parse_price(cells[1].text.strip())
    if price is None:
        return None

    return ScrapedProduct(
        name=cells[0].text.strip(),
        price=price,
        singular_price=cells[2].text.strip(),
        description=cells[3].text.strip(),
        in_stock="Да" in cells[4].text.strip(),
    )


def count_products(soup: BeautifulSoup) -> int | None:
    """Total product count of a store, from the "Прикажи … од N артикли" line."""
    match = _COUNT_RE.search(soup.get_text(" ", strip=True))
    if not match:
        return None
    raw = match.group(1).replace(".", "").replace(",", "")
    return int(raw) if raw.isdigit() else None


class ProverkanaceniScraper(MarketScraper):
    """One market on the proverkanaceni.mk platform."""

    #: Site root, e.g. "https://zito.proverkanaceni.mk/".
    base_url: str = ""
    #: Stores scraped concurrently (each store also threads its pages).
    market_workers: int = 8
    page_workers: int = 8

    def scrape_page(self, org_id: int, page_num: int) -> list[ScrapedProduct]:
        url = f"{self.base_url}?page={page_num}&perPage=10000&search=&org={org_id}"
        soup = get_soup(url)
        table = soup.find("table", class_=_PRODUCT_TABLE_CLASS)
        if not table:
            return []
        body = table.find("tbody")
        if not body:
            return []

        products = []
        for row in body.find_all("tr"):
            product = parse_row(row.find_all("td"))
            if product is not None:
                products.append(product)
        return products

    def scrape_market(self, org_id: int) -> list[ScrapedProduct]:
        """Every product of one store, pages fetched concurrently."""
        print(f"[{self.market}] store {org_id}")

        soup = get_soup(f"{self.base_url}?org={org_id}&search=&perPage=10")
        num_products = count_products(soup)
        if num_products is None:
            print(f"[{self.market}] could not read the product count for store {org_id}")
            return []

        pages = num_products // PAGE_SIZE + 1
        page_results: dict[int, list[ScrapedProduct]] = {}
        with ThreadPoolExecutor(max_workers=self.page_workers) as executor:
            futures = {
                executor.submit(self.scrape_page, org_id, page): page
                for page in range(1, pages + 1)
            }
            for future in as_completed(futures):
                page = futures[future]
                try:
                    page_results[page] = future.result()
                except Exception as e:
                    print(f"[{self.market}] store {org_id} page {page} failed: {e}")

        # Page order, not completion order: the same name can appear on several
        # pages of one store, and the surviving price must not depend on timing.
        products: list[ScrapedProduct] = []
        for page in sorted(page_results):
            products.extend(page_results[page])

        print(f"[{self.market}] store {org_id}: {num_products} listed, {len(products)} parsed")
        return products

    def fetch(self) -> list[ScrapedProduct]:
        if not self.base_url:
            raise ValueError(f"{type(self).__name__}.base_url is not set")

        soup = get_soup(self.base_url)
        selects = soup.find_all("select", class_="form-select")
        if not selects:
            print(f"[{self.market}] store selector not found")
            return []

        org_ids = [
            int(option["value"])
            for option in selects[0].find_all("option")[1:]
            if option.get("value")
        ]
        print(f"[{self.market}] {len(org_ids)} stores to scrape")

        failed = 0
        batches: dict[int, list[ScrapedProduct]] = {}
        with ThreadPoolExecutor(max_workers=self.market_workers) as executor:
            futures = {executor.submit(self.scrape_market, org): org for org in org_ids}
            for future in as_completed(futures):
                org = futures[future]
                try:
                    batches[org] = future.result()
                except Exception as e:
                    failed += 1
                    print(f"[{self.market}] store {org} failed: {e}")

        # Merge in store-id order: the same name can carry a different price in
        # different stores, and thread completion order would make the surviving
        # price (and products.last_updated) flip between runs.
        products: list[ScrapedProduct] = []
        for org in sorted(batches):
            products.extend(batches[org])

        if failed:
            print(f"[{self.market}] {failed} of {len(org_ids)} stores failed")
        return products
