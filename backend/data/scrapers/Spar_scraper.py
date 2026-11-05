"""SPAR — price list API.

`spar.mk/pricelist` is a Next.js front-end; the data comes from the Laravel API at
`api.prod.spar.mk`:

* ``GET /api/pricelists-web``                      → all stores (id, name, city, products_count)
* ``GET /api/pricelists-web/{id}/products?page=&per_page=`` → paginated products

So no HTML is parsed: one market list, then pages of JSON. Pagination is stable
(same page twice returns the same rows) and ``per_page`` is capped at 500, so a
74k-product store is 148 requests.

Only Skopje stores are scraped (``city == "Skopje"``); prices are chain-wide but
availability is per store, so the merge keeps a product if any Skopje store has
it.

Names arrive in the ERP's ASCII transliteration of Macedonian, using the physical
Macedonian keyboard layout for the letters Latin lacks (``[`` = Ш, ``^`` = Ч,
``@`` = Ж, ``W`` = Њ …) and then Latin for everything else. ``to_cyrillic``
reverses both: first the keyboard symbols, then a Macedonian Latin→Cyrillic
transliteration.
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import cyrtranslit
import requests

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

MARKET_NAME = "spar"

API_URL = "https://api.prod.spar.mk/api/pricelists-web"
CITY = "Skopje"

PER_PAGE = 500  # server cap: higher values still return 500
MAX_WORKERS = 4  # page workers inside one store
STORE_WORKERS = 6  # stores fetched in parallel
MAX_PAGES = 400

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CenaPlus/1.0",
        "Accept": "application/json",
        "Origin": "https://spar.mk",
        "Referer": "https://spar.mk/",
    }
)

#: Symbols that stand in for Macedonian letters on the physical keyboard.
#: `[` = Ш / `{` = ш, `]` = Ќ, `\` = Ѓ, `^` = Ч, `@` = Ж, and the shifted
#: variants are the lowercase forms (the ERP writes uppercase keys unshifted).
KEYBOARD_SYMBOLS = {
    "[": "Ш", "{": "ш",
    "]": "Ќ", "}": "ќ",
    "\\": "Ѓ", "|": "ѓ",
    "^": "Ч", "~": "ч",
    "@": "Ж", "`": "ж",
}

#: Latin letters that are really Macedonian letters on the same keyboard. They
#: are only rewritten inside words so model codes like "W2-1" survive.
WORD_LETTERS = {
    "w": "њ", "W": "Њ",
    "q": "љ", "Q": "Љ",
    "x": "џ", "X": "Џ",
}

_PRICE_RE = re.compile(r"^\s*(\d+)(?:[.,]\d+)?\s*$")


def _replace_word_letters(text: str) -> str:
    chars = list(text)
    for index, char in enumerate(chars):
        replacement = WORD_LETTERS.get(char)
        if replacement is None:
            continue
        previous = chars[index - 1] if index else ""
        following = chars[index + 1] if index + 1 < len(chars) else ""
        if previous.isalpha() or following.isalpha():
            chars[index] = replacement
    return "".join(chars)


def to_cyrillic(text: str) -> str:
    """Reverse the ERP's ASCII Macedonian: keyboard symbols first, then Latin."""
    for latin, cyrillic in KEYBOARD_SYMBOLS.items():
        text = text.replace(latin, cyrillic)
    return cyrtranslit.to_cyrillic(_replace_word_letters(text), "mk")


def parse_price(value) -> int | None:
    match = _PRICE_RE.match(str(value or ""))
    return int(match.group(1)) if match else None


def get_skopje_markets() -> list[dict]:
    response = SESSION.get(API_URL, timeout=60)
    response.raise_for_status()
    markets = response.json().get("data") or []
    return [m for m in markets if (m.get("city") or "").strip().lower() == CITY.lower()]


def fetch_page(market_id: int, page: int) -> list[dict]:
    response = SESSION.get(
        f"{API_URL}/{market_id}/products",
        params={"page": page, "per_page": PER_PAGE},
        timeout=120,
    )
    response.raise_for_status()
    return response.json().get("data") or []


def parse_row(row: dict) -> ScrapedProduct | None:
    price = parse_price(row.get("sale"))
    # A handful of rows carry a 0 (placeholder) price; they are not sellable.
    if price is None or price <= 0:
        return None

    name = to_cyrillic((row.get("name") or "").strip())
    if not name:
        return None

    return ScrapedProduct(
        name=name,
        price=price,
        description=(row.get("category") or "").strip() or None,
        in_stock=str(row.get("available")) == "1",
    )


class SparScraper(MarketScraper):
    market = MARKET_NAME
    # A product is in stock for the market if any Skopje store carries it.
    keep_in_stock_on_duplicate = True
    #: 89% of the catalogue is flagged unavailable in a given store. Ingesting
    #: those would add ~70k rows that can never appear in the app (and would have
    #: to be categorized and embedded). Flip to False to store the full list.
    only_available: bool = True

    def fetch_market(self, market: dict) -> list[ScrapedProduct]:
        market_id = market["id"]
        print(f"[{MARKET_NAME}] store {market_id} ({market.get('name')})")

        first = SESSION.get(
            f"{API_URL}/{market_id}/products",
            params={"page": 1, "per_page": PER_PAGE},
            timeout=120,
        )
        first.raise_for_status()
        payload = first.json()
        last_page = min(int(payload["meta"]["last_page"]), MAX_PAGES)

        pages = {1: payload.get("data") or []}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_page, market_id, page): page
                for page in range(2, last_page + 1)
            }
            for future in as_completed(futures):
                page = futures[future]
                try:
                    pages[page] = future.result()
                except Exception as e:
                    print(f"[{MARKET_NAME}] store {market_id} page {page} failed: {e}")

        products: list[ScrapedProduct] = []
        for page in sorted(pages):
            for row in pages[page]:
                product = parse_row(row)
                if product is None:
                    continue
                if self.only_available and not product.in_stock:
                    continue
                products.append(product)

        print(f"[{MARKET_NAME}] store {market_id}: {len(products)} products kept")
        return products

    def fetch(self) -> list[ScrapedProduct]:
        markets = get_skopje_markets()
        if not markets:
            print(f"[{MARKET_NAME}] no {CITY} markets found")
            return []
        print(f"[{MARKET_NAME}] {len(markets)} {CITY} stores: {[m['id'] for m in markets]}")

        # Stores in parallel, each store still fetching its pages in parallel.
        batches: dict[int, list[ScrapedProduct]] = {}
        with ThreadPoolExecutor(max_workers=min(STORE_WORKERS, len(markets))) as executor:
            futures = {
                executor.submit(self.fetch_market, market): market
                for market in markets
            }
            for future in as_completed(futures):
                market = futures[future]
                try:
                    batches[market["id"]] = future.result()
                except Exception as e:
                    print(f"[{MARKET_NAME}] store {market['id']} failed: {e}")

        # Store-id order, not completion order: prices are chain-wide but a
        # product can carry different availability per store.
        products: list[ScrapedProduct] = []
        for market_id in sorted(batches):
            products.extend(batches[market_id])
        return products


if __name__ == "__main__":
    raise SystemExit(SparScraper().run())
