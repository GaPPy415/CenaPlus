"""Рамстор — per-store HTML price tables (ramstore.com.mk) parsed with pandas."""

import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from tqdm import tqdm

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

_UPDATED_RE = re.compile(
    r"Датум и време на последно ажурирање на цените:\s*([0-9./-]+\s+[0-9:]+\s*(?:AM|PM)?)",
    re.IGNORECASE,
)

MARKET_NAME = 'ramstore'

#: Each store page is ~7 MB of HTML, so keep concurrency low: the server is not
#: happy with many parallel downloads (it has fallen over under load).
MAX_WORKERS = 3

stores = [
    "https://ramstore.com.mk/marketi/ramstore-vardar/",
    "https://ramstore.com.mk/marketi/ramstore-siti-mol/",
    "https://ramstore.com.mk/marketi/ramstore-taftalidhe/",
    "https://ramstore.com.mk/marketi/ramstore-karposh/",
    "https://ramstore.com.mk/marketi/ramstore-dhevahir/",
    "https://ramstore.com.mk/marketi/ramstore-park/",
    "https://ramstore.com.mk/marketi/ramstore-gorno-lisiche/",
    "https://ramstore.com.mk/marketi/ramstore-kapitol/",
    "https://ramstore.com.mk/marketi/ramstore-kapishtecz/",
    "https://ramstore.com.mk/marketi/ramstore-debar-maalo/",
    "https://ramstore.com.mk/marketi/ramstore-vodno/",
    "https://ramstore.com.mk/marketi/ramstore-aerodrom/",
    "https://ramstore.com.mk/marketi/ramstore-star-aerodrom/",
    "https://ramstore.com.mk/marketi/ramstore-michurin/",
    "https://ramstore.com.mk/marketi/ramstore-sever/",

    # "https://ramstore.com.mk/marketi/ramstore-tetovo/",
    # "https://ramstore.com.mk/marketi/ramstore-tetovo-3/",
    # "https://ramstore.com.mk/marketi/ramstore-topansko-pole/",
    # "https://ramstore.com.mk/marketi/ramstore-struga/",
    # "https://ramstore.com.mk/marketi/ramstore-ohrid/",
    # "https://ramstore.com.mk/marketi/ramstore-ohrid-ekspress/",
    # "https://ramstore.com.mk/marketi/ramstore-ohrid-turistichka/",
    # "https://ramstore.com.mk/marketi/ramstore-kichevo/",
    # "https://ramstore.com.mk/marketi/ramstore-kumanovo/",
    # "https://ramstore.com.mk/marketi/ramstore-strumicza/",
    # "https://ramstore.com.mk/marketi/ramstore-strumicza-bulevar/"
]

#: Column holding the discounted price, when the row has one.
DISCOUNT_PRICE_COLUMN = 'ЦЕНА СО ПОПУСТ'
REGULAR_PRICE_COLUMN = 'РЕДОВНА ЦЕНА'


_GROUPED_PRICE_RE = re.compile(r"(\d{1,3}(?:\.\d{3})+)(?:,\d+)?(?:\D|$)")
_PLAIN_PRICE_RE = re.compile(r"(\d+)(?:[.,]\d+)?")


def _parse_int_price(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        parsed = int(value)
        return parsed if parsed > 0 else None
    text = str(value).strip()
    if not text:
        return None
    grouped = _GROUPED_PRICE_RE.search(text)
    if grouped:
        parsed = int(grouped.group(1).replace(".", ""))
        return parsed if parsed > 0 else None
    match = _PLAIN_PRICE_RE.search(text)
    if not match:
        return None
    parsed = int(match.group(1))
    return parsed if parsed > 0 else None


def scrape_store(store: str) -> list[ScrapedProduct]:
    """Parse one store page into products."""
    response = requests.get(
        store,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=(10, 60),
    )
    response.raise_for_status()
    df_new = pd.read_html(io.StringIO(response.text))[0]

    if df_new.empty:
        print(f"No data found for store: {store}")
        return []

    product_col = df_new.columns[0]
    products: list[ScrapedProduct] = []
    for _, row in df_new.iterrows():
        name = str(row[product_col]).strip()
        data = {}
        for col in df_new.columns:
            if col == product_col:
                continue
            val = row[col]
            data[col] = None if pd.isna(val) else val

        price = _parse_int_price(data.get('ПРОДАЖНА ЦЕНА'))
        if price is None:
            continue
        regular_price = _parse_int_price(data.get(REGULAR_PRICE_COLUMN))
        discount_price = _parse_int_price(data.get(DISCOUNT_PRICE_COLUMN))
        if discount_price is not None:
            if regular_price is None:
                regular_price = price
            price = discount_price

        products.append(
            ScrapedProduct(
                name=name,
                price=price,
                regular_price=regular_price,
                singular_price=data.get('ЕДИНЕЧНА ЦЕНА'),
                description=data.get('ОПИС НА ПРОИЗВОД'),
                # The store page only lists what is on the shelf today.
                in_stock=True,
            )
        )
    return products


class RamstoreScraper(MarketScraper):
    market = MARKET_NAME
    probe_url = stores[0] if stores else None

    def get_source_marker(self) -> str | None:
        if not stores:
            return None
        # Stream only the header portion of one ~7 MB store page to read the timestamp kindly.
        with requests.get(
            stores[0],
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=(10, 30),
            stream=True,
        ) as response:
            response.raise_for_status()
            buf = bytearray()
            for chunk in response.iter_content(chunk_size=16384):
                if not chunk:
                    continue
                buf.extend(chunk)
                text = buf.decode("utf-8", errors="ignore")
                match = _UPDATED_RE.search(text)
                if match:
                    return " ".join(match.group(1).split())
                if len(buf) >= 262144:
                    break
        return None

    def fetch(self) -> list[ScrapedProduct]:
        products: list[ScrapedProduct] = []
        results: dict[int, list[ScrapedProduct]] = {}
        failures = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(scrape_store, store): index
                for index, store in enumerate(stores)
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=MARKET_NAME):
                index = futures[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    failures += 1
                    print(f"Failed to parse store {stores[index]}: {e}")

        # Store order, not completion order: the same product can appear in
        # several stores and the surviving price must not depend on timing.
        for index in sorted(results):
            products.extend(results[index])

        print(f"Products scraped: {len(products)} ({failures} stores failed)")
        return products


if __name__ == "__main__":
    raise SystemExit(RamstoreScraper().run())
