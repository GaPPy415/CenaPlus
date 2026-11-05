"""Рамстор — per-store HTML price tables (ramstore.com.mk) parsed with pandas."""

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

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


def extract_dates(period):
    if pd.isna(period) or period == '':
        return None, None
    try:
        from_date, to_date = period.split(' - ')
        return from_date.strip(), to_date.strip()
    except ValueError:
        return None, None


def scrape_store(store: str) -> list[ScrapedProduct]:
    """Parse one store page into products."""
    df_new = pd.read_html(store)[0]
    store_id = store.rsplit('_', 1)[0].split('marketi/')[1].replace('/', '')
    df_new["storeId"] = store_id

    if df_new.empty:
        print(f"No data found for store: {store}")
        return []

    df_new[['promotionDateFrom', 'promotionDateTo']] = df_new['ВРЕМЕТРАЕЊЕ НА АКЦИЈА'].apply(
        lambda x: pd.Series(extract_dates(x))
    )
    df_new = df_new.drop(columns='ВРЕМЕТРАЕЊЕ НА АКЦИЈА')

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

        price = int(data['ПРОДАЖНА ЦЕНА'])
        if data.get(DISCOUNT_PRICE_COLUMN) is not None:
            price = int(data[DISCOUNT_PRICE_COLUMN].rsplit(' ', 1)[0][:-3])

        products.append(
            ScrapedProduct(
                name=name,
                price=price,
                singular_price=data['ЕДИНЕЧНА ЦЕНА'],
                description=data['ОПИС НА ПРОИЗВОД'],
                # The store page only lists what is on the shelf today.
                in_stock=True,
            )
        )
    return products


class RamstoreScraper(MarketScraper):
    market = MARKET_NAME

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
