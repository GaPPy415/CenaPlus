"""BigShop (bigshop.mk) — WooCommerce Store API."""

import html
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

MAX_WORKERS = 6
MARKET_NAME = 'bigshop'
PER_PAGE = 100
BASE_URL = 'https://bigshop.mk/wp-json/wc/store/v1/products'


def parse_price(price_str: str, minor_unit: int) -> int:
    try:
        return int(price_str) // (10 ** minor_unit)
    except (ValueError, TypeError):
        return 0


def parse_singular_price(price_html: str) -> str | None:
    """Legacy per-unit price parsing inherited from the original Zito scraper."""
    singular_price = None
    if price_html:
        parts = re.split(r'<[^>]+>', price_html)
        if len(parts) >= 13:
            # Reconstruct: index 9 + index 12 + "ден"
            try:
                singular_price = parts[9] + parts[12] + "ден"
            except IndexError:
                pass
    return singular_price


def parse_product(product: dict) -> ScrapedProduct:
    prices = product.get('prices', {})
    minor_unit = prices.get('currency_minor_unit', 2)
    images = product.get('images', [])
    categories = [cat['name'] for cat in product.get('categories', [])]

    return ScrapedProduct(
        name=html.unescape(product.get('name', '')),
        # 'price' covers both regular and sale prices.
        price=parse_price(prices.get('price', '0'), minor_unit),
        singular_price=parse_singular_price(product.get('price_html', '')),
        description=str(categories) if categories else None,
        in_stock=bool(product.get('is_in_stock')),
        image=images[0]['src'] if images else '',
        link=product.get('permalink'),
    )


def fetch_page(page: int) -> list[dict]:
    url = f"{BASE_URL}?per_page={PER_PAGE}&page={page}"
    print(f"Fetching page: {page}")
    response = requests.get(url)
    response.raise_for_status()

    raw = response.text
    # Handle potential PHP warnings before JSON
    json_start = min(
        raw.index('{') if '{' in raw else len(raw),
        raw.index('[') if '[' in raw else len(raw),
    )
    return json.loads(raw[json_start:])


class BigshopScraper(MarketScraper):
    market = MARKET_NAME
    # Last category entry wins, as in the original scraper.
    keep_in_stock_on_duplicate = False

    def fetch(self) -> list[ScrapedProduct]:
        url = f"{BASE_URL}?per_page={PER_PAGE}&page=1"
        response = requests.get(url, timeout=45)
        response.raise_for_status()
        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        print(f"Total pages to scrape: {total_pages}")

        products: list[ScrapedProduct] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_page, page): page for page in range(1, total_pages + 1)}
            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    products.extend(parse_product(item) for item in future.result())
                except Exception as e:
                    print(f"Error scraping page {page_num}: {e}")
        return products


if __name__ == "__main__":
    raise SystemExit(BigshopScraper().run())
