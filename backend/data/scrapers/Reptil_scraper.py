"""Reptil (marketonline.mk) — WooCommerce Store API."""

import html
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

MAX_WORKERS = 6  # Max connection pool size is 10, be nice to the server and the other users and keep it at 7 or below
MARKET_NAME = 'reptil'
PER_PAGE = 100
BASE_URL = 'https://marketonline.mk/wp-json/wc/store/v1/products'
WP_PRODUCT_URL = 'https://marketonline.mk/wp-json/wp/v2/product'


def parse_price(price_str: str, minor_unit: int) -> int:
    """Convert API price string (e.g. '3200') to display integer (e.g. 32)"""
    return int(price_str) // (10 ** minor_unit)


def parse_optional_price(price_str: str | None, minor_unit: int) -> int | None:
    if not price_str:
        return None
    try:
        value = parse_price(price_str, minor_unit)
        return value if value > 0 else None
    except (ValueError, TypeError):
        return None


def parse_singular_price(price_html: str) -> str | None:
    """Extract the per-unit price string from price_html, e.g. '128 ден/kg'"""
    try:
        soup = BeautifulSoup(price_html, 'html.parser')
        recalc = soup.find('span', class_='mcmp_recalc_price_row')
        if not recalc:
            return None
        bdi = recalc.find('bdi')
        suffix = recalc.find('span', class_='mcmp-recalc-price-suffix')
        if bdi:
            price_part = bdi.get_text(strip=True).replace('\xa0', ' ')
            suffix_part = suffix.get_text(strip=True) if suffix else ''
            return f"{price_part}{suffix_part}"
    except Exception:
        pass
    return None


def parse_product(item: dict) -> ScrapedProduct:
    """Map a WooCommerce API product to a ScrapedProduct."""
    prices = item.get('prices', {})
    minor_unit = prices.get('currency_minor_unit', 2)

    categories = [c['name'] for c in item.get('categories', [])]
    return ScrapedProduct(
        name=html.unescape(item.get('name', '')),
        price=parse_price(prices.get('price', '0'), minor_unit),
        regular_price=parse_optional_price(prices.get('regular_price'), minor_unit),
        singular_price=parse_singular_price(item.get('price_html', '')),
        description=str(categories) if categories else None,
        in_stock=bool(item.get('is_in_stock')),
        image=item['images'][0]['src'] if item.get('images') else None,
        link=item.get('permalink'),
    )


def fetch_page(page: int) -> list[dict]:
    url = f"{BASE_URL}?per_page={PER_PAGE}&page={page}"
    print(f"Fetching page: {page}")
    response = requests.get(url, timeout=(10, 30))
    response.raise_for_status()
    raw = response.text
    json_start = min(
        raw.index('{') if '{' in raw else len(raw),
        raw.index('[') if '[' in raw else len(raw),
    )
    return json.loads(raw[json_start:])


class ReptilScraper(MarketScraper):
    market = MARKET_NAME
    probe_url = f"{BASE_URL}?per_page=1&page=1"
    # Last category entry wins, as in the original scraper.
    keep_in_stock_on_duplicate = False

    def get_source_marker(self) -> str | None:
        response = requests.get(
            WP_PRODUCT_URL,
            params={"per_page": 1, "orderby": "modified", "order": "desc"},
            timeout=(10, 30),
        )
        response.raise_for_status()
        items = response.json()
        if not isinstance(items, list) or not items:
            return None
        modified = items[0].get("modified_gmt") or items[0].get("modified")
        total = response.headers.get("X-WP-Total", "")
        return f"{modified}|total={total}" if modified else None

    def fetch(self) -> list[ScrapedProduct]:
        url = f"{BASE_URL}?per_page={PER_PAGE}&page=1"
        response = requests.get(url, timeout=(10, 30))
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
    raise SystemExit(ReptilScraper().run())
