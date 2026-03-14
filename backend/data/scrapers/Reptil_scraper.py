import requests
import time
import json
import html
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from backend.data.db_utils import *

MAX_WORKERS = 6 # Max connection pool size is 10, be nice to the server and the other users and keep it at 7 or below
MARKET_NAME = 'reptil'
PER_PAGE = 100
BASE_URL = f'https://marketonline.mk/wp-json/wc/store/v1/products'

def parse_price(price_str: str, minor_unit: int) -> int:
    """Convert API price string (e.g. '3200') to display integer (e.g. 32)"""
    return int(price_str) // (10 ** minor_unit)

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


def parse_product(item: dict) -> tuple[str, list]:
    """Map a WooCommerce API product to the same [price, img, href, singular_price, categories, in_stock] format"""
    prices = item.get('prices', {})
    minor_unit = prices.get('currency_minor_unit', 2)

    price = parse_price(prices.get('price', '0'), minor_unit)
    singular_price = parse_singular_price(item.get('price_html', ''))
    img = item['images'][0]['src'] if item.get('images') else None
    href = item.get('permalink')
    categories = [c['name'] for c in item.get('categories', [])]
    in_stock = 1 if item.get('is_in_stock') else 0
    name = html.unescape(item.get('name', ''))

    return name, [price, img, href, singular_price, categories, in_stock]

def fetch_page(page: int):
    url = f"{BASE_URL}?per_page={PER_PAGE}&page={page}"
    print(f"Fetching page: {page}")
    response = requests.get(url)
    raw = response.text
    json_start = min(
        raw.index('{') if '{' in raw else len(raw),
        raw.index('[') if '[' in raw else len(raw),
    )
    products = json.loads(raw[json_start:])
    return products


def main():
    start = time.time()
    url = f"{BASE_URL}?per_page={PER_PAGE}&page=1"
    response = requests.get(url)
    total_pages = int(response.headers.get('X-WP-TotalPages', 1))
    print(f"Total pages to scrape: {total_pages}")

    all_products = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page, page): page for page in range(1, total_pages + 1)}
        for future in as_completed(futures):
            page_num = futures[future]
            try:
                products = future.result()
                # print(f"Scraped page: {page_num}")
                for product in products:
                    name, values = parse_product(product)
                    all_products[name] = values
            except Exception as e:
                print(f"Error scraping page {page_num}: {e}")
    print(f"Total products scraped: {len(all_products)}, scraping done in {round(time.time() - start, 2)}s")
    db = connect_to_db()
    existing_products = get_products_by_market(db, MARKET_NAME)
    now = datetime.now()

    products_to_upsert = []
    for key, value in all_products.items():
        existing_id = existing_products.get((key, MARKET_NAME))
        products_to_upsert.append({
            'id': existing_id if existing_id else str(uuid.uuid4()),
            'name': key,
            'price': value[0],
            'image': value[1],
            'link': value[2],
            'singular_price': value[3],
            'description': str(value[4]) if value[4] else None,
            'in_stock': value[5] == 1,
            'market': MARKET_NAME,
            'ETL_loadtime': now,
            'last_updated': now
        })

    save_products_to_products_table(db, MARKET_NAME, products_to_upsert, set(all_products.keys()))
    print(f"Overall done in {round(time.time() - start, 2)}s")
    return all_products


if __name__ == "__main__":
    main()