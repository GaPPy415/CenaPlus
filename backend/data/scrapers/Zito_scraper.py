import requests
import time
import json
import html
import uuid
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from backend.data.db_utils import *

MAX_WORKERS = 6
MARKET_NAME = 'zito'
PER_PAGE = 100
BASE_URL = 'https://bigshop.mk/wp-json/wc/store/v1/products'


def parse_price(price_str: str, minor_unit: int) -> int:
    try:
        return int(price_str) // (10 ** minor_unit)
    except (ValueError, TypeError):
        return 0


def parse_product(product: dict) -> tuple[str, list]:
    name = html.unescape(product.get('name', ''))
    prices = product.get('prices', {})
    minor_unit = prices.get('currency_minor_unit', 2)

    # Use 'price' which works for both regular and sale
    price = parse_price(prices.get('price', '0'), minor_unit)

    images = product.get('images', [])
    image = images[0]['src'] if images else ''
    link = product.get('permalink')

    # Legacy singular price parsing from original Zito scraper
    singular_price_raw = product.get('price_html', '')
    singular_price = None
    if singular_price_raw:
        parts = re.split(r'<[^>]+>', singular_price_raw)
        if len(parts) >= 13:
            # Reconstruct: index 9 + index 12 + "ден"
            # Original: singular_price = singular_price[9] + singular_price[12] + "ден"
            try:
                val = parts[9] + parts[12] + "ден"
                singular_price = val
            except IndexError:
                pass

    in_stock = 1 if product.get('is_in_stock') else 0
    categories = [cat['name'] for cat in product.get('categories', [])]

    return name, [price, image, link, singular_price, categories, in_stock]


def fetch_page(page: int):
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


def main():
    start = time.time()

    # Initial request to get total pages
    try:
        url = f"{BASE_URL}?per_page={PER_PAGE}&page=1"
        response = requests.get(url, timeout=45)
        # If response is not 200, raise
        response.raise_for_status()

        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
    except Exception as e:
        print(f"Failed to fetch initial page/metadata: {e}")
        return

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

    print(f"Scraping done in {round(time.time() - start, 2)}s, total products: {len(all_products)}")
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


if __name__ == "__main__":
    main()
