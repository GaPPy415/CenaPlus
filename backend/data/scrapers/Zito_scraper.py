import asyncio
import aiohttp
import re
import requests

from backend.data.db_utils import *

MARKET_NAME = 'zito'

start = time.time()
url = 'https://bigshop.mk/wp-json/wc/store/v1/products/categories'
response = requests.get(url)
market_categories = response.json()
parent_categories = []
fields = ['id', 'name', 'count']
for category in market_categories:
    if category['parent'] == 0:
        parent_categories.append({"id": category['id'], "name": category['name'], "count": category['count']})

all_products = dict()

def parse_product(product):
    name = product['name']
    price = int(product['prices']['sale_price']) if product['prices']['sale_price'] else 0
    image = product['images'][0]['src'] if product['images'] else ''
    link = product['permalink']
    singular_price = product['price_html']
    singular_price = re.split(r'<[^>]+>', singular_price)
    if len(singular_price) < 13:
        singular_price = ""
    else:
        singular_price = singular_price[9] + singular_price[12] + "ден"
    in_stock = 1
    categories = [cat['name'] for cat in product['categories']]
    return name, [price, image, link, singular_price, categories, in_stock]


async def fetch_page(session, parent, page, semaphore):
    async with semaphore:
        cat_url = f"https://bigshop.mk/wp-json/wc/store/v1/products?category={parent['id']}&per_page=100&page={page}"
        print(f"Fetching products for category: {parent['name']} Page: {page}")
        async with session.get(cat_url) as response:
            if response.status != 200:
                print(f"Error {response.status} for {parent['name']} page {page}, skipping")
                return []
            result = await response.json()
        return result


async def fetch_all_products():
    semaphore = asyncio.Semaphore(20)  # Limit concurrent requests - RateLimit
    async with aiohttp.ClientSession() as session:
        tasks = []
        for parent in parent_categories:
            pages = parent['count'] // 100 + 1
            for page in range(1, pages + 1):
                tasks.append(fetch_page(session, parent, page, semaphore))

        results = await asyncio.gather(*tasks)

        for products in results:
            for product in products:
                name, data = parse_product(product)
                all_products[name] = data


asyncio.run(fetch_all_products())

print(f"Total products fetched: {len(all_products)}")
print(f"Execution time: {round(time.time() - start, 3)} seconds")

db = connect_to_db()
existing_products = get_products_by_market(db, MARKET_NAME)
products_to_insert = []
products_to_upsert = []
now = datetime.now()

for key, value in all_products.items():
    fields = {
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
    }
    handle_product_for_products_table(products_to_insert, products_to_upsert, existing_products, fields, MARKET_NAME)

save_products_to_products_table(db, MARKET_NAME, products_to_insert, products_to_upsert, set(all_products.keys()))
print(f"Overall done in {round(time.time() - start, 2)} seconds")
