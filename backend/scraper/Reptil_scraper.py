from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import unquote
from backend.db_utils import *

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
MAX_CATEGORY_WORKERS = 2
MAX_PAGE_WORKERS = 3
REQUEST_TIMEOUT = 10  # seconds
RETRIES = 3
BACKOFF_FACTOR = 1  # exponential backoff factor
SLEEP_MIN = 0.3
SLEEP_MAX = 1.0

logger.info("Starting Reptil scraper")

# Create a session with Retry/backoff and keep‑alive
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/58.0.3029.110 Safari/537.3'
})
retry_strategy = Retry(
    total=RETRIES,
    backoff_factor=BACKOFF_FACTOR,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def scrape_single_page(url):
    """Scrape a single page and return the products found"""
    categories = unquote(url).split('/')
    # print(categories[4:-3])
    categories = categories[4:-3]
    tries = 0
    while tries <= RETRIES:
        try:
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, 'html.parser')
            products = soup.find_all('div', class_='wd-product')
            logger.info(f"Scraped category - found {len(products)} products")

            results = {}

            for product in products:
                title = product.h3.text.strip() if getattr(product, 'h3', None) else None
                img = product.img['src'] if product.img and product.img.get('src') else None
                href = product.a['href'] if product.a and product.a.get('href') else None
                price_wrapper = product.find('span', class_='price')
                bdis = price_wrapper.find_all('bdi')
                out_of_stock = 0 if product.find('span', class_='out-of-stock') is not None else 1
                # price bdi text has \xa and "ден" at the end
                price = bdis[0].text.replace('\xa0ден', '').strip(' ')
                # price might be over 1000 and have a comma
                price = int(price.replace(',', '')) if price is not None else None
                singular_price = bdis[1].text if len(bdis) >= 2 else None  # "282.86 ден"

                discounted_price = product.find('span', class_='woocommerce-Price-amount amount')
                if len(discounted_price) > 2:
                    price = discounted_price[1].bdi.text.replace('\xa0ден', '').strip(' ')
                    singular_price = discounted_price[3].bdi.text.replace('\xa0ден', '').strip(' ')

                singular_price_suffix = product.find('span', class_='mcmp-recalc-price-suffix')


                if singular_price_suffix is not None:
                    singular_price += singular_price_suffix.text.strip()

                if price is None:
                    print(f"Could not find price for product {title} on page {url}")

                results[title] = [price, img, href, singular_price, categories, out_of_stock]

                # except Exception as e:
                #     logger.debug(f"Error processing product: {e}")
            return results

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error for {url}: {e} (try {tries + 1}/{RETRIES})")
            tries += 1
            if tries > RETRIES:
                logger.error(f"Failed to fetch {url} after {RETRIES} retries: {e}")
                return {}
            time.sleep(BACKOFF_FACTOR * (2 ** (tries - 1)) + random.uniform(0, 0.5))
        except Exception as e:
            logger.error(f"Unexpected error scraping {url}: {e}")
            return {}

def scrape_category(category_info):
    """Scrape all pages for a given category"""
    category_url, count = category_info
    page_count = (count + 99) // 100
    page_urls = [f"{category_url}page/{i}/?per_page=100" for i in range(1, page_count + 1)]

    results = {}
    with ThreadPoolExecutor(max_workers=MAX_PAGE_WORKERS) as executor:
        page_results = list(executor.map(scrape_single_page, page_urls))

    for page_result in page_results:
        results.update(page_result or {})
    return results


def main():
    start = time.time()
    link = 'https://marketonline.mk/%d0%ba%d0%b0%d1%82%d0%b5%d0%b3%d0%be%d1%80%d0%b8%d0%b8/'
    try:
        resp = session.get(link, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        categories = soup.find_all('div', class_='wd-col')

        category_info_list = []
        for category in categories[1:]:
            try:
                count = int(category.text.strip().split('\t')[-1].split(' ')[0])
                category_url = category.a['href']
                category_info_list.append((category_url, count))
                logger.info(f"Category: {unquote(category_url)} - Products: {count}")
                logger.info(f"New category - Products: {count}")
            except Exception as e:
                logger.debug(f"Error processing category: {e}")

        all_products = {}
        with ThreadPoolExecutor(max_workers=MAX_CATEGORY_WORKERS) as executor:
            category_results = list(executor.map(scrape_category, category_info_list))

        for category_result in category_results:
            all_products.update(category_result or {})

        logger.info(f"Total products scraped: {len(all_products)}")

        logger.info(f"Done scraping in {round(time.time() - start, 3)} seconds")


        collection = "reptil_products"
        db = connect_to_db(collection)

        fields = {
            'name': 'VARCHAR(255)',
            'price': "INTEGER",
            'image': 'VARCHAR(255)',
            'link': 'VARCHAR(255)',
            'singular_price': 'VARCHAR(255)',
            'categories': 'VARCHAR(255)',
            'in_stock': 'INTEGER'
        }
        create_table(db, collection, fields)

        db_products = get_products_from_table(db, collection)
        names_ids = {prod['name']: prod['id'] for prod in db_products}

        products_to_insert = []
        products_to_upsert = []

        for key, value in all_products.items():
            fields = {
                'name': key,
                'price': value[0],
                'image': value[1],
                'link': value[2],
                'singular_price': value[3],
                'categories': value[4],
                'in_stock': value[5]
            }
            handle_product(products_to_insert, products_to_upsert, names_ids, fields)

        save_products(db, collection, products_to_insert, products_to_upsert, all_products)
        print(f"Overall done in {round(time.time() - start, 2)}s")

        return all_products

    except requests.exceptions.RequestException as e:
        logger.error(f"Error in main scraper: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error in main scraper: {e}")
        return {}


if __name__ == "__main__":
    main()
