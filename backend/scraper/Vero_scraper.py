import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from backend.db_utils import *

# --- Configuration ---
BASE_URL = 'https://pricelist.vero.com.mk/'
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
MAX_WORKERS = 10  # Adjust the number of threads as needed

# --- Global shared data ---
products = {}
products_lock = Lock()


def scrape_shop(shop_link: str):
    """Scrapes all pages for a single shop category and returns its products."""
    shop_products = {}
    counter = 1
    base_link = shop_link[:-6]  # Removes '1.html' or similar

    while True:
        page_link = base_link + f"{counter}.html"
        try:
            page = SESSION.get(page_link, timeout=10)
            if page.status_code == 404:
                break  # No more pages in this category
            page.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

            print(f"Processing shop link: {page_link} -- ")
            soup = BeautifulSoup(page.content, 'html.parser')
            tables = soup.find_all('table')

            for table in tables:
                rows = table.find_all('tr')
                for row in rows[3:-1]:  # Skip header/footer rows
                    cols = [ele.text.strip() for ele in row.find_all('td')]
                    if len(cols) < 5:
                        continue

                    name = cols[0]
                    try:
                        price = int(cols[1])
                    except ValueError:
                        continue  # Skip if price is not a valid integer

                    unit_price = cols[2]
                    in_stock = 1 if cols[3] == 'Да' else 0
                    category = cols[4]

                    # Use local dictionary to avoid locking on every product
                    if name not in shop_products:
                        shop_products[name] = [price, unit_price, category, in_stock]

            counter += 1
        except requests.RequestException as e:
            print(f"Error fetching {page_link}: {e}")
            break  # Stop trying this category on network error

    return shop_products


def main():
    """Main function to orchestrate the scraping process."""
    start = time.time()

    # 1. Get all shop category links from the main page
    index_page = SESSION.get(BASE_URL + "index.html")
    index_page.raise_for_status()
    soup = BeautifulSoup(index_page.content, 'html.parser')

    shop_links = []
    for shop in soup.find_all('td'):
        a_tag = shop.find('a')
        if a_tag and a_tag.has_attr('href'):
            shop_links.append(BASE_URL + a_tag['href'])

    # Exclude first and last two links as in the original script
    links_to_scrape = shop_links[1:-2]
    print(f"Found {len(links_to_scrape)} shop categories to scrape.")

    # 2. Use a thread pool to scrape all categories in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all scraping tasks
        future_to_link = {executor.submit(scrape_shop, link): link for link in links_to_scrape}

        for future in as_completed(future_to_link):
            shop_products = future.result()
            if shop_products:
                # Safely merge results into the global dictionary
                with products_lock:
                    products.update(shop_products)
                print(f"Finished shop. Total unique products so far: {len(products)}")

    print(f"\nTotal unique products: {len(products)}")
    print(f"Scraping took {round(time.time() - start, 2)}s")
    print("Saving to MongoDB vero_products collection")
    collection = "vero_products"
    db = connect_to_db(collection)
    products_to_insert = []
    products_to_upsert = []
    fields = {
        'name': 'VARCHAR(255)',
        'price': 'INTEGER',
        'singular_price': 'INTEGER',
        'category': 'VARCHAR(255)',
        'in_stock': 'INTEGER'
    }

    db_products = get_products_from_table(db, collection)
    create_table(db, collection, fields)
    names_ids = {prod['name']: prod['id'] for prod in db_products}

    for key, value in products.items():
        fields['name'] = key
        fields['price'] = value[0]
        fields['singular_price'] = value[1]
        fields['category'] = value[2]
        fields['in_stock'] = value[3]
        handle_product(products_to_insert, products_to_upsert, names_ids, fields)

    save_products(db, collection, products_to_insert, products_to_upsert, products)
    print(f"Overall done in {round(time.time() - start, 2)}s")

if __name__ == "__main__":
    main()
