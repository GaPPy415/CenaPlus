from backend.db_utils import *
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

BASE_URL = "https://stokomak.proverkanaceni.mk/"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

products = {}
products_lock = Lock()


def get_soup(url: str) -> BeautifulSoup:
    resp = SESSION.get(url, timeout=10)
    resp.raise_for_status()
    return BeautifulSoup(resp.content, "html.parser")


def scrape_page(org_id: int, page_num: int) -> dict:
    """Scrape a single page for a given org and return {product_name: [price, unit_price, category, in_stock]}."""
    url = f"{BASE_URL}?page={page_num}&perPage=10000&search=&org={org_id}"
    soup = get_soup(url)
    table = soup.find("table", class_="table table-bordered table-striped table-hover")
    if not table:
        return {}

    tbody = table.find("tbody")
    if not tbody:
        return {}

    page_products = {}
    for row in tbody.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        product_name = cols[0].text.strip()
        price = cols[1].text.strip()
        unit_price = cols[2].text.strip()
        category = cols[3].text.strip()
        availability = cols[4].text.strip()
        in_stock = 1 if "Да" in availability else 0

        # Local merge logic for this page only
        if product_name not in page_products:
            page_products[product_name] = [int(price[0:-5]), unit_price, category, in_stock]
        else:
            if in_stock == 1:
                page_products[product_name][3] = 1

    return page_products


def scrape_market(org_id: int, max_workers_pages: int = 8) -> dict:
    """Scrape all pages for a given market (org) using threads for pages."""
    print(f"Scraping market: {org_id}")
    # First request to get number of products and pages
    url = f"{BASE_URL}?org={org_id}&search=&perPage=10"
    soup = get_soup(url)

    # Extract product count
    p_tag = soup.find("p")
    if not p_tag:
        print(f"Could not find product count for market {org_id}")
        return {}

    try:
        num_products = int(p_tag.text.strip().split(" ")[-2])
    except (ValueError, IndexError):
        print(f"Failed to parse product count for market {org_id}: {p_tag.text.strip()}")
        return {}

    num_pages = (num_products // 100) + 1
    market_products = {}

    # Parallelize pages
    with ThreadPoolExecutor(max_workers=max_workers_pages) as executor:
        future_to_page = {
            executor.submit(scrape_page, org_id, page_num): page_num
            for page_num in range(1, num_pages + 1)
        }

        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                page_result = future.result()
                # Merge page results into market_products
                for name, values in page_result.items():
                    if name not in market_products:
                        market_products[name] = values
                    else:
                        # Update stock if any page has it in stock
                        if values[3] == 1:
                            market_products[name][3] = 1
            except Exception as e:
                print(f"Error scraping org {org_id}, page {page_num}: {e}")

    print(f"Scraped market {org_id}, products in this market: {len(market_products)}")
    return market_products


def main():
    start = time.time()
    # Get list of markets
    soup = get_soup(BASE_URL)
    markets_numbers_select = soup.find_all("select", class_="form-select")
    if not markets_numbers_select:
        print("Could not find markets select")
        return

    markets_numbers = [
        int(option["value"])
        for option in markets_numbers_select[0].find_all("option")[1:]
        if option.get("value")
    ]

    print(f"Found {len(markets_numbers)} markets")

    # Thread pool over markets
    max_workers_markets = 10
    with ThreadPoolExecutor(max_workers=max_workers_markets) as executor:
        future_to_org = {
            executor.submit(scrape_market, org_id): org_id
            for org_id in markets_numbers
        }

        for future in as_completed(future_to_org):
            org_id = future_to_org[future]
            try:
                market_products = future.result()
                # Merge into global products with lock
                with products_lock:
                    for name, values in market_products.items():
                        if name not in products:
                            products[name] = values
                        else:
                            if values[3] == 1:
                                products[name][3] = 1
                # print(f"Total unique products so far: {len(products)} (after org {org_id})")
            except Exception as e:
                print(f"Error scraping market {org_id}: {e}")

    print(f"Total unique products scraped: {len(products)}")
    print("Done in ", round(time.time() - start, 2), " seconds")
    # Save to mongoDB
    save = time.time()
    collection = "stokomak_products"
    db = connect_to_db(collection)
    fields = {
        'name': 'VARCHAR(255)',
        'price': "INTEGER",
        'singular_price': "INTEGER",
        'category': 'VARCHAR(255)',
        'in_stock': 'INTEGER'
    }
    db_products = get_products_from_table(db, collection)
    create_table(db, collection, fields)
    names_ids = {prod['name']: prod['id'] for prod in db_products}
    products_to_insert = []
    products_to_upsert = []

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
