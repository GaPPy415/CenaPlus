"""Веро — HTML price list (pricelist.vero.com.mk).

The site is a static export: one index page, one page per shop category and
`N.html` for the following pages until a 404. Category pages are scraped in
parallel, and each category walks its pages in small parallel batches (the
tail of a batch may hit 404s, which simply end the category).
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

BASE_URL = 'https://pricelist.vero.com.mk/'
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})
MAX_WORKERS = 10  # shop categories in parallel
PAGE_BATCH = 3  # pages of one category in parallel
MAX_PAGES = 2000

MARKET_NAME = 'vero'


def fetch_page(page_link: str) -> tuple[int | None, bytes | None]:
    """GET one page; returns (status, body) or (None, None) on a network error."""
    try:
        page = SESSION.get(page_link, timeout=15)
        return page.status_code, page.content
    except requests.RequestException as e:
        print(f"Error fetching {page_link}: {e}")
        return None, None


def parse_page(content: bytes) -> list[ScrapedProduct]:
    """Rows of one price-list page."""
    soup = BeautifulSoup(content, 'html.parser')
    products: list[ScrapedProduct] = []

    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        for row in rows[3:-1]:  # Skip header/footer rows
            cols = [ele.text.strip() for ele in row.find_all('td')]
            if len(cols) < 5:
                continue

            try:
                price = int(cols[1])
            except ValueError:
                continue  # Skip if price is not a valid integer

            products.append(
                ScrapedProduct(
                    name=cols[0],
                    price=price,
                    singular_price=cols[2],
                    description=cols[4],
                    in_stock=cols[3] == 'Да',
                )
            )
    return products


def scrape_shop(shop_link: str) -> list[ScrapedProduct]:
    """Scrape all pages for a single shop category and return its products."""
    shop_products: list[ScrapedProduct] = []
    base_link = shop_link[:-6]  # Removes '1.html' or similar
    counter = 1

    while counter <= MAX_PAGES:
        batch = list(range(counter, counter + PAGE_BATCH))
        links = [f"{base_link}{number}.html" for number in batch]

        with ThreadPoolExecutor(max_workers=len(links)) as executor:
            responses = list(executor.map(fetch_page, links))

        stop = False
        for page_link, (status, content) in zip(links, responses):
            if status is None or status == 404:
                stop = True  # end of this category (or a broken page)
                break
            if status >= 400:
                print(f"Error fetching {page_link}: HTTP {status}")
                stop = True
                break

            print(f"Processing shop link: {page_link} -- ")
            shop_products.extend(parse_page(content))

        if stop:
            break
        counter += PAGE_BATCH

    return shop_products


class VeroScraper(MarketScraper):
    market = MARKET_NAME

    def fetch(self) -> list[ScrapedProduct]:
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

        products: list[ScrapedProduct] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_link = {
                executor.submit(scrape_shop, link): link for link in links_to_scrape
            }
            for future in as_completed(future_to_link):
                products.extend(future.result())
                print(f"Finished shop. Total unique products so far: {len(products)}")
        return products


if __name__ == "__main__":
    raise SystemExit(VeroScraper().run())
