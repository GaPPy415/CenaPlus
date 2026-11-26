"""КАМ — PDF price lists per store (kam.com.mk), parsed by ``kam_pdf_utils``."""

import concurrent.futures
import os
import time
from datetime import datetime

import requests

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct

try:  # script mode: `scrapers/` is on sys.path
    from kam_pdf_utils import extract_name_price
except ImportError:  # module mode: `python -m backend.data.scrapers.Kam_scraper`
    from backend.data.scrapers.kam_pdf_utils import extract_name_price

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

numbers_skopje = [1, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 19, 20, 27, 31, 33, 37, 39, 41, 42, 43, 52, 53, 57, 66, 76, 78, 89, 91, 92, 93, 95, 98]

MARKET_NAME = 'kam'


def download_pdf(url: str, timeout: tuple[int, int] = (10, 30)) -> str | None:
    filename = url.split('/')[-1]
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(r.content)
        return filename
    except Exception:
        # failed download
        return None


def merge_products(dest: dict, new: dict) -> None:
    for k, v in new.items():
        dest[k] = v


class KamScraper(MarketScraper):
    market = MARKET_NAME
    probe_url = "https://kam.com.mk/"
    probe_headers = headers

    def fetch(self) -> list[ScrapedProduct]:
        start = time.time()
        all_products: dict = {}
        download_futures = {}
        extract_futures = {}

        # https://kam.com.mk/2025/11/25/73.pdf
        #  year / month / day / number.pdf
        year = datetime.now().year
        month = f"{datetime.now().month:02d}"
        day = f"{datetime.now().day:02d}"
        urls = [f'https://kam.com.mk/{year}/{month}/{day}/{n}.pdf' for n in numbers_skopje]

        # adjust worker counts to your CPU / network
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as dl_pool, \
             concurrent.futures.ProcessPoolExecutor(max_workers=4) as proc_pool:

            # submit downloads
            for url in urls:
                fut = dl_pool.submit(download_pdf, url)
                download_futures[fut] = url

            # as downloads finish, submit extraction jobs
            for dl_done in concurrent.futures.as_completed(download_futures):
                filename = dl_done.result()
                url = download_futures[dl_done]
                if not filename:
                    print(f"Download failed: {url}")
                    continue
                # submit extraction to process pool
                ef = proc_pool.submit(extract_name_price, filename)
                extract_futures[ef] = filename

            # collect extraction results and cleanup files
            for ex_done in concurrent.futures.as_completed(extract_futures):
                filename = extract_futures[ex_done]
                try:
                    new_products = ex_done.result(timeout=300)
                    if isinstance(new_products, dict):
                        merge_products(all_products, new_products)
                        print(f"Extracted {len(new_products)} from {filename}")
                    else:
                        print(f"No products from {filename}")
                except Exception as e:
                    print(f"Extraction failed for {filename}: {e}")
                finally:
                    # remove temp file
                    try:
                        os.remove(filename)
                    except Exception:
                        pass

        print(f"Total products: {len(all_products)} in {round(time.time() - start, 2)}s")

        return [
            ScrapedProduct(
                name=name,
                price=value[0],
                regular_price=value[2] if len(value) > 2 else None,
                singular_price=value[1],
                # A published price list only lists what is on the shelf today.
                in_stock=True,
            )
            for name, value in all_products.items()
        ]


if __name__ == "__main__":
    raise SystemExit(KamScraper().run())
