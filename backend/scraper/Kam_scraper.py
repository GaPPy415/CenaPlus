import concurrent.futures
import requests
from backend.db_utils import *
from funcs import extract_name_price

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

stores = ["https://kam.com.mk/price_markets/madzari/",
"https://kam.com.mk/price_markets/novo-lisiche-1/",
"https://kam.com.mk/price_markets/drachevo/",
"https://kam.com.mk/price_markets/valandovo/",
"https://kam.com.mk/price_markets/kozle-1/",
"https://kam.com.mk/price_markets/gtts/",
"https://kam.com.mk/price_markets/kapishtets-nov/",
"https://kam.com.mk/price_markets/chento/",
"https://kam.com.mk/price_markets/sever/",
"https://kam.com.mk/price_markets/avtokomanda/",
"https://kam.com.mk/price_markets/taftalidhe/",
"https://kam.com.mk/price_markets/bis-oil-vlae/",
"https://kam.com.mk/price_markets/kam-tsentar/",
"https://kam.com.mk/price_markets/chair/",
"https://kam.com.mk/price_markets/nastel-2/",
"https://kam.com.mk/price_markets/madhari-2/",
"https://kam.com.mk/price_markets/gorche/",
"https://kam.com.mk/price_markets/bisera/",
"https://kam.com.mk/price_markets/karposh/",
"https://kam.com.mk/price_markets/kisela-voda/",
"https://kam.com.mk/price_markets/skopjanka/",
"https://kam.com.mk/price_markets/michurin/",
"https://kam.com.mk/price_markets/aerodrom-3/",
"https://kam.com.mk/price_markets/novo-lisiche-2/",
"https://kam.com.mk/price_markets/butel/",
"https://kam.com.mk/price_markets/kisela-voda-2/",
"https://kam.com.mk/price_markets/mvr/",
"https://kam.com.mk/price_markets/karposh-4/",
"https://kam.com.mk/price_markets/bunakovets/",
"https://kam.com.mk/price_markets/porta-nov/",
"https://kam.com.mk/price_markets/tsrniche/",
"https://kam.com.mk/price_markets/debarmaalo/",
"https://kam.com.mk/price_markets/dhon-kenedi-2/",
"https://kam.com.mk/price_markets/mlechen/",
"https://kam.com.mk/price_markets/riteil-park/",
# "https://kam.com.mk/price_markets/bitola-1/",
# "https://kam.com.mk/price_markets/shtip-1/",
# "https://kam.com.mk/price_markets/gostivar-2/",
# "https://kam.com.mk/price_markets/kumanovo-2/",
# "https://kam.com.mk/price_markets/kichevo/",
# "https://kam.com.mk/price_markets/ohrid-1/",
# "https://kam.com.mk/price_markets/tetovo-5/",
# "https://kam.com.mk/price_markets/kriva-palanka/",
# "https://kam.com.mk/price_markets/shtip-2/",
# "https://kam.com.mk/price_markets/veles/",
# "https://kam.com.mk/price_markets/bitola-2/",
# "https://kam.com.mk/price_markets/gostivar-1/",
# "https://kam.com.mk/price_markets/shtip-3/",
# "https://kam.com.mk/price_markets/kumanovo-1/",
# "https://kam.com.mk/price_markets/kochani/",
# "https://kam.com.mk/price_markets/kavadartsi/",
# "https://kam.com.mk/price_markets/strumitsa/",
# "https://kam.com.mk/price_markets/ohrid-2/",
# "https://kam.com.mk/price_markets/prilep-3/",
# "https://kam.com.mk/price_markets/debar/",
# "https://kam.com.mk/price_markets/radishani/",
# "https://kam.com.mk/price_markets/drachevo-2/",
# "https://kam.com.mk/price_markets/veles-2/",
# "https://kam.com.mk/price_markets/ohrid-4/",
# "https://kam.com.mk/price_markets/strumitsa-2/",
# "https://kam.com.mk/price_markets/struga-3/",
# "https://kam.com.mk/price_markets/tetovo-4/",
# "https://kam.com.mk/price_markets/novoselski-pat/",
# "https://kam.com.mk/price_markets/struga-4/",
# "https://kam.com.mk/price_markets/ilinden/",
# "https://kam.com.mk/price_markets/saraj/",
# "https://kam.com.mk/price_markets/negotino/",
# "https://kam.com.mk/price_markets/radovish/",
# "https://kam.com.mk/price_markets/resen/",
# "https://kam.com.mk/price_markets/prilep-4/",
# "https://kam.com.mk/price_markets/gents/",
# "https://kam.com.mk/price_markets/probishtip/",
# "https://kam.com.mk/price_markets/tetovo-prima/",
# "https://kam.com.mk/price_markets/bitola-3/",
# "https://kam.com.mk/price_markets/prilep-5/",
# "https://kam.com.mk/price_markets/delchevo/",
# "https://kam.com.mk/price_markets/gevgelija-2/",
# "https://kam.com.mk/price_markets/prilep-6/",
# "https://kam.com.mk/price_markets/kumanovo-3/",
# "https://kam.com.mk/price_markets/petrovets/",
# "https://kam.com.mk/price_markets/vinitsa/",
# "https://kam.com.mk/price_markets/tetovo-remol/",
# "https://kam.com.mk/price_markets/veles-3/",
# "https://kam.com.mk/price_markets/bitola-4/",
# "https://kam.com.mk/price_markets/strumitsa-3/",
# "https://kam.com.mk/price_markets/gostivar-3/"
]
numbers_all_markets = [1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 26, 27, 29, 31, 32, 33, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 60, 61, 62, 64, 65, 66, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 97, 98, 99]
numbers_skopje = [1, 3, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 17, 19, 20, 27, 31, 33, 37, 39, 41, 42, 43, 52, 53, 57, 66, 76, 78, 89, 91, 92, 93, 95, 98]

def download_pdf(url: str, timeout: int = 30) -> str | None:
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

if __name__ == "__main__":
    start = time.time()
    all_products = {}
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
    print("Saving to MongoDB kam_products collection")

    collection = "kam_products"
    db = connect_to_db(collection)
    fields = {
        'name': '',
        'price': 0,
        'singular_price': 0,
        'in_stock': 1
    }

    db_products = db[collection].find()
    names_ids = {prod['name']: prod['_id'] for prod in db_products}
    products_to_insert = []
    products_to_upsert = []

    for key, value in all_products.items():
        fields['name'] = key
        fields['price'] = value[0]
        fields['singular_price'] = value[1]
        fields['in_stock'] = 1
        handle_product(products_to_insert, products_to_upsert, names_ids, fields)

    save_products(db, collection, products_to_insert, products_to_upsert, all_products)
    print(f"Overall done in {round(time.time() - start, 2)}s")
