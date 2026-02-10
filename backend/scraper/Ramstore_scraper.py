import pandas as pd
from tqdm import tqdm
from backend.db_utils import *

start = time.time()

stores = [
    "https://ramstore.com.mk/marketi/ramstore-vardar/",
    "https://ramstore.com.mk/marketi/ramstore-siti-mol/",
    "https://ramstore.com.mk/marketi/ramstore-taftalidhe/",
    "https://ramstore.com.mk/marketi/ramstore-karposh/",
    "https://ramstore.com.mk/marketi/ramstore-dhevahir/",
    "https://ramstore.com.mk/marketi/ramstore-park/",
    "https://ramstore.com.mk/marketi/ramstore-gorno-lisiche/",
    "https://ramstore.com.mk/marketi/ramstore-kapitol/",
    "https://ramstore.com.mk/marketi/ramstore-kapishtecz/",
    "https://ramstore.com.mk/marketi/ramstore-debar-maalo/",
    "https://ramstore.com.mk/marketi/ramstore-vodno/",
    "https://ramstore.com.mk/marketi/ramstore-aerodrom/",
    "https://ramstore.com.mk/marketi/ramstore-star-aerodrom/",
    "https://ramstore.com.mk/marketi/ramstore-michurin/",
    "https://ramstore.com.mk/marketi/ramstore-sever/",

    # "https://ramstore.com.mk/marketi/ramstore-tetovo/",
    # "https://ramstore.com.mk/marketi/ramstore-tetovo-3/",
    # "https://ramstore.com.mk/marketi/ramstore-topansko-pole/",
    # "https://ramstore.com.mk/marketi/ramstore-struga/",
    # "https://ramstore.com.mk/marketi/ramstore-ohrid/",
    # "https://ramstore.com.mk/marketi/ramstore-ohrid-ekspress/",
    # "https://ramstore.com.mk/marketi/ramstore-ohrid-turistichka/",
    # "https://ramstore.com.mk/marketi/ramstore-kichevo/",
    # "https://ramstore.com.mk/marketi/ramstore-kumanovo/",
    # "https://ramstore.com.mk/marketi/ramstore-strumicza/",
    # "https://ramstore.com.mk/marketi/ramstore-strumicza-bulevar/"
]

df = pd.DataFrame()
now = datetime.now()
date_time = now.strftime("%d-%m-%Y")

def extract_dates(period):
    if pd.isna(period) or period == '':
        return None, None
    try:
        from_date, to_date = period.split(' - ')
        return from_date.strip(), to_date.strip()
    except ValueError:
        return None, None

for store in tqdm(stores):
    counter = 1
    store_prefix = store.rsplit('_', 1)[0]
    store_id = store.rsplit('_', 1)[0].split('marketi/')[1].replace('/', '')
    df_new = pd.read_html(store)[0]
    df_new["storeId"] = store_id

    if df_new.empty:
        print(f"No data found for store: {store}")
        continue

    df_new[['promotionDateFrom', 'promotionDateTo']] = df_new['ВРЕМЕТРАЕЊЕ НА АКЦИЈА'].apply(
        lambda x: pd.Series(extract_dates(x))
    )
    df_new = df_new.drop(columns='ВРЕМЕТРАЕЊЕ НА АКЦИЈА')
    df = pd.concat([df, df_new])

# python
product_col = df.columns[0]
items_map = {}
cena_popust = 'ЦЕНА СО ПОПУСТ'

for _, row in df.iterrows():
    name = str(row[product_col]).strip()
    data = {}
    for col in df.columns:
        if col == product_col:
            continue
        val = row[col]
        data[col] = None if pd.isna(val) else val
    items_map[name]=[int(data['ПРОДАЖНА ЦЕНА']), data['ОПИС НА ПРОИЗВОД'], data['ЕДИНЕЧНА ЦЕНА']]

    if cena_popust in data.keys() and data[cena_popust] is not None:
        price = data[cena_popust].rsplit(' ', 1)[0]
        items_map[name][0]=int(price[:-3])

print(f"Products scraped: {len(items_map)}")
print(f"Scraping finished in {round(time.time() - start, 3)} seconds.")

collection = "ramstore_products"
db = connect_to_db(collection)

fields={
    "name": "",
    "price": 0,
    "description": "",
    "singular_price": 0,
    "in_stock": 1
}

db_products = db[collection].find()
names_ids = {prod['name']: prod['_id'] for prod in db_products}
products_to_insert = []
products_to_upsert = []

for key, value in items_map.items():
    fields['name'] = key
    fields['price'] = value[0]
    fields['description'] = value[1]
    fields['singular_price'] = value[2]
    fields['in_stock'] = 1
    handle_product(products_to_insert, products_to_upsert, names_ids, fields)

save_products(db, collection, products_to_insert, products_to_upsert, items_map)
print(f"Overall done in {round(time.time() - start, 2)}s")