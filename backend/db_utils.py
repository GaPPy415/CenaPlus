import time
import uuid
from datetime import datetime
from typing import Tuple, List
import pymongo
import os
from dotenv import load_dotenv, find_dotenv
from pymongo import TEXT
from urllib.parse import quote_plus
from pymongo import UpdateOne

def connect_to_db(collection: str = 'all_products') -> pymongo.database.Database:
    load_dotenv(find_dotenv())
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    db_name = os.getenv("MONGO_DB", "products")
    auth_source = os.getenv("MONGO_AUTHSOURCE", "admin")  # 'admin' if using root user

    # Safely encode credentials
    u = quote_plus(username)
    p = quote_plus(password)

    # Build URI; adjust authSource if your user belongs to a specific DB
    uri = f"mongodb://{u}:{p}@{host}:{port}/{db_name}?authSource={auth_source}"

    # Connect and ping
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")  # raises if not reachable/auth fails

    db = client[db_name]
    if collection not in db.list_collection_names():
        db.create_collection(collection)
        # make name_text unique index on name field
        db[collection].create_index([("name", TEXT)], unique=False)
    print(f"Connected to MongoDB at {host}:{port}, db '{db_name}'")

    return db

def handle_product(products_to_insert: list, products_to_upsert: list, names_ids: dict, fields: dict):
    key = fields['name']
    if key in names_ids.keys():
        fields['_id'] = names_ids[key]
        products_to_upsert.append(fields.copy())
    else:
        fields['_id'] = str(uuid.uuid4())
        products_to_insert.append(fields.copy())

def mark_out_of_stock(db: pymongo.database.Database, collection: str, products: dict):
    in_stock_names = set(products.keys())
    products_in_db = db[collection].find()
    for product in products_in_db:
        key = product['name']
        if key not in in_stock_names and product.get('in_stock', 1) != 0:
            # mark as out of stock
            # print("Marking out of stock:", key)
            db[f"{collection}"].update_one({'name': key}, {'$set': {'in_stock': 0}})

# Deprecated because slow, use bulk_upsert instead
def upsert_product(fields: dict, product, db: pymongo.database.Database, collection: str, products_to_insert: list):
    if product is not None:
        if fields['price'] == product["price"] and product['in_stock']==1: # No change in price, product in stock
            return
        else:
            for field, value in fields.items():
                 product[field] = value
            db[f"{collection}"].update_one({'name': fields['name']}, {'$set': product})
    else:
        products_to_insert.append({'_id': str(uuid.uuid4()), **fields})

def bulk_upsert(db, collection: str, products: list, key_field: str = 'name'):
    """
    Bulk upsert products with any fields.

    Args:
        db: MongoDB database object
        collection: Collection name
        products: List of dicts with '_id' and fields to update
        key_field: Field to match on (default: 'name')
    """
    if not products:
        return

    operations = []
    for prod in products:
        # Get all fields except '_id' for the $set operation
        update_fields = {k: v for k, v in prod.items() if k != '_id'}

        operations.append(
            UpdateOne(
                {'_id': prod['_id']},
                {'$set': update_fields},
                upsert=True
            )
        )

    result = db[collection].bulk_write(operations)
    print(f"Bulk upsert: {result.upserted_count} inserted, {result.modified_count} modified")
    return result

def save_products(db, collection: str, products_to_insert: list, products_to_upsert: list, all_products: dict):
    """
    Save products to MongoDB with bulk operations and mark out of stock.

    Args:
        db: MongoDB database object
        collection: Collection name
        products_to_insert: List of products to insert
        products_to_upsert: List of products to upsert
        all_products: Dict of all scraped products (to mark out of stock)
    """
    save_start = time.time()

    if products_to_upsert:
        bulk_upsert(db, collection, products_to_upsert)

    if products_to_insert:
        db[collection].insert_many(products_to_insert)

    print(f"Saved to MongoDB in {round(time.time() - save_start, 2)}s")

    mark_start = time.time()
    mark_out_of_stock(db, collection, all_products)
    print(f"Marked out of stock products in {round(time.time() - mark_start, 2)}s")

    db.client.close()

def load_products_to_categorize(db, limit_per_collection: int = None) -> Tuple[List[dict], dict]:
    """
    Load products from MongoDB that need categorization.

    Args:
        db: MongoDB database object
        limit_per_collection: Max products per collection (None = all)

    Returns:
        (products_list, products_to_markets_mapping)
    """
    products = []
    products_markets = {}

    collections = [c for c in db.list_collection_names() if not c.startswith("products_categorized") and c!='all_products']

    print(f"📂 Loading products from {len(collections)} collections...")

    for collection in collections:
        query = {}
        cursor = db[collection].find(query)

        if limit_per_collection:
            cursor = cursor.limit(limit_per_collection)

        collection_count = 0

        for product in cursor:
            # Check if already categorized
            existing = db['products_categorized'].find_one({'_id': product['_id']})
            if existing and existing.get('categorization', {}).get('main_category') and existing['categorization']['reasoning'] != "Missing from batch response":
                continue  # Skip already categorized

            # Extract description from various possible fields
            description = ""
            for field in ['description', 'category', 'categories']:
                if field in product:
                    desc_value = product[field]
                    if isinstance(desc_value, list):
                        description = ", ".join(str(x) for x in desc_value)
                    else:
                        description = str(desc_value)
                    break

            # Create normalized product
            new_product = {
                '_id': product.get('_id', ''),
                'name': product.get('name', ''),
                'description': description,
                'existing_categories': description  # Use same field for existing categories
            }

            products.append(new_product)
            products_markets[product['_id']] = collection
            collection_count += 1

        print(f"   {collection}: {collection_count} products")

    print(f"📊 Total products to categorize: {len(products)}")
    return products, products_markets

def save_categorizations_to_db(db, products: List[dict], products_markets: dict):
    """
    Save categorized products to MongoDB.

    Args:
        db: MongoDB database object
        products: List of categorized products
        products_markets: Mapping of product_id to source market
    """
    print(f"\n💾 Saving {len(products)} categorizations to database...")

    to_insert = []
    updated_count = 0

    for product in products:
        product['market'] = products_markets.get(product['_id'], 'unknown')
        product['categorized_at'] = datetime.now()

        # Try to update existing
        result = db['products_categorized'].update_one(
            {'_id': product['_id']},
            {'$set': {
                'categorization': product['categorization'],
                'categorized_at': product['categorized_at']
            }},
            upsert=False
        )

        if result.matched_count > 0:
            updated_count += 1
        else:
            to_insert.append(product)

    # Bulk insert new ones
    if to_insert:
        if len(to_insert) == 1:
            db['products_categorized'].insert_one(to_insert[0])
        else:
            db['products_categorized'].insert_many(to_insert)

    print(f"   Updated: {updated_count}")
    print(f"   Inserted: {len(to_insert)}")
    print(f"   Total saved: {len(products)}")

if __name__ == "__main__":
    db = connect_to_db()
    # 'categorziation' is a dictionary with keys: 'main_category', 'sub_category', 'confidence' and 'reasoning
    # get all products with main_category=='None'
    products = db['products_categorized'].find({'categorization.main_category':None})
    ct=0
    for p in products:
        ct+=1
        # print("product:", p['name'])
        # time.sleep(1)
        # print(p['categorization']['reasoning'])
        # print(p['categorization']['reasoning'].split('.')[1].strip().split(' ')[-1])
        if "Request too large for gpt-4o-mini" in p['categorization']['reasoning']:
            print(p['categorization']['reasoning'])
        # else:
        #     continue
        # seconds = p['categorization']['reasoning'].split('.')[1].strip().split(' ')[-1]
        # if seconds[-2:]=='ms':
        #     seconds = int(seconds[:-2])
        # else:
        #     seconds = (int(seconds)+1)*1000
        # print(f"Updating product {p['name']} with {seconds}ms delay")
    print(ct)
    # products = []
    # for collection in db.list_collection_names():
    #     for product in db[collection].find():
    #         if "чипс" in product['name'].lower():
    #             products.append(product)
    #
    # print(f"Found {len(products)} products with 'млеко' and 'битолско' in the name.")
    # for p in products:
    #     print(p['name'])
    db.client.close()


