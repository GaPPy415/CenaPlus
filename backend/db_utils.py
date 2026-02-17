import time
import uuid
from datetime import datetime
from typing import Tuple, List
import psycopg2
from psycopg2.extras import execute_batch, execute_values, RealDictCursor
import os
from dotenv import load_dotenv, find_dotenv
from backend.constants import *


def connect_to_db(table: str = None):
    load_dotenv(find_dotenv())
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password")
    )
    print(
        f"Connected to PostgreSQL at {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}, db '{os.getenv('POSTGRES_DB')}'")
    return conn


def get_table_columns(conn, table: str) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns


def handle_product(products_to_insert: list, products_to_upsert: list, names_ids: dict, fields: dict):
    key = fields['name']
    if key in names_ids.keys():
        fields['id'] = names_ids[key]
        products_to_upsert.append(fields.copy())
    else:
        fields['id'] = str(uuid.uuid4())
        products_to_insert.append(fields.copy())


def handle_product_for_products_table(products_to_insert: list, products_to_upsert: list, existing_products: dict, fields: dict, market: str):
    key = (fields['name'], market)
    if key in existing_products:
        fields['id'] = existing_products[key]
        products_to_upsert.append(fields.copy())
    else:
        fields['id'] = str(uuid.uuid4())
        products_to_insert.append(fields.copy())


def mark_out_of_stock(conn, table: str, products: dict):
    in_stock_names = set(products.keys())
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(f"SELECT id, name, in_stock FROM {table}")
    for product in cursor.fetchall():
        key = product['name']
        if key not in in_stock_names and product.get('in_stock', 1) != 0:
            cursor.execute(f"UPDATE {table} SET in_stock = 0 WHERE name = %s", (key,))
    conn.commit()
    cursor.close()


def mark_out_of_stock_products_table(conn, market: str, product_names: set):
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM products WHERE market = %s AND in_stock = true", (market,))
    for row in cursor.fetchall():
        if row[1] not in product_names:
            cursor.execute("UPDATE products SET in_stock = false, last_updated = %s WHERE id = %s", (datetime.now(), row[0]))
    conn.commit()
    cursor.close()


def bulk_upsert(conn, table: str, products: list, key_field: str = 'name'):
    if not products:
        return
    cursor = conn.cursor()
    columns = list(products[0].keys())
    placeholders = ', '.join(['%s'] * len(columns))
    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
    insert_sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_set}
    """
    data = [tuple(prod[col] for col in columns) for prod in products]
    execute_batch(cursor, insert_sql, data)
    conn.commit()
    print(f"Bulk upsert: {len(products)} products processed")
    cursor.close()


def bulk_upsert_products_table(conn, products: list):
    if not products:
        return
    cursor = conn.cursor()
    columns = list(products[0].keys())
    placeholders = ', '.join(['%s'] * len(columns))
    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
    insert_sql = f"""
        INSERT INTO products ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_set}
    """
    data = [tuple(prod[col] for col in columns) for prod in products]
    execute_batch(cursor, insert_sql, data)
    conn.commit()
    cursor.close()


def save_products(conn, table: str, products_to_insert: list, products_to_upsert: list, all_products: dict):
    save_start = time.time()
    if products_to_upsert:
        bulk_upsert(conn, table, products_to_upsert)
    if products_to_insert:
        bulk_upsert(conn, table, products_to_insert)
    print(f"Saved to PostgreSQL in {round(time.time() - save_start, 2)}s")
    mark_start = time.time()
    mark_out_of_stock(conn, table, all_products)
    print(f"Marked out of stock products in {round(time.time() - mark_start, 2)}s")
    conn.close()


def save_products_to_products_table(conn, market: str, products_to_insert: list, products_to_upsert: list, all_product_names: set):
    save_start = time.time()
    if products_to_upsert:
        bulk_upsert_products_table(conn, products_to_upsert)
    if products_to_insert:
        bulk_upsert_products_table(conn, products_to_insert)
    print(f"Saved to PostgreSQL in {round(time.time() - save_start, 2)}s")
    mark_start = time.time()
    mark_out_of_stock_products_table(conn, market, all_product_names)
    print(f"Marked out of stock products in {round(time.time() - mark_start, 2)}s")
    conn.close()


def get_existing_products_by_market(conn, market: str) -> dict:
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, name FROM products WHERE market = %s", (market,))
    result = {(row['name'], market): row['id'] for row in cursor.fetchall()}
    cursor.close()
    return result


def load_products_to_categorize(conn, limit: int = None) -> List[dict]:
    products = []
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    main_cats = set(CATEGORIES.keys())

    # First: get all products that need categorization (null main_category, confidence, or low confidence)
    query = """
        SELECT id, name, description, market
        FROM products 
        WHERE main_category IS NULL 
           OR confidence IS NULL 
           OR confidence < 0.5
    """
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)

    for row in cursor.fetchall():
        products.append({
            'id': row['id'],
            'name': row['name'],
            'description': row['description'] or '',
            'existing_categories': row['description'] or '',
            'market': row['market']
        })

    # Second: get already-categorized products and check if they have valid categories
    query2 = """
        SELECT id, name, description, market, main_category, sub_category
        FROM products 
        WHERE main_category IS NOT NULL 
          AND sub_category IS NOT NULL 
          AND confidence IS NOT NULL 
          AND confidence >= 0.5
    """
    cursor.execute(query2)

    for row in cursor.fetchall():
        main_cat = row['main_category']
        sub_cat = row['sub_category']

        if main_cat not in main_cats:
            products.append({
                'id': row['id'],
                'name': row['name'],
                'description': row['description'] or '',
                'existing_categories': row['description'] or '',
                'market': row['market']
            })
        elif sub_cat not in CATEGORIES[main_cat]:
            products.append({
                'id': row['id'],
                'name': row['name'],
                'description': row['description'] or '',
                'existing_categories': row['description'] or '',
                'market': row['market']
            })

    cursor.close()
    print(f"📊 Total products to categorize: {len(products)}")
    return products


def batch_update_products(conn, products: List[dict], fields_to_update: List[str], batch_size: int = 500):
    if not products:
        return
    cursor = conn.cursor()
    batch = []
    for product in products:
        row = [product.get(f) for f in fields_to_update]
        row.append(product['id'])
        batch.append(tuple(row))
        if len(batch) >= batch_size:
            set_clause = ', '.join([f"{f} = v.{f}" for f in fields_to_update])
            value_cols = ', '.join(fields_to_update + ['id'])
            execute_values(
                cursor,
                f"""
                UPDATE products AS p
                SET {set_clause}
                FROM (VALUES %s) AS v({value_cols})
                WHERE p.id = v.id::uuid
                """,
                batch
            )
            conn.commit()
            batch.clear()
    if batch:
        set_clause = ', '.join([f"{f} = v.{f}" for f in fields_to_update])
        value_cols = ', '.join(fields_to_update + ['id'])
        execute_values(
            cursor,
            f"""
            UPDATE products AS p
            SET {set_clause}
            FROM (VALUES %s) AS v({value_cols})
            WHERE p.id = v.id::uuid
            """,
            batch
        )
        conn.commit()
    cursor.close()


def save_categorizations_to_db(conn, products: List[dict], batch_size: int = 500):
    print(f"\n💾 Saving {len(products)} categorizations to database...")
    updates = []
    for product in products:
        cat = product.get('categorization', {})
        updates.append({
            'id': str(product['id']),
            'main_category': cat.get('main_category'),
            'sub_category': cat.get('sub_category'),
            'confidence': cat.get('sub_confidence'),
            'reasoning': cat.get('sub_reasoning')
        })
    batch_update_products(conn, updates, ['main_category', 'sub_category', 'confidence', 'reasoning'], batch_size)
    print(f"   Updated: {len(products)}")


def load_products(conn):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM products")
    products = cursor.fetchall()
    cursor.close()
    return products


def create_table(conn, name: str, fields: dict):
    cursor = conn.cursor()
    columns = ["id UUID PRIMARY KEY"]
    for field_name, field_type in fields.items():
        columns.append(f"{field_name} {field_type}")
    columns_sql = ',\n\t'.join(columns)
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {name} (
            {columns_sql}
        )
    """
    cursor.execute(create_table_sql)
    if 'name' in fields:
        index_name = f"{name}_name_idx"
        create_index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {name} (name)"
        cursor.execute(create_index_sql)
    conn.commit()
    cursor.close()


def get_products_from_table(conn, table: str) -> List[dict]:
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(f"SELECT * FROM {table}")
    products = cursor.fetchall()
    cursor.close()
    return products


if __name__ == "__main__":
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM products WHERE main_category IS NULL")
    ct = 0
    for p in cursor.fetchall():
        ct += 1
        if p.get('reasoning') and "Request too large for gpt-4o-mini" in p['reasoning']:
            print(p['reasoning'])
    print(ct)
    cursor.close()
    conn.close()
