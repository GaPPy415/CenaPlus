import time
import uuid
from datetime import datetime
from typing import Tuple, List
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
import os
from dotenv import load_dotenv, find_dotenv


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


def load_products_to_categorize(conn, limit_per_table: int = None) -> Tuple[List[dict], dict]:
    products = []
    products_markets = {}
    cursor = conn.cursor()

    # Load all categorized products once
    cat_cursor = conn.cursor(cursor_factory=RealDictCursor)
    cat_cursor.execute("SELECT * FROM products_categorized")
    ids_products = {p['id']: p for p in cat_cursor.fetchall()}
    cat_cursor.close()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = [row[0] for row in cursor.fetchall() if row[0] != 'all_products' and not row[0].startswith('products_categorized')]
    print(f"📂 Loading products from {len(tables)} tables...")

    for table in tables:
        query = f"SELECT * FROM {table}"
        if limit_per_table:
            query += f" LIMIT {limit_per_table}"
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        table_count = 0

        for row in cursor.fetchall():
            product = dict(zip(columns, row))
            product_id = product['id']

            # Check locally if already categorized
            existing = ids_products.get(product_id)
            if existing and existing.get('main_category') and existing.get('reasoning') != "Missing from batch response":
                continue

            description = ""
            for field in ['description', 'categories', 'category']:
                if field in product and product[field]:
                    description = str(product[field])
                    break

            new_product = {
                'id': product_id,
                'name': product.get('name', ''),
                'description': description,
                'existing_categories': description
            }
            products.append(new_product)
            products_markets[product_id] = table
            table_count += 1
        print(f"   {table}: {table_count} products")
    cursor.close()
    print(f"📊 Total products to categorize: {len(products)}")
    return products, products_markets


def save_categorizations_to_db(conn, products: List[dict], products_markets: dict):
    print(f"\n💾 Saving {len(products)} categorizations to database...")

    # Check if products_categorized table exists
    fields = {'name': 'VARCHAR(255)',
              'description': 'TEXT',
              'existing_categories': 'TEXT',
              'main_category': 'VARCHAR(255)',
              'sub_category': 'VARCHAR(255)',
              'confidence': 'DECIMAL',
              'reasoning': 'TEXT',
              'market': 'VARCHAR(255)',
              'categorized_at': 'TIMESTAMP'}
    create_table(conn, 'products_categorized', fields)

    cursor = conn.cursor()
    updated_count = 0
    inserted_count = 0
    for product in products:
        market = products_markets.get(product['id'], 'unknown')
        cat = product.get('categorization', {})
        cursor.execute("SELECT id FROM products_categorized WHERE id = %s", (product['id'],))
        exists = cursor.fetchone()
        if exists:
            cursor.execute("""
                UPDATE products_categorized 
                SET main_category = %s, sub_category = %s, confidence = %s, reasoning = %s, categorized_at = %s
                WHERE id = %s
            """, (cat.get('main_category'), cat.get('sub_category'), cat.get('sub_confidence'), cat.get('sub_reasoning'), datetime.now(), product['id']))
            updated_count += 1
        else:
            cursor.execute("""
                INSERT INTO products_categorized (id, name, main_category, sub_category, confidence, reasoning, market, categorized_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (product['id'], product.get('name', ''), cat.get('main_category'), cat.get('sub_category'), cat.get('sub_confidence'), cat.get('sub_reasoning'), market, datetime.now()))
            inserted_count += 1
    conn.commit()
    cursor.close()
    print(f"   Updated: {updated_count}")
    print(f"   Inserted: {inserted_count}")
    print(f"   Total saved: {len(products)}")


def load_products(conn): # Experimental, for debugging
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM products_categorized")
    products = cursor.fetchall()
    cursor.close()
    return products


def create_table(conn, name: str, fields: dict):
	"""
	Create a new table with the specified name and fields.

	Args:
		conn: PostgreSQL connection
		name: Table name
		fields: Dictionary mapping field names to SQL types (e.g., {'id': 'UUID PRIMARY KEY', 'name': 'VARCHAR(255)', 'price': 'DECIMAL'})

	The 'name' field will be automatically indexed if present in fields.
	"""
	cursor = conn.cursor()

	# Build the column definitions
	columns = ["id UUID PRIMARY KEY"]
	for field_name, field_type in fields.items():
		columns.append(f"{field_name} {field_type}")

	# Create table
	columns_sql = ',\n\t'.join(columns)
	create_table_sql = f"""
		CREATE TABLE IF NOT EXISTS {name} (
			{columns_sql}
		)
	"""

	cursor.execute(create_table_sql)

	# Create index on 'name' field if it exists
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
    cursor.execute("SELECT * FROM products_categorized WHERE main_category IS NULL")
    ct = 0
    for p in cursor.fetchall():
        ct += 1
        if p.get('reasoning') and "Request too large for gpt-4o-mini" in p['reasoning']:
            print(p['reasoning'])
    print(ct)
    cursor.close()
    conn.close()
