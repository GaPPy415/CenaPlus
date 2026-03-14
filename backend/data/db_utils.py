import html
import time
import uuid
from datetime import datetime
from typing import List
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import os
from dotenv import load_dotenv, find_dotenv
from backend.data.constants import *


def connect_to_db() -> psycopg2.extensions.connection:
    load_dotenv(find_dotenv())
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password")
    )
    print(f"Connected to PostgreSQL at {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}, db '{os.getenv('POSTGRES_DB')}'")
    return conn


def mark_out_of_stock_products_table(conn: psycopg2.extensions.connection, market: str, product_names: set):
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM products WHERE market = %s AND in_stock = true", (market,))

    ids_to_update = [row[0] for row in cursor.fetchall() if row[1] not in product_names]

    if ids_to_update:
        cursor.execute(
            "UPDATE products SET in_stock = false, last_updated = %s WHERE id = ANY(%s::uuid[])",
            (datetime.now(), ids_to_update)
        )
    conn.commit()
    cursor.close()


def bulk_upsert_products_table(conn: psycopg2.extensions.connection, products: list):
    if not products:
        return
    cursor = conn.cursor()
    columns = list(products[0].keys())

    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])

    insert_sql = f"""
        INSERT INTO products ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET {update_set}
    """
    data = [tuple(prod.get(col) for col in columns) for prod in products]
    execute_values(cursor, insert_sql, data, page_size=5000)
    conn.commit()
    cursor.close()


def save_products_to_products_table(conn: psycopg2.extensions.connection, market: str, products_to_upsert: list, all_product_names: set):
    save_start = time.time()

    for prod in products_to_upsert:
        prod['name'] = html.unescape(prod['name'])

    for prod in list(all_product_names):
        cleaned = html.unescape(prod)
        if cleaned not in all_product_names:
            all_product_names.add(cleaned)
            if prod in all_product_names:
                all_product_names.remove(prod)

    if products_to_upsert:
        bulk_upsert_products_table(conn, products_to_upsert)

    print(f"Saved to PostgreSQL in {round(time.time() - save_start, 2)}s")
    mark_start = time.time()
    mark_out_of_stock_products_table(conn, market, all_product_names)
    print(f"Marked out of stock products in {round(time.time() - mark_start, 2)}s")
    conn.close()


def get_products_by_market(conn: psycopg2.extensions.connection, market: str) -> dict:
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, name FROM products WHERE market = %s", (market,))
    result = {(row['name'], market): row['id'] for row in cursor.fetchall()}
    cursor.close()
    return result


def load_products_to_categorize(conn: psycopg2.extensions.connection, limit: int = None) -> List[dict]:
    products = []
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    main_cats = set(CATEGORIES.keys())

    # First: get all products that need categorization (null main_category, confidence, or low confidence)
    query = f"""
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


def batch_update_products(conn: psycopg2.extensions.connection, products: List[dict], fields_to_update: List[str], batch_size: int = 500):
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


def save_categorizations_to_db(conn: psycopg2.extensions.connection, products: List[dict], batch_size: int = 500):
    print(f"\n💾 Saving {len(products)} categorizations to database...")
    updates = []
    for product in products:
        cat = product.get('categorization', {})
        updates.append({
            'id': str(product['id']),
            'main_category': cat.get('main_category'),
            'sub_category': cat.get('sub_category'),
            'confidence': cat.get('sub_confidence'),
            'reasoning': cat.get('sub_reasoning'),
            'categorized_at': datetime.now()
        })
    batch_update_products(conn, updates, ['main_category', 'sub_category', 'confidence', 'reasoning', 'categorized_at'], batch_size)
    print(f"   Updated: {len(products)}")

def group_products_by_category(conn: psycopg2.extensions.connection, main_category: str, sub_category: str, similarity_threshold: float = 0.98):
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT id, name, name_embedding, market
        FROM products 
        WHERE main_category = %s
        AND sub_category = %s 
        AND name_embedding IS NOT NULL 
        AND group_id IS NULL
    """, (main_category, sub_category,))
    ungrouped_products = cur.fetchall()

    if not ungrouped_products:
        print(f"No ungrouped products found in sub-category '{sub_category}'")
        cur.close()
        return

    print(f"Found {len(ungrouped_products)} ungrouped products in '{sub_category}'")

    for product in ungrouped_products:
        product_id = product['id']
        product_name = product['name']
        product_market = product['market']
        product_embedding = product['name_embedding']

        cur.execute("SELECT group_id FROM products WHERE id = %s", (product_id,))
        current_group = cur.fetchone()
        if current_group and current_group['group_id'] is not None:
            continue

        cur.execute("""
            SELECT g.id, g.name,
                   1 - (g.name_embedding <=> %s) AS similarity
            FROM groups g
            WHERE g.main_category = %s
            AND g.sub_category = %s
            AND g.name_embedding IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM products p
                WHERE p.group_id = g.id
                AND p.market = %s
            )
            ORDER BY similarity DESC
            LIMIT 1
        """, (product_embedding, main_category, sub_category, product_market))
        existing_group = cur.fetchone()

        if existing_group and existing_group['similarity'] >= similarity_threshold:
            cur.execute(
                "UPDATE products SET group_id = %s WHERE id = %s",
                (existing_group['id'], product_id)
            )
            conn.commit()
            continue

        cur.execute("""
            SELECT p.id, p.name,
                   1 - (p.name_embedding <=> %s) AS similarity
            FROM products p
            WHERE p.main_category = %s
            AND p.sub_category = %s
            AND p.name_embedding IS NOT NULL
            AND p.group_id IS NULL
            AND p.id != %s
            AND p.market != %s
            AND 1 - (p.name_embedding <=> %s) >= %s
            ORDER BY similarity DESC
        """, (product_embedding, main_category, sub_category, product_id, product_market, product_embedding, similarity_threshold))
        similar_products = cur.fetchall()

        group_id = str(uuid.uuid4())
        clean_name = product_name

        cur.execute("""
            INSERT INTO groups (id, name, main_category, sub_category, name_embedding, clean_name)
            SELECT %s, name, main_category, sub_category, name_embedding, %s
            FROM products WHERE id = %s
        """, (group_id, clean_name, product_id))

        cur.execute("UPDATE products SET group_id = %s WHERE id = %s", (group_id, product_id))

        for similar in similar_products:
            cur.execute(
                "UPDATE products SET group_id = %s WHERE id = %s",
                (group_id, similar['id'])
            )

        conn.commit()

    cur.close()



# Old function, with this one two products from the same market could end up in the same group
# The new function doesn't allow this behavior
def group_products_by_category_old(conn: psycopg2.extensions.connection, main_category: str, sub_category: str, similarity_threshold: float = 0.99):
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT id, name, name_embedding 
        FROM products 
        WHERE main_category = %s
        AND sub_category = %s 
        AND name_embedding IS NOT NULL 
        AND group_id IS NULL
    """, (main_category, sub_category,))
    ungrouped_products = cur.fetchall()

    if not ungrouped_products:
        print(f"No ungrouped products found in sub-category '{sub_category}'")
        cur.close()
        return

    print(f"Found {len(ungrouped_products)} ungrouped products in '{sub_category}'")

    for product in ungrouped_products:
        product_id = product['id']
        product_name = product['name']
        product_embedding = product['name_embedding']

        cur.execute("SELECT group_id FROM products WHERE id = %s", (product_id,))
        current_group = cur.fetchone()
        if current_group and current_group['group_id'] is not None:
            continue

        cur.execute("""
            SELECT g.id, g.name, 1 - (g.name_embedding <=> %s) as similarity
            FROM groups g
            WHERE g.main_category = %s
            AND g.sub_category = %s
            AND g.name_embedding IS NOT NULL
            ORDER BY similarity DESC
            LIMIT 1
        """, (product_embedding, main_category, sub_category))
        existing_group = cur.fetchone()

        if existing_group and existing_group['similarity'] >= similarity_threshold:
            cur.execute("UPDATE products SET group_id = %s WHERE id = %s", (existing_group['id'], product_id))
            conn.commit()
            continue

        cur.execute("""
            SELECT p.id, p.name, 1 - (p.name_embedding <=> %s) as similarity
            FROM products p
            WHERE p.main_category = %s
            AND p.sub_category = %s
            AND p.name_embedding IS NOT NULL
            AND p.group_id IS NULL
            AND p.id != %s
            AND 1 - (p.name_embedding <=> %s) >= %s
            ORDER BY similarity DESC
        """, (product_embedding, main_category, sub_category, product_id, product_embedding, similarity_threshold))
        similar_products = cur.fetchall()

        if similar_products:
            # Create new group
            group_id = str(uuid.uuid4())
            clean_name = product_name #' '.join(sorted(product_name.lower().split(' ')))
            cur.execute("""
                INSERT INTO groups (id, name, main_category, sub_category, name_embedding, clean_name)
                SELECT %s, name, main_category, sub_category, name_embedding, %s
                FROM products WHERE id = %s
            """, (group_id, clean_name, product_id))

            # Assign the seed product to the group
            cur.execute("UPDATE products SET group_id = %s WHERE id = %s", (group_id, product_id))

            # Assign all similar products to the group
            for similar in similar_products:
                cur.execute("UPDATE products SET group_id = %s WHERE id = %s", (group_id, similar['id']))

            conn.commit()
            print(f"Created group '{product_name}' with {len(similar_products) + 1} products")

    cur.close()
    print(f"Grouping complete for sub-category '{sub_category}'")


def load_products(conn: psycopg2.extensions.connection):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM products")
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
