# from backend.db_utils import connect_to_db
# from urllib.parse import urlparse, urlunparse
# import requests
# import tldextract
# from bs4 import BeautifulSoup
# import sys
# import json

# # Increase recursion depth just in case the nesting is deep
# sys.setrecursionlimit(2000)
#
# def solve(expression):
#     """
#     Parses and evaluates the FINKI Kernel Language (FKL) expression.
#     """
#     n = len(expression)
#     idx = 0  # Global pointer to track our position in the string
#
#     def parse():
#         nonlocal idx
#
#         # 1. Read the current token (Function Name or Literal)
#         start = idx
#         # Read until we hit a delimiter (parenthesis, comma, or end of string)
#         while idx < n and expression[idx] not in '(),':
#             idx += 1
#
#         token = expression[start:idx]
#
#         # 2. Check context: Is this a function call?
#         # If the next char is '(', it's a function.
#         if idx < n and expression[idx] == '(':
#             func_name = token
#             idx += 1  # Skip '('
#
#             args = []
#
#             # Parse arguments until we hit the closing ')'
#             while idx < n and expression[idx] != ')':
#                 # Recursive call to get the nested argument
#                 arg_val = parse()
#                 args.append(arg_val)
#
#                 # If followed by a comma, skip it and continue to next arg
#                 if idx < n and expression[idx] == ',':
#                     idx += 1
#
#             if idx < n and expression[idx] == ')':
#                 idx += 1  # Skip ')'
#
#             # Execute the function
#             return execute(func_name, args)
#
#         else:
#             # It's not a function; it's a raw string or number literal.
#             return token
#
#     def execute(name, args):
#         if name == 'HEX':
#             # Decodes Hex string to ASCII
#             # args[0] is the hex string (e.g., "4f454e")
#             return bytes.fromhex(args[0]).decode('utf-8')
#
#         elif name == 'REV':
#             # Reverses the string
#             return args[0][::-1]
#
#         elif name == 'REP':
#             # Repeats string s, n times.
#             # args[0] is n, args[1] is s
#             count = int(args[0])
#             text = args[1]
#             return text * count
#
#         elif name == 'CAT':
#             # Concatenates all arguments
#             return "".join(args)
#
#         return ""
#
#     # Start the parsing process
#     return parse()
#
# # --- Test with the provided example ---
# input_str = input()
# result = solve(input_str)
#
# print(f"Input:  {input_str}")
# print(f"Output: {result}")

# from bs4 import BeautifulSoup
# import requests
# url = "https://ceni.mk/old-ceno-mini/categories/"
# response = requests.get(url)
# soup = BeautifulSoup(response.content, 'html.parser')
# categories = soup.find_all('div', class_='category-pill-scroll')
# for category in categories[2:]:
#     print(category.text.strip())
#     # get the onclick attribute
#     onclick = category.get('onclick', '')
#     # extract the URL from the onclick attribute
#     cat_number = onclick.split("=")[-1][:-1]
#     link = f"https://ceni.mk/old-ceno-mini/categories/?category_id={cat_number}"
#     new_response = requests.get(link)
#     new_soup = BeautifulSoup(new_response.content, 'html.parser')
#     sub_categories = new_soup.find_all('div', class_='category-name')
#     for sub in sub_categories:
#         print("\t" + sub.text.strip())


# grades = [10, 9, 10, 8, 10, 10, 10, 8, 8, 10, 10, 10, 7, 9, 7, 10, 9, 7, 8, 10, 8, 7, 6, 7, 9, 7, 9, 6, 7]
# sum = sum(grades)
# # print average with 2 decimal places
# print(f"Average: {sum/len(grades):.2f}")
# print(f"Average: {sum/len(grades)}")
# print(f"Grades: {grades}")
# print(f"Sum: {sum}")


# from backend.db_utils import connect_to_db
# collection = 'kam_products'
# db = connect_to_db()
# products = db[collection].find()
# for p in products[:10]:
#     print(p)
# db.client.close()

# first_path = "backend/logs/run_summary-20260130-194121.json"
# second_path = "backend/logs/run_summary-20260130-221100.json"
# first_run = json.loads(open(first_path).read())
# second_run = json.loads(open(second_path).read())
# first_times = []
# second_times = []
# for entry in first_run:
#     first_times.append([entry['name'], entry['duration_seconds']])
# for entry in second_run:
#     second_times.append([entry['name'], entry['duration_seconds']])

# Format to 2 decimals
# first_time = round(first_time, 2)
# second_time = round(second_time, 2)
# print(f"First run total time: {first_time}s")
# print(f"Second run total time: {second_time}s")
# print(f"Difference: {round(second_time - first_time, 2)}s")

# for i in range(len(first_times)):
#     first_name, first_time = first_times[i][0], first_times[i][1]
#     second_name, second_time = second_times[i][0], second_times[i][1]
#     if first_name != second_name:
#         print(f"Mismatch in script names: {first_name} vs {second_name}")
#     else:
#         first_time = round(first_time, 2)
#         second_time = round(second_time, 2)
#         diff = round(second_time - first_time, 2)
#         print(f"{first_name:<20} First: {first_time:>6}s | Second: {second_time:>6}s | Diff: {diff:>+6}s")


# Fetch products from a subcategory
# for main, categories in CATEGORIES.items():
#     print("Processing main:", main)
#     for category in categories:
#         print("Processing category:", category)
#         cur.execute(f"""
#                 SELECT id, name
#                 FROM products_categorized
#                 WHERE sub_category = '{category}'
#                 AND main_category = '{main}'
# --                 AND name_embedding IS NULL
#                 """)
#         products = cur.fetchall()
#         print(f"Found {len(products)} products without embeddings in category '{category}'")

# cur.execute("SELECT id, name FROM products_categorized where sub_category = 'Млеко'")
# products = cur.fetchall()
# # for product in products:
# #     print(product[1])
# #     name = ' '.join(sorted(product[1].lower().split(' ')))  # Use only the first 5 words for embedding
# #     print("New name: ", name)
#
# # Generate and store embeddings
# for product_id, name in products:
#     # Generate embedding
#     new_name = ' '.join(sorted(name[1].lower().split(' ')))
#     embedding = model.encode(new_name)
#
#     # Store in database
#     cur.execute(f"""
#                 UPDATE products_categorized
#                 SET name_embedding = %s
#                 WHERE id = %s
#                 """, (embedding.tolist(), product_id))
#
# conn.commit()
# cur.close()
# conn.close()


# import psycopg2
# import numpy as np
# from numpy.linalg import norm
# from dotenv import find_dotenv, load_dotenv
# import os
# import asyncio
# from langchain_google_genai import GoogleGenerativeAIEmbeddings
# import time
# from backend.constants import *
# from psycopg2.extras import execute_values
# from backend.RateLimiter import RateLimiter
#
# load_dotenv(find_dotenv())
# BATCH_SIZE = 100
# conn = psycopg2.connect(
#     host=os.getenv("POSTGRES_HOST", "localhost"),
#     port=os.getenv("POSTGRES_PORT", "5432"),
#     database=os.getenv("POSTGRES_DB", "postgres"),
#     user=os.getenv("POSTGRES_USER", "user"),
#     password=os.getenv("POSTGRES_PASSWORD", "password")
# )
#
# google_api_key = os.getenv("GOOGLE_API_KEY")
# embeddings = GoogleGenerativeAIEmbeddings(
#     model="models/gemini-embedding-001",
#     task_type='semantic_similarity',
#     output_dimensionality=768
# )
# rate_limiter = RateLimiter(rpm_limit=2850, tpm_limit=1000000)
#
# def normalize_embedding(embedding: np.ndarray) -> np.ndarray:
#     norm = np.linalg.norm(embedding)
#     if norm == 0:
#         return embedding
#     return embedding / norm
#
# def embed_category_products(category: str, sub_category: str, embeddings: GoogleGenerativeAIEmbeddings, conn: psycopg2.extensions.connection):
#     start = time.time()
#     print(f"Embedding products for category '{category}' and sub-category '{sub_category}'...")
#     cur = conn.cursor()
#     cur.execute(f"""SELECT id, name FROM products_categorized
#                     WHERE main_category = '{category}'
#                     AND sub_category = '{sub_category}'
#                     AND name_embedding IS NULL""")
#     products = cur.fetchall()
#     if not products:
#         cur.close()
#         print(f"No products need embedding category '{category}' and sub-category '{sub_category}'.")
#         return
#
#     all_rows = []
#     names = [' '.join(sorted(p[1].lower().split(' '))) for p in products]
#
#     for batch_start in range(0, len(names), BATCH_SIZE):
#         batch_names = names[batch_start:batch_start + BATCH_SIZE]
#         batch_products = products[batch_start:batch_start + BATCH_SIZE]
#         batch_tokens = sum(len(n) for n in batch_names) // 4
#
#         for _ in range(len(batch_names)):
#             asyncio.run(rate_limiter.acquire(batch_tokens // len(batch_names)))
#
#         vectors = embeddings.embed_documents(batch_names, batch_size=len(batch_names))
#
#         for i, product in enumerate(batch_products):
#             embedding = np.array(vectors[i])
#             embedding = normalize_embedding(embedding)
#             all_rows.append((embedding.tolist(), product[0]))
#
#     execute_values(
#         cur,
#         """
#         UPDATE products_categorized AS p
#         SET name_embedding = v.embedding
#         FROM (VALUES %s) AS v(embedding, id)
#         WHERE p.id = v.id
#         """,
#         all_rows
#     )
#     conn.commit()
#     cur.close()
#     print(f"Finished embedding {len(all_rows)} products for '{category}' -> '{sub_category}' in {round(time.time() - start, 2)}s.")
#
# # embed_category_products("Млечни производи", "Млеко", embeddings, conn)
#
# for main_category in CATEGORIES.keys():
#     main_start = time.time()
#     if main_category=='Разно':
#         continue
#     sub_categories = CATEGORIES[main_category]
#     for sub_category in sub_categories:
#         if sub_category=='Останато':
#             print(f"Skipping sub-category '{sub_category}' in main category '{main_category}'")
#             continue
#         embed_category_products(main_category, sub_category, embeddings, conn)
#     print(f"Finished embedding for main category '{main_category}' in {round(time.time() - main_start, 2)} seconds.")
#
# conn.close()
#
#
# # cur = conn.cursor()
# # cur.execute("SELECT id, name FROM products_categorized where sub_category = 'Млеко'")
# #
# # products = cur.fetchall()
# # names = [' '.join(sorted(product[1].lower().split(' '))) for product in products]
# #
# # vectors = embeddings.embed_documents([
# #     name for name in tqdm(names, desc="Generating embeddings")
# # ])
# #
# # for v in vectors:
# #     v = np.array(v)
# #     v = normalize_embedding(v)
# #
# # print("Generated embeddings for all products, now storing in database...")
# # for i, product in enumerate(products):
# #     embedding = vectors[i]
# #     product_id = product[0]
# #     # Store in database
# #     cur.execute(f"""
# #                 UPDATE products_categorized
# #                 SET name_embedding = %s
# #                 WHERE id = %s
# #                 """, (embedding, product_id))


# import psycopg2
# from backend.db_utils import *

# conn = connect_to_db()
# cur = conn.cursor()
# # get all table schemas
# cur.execute("""
#     SELECT table_name, column_name, data_type
#     FROM information_schema.columns
#     WHERE table_schema = 'public'
# """)
# tables = {}
# for table_name, column_name, data_type in cur.fetchall():
#     if table_name not in tables.keys():
#         tables[table_name] = []
#     tables[table_name].append((column_name, data_type))
# for table_name, columns in tables.items():
#     print(f"Table: {table_name}")
#     for column_name, data_type in columns:
#         print(f"\t{column_name}: {data_type}")
# cur.close()
# conn.close()

from backend.data.db_utils import *
import html
import time
start = time.time()
print("Starting to unescape product names...")
db = connect_to_db()
cur = db.cursor()
# update all product names to be unescaped with html
cur.execute("""
    SELECT id, name
    FROM products
""")
products = cur.fetchall()
for product_id, name in products:
    unescaped_name = html.unescape(name)
    if unescaped_name != name:
        cur.execute("""
            UPDATE products
            SET name = %s
            WHERE id = %s
        """, (unescaped_name, product_id))
db.commit()
cur.close()
db.close()
print(f"Finished unescaping product names in {round(time.time() - start, 2)} seconds.")