import psycopg2
import numpy as np
from dotenv import find_dotenv, load_dotenv
import os
import asyncio
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import time
from backend.data.constants import *
from psycopg2.extras import execute_values
from backend.data.RateLimiter import RateLimiter
from backend.data.text_utils import normalize_name, get_embeddings_client, normalize_embedding

load_dotenv(find_dotenv())
BATCH_SIZE = 100
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DB", "postgres"),
    user=os.getenv("POSTGRES_USER", "user"),
    password=os.getenv("POSTGRES_PASSWORD", "password")
)

embeddings = get_embeddings_client()
rate_limiter = RateLimiter(rpm_limit=2850, tpm_limit=1000000)


def embed_category_products(category: str, sub_category: str, embeddings: GoogleGenerativeAIEmbeddings, conn: psycopg2.extensions.connection):
    start = time.time()
    print(f"Embedding products for category '{category}' and sub-category '{sub_category}'...")
    cur = conn.cursor()
    cur.execute("""SELECT id, name FROM products 
                    WHERE main_category = %s 
                    AND sub_category = %s
                    AND name_embedding IS NULL""", (category, sub_category))
    products = cur.fetchall()
    if not products:
        cur.close()
        print(f"No products need embedding category '{category}' and sub-category '{sub_category}'.")
        return

    all_rows = []
    names = [normalize_name(p[1]) for p in products]

    for batch_start in range(0, len(names), BATCH_SIZE):
        batch_names = names[batch_start:batch_start + BATCH_SIZE]
        batch_products = products[batch_start:batch_start + BATCH_SIZE]
        batch_tokens = sum(len(n) for n in batch_names) // 4

        for _ in range(len(batch_names)):
            asyncio.run(rate_limiter.acquire(batch_tokens // len(batch_names)))

        vectors = embeddings.embed_documents(batch_names, batch_size=len(batch_names))

        for i, product in enumerate(batch_products):
            embedding = np.array(vectors[i])
            embedding = normalize_embedding(embedding)
            all_rows.append((embedding.tolist(), str(product[0])))

    execute_values(
        cur,
        """
        UPDATE products AS p
        SET name_embedding = v.embedding
        FROM (VALUES %s) AS v(embedding, id)
        WHERE p.id = v.id::uuid
        """,
        all_rows
    )
    conn.commit()
    cur.close()
    print(f"Finished embedding {len(all_rows)} products for '{category}' -> '{sub_category}' in {round(time.time() - start, 2)}s.")

for main_category in CATEGORIES.keys():
    main_start = time.time()
    if main_category == 'Разно':
        continue
    sub_categories = CATEGORIES[main_category]
    for sub_category in sub_categories:
        if sub_category == 'Останато':
            print(f"Skipping sub-category '{sub_category}' in main category '{main_category}'")
            continue
        embed_category_products(main_category, sub_category, embeddings, conn)
    print(f"Finished embedding for main category '{main_category}' in {round(time.time() - main_start, 2)} seconds.")
conn.close()
