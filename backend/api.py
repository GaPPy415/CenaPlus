from enum import Enum
import numpy as np
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor
from backend.data.db_utils import connect_to_db
from backend.data.constants import CATEGORIES
from backend.data.text_utils import normalize_name, get_embeddings_client, normalize_embedding

app = FastAPI()
conn = connect_to_db()
embeddings_client = get_embeddings_client()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
    ],
    # allow_credentials=True,  # only if using cookies
    allow_methods=["*"],
    allow_headers=["*"],
)


class PerPage(int, Enum):
    twelve = 12
    twenty_four = 24
    thirty_six = 36


@app.get("/categories")
def get_categories():
    return {k: v for k, v in CATEGORIES.items()}


@app.get("/search")
def search_products(q: str = Query(..., min_length=1)):
    normalized = normalize_name(q)
    vector = normalize_embedding(np.array(embeddings_client.embed_query(normalized))).tolist()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *, 1 - (name_embedding <=> %s::vector) AS similarity
            FROM grouped_products
            WHERE 1 - (name_embedding <=> %s::vector) >= 0.80
            ORDER BY similarity DESC
            LIMIT 15
            """,
            (vector, vector),
        )
        rows = cur.fetchall()

    return {"data": rows}


@app.get("/{main_category}/{sub_category}")
def get_grouped_products(
    main_category: str,
    sub_category: str,
    page: int = Query(1, ge=1),
    per_page: PerPage = Query(PerPage.twelve),
):
    if main_category not in CATEGORIES:
        raise HTTPException(404, "Main category not found")
    if sub_category not in CATEGORIES[main_category]:
        raise HTTPException(404, "Sub-category not found")

    offset = per_page.value * (page - 1)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT * FROM grouped_products
            WHERE main_category = %s AND sub_category = %s
            ORDER BY group_name
            LIMIT %s OFFSET %s
            """,
            (main_category, sub_category, per_page.value, offset),
        )
        rows = cur.fetchall()

        cur.execute(
            """
            SELECT COUNT(*) FROM grouped_products
            WHERE main_category = %s AND sub_category = %s
            """,
            (main_category, sub_category),
        )
        total = cur.fetchone()["count"]

    return {
        "total": total,
        "page": page,
        "per_page": per_page.value,
        "data": rows,
    }


