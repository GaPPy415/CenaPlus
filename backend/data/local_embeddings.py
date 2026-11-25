"""Layer 5: Local Ollama (`bge-m3`) embedding cache & semantic merge inside blocks.

Embeddings are stored in `public.bge_text_embeddings(text_key, embedding vector(1024))`
keyed on the original-script `product_name` text so identical names across shops are
embedded only once, leaving `products.name_embedding` (768-dim) untouched.

Usage (optional standalone cache warmup):
    python -m backend.data.local_embeddings --check
"""

from __future__ import annotations

import argparse
import os
import time
from collections import Counter, defaultdict

import numpy as np
import requests
from psycopg2.extras import execute_values

from backend.data.grouping_rules import (
    CORE_GROCERY_CATEGORIES,
    build_cluster_summary,
    discover_same_market_differentiators,
    hard_veto_check,
)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "bge-m3:latest")
OLLAMA_EMBED_BATCH_SIZE = int(os.getenv("OLLAMA_EMBED_BATCH_SIZE", "128"))

DEFAULT_BGE_THRESHOLD = 0.72
DEFAULT_BGE_MARGIN = 0.04
DEFAULT_BGE_MAX_PRICE_RATIO = 1.80

CREATE_CACHE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS public.bge_text_embeddings (
        text_key   text PRIMARY KEY,
        embedding  vector(1024) NOT NULL,
        model      text NOT NULL DEFAULT 'bge-m3:latest',
        created_at timestamptz NOT NULL DEFAULT now()
    )
"""


def parse_pgvector(val) -> np.ndarray | None:
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        arr = np.asarray(val, dtype=np.float32)
    else:
        s = str(val).strip()
        if not s or s[0] != "[":
            return None
        arr = np.fromstring(s[1:-1], sep=",", dtype=np.float32)
    if arr.size == 0:
        return None
    n = np.linalg.norm(arr)
    return arr / n if n > 0 else None


def is_ollama_available(
    model: str = OLLAMA_EMBED_MODEL,
    base_url: str = OLLAMA_BASE_URL,
    timeout: float = 3.0,
) -> bool:
    """Return True when Ollama is reachable and has `model` pulled."""
    try:
        resp = requests.get(f"{base_url}/api/tags", timeout=timeout)
        if resp.status_code != 200:
            return False
        names = {m.get("name", "") for m in resp.json().get("models", [])}
        base_name = model.split(":")[0]
        return model in names or any(n == base_name or n.startswith(f"{base_name}:") for n in names)
    except requests.RequestException:
        return False


def embed_texts_ollama(
    texts: list[str],
    model: str = OLLAMA_EMBED_MODEL,
    batch_size: int = OLLAMA_EMBED_BATCH_SIZE,
    base_url: str = OLLAMA_BASE_URL,
) -> np.ndarray:
    """Embed texts via Ollama `/api/embed` and return L2-normalized float32 vectors."""
    if not texts:
        return np.empty((0, 1024), dtype=np.float32)

    session = requests.Session()
    url = f"{base_url}/api/embed"
    chunks: list[np.ndarray] = []
    started = time.perf_counter()
    total = len(texts)

    for i in range(0, total, batch_size):
        batch = texts[i : i + batch_size]
        resp = session.post(
            url,
            json={"model": model, "input": batch, "truncate": True},
            timeout=300,
        )
        resp.raise_for_status()
        embs = np.asarray(resp.json()["embeddings"], dtype=np.float32)
        norms = np.linalg.norm(embs, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        chunks.append(embs / norms)

        done = min(i + batch_size, total)
        if total >= 2000 and (done == total or (i // batch_size) % 20 == 19):
            rate = done / max(1e-6, time.perf_counter() - started)
            print(f"  [bge-m3] embedded {done:,}/{total:,} ({rate:.1f} str/s)", flush=True)

    return np.vstack(chunks)


def fetch_or_compute_embeddings(
    conn,
    texts: list[str],
    model: str = OLLAMA_EMBED_MODEL,
    write_cache: bool = True,
    base_url: str = OLLAMA_BASE_URL,
) -> dict[str, np.ndarray]:
    """Load cached bge-m3 vectors from Postgres and embed only missing strings via Ollama."""
    unique_texts = sorted({t for t in texts if t})
    if not unique_texts:
        return {}

    vectors: dict[str, np.ndarray] = {}
    table_exists = False
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.bge_text_embeddings')")
        table_exists = cur.fetchone()[0] is not None
        if table_exists:
            chunk = 5000
            for i in range(0, len(unique_texts), chunk):
                cur.execute(
                    "SELECT text_key, embedding::text FROM public.bge_text_embeddings WHERE text_key = ANY(%s)",
                    (unique_texts[i : i + chunk],),
                )
                for text_key, emb_text in cur.fetchall():
                    vec = parse_pgvector(emb_text)
                    if vec is not None:
                        vectors[text_key] = vec

    missing = [t for t in unique_texts if t not in vectors]
    print(f"bge-m3 cache: {len(vectors):,} hit | {len(missing):,} missing (of {len(unique_texts):,} unique texts)")

    if not missing:
        return vectors

    if not is_ollama_available(model=model, base_url=base_url):
        print(f"  [bge-m3] Ollama ({base_url}) unavailable or '{model}' not pulled; using cached vectors only.")
        return vectors

    t0 = time.perf_counter()
    new_vecs = embed_texts_ollama(missing, model=model, base_url=base_url)
    elapsed = time.perf_counter() - t0
    print(f"  [bge-m3] computed {len(missing):,} vectors in {elapsed:.1f}s ({len(missing)/max(elapsed,1e-6):.1f} str/s)")

    for text_key, vec in zip(missing, new_vecs):
        vectors[text_key] = vec

    if write_cache:
        with conn.cursor() as cur:
            if not table_exists:
                cur.execute(CREATE_CACHE_TABLE_SQL)
            payload = [
                (text_key, "[" + ",".join(f"{float(x):.6f}" for x in vec) + "]", model)
                for text_key, vec in zip(missing, new_vecs)
            ]
            execute_values(
                cur,
                """
                INSERT INTO public.bge_text_embeddings (text_key, embedding, model)
                VALUES %s
                ON CONFLICT (text_key) DO NOTHING
                """,
                payload,
                template="(%s, %s::vector, %s)",
                page_size=500,
            )
        conn.commit()
        print(f"  [bge-m3] cached {len(missing):,} new vectors into public.bge_text_embeddings")

    return vectors


def layer5_merge_block(
    components: list[list[dict]],
    vec_lookup: dict[str, np.ndarray],
    cos_thresh: float = DEFAULT_BGE_THRESHOLD,
    margin_min: float = DEFAULT_BGE_MARGIN,
    max_price_ratio: float = DEFAULT_BGE_MAX_PRICE_RATIO,
) -> tuple[list[list[dict]], list[dict]]:
    """Layer 5: Guarded semantic merge strictly inside one Core Grocery `(brand, base_qty)` block.

    Enforces:
      - Core grocery categories only (never merges Облека / Дом и домаќинство / Разно)
      - Same `sub_category` (blocks Кафе vs Какао, Пиперчиња vs Чипс)
      - All Layer 1-4 hard vetoes (`hard_veto_check`)
      - Mutual-Nearest-Neighbor across markets with `margin >= margin_min`
      - Non-transitive full-cluster veto verification before merging
    """
    if len(components) < 2 or len(components) > 250:
        return components, []

    all_rows = [r for comp in components for r in comp]
    top_cat = Counter(r.get("main_category") for r in all_rows).most_common(1)[0][0]
    if top_cat not in CORE_GROCERY_CATEGORIES:
        return components, []

    clusters = [build_cluster_summary(i, comp) for i, comp in enumerate(components)]
    emb_indices = [c["idx"] for c in clusters if c["embed_text"] in vec_lookup]
    if len(emb_indices) < 2:
        return components, []

    all_block_mkts = set().union(*(clusters[i]["all_markets"] for i in emb_indices))
    if len(all_block_mkts) < 2:
        return components, []

    block_protected = discover_same_market_differentiators(clusters)

    mat = np.stack([vec_lookup[clusters[i]["embed_text"]] for i in emb_indices])
    sim_mat = mat @ mat.T

    best_per_node_mkt: dict[tuple[int, str], list[tuple[float, int]]] = defaultdict(list)
    cand_edges: list[tuple[float, int, int, float]] = []

    m_count = len(emb_indices)
    for a_pos in range(m_count):
        i = emb_indices[a_pos]
        ci = clusters[i]
        for b_pos in range(a_pos + 1, m_count):
            j = emb_indices[b_pos]
            cj = clusters[j]
            cos = float(sim_mat[a_pos, b_pos])
            if cos < 0.55:
                continue
            for m in cj["all_markets"]:
                if m not in ci["all_markets"]:
                    best_per_node_mkt[(i, m)].append((cos, j))
            for m in ci["all_markets"]:
                if m not in cj["all_markets"]:
                    best_per_node_mkt[(j, m)].append((cos, i))
            if cos >= cos_thresh:
                if ci["sub_category"] != cj["sub_category"]:
                    continue
                passed, pr, _, _, _, _ = hard_veto_check(
                    ci, cj, block_protected, max_price_ratio=max_price_ratio
                )
                if passed and pr <= max_price_ratio:
                    cand_edges.append((cos, i, j, pr))

    for k in best_per_node_mkt:
        best_per_node_mkt[k].sort(reverse=True)

    qualifying: list[tuple[float, int, int, float]] = []
    for cos, i, j, pr in cand_edges:
        ci, cj = clusters[i], clusters[j]
        is_mnn = False
        for ma in ci["all_markets"]:
            for mb in cj["all_markets"]:
                if ma == mb:
                    continue
                la = best_per_node_mkt.get((i, mb), [])
                lb = best_per_node_mkt.get((j, ma), [])
                if la and lb and la[0][1] == j and lb[0][1] == i:
                    margin_a = la[0][0] - (la[1][0] if len(la) > 1 else 0.0)
                    margin_b = lb[0][0] - (lb[1][0] if len(lb) > 1 else 0.0)
                    if min(margin_a, margin_b) >= margin_min:
                        is_mnn = True
                        break
            if is_mnn:
                break
        if is_mnn:
            qualifying.append((cos, i, j, pr))

    qualifying.sort(key=lambda x: x[0], reverse=True)
    parent = {c["idx"]: c["idx"] for c in clusters}
    members: dict[int, list[int]] = {c["idx"]: [c["idx"]] for c in clusters}
    accepted: list[dict] = []

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for cos, i, j, pr in qualifying:
        ri, rj = find(i), find(j)
        if ri == rj:
            continue
        ok = True
        for a_idx in members[ri]:
            for b_idx in members[rj]:
                if clusters[a_idx]["sub_category"] != clusters[b_idx]["sub_category"]:
                    ok = False
                    break
                passed, _, _, _, _, _ = hard_veto_check(
                    clusters[a_idx], clusters[b_idx], block_protected, max_price_ratio=max_price_ratio
                )
                if not passed:
                    ok = False
                    break
            if not ok:
                break
        if not ok:
            continue

        parent[rj] = ri
        members[ri].extend(members[rj])
        del members[rj]
        accepted.append({
            "cos": round(cos, 3),
            "pr": round(pr, 2),
            "sub_cat": clusters[i]["sub_category"],
            "a_raw": clusters[i]["rows"][0].get("name"),
            "b_raw": clusters[j]["rows"][0].get("name"),
            "a_mkt": clusters[i]["rows"][0].get("market"),
            "b_mkt": clusters[j]["rows"][0].get("market"),
            "a_price": clusters[i]["median_price"],
            "b_price": clusters[j]["median_price"],
        })

    final_components = [
        [row for idx in idxs for row in clusters[idx]["rows"]]
        for idxs in members.values()
    ]
    return final_components, accepted


def main() -> int:
    parser = argparse.ArgumentParser(description="Check local Ollama bge-m3 availability.")
    parser.add_argument("--check", action="store_true", help="Verify Ollama reachability and model")
    args = parser.parse_args()
    if args.check:
        ok = is_ollama_available()
        print(f"Ollama ({OLLAMA_BASE_URL}) model '{OLLAMA_EMBED_MODEL}': {'OK' if ok else 'UNAVAILABLE'}")
        return 0 if ok else 1
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
