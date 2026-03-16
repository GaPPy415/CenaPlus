# CenaPlus — Agent Guidelines

## Project Overview

**CenaPlus** is a Macedonian grocery price comparison web application. It scrapes product data from 6+ supermarket chains in North Macedonia, categorizes and groups equivalent products using AI (Google Gemini), and presents a React frontend where users compare prices side-by-side.

### Architecture

```
Scrapers (6 markets) → PostgreSQL (products table)
        ↓
Gemini 2.0 Flash → categorizes into main_category + sub_category
        ↓
Gemini Embedding 001 → 768-dim name_embedding vectors
        ↓
pgvector cosine similarity → groups table → grouped_products view
        ↓
FastAPI serves grouped_products → React frontend displays price comparisons
        ↓
Docker Compose (PostgreSQL + FastAPI backend + Nginx frontend)
```

---

## Tech Stack

| Layer      | Technologies                                                                                     |
| ---------- | ------------------------------------------------------------------------------------------------ |
| Frontend   | React 18, TypeScript, Vite, TanStack React Query, shadcn/ui (Radix), Tailwind CSS, next-themes   |
| Backend    | Python 3, FastAPI, psycopg2, PostgreSQL with pgvector extension                                  |
| AI/ML      | Google Gemini 2.0 Flash (categorization), Gemini Embedding 001 (vector embeddings)               |
| Scrapers   | BeautifulSoup, aiohttp, requests, pandas, concurrent.futures                                     |
| Testing    | Vitest, @testing-library/react, jsdom                                                            |
| Tooling    | ESLint (flat config), PostCSS, Autoprefixer, Bun (lockfile)                                      |
| Deployment | Docker Compose, Nginx (frontend static serving), python:3.12-alpine (backend), oven/bun (build)  |

---

## Directory Structure

```
CenaPlus/
├── backend/
│   ├── api.py                      # FastAPI server (endpoints: /categories, /search, /{main}/{sub})
│   └── data/
│       ├── constants.py            # Category taxonomy, descriptions, compressed taxonomy
│       ├── db_utils.py             # PostgreSQL helpers (upsert, batch update, grouping via pgvector)
│       ├── categorize_products.py  # Gemini-based two-stage categorization pipeline
│       ├── embed_products.py       # Gemini embedding generation + normalization
│       ├── group_products.py       # Cross-market product grouping by embedding similarity
│       ├── RateLimiter.py          # Async token-aware rate limiter for Gemini API
│       ├── text_utils.py           # Shared utilities (normalize_name, get_embeddings_client, normalize_embedding)
│       ├── run_scrapers.py         # Orchestrator: discovers and runs all scrapers
│       ├── run_pipeline.py         # Full pipeline runner: scrape → categorize → embed → group
│       ├── scrapers/               # Market-specific scraping scripts
│       │   ├── Kam_scraper.py      # KAM market (PDF price lists)
│       │   ├── Ramstore_scraper.py # Ramstore (web scraping)
│       │   ├── Reptil_scraper.py   # Reptil (BeautifulSoup + retry, multithreaded)
│       │   ├── Stokomak_scraper.py # Stokomak (HTML tables)
│       │   ├── Vero_scraper.py     # Vero (HTML tables, threaded)
│       │   ├── Zito_scraper.py     # Žito Market (WooCommerce REST API, async)
│       │   └── kam_pdf_utils.py    # PDF parsing utilities for KAM price lists
│       ├── deprecated/             # Old/unused scraper versions
│       └── logs/                   # Scraper run logs
```

---

## Key Conventions

### Frontend

- **Path alias:** `@/` maps to `./src/` (configured in tsconfig and vite)
- **Styling:** Tailwind CSS with HSL CSS variables. Custom `market-*` color tokens for each store. Dark mode via `class` strategy (next-themes)
- **Fonts:** DM Sans (body), Inter (headings)
- **State management:** URL search params (`useSearchParams`) drive navigation; TanStack React Query handles server state
- **Component library:** shadcn/ui — components live in `src/components/ui/` and are customized Radix primitives
- **TypeScript:** Relaxed strictness (`noImplicitAny: false`, `strictNullChecks: false`)
- **Testing:** Vitest with jsdom environment, `@testing-library/react` for component tests. Tests in `src/**/*.{test,spec}.{ts,tsx}`
- **Search:** Header contains a search bar. On submit, navigates to `/search?q=...` which uses semantic vector search via the API
- **Market filtering:** SubCategoryView has per-market toggle buttons to filter displayed products
- **Pagination:** First/last page buttons, numbered pages with ellipsis, custom "go to page" input field
- **Product hover:** Each product row in ProductCard shows a tooltip on hover with full name, singular price, and in-stock/out-of-stock status (instant appearance, no delay)

### Backend

- **Database:** PostgreSQL with pgvector extension for vector similarity search. Single `products` table for all markets.
- **Connection:** Single shared `psycopg2` connection via `connect_to_db()`, env vars: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- **API server:** FastAPI on port 8000 (default), CORS allowed for `localhost:8080`
- **API endpoints:** 
    - `GET /categories`: Returns category taxonomy
    - `GET /search?q=...`: Semantic vector search (≥0.90 similarity)
    - `GET /{main_category}/{sub_category}?page=&per_page=&market=`: Paginated products
- **Shared utilities:** `text_utils.py` contains `normalize_name()`, `get_embeddings_client()`, and `normalize_embedding()` — used by both `api.py` (search) and `embed_products.py`
- **AI integration:** Google Gemini via `google-genai` package; rate-limited with custom async `RateLimiter` (RPM/TPM tracking)
- **Categorization:** Two-stage pipeline — first classify into main category, then subcategory. Uses Pydantic models with `response_schema` for structured Gemini output (JSON schema enforcement).
- **Embeddings:** Product names normalized (Cyrillic transliteration, alphabetical word sort), embedded with Gemini Embedding 001 (768 dimensions), L2-normalized, stored in pgvector
- **Grouping:** Cosine similarity matching (default 0.95). One product per market per group enforced. Solo products get their own group.
- **Scrapers:** Standalone scripts in `backend/data/scrapers/`. All scrapers upsert to the shared `products` table.

### Language & Locale

- All product names, categories, and subcategories are in **Macedonian Cyrillic**
- Category constants are duplicated between `backend/data/constants.py` (Python) and `src/lib/categories.ts` (TypeScript) — keep them in sync
- The `cyrtranslit` library is used for Latin↔Cyrillic transliteration in the embedding pipeline

---

## Data Pipeline

### 1. Scraping (`run_scrapers.py`)

Run all scrapers sequentially or in parallel:

```bash
python -m backend.data.run_scrapers            # sequential
python -m backend.data.run_scrapers --parallel 6 # concurrent scrapers
python -m backend.data.run_scrapers --dry-run   # list scrapers without running
```

Scrapers discover products and call `bulk_upsert_products_table()` to save to the unified `products` table.

### 2. Categorization (`categorize_products.py`)

```bash
python -m backend.data.categorize_products
```

Loads uncategorized products from `products` table, classifies them via Gemini (main → sub), and updates the table. Uses `response_schema` for robust batch processing.

### 3. Embedding (`embed_products.py`)

```bash
python -m backend.data.embed_products
```

Generates vector embeddings for products where `name_embedding` is NULL. Normalizes names, calls Gemini Embedding 001, stores 768-dim vectors.

### 4. Grouping (`group_products.py`)

```bash
python -m backend.data.group_products
```

Groups equivalent products across markets using pgvector cosine similarity. Enforces "one product per market per group" rule. Writes to `groups` table.

### 5. Full Pipeline (`run_pipeline.py`)

```bash
python -m backend.data.run_pipeline
```

Runs all stages: scrapers (parallel) → categorize → embed → group.

### 6. Serving (`api.py`)

```bash
uvicorn backend.api:app --reload
```

---

## Docker Deployment

The full stack runs via Docker Compose:

```bash
docker compose up --build
```

| Service    | Image                     | Port          | Description                                              |
| ---------- | ------------------------- | ------------- | -------------------------------------------------------- |
| `db`       | `pgvector/pgvector:pg18`  | 5454 → 5432  | PostgreSQL with pgvector; init scripts in `init/`        |
| `backend`  | `python:3.12-alpine`      | 8000          | FastAPI + uvicorn; depends on db health check            |
| `frontend` | `oven/bun` → `nginx:alpine` | 8080 → 80 | Multi-stage build: Bun builds Vite app, Nginx serves SPA |

---

## Frontend Development

```bash
bun install          # or npm install
bun run dev          # starts Vite dev server on port 8080
bun run build        # production build
bun run test         # run tests with Vitest
bun run lint         # ESLint
```

The API base URL is configurable via `VITE_API_BASE_URL` environment variable (defaults to `http://localhost:8000`).

---

## Supported Markets

| Key        | Display Name  | Color     | Scraper                  | Method                     |
| ---------- | ------------- | --------- | ------------------------ |----------------------------|
| `reptil`   | Reptil        | `#028261` | `Reptil_scraper.py`      | WooCommerce REST API       |
| `zito`     | Жито Маркет   | `#00ae41` | `Zito_scraper.py`        | WooCommerce REST API       |
| `kam`      | КАМ           | `#ea1a24` | `Kam_scraper.py`         | PDF price list parsing     |
| `ramstore` | Рамстор       | `#f78223` | `Ramstore_scraper.py`    | Web scraping + pandas      |
| `stokomak` | Стокомак      | `#f8d000` | `Stokomak_scraper.py`    | HTML table scraping        |
| `vero`     | Веро          | `#eb1c26` | `Vero_scraper.py`        | HTML table scraping        |

Market colors are defined in `tailwind.config.ts` and referenced in `src/lib/categories.ts` as `MARKET_INFO`.

---

## Database Schema (Tables & Views)

- **`products`** (table) — Unified table for all market products:
    - `id` (UUID), `name`, `price`, `singular_price` (text/real), `in_stock` (bool)
    - `market`, `link`, `image`, `description`
    - `main_category`, `sub_category`, `confidence`, `reasoning`
    - `name_embedding` (vector(768))
    - `group_id` (UUID, null), `existing_categories`
    - `categorized_at`, `ETL_loadtime`, `last_updated`

- **`groups`** (table) — Product groups:
    - `id`, `name`, `clean_name`, `main_category`, `sub_category`, `name_embedding`

- **`grouped_products`** (view) — Products grouped by equivalence. Defined as:

```sql
CREATE OR REPLACE VIEW public.grouped_products AS
SELECT g.id AS group_id,
    g.name AS group_name,
    g.main_category,
    g.sub_category,
    g.name_embedding,
    json_agg(json_build_object(
        'product_id', p.id,
        'name', p.name,
        'price', p.price,
        'market', p.market,
        'in_stock', p.in_stock,
        'image', p.image,
        'link', p.link,
        'singular_price', p.singular_price
    ) ORDER BY p.price) AS products
FROM groups g
    JOIN products p ON p.group_id = g.id
WHERE p.in_stock = true
GROUP BY g.id;
```

The `name_embedding` column uses pgvector's `vector(768)` type. Grouping uses the `<=>` (cosine distance) operator.

---

## Environment Variables

| Variable             | Used By  | Description                                                  |
| -------------------- | -------- | ------------------------------------------------------------ |
| `POSTGRES_HOST`      | Backend  | PostgreSQL host (default `db` in Docker, `localhost` locally) |
| `POSTGRES_PORT`      | Backend  | PostgreSQL port (default `5432` inside Docker, `5454` host)  |
| `POSTGRES_DB`        | Backend  | PostgreSQL database name (default `postgres`)                |
| `POSTGRES_USER`      | Backend  | PostgreSQL user (default `user`)                             |
| `POSTGRES_PASSWORD`  | Backend  | PostgreSQL password (default `password`)                     |
| `GOOGLE_API_KEY`     | Backend  | Google Gemini API key                                        |
| `VITE_API_BASE_URL`  | Frontend | API base URL (default: `http://localhost:8000`)              |

---

## Important Notes for Agents

1. **Category sync:** The category taxonomy exists in two places — `backend/data/constants.py` and `src/lib/categories.ts`. Any changes must be applied to both files.
2. **Scraper pattern:** New scrapers should follow the existing `{Market}_scraper.py` naming convention and call `save_products_to_products_table()` for persistence. They will be auto-discovered by `run_scrapers.py`.
3. **Embedding normalization:** Product names are transliterated to Cyrillic (`cyrtranslit.to_cyrillic`), lowercased, and words sorted alphabetically before embedding. The shared `normalize_name()` in `text_utils.py` must be used consistently in both `embed_products.py` and `api.py` search.
4. **Shared embedding client:** `get_embeddings_client()` and `normalize_embedding()` in `text_utils.py` are used by both the API (search) and the embedding pipeline. Do not duplicate this initialization.
5. **Rate limiting:** All Gemini API calls go through `RateLimiter` with conservative limits (3K RPM, batches of 100). Respect RPM/TPM budgets when adding new AI features.
6. **pgvector dependency:** The PostgreSQL instance must have the `pgvector` extension installed. The Docker Compose setup uses `pgvector/pgvector:pg18` image which includes it.
7. **No authentication:** The API currently has no auth. CORS is restricted to `localhost:8080`.
8. **Deprecated code:** `backend/data/deprecated/` contains old scraper implementations and a MongoDB-based db_utils — these are not in use.
9. **Test coverage:** Minimal — only a placeholder test exists. New features should include tests.
10. **Docker:** The backend uses `python:3.12-alpine` with `requirements-api.txt` (minimal deps). The frontend uses a multi-stage Bun → Nginx build. The DB uses health checks; the backend waits for DB readiness before starting.
