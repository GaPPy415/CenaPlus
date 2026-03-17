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
```

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
    - `GET /search?q=...`: Semantic vector search (≥0.80 similarity)
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

```bash
# Full pipeline: scrape → categorize → embed → group
python -m backend.data.run_pipeline

# Individual stages
python -m backend.data.run_scrapers              # sequential
python -m backend.data.run_scrapers --parallel 6 # concurrent
python -m backend.data.categorize_products       # Gemini categorization
python -m backend.data.embed_products            # Generate embeddings
python -m backend.data.group_products            # Group by similarity
uvicorn backend.api:app --reload               # Serve API
```

---

## Docker Deployment

Full stack runs via Docker Compose (`docker compose up --build`). PostgreSQL with pgvector, FastAPI backend (port 8000), Nginx frontend (port 8080). See `docker-compose.yml` for service configuration.

---

## Frontend Development

`bun install`, `bun run dev` (port 8080), `bun run build`, `bun run test`, `bun run lint`. API base URL via `VITE_API_BASE_URL` env var (default: `http://localhost:8000`).

---

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

## Database Schema

- **`products`** — Unified table for all markets: name, price, market, embeddings, categories, group assignment
- **`groups`** — Product groups created by embedding similarity matching
- **`grouped_products`** (view) — Joins groups with in-stock products, ordered by price
- Vectors use pgvector's `vector(768)` type; grouping uses `<=>` (cosine distance) operator

---

## Environment Variables

See `docker-compose.yml` and `.env.example` for configuration. Key variables: `POSTGRES_*` (database connection), `GOOGLE_API_KEY` (Gemini API), `VITE_API_BASE_URL` (frontend API endpoint).

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
11. **Category taxonomy resolved:** Categories are centrally defined. Refer to `backend/data/constants.py` for the single source of truth (duplicated in `src/lib/categories.ts`).
12. **Requirements files:** `requirements.txt` (full dependencies), `requirements-api.txt` (API only), `requirements-data.txt` (pipeline with pinned versions).
