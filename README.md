# 🛒 CenaPlus

**Macedonian grocery price comparison app.** CenaPlus scrapes product data from 12 supermarket chains across North Macedonia, categorizes and groups equivalent products using Google Gemini AI, and presents a React frontend where users can compare prices side-by-side.

---

## Features

- **Multi-market scraping** — automated scrapers for 12 Macedonian supermarkets (Reptil, BigShop, Жито Маркет, КАМ, Рамстор, Стокомак, Веро, Дим, ЗУР, Елида, Спар, Кипер)
- **AI-powered categorization** — two-stage classification pipeline using Gemini 3.1 Flash Lite with structured JSON schema enforcement
- **Deterministic product grouping** — identical manufacturer + name + size + variant, with guarded typo/word-order merging; no per-category similarity thresholds
- **Fuzzy search** — trigram matching (pg_trgm) over canonical product and manufacturer names, similarity ≥ 0.8
- **Product detail pages** — every card links to `/product?g_id=…`, showing all markets (in stock and out), manufacturer, size, price spread and short-expiry flags
- **Manufacturer pages** — the manufacturer chip opens `/manufacturer?manufacturer_name=…`, listing every product from that brand across all categories
- **Market filtering** — filter products by specific store on any subcategory page
- **Shopping cart** — client-side cart persisted in `localStorage`
- **Pagination** — configurable page size (12/24/36), page navigation with go-to-page input
- **Dark/Light mode** — theme toggle with system preference detection
- **Dockerized deployment** — full stack runs with a single `docker compose up`

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Frontend** | React 18, TypeScript, Vite, TanStack React Query, shadcn/ui, Tailwind CSS |
| **Backend** | Python 3.12, FastAPI, psycopg2, PostgreSQL + pgvector |
| **AI/ML** | Gemini 3.1 Flash Lite (categorization + feature extraction), local Ollama `bge-m3` (cached Layer 5 grouping vectors) |
| **Scrapers** | BeautifulSoup, aiohttp, requests, pandas |
| **Deployment** | Docker Compose, Nginx, python:3.12-alpine, oven/bun |

---

## Supported Markets

| Market | Scraper Method | Color |
|--------|---------------|-------|
| Reptil | WooCommerce Store API (`marketonline.mk`) | `#028261` |
| BigShop | WooCommerce Store API (`bigshop.mk`) | `#f88520` |
| Жито Маркет | HTML scraping (`zito.proverkanaceni.mk`) | `#00ae41` |
| КАМ | PDF price list parsing (per store) | `#ea1a24` |
| Рамстор | Web scraping + pandas (per store page) | `#f78223` |
| Стокомак | HTML table scraping | `#f8d000` |
| Веро | HTML price list scraping (threaded) | `#eb1c26` |
| Дим | AJAX price list on `ceni.zur.mk` | `#a8adb2` |
| ЗУР | AJAX price list on `ceni.zur.mk` | `#f5a600` |
| Елида | HTML scraping (`proverkanaceni.mk` platform) | `#1b3a74` |
| Спар | JSON API `api.prod.spar.mk` (Skopje stores) | `#00954a` |
| Кипер | `admin-ajax.php` per store (Skopje) | `#e11d48` |

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- A [Google Gemini API key](https://ai.google.dev/) (for the data pipeline)

### Quick Start (Docker)

1. **Clone the repo:**
   ```bash
   git clone https://github.com/your-username/CenaPlus.git
   cd CenaPlus
   ```

2. **Set environment variables** — create a `.env` file in the project root:
   ```env
   POSTGRES_DB=postgres
   POSTGRES_USER=user
   POSTGRES_PASSWORD=password
   POSTGRES_PORT=5454
   GOOGLE_API_KEY=your_gemini_api_key
   ```

3. **Start the stack:**
   ```bash
   docker compose up --build
   ```

4. **Access the app:**
   - Frontend: [http://localhost:8080](http://localhost:8080)
   - API: served behind the frontend at `/api` (compose does not publish the backend port)
   - Database: `localhost:5454`

### Local Development

#### Frontend
```bash
cd frontend
bun install        # or npm install
bun run dev        # Vite dev server on port 8080 (proxies /api → 127.0.0.1:8000)
bun run build      # production build (Vite only, no typecheck)
bunx tsc -b        # typecheck
bun run test       # run tests (Vitest)
bun run lint       # ESLint
```

#### Backend API
```bash
pip install -r backend/requirements.txt
uvicorn backend.api:app --reload
```

The API connects to PostgreSQL at import time and needs a reachable database. It requires no
`GOOGLE_API_KEY` (search uses PostgreSQL `pg_trgm`); `GOOGLE_API_KEY` is only needed by the
categorization and feature-extraction pipeline stages.

---

## Data Pipeline

The pipeline runs in four sequential stages. Each can be run independently or all at once:

```bash
# Full pipeline
python backend/data/run_pipeline.py

# Individual stages
python -m backend.data.run_scrapers              # 1. Scrape products
python -m backend.data.categorize_products        # 2. Categorize with Gemini
python -m backend.data.extract_features           # 3. Manufacturer / name / quantity features
python -m backend.data.group_products --apply     # 4. 5-layer non-LLM grouping (rebuilds `groups`)
```

Each stage is safe to re-run and only picks up rows that still need work (missing category or
features), so the stages must run in order. Grouping is the exception: `--apply` truncates
`groups` and rebuilds every `group_id`, so run it after extraction, not concurrently with it.

**Scraper options:**
```bash
python -m backend.data.run_scrapers --parallel 3   # run 3 scrapers concurrently
python -m backend.data.run_scrapers --dry-run      # list scrapers without running
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check (`{"status": "healthy"}`) |
| `GET` | `/categories` | Returns the full category taxonomy |
| `GET` | `/search?q=...` | Fuzzy search (pg_trgm, similarity ≥ 0.8, ordered by similarity) |
| `GET` | `/product?g_id=...` | Full product detail: metadata, price stats, every market offer (in and out of stock) |
| `GET` | `/manufacturer?manufacturer_name=...` | Every grouped product of one manufacturer, across all categories (alias/`merged_into` aware) |
| `GET` | `/{main_category}/{sub_category}` | Paginated grouped products with optional market filter |

**Query parameters for product listing:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `page` | int | 1 | Page number |
| `per_page` | 12 \| 24 \| 36 | 12 | Items per page |
| `market` | list[str] | all | Filter by market name(s) |

---

## Project Structure

```
CenaPlus/
├── backend/
│   ├── Dockerfile                    # API container (python:3.12-alpine)
│   ├── requirements.txt              # API-only dependencies (Docker backend image)
│   ├── api.py                        # FastAPI server
│   ├── data/
│   │   ├── constants.py              # Category taxonomy (loaded from shared/categories.json)
│   │   ├── db_utils.py               # PostgreSQL helpers
│   │   ├── categorize_products.py    # Gemini categorization pipeline
│   │   ├── extract_features.py       # Gemini manufacturer/name/quantity extraction
│   │   ├── group_products.py         # 5-layer non-LLM cross-market grouping
│   │   ├── grouping_rules.py         # Normalization, safety specs, and IR matching rules
│   │   ├── local_embeddings.py       # Local Ollama bge-m3 embedding cache (Layer 5)
│   │   ├── text_utils.py             # Shared text normalization helpers
│   │   ├── RateLimiter.py            # Async rate limiter for Gemini API
│   │   ├── run_scrapers.py           # Scraper orchestrator
│   │   ├── run_pipeline.py           # Full pipeline runner
│   │   ├── tools/                    # Maintenance CLIs (aliases, manufacturer merges, keys)
│   │   ├── requirements-data.txt     # Pinned pipeline & scraper dependencies
│   │   └── scrapers/                 # Market-specific scrapers
│   └── tests/                        # stdlib unittest suite
├── frontend/
│   ├── Dockerfile                    # Multi-stage Bun build + Nginx container
│   ├── nginx.conf                    # Reverse proxy + rate limiting
│   ├── src/
│   │   ├── pages/                    # Index, SearchResults, ProductDetail, ManufacturerView, NotFound
│   │   ├── components/               # Header, ProductCard, SubCategoryView, Pagination, CartSidebar, etc.
│   │   ├── context/                  # CartContext (localStorage-backed cart)
│   │   ├── lib/                      # API client, categories, utilities
│   │   └── hooks/                    # Custom React hooks
│   └── public/                       # Static assets
├── shared/
│   └── categories.json               # Single source of truth for the taxonomy
├── init/                             # 00_schema.sql - complete DB schema (tables, indexes, trigger, view)
└── docker-compose.yml
```

---

## Database Schema

- **`products`** — all scraped products (name, price, market, categories, extracted features, group assignment)
- **`groups`** — cross-market product groups, one row per product family
- **`manufacturers` / `manufacturer_aliases`** — canonical brand names, aliases and `merged_into` chains
- **`product_name_token_aliases`** — cross-market synonym/abbreviation aliases for grouping
- **`bge_text_embeddings`** — cached 1024-dim local `bge-m3` vectors for Layer 5 grouping
- **`grouped_products`** (view) — joins groups with their in-stock products, ordered by price

> The complete schema is `init/00_schema.sql` — extensions, all tables, constraints, indexes, the
> timestamp trigger and the `grouped_products` view. A fresh Docker volume creates it automatically;
> to recover a dropped database: `createdb products_db && psql -d products_db -f init/00_schema.sql`.

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `POSTGRES_HOST` | DB host (`db` in Docker, `localhost` locally) |
| `POSTGRES_PORT` | DB port (`5432` internal, `5454` host) |
| `POSTGRES_DB` | Database name (default: `postgres`) |
| `POSTGRES_USER` | Database user (default: `user`) |
| `POSTGRES_PASSWORD` | Database password (default: `password`) |
| `GOOGLE_API_KEY` | Google Gemini API key |
| `VITE_API_BASE_URL` | API base URL for the frontend (default: `/api`, proxied to the backend by Vite in dev and nginx in production) |

---

## License

[MIT](LICENSE) © GaPPy the Capy
