# 🛒 CenaPlus

**Macedonian grocery price comparison app.** CenaPlus scrapes product data from 6 supermarket chains across North Macedonia, categorizes and groups equivalent products using Google Gemini AI, and presents a React frontend where users can compare prices side-by-side.

---

## Features

- **Multi-market scraping** — automated scrapers for 6 Macedonian supermarkets (Reptil, Жито Маркет, КАМ, Рамстор, Стокомак, Веро)
- **AI-powered categorization** — two-stage classification pipeline using Gemini 2.0 Flash with structured JSON schema enforcement
- **Semantic product grouping** — equivalent products matched across markets via Gemini embedding cosine similarity (pgvector)
- **Semantic search** — embed search queries and find products by meaning, not just keywords
- **Market filtering** — filter products by specific store on any subcategory page
- **Pagination** — configurable page size (12/24/36), page navigation with go-to-page input
- **Dark/Light mode** — theme toggle with system preference detection
- **Dockerized deployment** — full stack runs with a single `docker compose up`

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Frontend** | React 18, TypeScript, Vite, TanStack React Query, shadcn/ui, Tailwind CSS |
| **Backend** | Python 3.12, FastAPI, psycopg2, PostgreSQL + pgvector |
| **AI/ML** | Google Gemini 2.0 Flash (categorization), Gemini Embedding 001 (768-dim vectors) |
| **Scrapers** | BeautifulSoup, aiohttp, requests, pandas |
| **Deployment** | Docker Compose, Nginx, python:3.12-alpine, oven/bun |

---

## Supported Markets

| Market | Scraper Method | Color |
|--------|---------------|-------|
| Reptil | BeautifulSoup + retry | `#028261` |
| Жито Маркет | WooCommerce REST API (async) | `#00ae41` |
| КАМ | PDF price list parsing | `#ea1a24` |
| Рамстор | Web scraping + pandas | `#f78223` |
| Стокомак | HTML table scraping | `#f8d000` |
| Веро | HTML table scraping (threaded) | `#eb1c26` |

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
   - API: [http://localhost:8000](http://localhost:8000)
   - Database: `localhost:5454`

### Local Development

#### Frontend
```bash
bun install        # or npm install
bun run dev        # Vite dev server on port 8080
bun run build      # production build
bun run test       # run tests (Vitest)
bun run lint       # ESLint
```

#### Backend API
```bash
pip install -r requirements-api.txt
uvicorn backend.api:app --reload
```

---

## Data Pipeline

The pipeline runs in four sequential stages. Each can be run independently or all at once:

```bash
# Full pipeline
python -m backend.data.run_pipeline

# Individual stages
python -m backend.data.run_scrapers              # 1. Scrape products
python -m backend.data.categorize_products        # 2. Categorize with Gemini
python -m backend.data.embed_products             # 3. Generate embeddings
python -m backend.data.group_products             # 4. Group equivalent products
```

**Scraper options:**
```bash
python -m backend.data.run_scrapers --parallel 3   # run 3 scrapers concurrently
python -m backend.data.run_scrapers --dry-run      # list scrapers without running
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/categories` | Returns the full category taxonomy |
| `GET` | `/search?q=...` | Semantic vector search (≥0.80 similarity, top 15 results) |
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
│   ├── api.py                        # FastAPI server
│   └── data/
│       ├── constants.py              # Category taxonomy
│       ├── db_utils.py               # PostgreSQL helpers
│       ├── categorize_products.py    # Gemini categorization pipeline
│       ├── embed_products.py         # Embedding generation
│       ├── group_products.py         # Cross-market product grouping
│       ├── text_utils.py             # Shared utilities (normalize, embed client)
│       ├── RateLimiter.py            # Async rate limiter for Gemini API
│       ├── run_scrapers.py           # Scraper orchestrator
│       ├── run_pipeline.py           # Full pipeline runner
│       └── scrapers/                 # Market-specific scrapers
├── src/
│   ├── pages/                        # Index, SearchResults, NotFound
│   ├── components/                   # Header, ProductCard, SubCategoryView, etc.
│   ├── lib/                          # API client, categories, utilities
│   └── hooks/                        # Custom React hooks
├── init/                             # SQL init scripts for Docker DB
├── docker-compose.yml
├── Dockerfile.backend
├── Dockerfile.frontend
├── nginx.conf
├── requirements.txt                  # Full pipeline dependencies
└── requirements-api.txt              # API-only dependencies
```

---

## Database Schema

- **`products`** — all scraped products (name, price, market, embeddings, categories, group assignment)
- **`groups`** — product groups created by embedding similarity matching
- **`grouped_products`** (view) — joins groups with their in-stock products, ordered by price

Vectors use pgvector's `vector(768)` type with the `<=>` (cosine distance) operator.

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
| `VITE_API_BASE_URL` | API base URL for frontend (default: `http://localhost:8000`) |

---

## License

[MIT](LICENSE) © GaPPy the Capy
