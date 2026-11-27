# CenaPlus — Agent Guidelines

Macedonian grocery price-comparison app. Twelve market scrapers write into one PostgreSQL `products`
table; Gemini categorizes and extracts features, and grouping is non-LLM (Layers 1–4 deterministic/IR +
Layer 5 local Ollama `bge-m3` cache). FastAPI serves the grouped view to a React SPA.

```
scrape → categorize + extract features (gemini-3.1-flash-lite)
       → group (Layers 1–4 IR + Layer 5 Ollama bge-m3 cache) → FastAPI → React
```

## Commands

Run everything from the repo root.

**Frontend** (run inside `frontend/`, or via `bun --cwd frontend ...` — `frontend/bun.lockb` is the lockfile the Docker build consumes):

- `bun install`, `bun run dev` — Vite on :8080, proxies `/api/*` → `http://127.0.0.1:8000/*` (prefix stripped)
- `bun run test` / `bun run test:watch` — single file: `bunx vitest run src/test/categories-sync.test.ts`
- `bunx tsc -b` — the **only** typecheck. `bun run build` is `vite build` and does not typecheck.
- `bun run lint` — ESLint

**API / pipeline:**

- `uvicorn backend.api:app --reload`
- `python -m backend.data.run_scrapers [--parallel N] [--dry-run] [--force] [--no-refresh]`
- `python -m backend.data.categorize_products` / `.extract_features` / `.group_products --apply`
- Full pipeline: `python backend/data/run_pipeline.py [--force-scrape]` (or `python -m backend.data.run_pipeline`)

## Gotchas

1. **No package `__init__.py` files anywhere** — everything relies on namespace packages plus
   `PYTHONPATH`. Always import absolutely (`from backend.data.db_utils import ...`).
2. **Scrapers are launched as standalone scripts**, not modules: `run_scrapers.py` spawns each
   `*_scraper.py` with `cwd=scrapers/` and `PYTHONPATH=<root>:<scrapers/>`. So a scraper imports
   siblings unqualified (`from kam_pdf_utils import ...`) and project code absolutely.
3. **The DB schema lives in `init/00_schema.sql`** — one idempotent file that creates the
   extensions (`uuid-ossp`, `vector`, `pg_trgm`), tables (`manufacturers`, `manufacturer_aliases`,
   `products`, `groups`, `product_name_token_aliases`, `bge_text_embeddings`, `scraper_state`),
   constraints, indexes, the timestamp trigger, and the materialized read-model views
   (`grouped_products`, `manufacturer_stats`, `category_stats`). It is a *fresh-database bootstrap*:
   apply schema changes to a live DB with explicit statements, then fold them back into that file.
   Scrapers and `group_products.py --apply` auto-refresh the materialized views via
   `refresh_read_views()` (`REFRESH MATERIALIZED VIEW CONCURRENTLY`); for manual DB edits run
   `python -m backend.data.tools.refresh_views`.
4. `backend/api.py` connects to Postgres **at import time** — importing it needs a reachable DB.
   The API requires **no `GOOGLE_API_KEY`** (search is `pg_trgm`); `GOOGLE_API_KEY` is only needed
   by the `categorize_products` and `extract_features` pipeline stages.
5. `VITE_API_BASE_URL` defaults to `/api` (relative), not `http://localhost:8000`. Dev uses the Vite
   proxy, production uses nginx — both strip the `/api` prefix.
6. `frontend/nginx.conf` gates `/api/` on `valid_referers` (localhost, `(www.)cenaplus.net`) and rate-limits
   `10r/s` burst 10 per IP. Direct `curl` / non-browser clients get **403**; that is intentional.
7. `frontend/Dockerfile` runs `bun install --frozen-lockfile`. Add/remove deps inside `frontend/` with `bun add` /
   `bun remove` and commit `frontend/bun.lockb`.
8. `docker-compose.yml` publishes **no port for the backend**; the frontend container is the only
   entrypoint (`:8080`) and proxies `/api/` to `backend:8000`. For host-side dev use uvicorn directly.
9. `.dockerignore` excludes `*.md`, `init/`, `.opencode/`, `.playwright-mcp/`, and `node_modules/` (init is mounted by compose, not baked).
10. All Gemini calls go through `RateLimiter` (`get_rate_limiter(...)`). The two text-model stages
    (`categorize_products.py`, `extract_features.py`) share one scope, so the rolling 60 s window is
    carried across the `run_pipeline.py` subprocess boundary. Budgets come from `GEMINI_RPM_LIMIT` /
    `GEMINI_TPM_LIMIT` (defaults 3850 RPM / 3.85M TPM) and are lowered automatically to the
    `quotaValue` the server reports in a 429. Do not bypass `await rate_limiter.acquire(tokens)` —
    use `call_with_retries(...)` instead.
11. `products.etl_loadtime` = when the row **first** entered the warehouse (never rewritten);
    `products.last_updated` = when a **scraped** field last actually changed. `bulk_upsert_products_table`
    enforces both in SQL and a `BEFORE INSERT OR UPDATE` trigger (`init/00_schema.sql`)
    enforces them for direct writes. Pipeline stages (categorization, extraction, grouping) must not
    bump either one. Both are naive `timestamp` columns written with Python local time while the DB
    session runs `Etc/UTC`, so never author these from SQL `now()`.

## Pipeline stages are incremental (grouping is a full rebuild)

- Scrapers upsert by market and mark unseen names out of stock (`mark_out_of_stock_products_table`).
  An upsert whose rows are unchanged writes nothing at all (see gotcha 11).
- `categorize_products.py` re-picks rows with `main_category IS NULL`, `confidence IS NULL` or `< 0.5`.
- `extract_features.py` re-picks rows missing the extracted fields and writes `manufacturer_id`,
  `product_name_sorted`, `quantity_value/unit`, `is_fluid`. A batch that fails stays unsaved so the
  next run retries it (no `Разно`/0.0 fallback).
- `group_products.py` is **not** incremental: it requires `--dry-run` or `--apply`, and `--apply`
  truncates `groups`, rewrites every `products.group_id`, and seeds singleton groups for rows without
  extracted features. Grouping runs 5 non-LLM layers: Layer 1 unified `(manufacturer, base_qty)`
  blocking + `singular_price` qty recovery + raw safety specs + token aliases, Layers 2–4 pure-IR
  matcher (same-market differentiators, Block-IDF, phonetic/prefix alignment, MNN), and Layer 5
  optional Ollama `bge-m3` (`bge_text_embeddings` cache, Core Groceries + same `sub_category` only;
  skips cleanly if Ollama is offline or `--no-embeddings`). Display titles use a 4-tier rule
  (`reptil` → `bigshop` → synthesized `Brand – Product Size` with Latin units → cleanest raw name).

Re-running any stage is safe; running them out of order is not (extraction needs categories, grouping
needs extraction).

## Layout

| Path | Owns |
| ---- | ---- |
| `shared/categories.json` | **Single source of truth** for the taxonomy (21 main, 120 sub) |
| `backend/api.py` | FastAPI: `/health`, `/categories`, `/search` (pg_trgm fuzzy), `/product?g_id=` (detail), `/manufacturer?manufacturer_name=` (all groups by brand), `/{main}`, `/{main}/{sub}` |
| `backend/data/` | Pipeline stages (`run_pipeline`, `run_scrapers`, `categorize_products`, `extract_features`, `group_products`), `grouping_rules.py`, `local_embeddings.py`, `db_utils.py`, `text_utils.py`, `constants.py`, `RateLimiter.py` |
| `backend/data/tools/` | Manual maintenance CLIs: `grouping_keys.py`, `manufacturer_merges.py`, `product_name_aliases.py`, `refresh_views.py` |
| `backend/data/scrapers/` | One `{Market}_scraper.py` per market (all subclass `scraper_base.MarketScraper`), auto-discovered by `run_scrapers.py` |
| `backend/tests/` | stdlib `unittest` suite; no `__init__.py`, so run module paths |
| `backend/data/logs/` | Per-scraper logs + `run_summary-*.json` (untracked; `*.log` is git-ignored) |
| `frontend/` | React SPA (`src/`, `public/`, Vite/Tailwind/TS/ESLint configs, `nginx.conf`, `Dockerfile`); `frontend/src/components/ui/` holds only the 3 used shadcn/ui primitives |
| `init/` | `00_schema.sql` — the whole schema (extensions, tables, indexes, trigger, view), mounted into `docker-entrypoint-initdb.d` |

Per-directory detail: `backend/data/AGENTS.md`, `backend/data/scrapers/AGENTS.md`.

## Markets

Keys must match the `market` column, `MARKET_INFO` in `frontend/src/lib/categories.ts`, and the `--market-*`
tokens in `frontend/tailwind.config.ts`.

| Key | Scraper | Method |
| --- | ------- | ------ |
| `reptil` | `Reptil_scraper.py` | WooCommerce Store API (marketonline.mk) |
| `bigshop` | `Bigshop_scraper_maintenance.py` | WooCommerce Store API (bigshop.mk) — paused (filename skips `*_scraper.py` auto-discovery) |
| `zito` | `Zito_scraper.py` | HTML (zito.proverkanaceni.mk) |
| `stokomak` | `Stokomak_scraper.py` | HTML (stokomak.proverkanaceni.mk) |
| `vero` | `Vero_scraper.py` | HTML price list (pricelist.vero.com.mk) |
| `ramstore` | `Ramstore_scraper.py` | HTML per store page (15 Skopje stores) + pandas |
| `kam` | `Kam_scraper.py` | PDF price lists (camelot, pdfplumber fallback) |
| `dim` | `Dim_scraper.py` | AJAX price list on `dim.marketceni.mk` (the iframe behind dim.mk/cenovnik), one request per store |
| `zur` | `Zur_scraper.py` | Same ZUR platform as `dim`, own store list (`ceni.zur.mk`), one request per store |
| `elida` | `Elida_scraper.py` | Same proverkanaceni.mk platform as `zito`/`stokomak` (100-row pages, 43 stores) |
| `spar` | `Spar_scraper.py` | JSON API `api.prod.spar.mk` (behind spar.mk/pricelist), Skopje stores only; names transliterated to Cyrillic |
| `kipper` | `Kipper_scraper.py` | `admin-ajax.php` action `get_products_data` per store (Skopje city term), products via WP REST |

## Frontend conventions

- Routing is **query-param based**, not path based: `/?category=<main>` and `/?category=<main>&sub=<sub>`
  render `SubCategoryView`. Only routes are `/`, `/search`, `/product`, `/manufacturer`, `*`. `ProductCard`
  links to `/product?g_id=<uuid>`; the detail page is the only place out-of-stock offers are shown.
  The manufacturer chip there links to `/manufacturer?manufacturer_name=<canonical>`.
- Page state (`page`, `per_page` 12|24|36, market toggles, `on_sale`, `sort`) is local component state in
  `SubCategoryView`, not URL state; only category selection is in the URL. `ManufacturerView` keeps
  `manufacturer_name`, `page` and `per_page` in the URL so a brand listing is linkable.
- `frontend/src/components/Pagination.tsx` is shared by both listings (first/prev/numbered/next/last +
  go-to-page). `ProductCard` takes `showCategory` to render the main-category chip, which is what
  distinguishes a manufacturer listing (spans categories) from a subcategory listing.
- Server state via TanStack Query (`frontend/src/lib/api.ts` is the only fetch layer); listings and
  `/search` return `{ total, page, per_page, data }`.
- `/search` is **pg_trgm fuzzy matching** over `groups.product_name` + manufacturer names
  (similarity ≥ 0.8, capped at 500 matches, paginated, with market/on-sale/sort filters).
- Cart is frontend-only: `frontend/src/context/CartContext.tsx` persists to `localStorage["cenaplus_cart"]`.
  There is no cart API or checkout.
- All UI copy, categories and product names are Macedonian Cyrillic. Keep new strings Cyrillic.
- `@/` → `frontend/src/`. TypeScript is deliberately loose (`strict: false`, `noImplicitAny: false`).

## Environment

`.env` at the repo root (loaded by `dotenv`, resolved from the cwd — stage scripts are spawned with
`cwd=backend/data`, so the parent search finds it): `POSTGRES_HOST/PORT/DB/USER/PASSWORD`,
`GOOGLE_API_KEY`, `VITE_API_BASE_URL`.

Host-side Docker map: Postgres on `localhost:5454` (`POSTGRES_PORT`), API on `:8000` from uvicorn,
frontend on `:8080`.

Dependency files: `backend/requirements.txt` (what the backend Docker image installs),
`backend/data/requirements-data.txt` (pinned pipeline/scraper deps, incl. `pdfplumber`/`lxml`).

## Known state

- Tests: 14 Python `unittest` modules in `backend/tests/` (run as module paths, no `__init__.py`) and
  2 Vitest files in `frontend/src/test/` (`categories-sync.test.ts` asserts the TS taxonomy matches
  `shared/categories.json` with 21 main categories — update both sides together; `sale.test.ts` covers
  sale-price helpers).
- No CI, no migration tool (schema is bootstrapped by `init/00_schema.sql`; live changes are applied
  by hand and folded back into that file), no auth
  (CORS: `localhost:8080`, `cenaplus.net`, `www.cenaplus.net`).
