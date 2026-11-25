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
- `bun run lint` — currently 0 errors, 2 pre-existing `react-refresh` warnings

**API / pipeline:**

- `uvicorn backend.api:app --reload`
- `python -m backend.data.run_scrapers [--parallel N] [--dry-run]`
- `python -m backend.data.categorize_products` / `.extract_features` / `.group_products --apply`
- Full pipeline: `python backend/data/run_pipeline.py` (or `python -m backend.data.run_pipeline`)

## Gotchas

1. **No package `__init__.py` files anywhere** — everything relies on namespace packages plus
   `PYTHONPATH`. Always import absolutely (`from backend.data.db_utils import ...`).
2. **Scrapers are launched as standalone scripts**, not modules: `run_scrapers.py` spawns each
   `*_scraper.py` with `cwd=scrapers/` and `PYTHONPATH=<root>:<scrapers/>`. So a scraper imports
   siblings unqualified (`from kam_pdf_utils import ...`) and project code absolutely.
3. **The DB schema lives in `init/00_schema.sql`** — one idempotent file that creates the
   extensions (`uuid-ossp`, `vector`, `pg_trgm`), tables (`manufacturers`, `manufacturer_aliases`,
   `products`, `groups`, `product_name_token_aliases`, `bge_text_embeddings`), constraints, indexes,
   the timestamp trigger and the `grouped_products` view. It is a *fresh-database bootstrap*: apply
   schema changes to a live DB with explicit statements, then fold them back into that file.
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
| `backend/api.py` | FastAPI: `/health`, `/categories`, `/search` (pg_trgm fuzzy), `/product?g_id=` (detail), `/manufacturer?manufacturer_name=` (all groups by brand), `/{main}/{sub}` |
| `backend/data/` | Pipeline stages (`run_pipeline`, `run_scrapers`, `categorize_products`, `extract_features`, `group_products`), `grouping_rules.py`, `local_embeddings.py`, `db_utils.py`, `text_utils.py`, `constants.py`, `RateLimiter.py` |
| `backend/data/tools/` | Manual maintenance CLIs: `grouping_keys.py`, `manufacturer_merges.py`, `product_name_aliases.py` |
| `backend/data/scrapers/` | One `{Market}_scraper.py` per market (all subclass `scraper_base.MarketScraper`), auto-discovered by `run_scrapers.py` |
| `backend/tests/` | stdlib `unittest` suite; no `__init__.py`, so run module paths |
| `backend/data/logs/` | Per-scraper logs + `run_summary-*.json` (untracked; `*.log` is git-ignored) |
| `frontend/` | React SPA (`src/`, `public/`, Vite/Tailwind/TS/ESLint configs, `nginx.conf`, `Dockerfile`); `frontend/src/components/ui/` holds only the 7 used shadcn/ui primitives |
| `init/` | `00_schema.sql` — the whole schema (extensions, tables, indexes, trigger, view), mounted into `docker-entrypoint-initdb.d` |

Per-directory detail: `backend/data/AGENTS.md`, `backend/data/scrapers/AGENTS.md`.

## Markets

Keys must match the `market` column, `MARKET_INFO` in `frontend/src/lib/categories.ts`, and the `--market-*`
tokens in `frontend/tailwind.config.ts`.

| Key | Scraper | Method |
| --- | ------- | ------ |
| `reptil` | `Reptil_scraper.py` | WooCommerce Store API (marketonline.mk) |
| `bigshop` | `Bigshop_scraper_maintenance.py` | WooCommerce Store API (bigshop.mk) |
| `zito` | `Zito_scraper.py` | HTML (zito.proverkanaceni.mk) |
| `stokomak` | `Stokomak_scraper.py` | HTML (stokomak.proverkanaceni.mk) |
| `vero` | `Vero_scraper.py` | HTML price list (pricelist.vero.com.mk) |
| `ramstore` | `Ramstore_scraper.py` | HTML per store page (16 stores) + pandas |
| `kam` | `Kam_scraper.py` | PDF price lists (camelot, pdfplumber fallback) |
| `dim` | `Dim_scraper.py` | AJAX price list on `ceni.zur.mk` (the iframe behind dim.mk/cenovnik), one request per store |
| `zur` | `Zur_scraper.py` | Same ZUR platform as `dim`, own store list (`ceni.zur.mk`), one request per store |
| `elida` | `Elida_scraper.py` | Same proverkanaceni.mk platform as `zito`/`stokomak` (100-row pages, 43 stores) |
| `spar` | `Spar_scraper.py` | JSON API `api.prod.spar.mk` (behind spar.mk/pricelist), Skopje stores only; names transliterated to Cyrillic |
| `kipper` | `Kipper_scraper.py` | `admin-ajax.php` action `get_products_data` per store (Skopje city term), products via WP REST |

## Frontend conventions

- Routing is **query-param based**, not path based: `/?category=<main>&sub=<sub>` renders
  `SubCategoryView`. Only routes are `/`, `/search`, `/product`, `/manufacturer`, `*`. `ProductCard`
  links to `/product?g_id=<uuid>`; the detail page is the only place out-of-stock offers are shown.
  The manufacturer chip there links to `/manufacturer?manufacturer_name=<canonical>`.
- Page state (`page`, `per_page` 12|24|36, market toggles) is local component state in
  `SubCategoryView`, not URL state; only category selection is in the URL. `ManufacturerView` keeps
  `manufacturer_name`, `page` and `per_page` in the URL so a brand listing is linkable.
- `frontend/src/components/Pagination.tsx` is shared by both listings (first/prev/numbered/next/last +
  go-to-page). `ProductCard` takes `showCategory` to render the main-category chip, which is what
  distinguishes a manufacturer listing (spans categories) from a subcategory listing.
- Server state via TanStack Query (`frontend/src/lib/api.ts` is the only fetch layer); the API returns
  `{ total, page, per_page, data }` for listings and `{ data }` for search.
- `/search` is **pg_trgm fuzzy matching** over `groups.product_name` + manufacturer names
  (similarity ≥ 0.8, ordered by similarity, no row cap).
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

- Tests: 10 Python `unittest` modules in `backend/tests/` (run as module paths, no `__init__.py`) and
  1 Vitest file (`frontend/src/test/categories-sync.test.ts`, which asserts the TS taxonomy matches
  `shared/categories.json` and has exactly 21 main categories — update both sides together).
- No CI, no migration tool (schema is bootstrapped by `init/00_schema.sql`; live changes are applied
  by hand and folded back into that file), no auth
  (CORS: `localhost:8080`, `cenaplus.net`, `www.cenaplus.net`).

<!-- BEGIN opencode-rag -->
## Code Navigation

ALWAYS use OpenCodeRAG tools before reading or editing:
- **Search first** — `search_semantic(query)` instead of grep/glob
- **Skeleton before read** — `get_file_skeleton(filePath)` then read specific lines
- **Usages before edit** — `find_usages(symbolName)` before modifying any symbol
- **Images via describe** — `describe_image(filePath, systemPrompt?)` — never read raw bytes
- **Recall quirks** — `recall_quirks(query)` when you hit a known pitfall
- **Add quirks** — `add_quirk(content)` when you discover a non-obvious fact
- **Fix quirks** — `update_quirk(id, ...)` / `delete_quirk(id)` when a stored quirk is outdated or wrong

If no results, run `opencode-rag index`.

### Decision tree — ALWAYS follow this order
1. User mentions code behavior/architecture → `search_semantic(query)`
2. User mentions a file path → `get_file_skeleton(filePath)` THEN `read` on specific lines
3. User mentions a function/class/variable to edit → `find_usages(symbolName)` THEN `search_semantic` THEN `edit`
4. User asks a code question → `search_semantic` to gather context before answering
5. User asks about an image or visual asset → `describe_image(filePath)` (optionally pass `systemPrompt` to focus on specific features) to retrieve its generated description, then optionally `search_semantic` for related code
6. You encounter an error or need to recall a known pitfall → `recall_quirks(query)`
7. You discover a non-obvious fact or workaround → `add_quirk(content)` to persist it for future sessions
8. A recalled quirk is outdated or wrong → `update_quirk(id, ...)` to fix it, or `delete_quirk(id)` if it no longer applies

### Proactive triggers — you MUST call these tools when
- User asks about code behavior, architecture, or implementation details
- User asks to edit, refactor, or fix code — call `find_usages` first
- User references files or functions you haven't read yet
- User says "find", "search", "look up", "where is", "how does"
- User refers to an image, screenshot, diagram, or visual asset
- Before answering ANY code-related question, retrieve context first
- Before reading ANY file, call `get_file_skeleton` to orient first

### Anti-patterns — NEVER do these
- Reading full files without calling `get_file_skeleton` first (wastes tokens)
- Editing a function without calling `find_usages` first (breaks call sites)
- Answering code questions without calling `search_semantic` first (you guess at behavior)
- Using `grep`/`glob` when `search_semantic` would find the answer faster
- Treating image files as text — use `describe_image` instead of reading raw bytes
- Using `npx opencode-rag quirk` shell commands instead of the built-in quirk tools (`add_quirk` / `recall_quirks` / `update_quirk` / `delete_quirk`) (the tools are faster, already loaded in-process, and go through the trust monitor)

### MANDATORY quirk capture rules — you MUST call `add_quirk` when
- A build, test, or type-check command fails and you resolve it
- You discover an undocumented library constraint, peer dep, or workaround
- You learn an environment-specific requirement (OS, tool version, etc.)
- You make a design decision that future sessions should remember
- You resolve a gotcha that cost more than one attempt

### MANDATORY quirk hygiene — you MUST call `update_quirk` or `delete_quirk` when
- A stored quirk is outdated, wrong, or has been fixed — update it or delete it instead of adding a contradicting duplicate
- NEVER finish a coding session without adding quirks for resolved errors.

### Long-running commands
Long-running commands:
- Any `shell` call expected to take more than 30s MUST set `background: true` (returns immediately; the session is notified on completion) or set an explicit `timeout` (ms).
- Default foreground timeout: 120000ms. Never run builds/tests/indexes in the foreground beyond 30s; if a call is already running, press Ctrl+B to background it (OpenCode: `session.background`).
- OpenCodeRAG measures foreground shell duration and records a quirk when a call exceeds the threshold, so repeated foreground runs become visible.
<!-- END opencode-rag -->
