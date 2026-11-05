# backend/data — Data Pipeline Module Guide

Data pipeline orchestration for CenaPlus: scrape → categorize + extract features → embed → group products.

## Module Responsibilities

| Module | Role |
|--------|------|
| `constants.py` | Category taxonomy (loaded from `shared/categories.json`), Gemini prompt descriptions (`CATEGORY_DESCRIPTIONS`), computed compressed taxonomy string (`TAXONOMY_COMPRESSED`), market kipper links |
| `db_utils.py` | PostgreSQL connection, upsert/batch update helpers, in-stock status management. `group_products_by_category` is a legacy similarity helper kept for reference — the live grouper does not call it |
| `categorize_products.py` | Two-stage Gemini 3.1 Flash-Lite categorization (main → sub), Pydantic `response_schema` for structured output, batched via the shared `RateLimiter` + `call_with_retries` |
| `extract_features.py` | Gemini 3.1 Flash-Lite feature extraction (manufacturer / product_name / quantity / metadata) into `products`; deterministic canonicalisation in Python; skips `Разно`/`Останато` |
| `embed_products.py` | Gemini Embedding 001 generation (768-dim), name normalization via `normalize_name()`, L2 normalization, batch upsert to `name_embedding` column |
| `group_products.py` | Deterministic grouping: blocks by (manufacturer, canonical name, quantity, unit, fluid), refines with `grouping_rules.py`, seeds singleton groups for unextracted rows. `--apply` truncates `groups` and rewrites every `group_id`; no similarity thresholds |
| `text_utils.py` | **Shared utilities:** `normalize_string()` (lowercase + Cyrillic transliteration + punctuation flattening, used for search and manufacturer resolution), `normalize_name()` (alphabetical token sort), `strip_quantity_tokens()`, lazy `get_embeddings_client()` / `normalize_embedding()` — used by `embed_products.py` and `backend/api.py` |
| `RateLimiter.py` | Async token-aware rate limiter for Gemini API, tracks RPM/TPM per 60-sec window; budgets are passed per call site (see below) |
| `run_scrapers.py` | Orchestrator: auto-discovers `*_scraper.py` files, runs sequential or parallel (ThreadPoolExecutor), generates JSON run summaries to `logs/` |
| `scrapers/scraper_base.py` | `ScrapedProduct` + `MarketScraper` ABC: each market supplies `fetch()`, the base owns dedupe, UUID reuse, timestamping, upsert, out-of-stock marking |
| `run_pipeline.py` | Full pipeline runner: runs `run_scrapers.py` → `categorize_products.py` → `extract_features.py` → `embed_products.py` → `group_products.py --apply` sequentially as subprocesses, stops on first failure |
| `grouping_rules.py` | Variant/normalisation rules (canonical variant, key aliases, unit folding) shared by the grouper and collision tooling |
| `manufacturer_merges.py` | Alias/merge proposal + apply tool (`propose`, `--csv`, `--apply`, `--pair`) writing `merged_into` |
| `product_name_aliases.py` | Token-spelling alias proposer/apply tool (`--report`, `--csv`, `--apply`) for `product_name_token_aliases`; the grouper resolves aliases before blocking |

## Key Conventions

### Product timestamps
- `products.etl_loadtime` is written on INSERT only (first load); `last_updated` moves only when a
  **scraped** field (`name`, `description`, `price`, `singular_price`, `in_stock`, `image`, `link`,
  `market`) actually changed. Both are enforced by `db_utils.bulk_upsert_products_table` plus the
  `products_set_timestamps_trg` trigger from `init/00_schema.sql`.
- The upsert's `ON CONFLICT ... WHERE <row> IS DISTINCT FROM <excluded row>` means an unchanged
  re-scrape writes **nothing** — no dead tuples, no timestamp movement. `xmin` is asserted unchanged in
  `backend/tests/test_product_timestamps.py`.
- Pipeline stages must not touch them: categorization writes `categorized_at`, embeddings write
  `name_embedding`, grouping writes `group_id`.
- They are naive `timestamp` columns holding Python local time, while the DB session timezone is
  `Etc/UTC` — never author them from SQL `now()`.

### Pipeline indexes
`init/00_schema.sql` (the single idempotent schema file) defines the indexes the stages rely on,
notably `groups (main_category, sub_category)` and
`products (main_category, sub_category) WHERE group_id IS NULL AND name_embedding IS NOT NULL`.
The partial index keeps the pending-embedding scan cheap as the table grows; the old similarity
grouping measured 6.7 ms → 0.6 ms per product with the `groups` index.

### Embedding Normalization
- `normalize_name()` in `text_utils.py` MUST be used consistently in both:
  - `embed_products.py` (during initial embedding generation)
  - `backend/api.py` (during search query embedding)
- Ensures query embeddings match product embeddings for semantic search

### Gemini Clients (Mixed Usage)
- **`text_utils.py`** uses langchain wrapper: `GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001")`
- **`categorize_products.py`** and **`extract_features.py`** use the direct SDK: `google.genai.Client()` with Pydantic `response_schema` for structured JSON output
- Do NOT mix clients — each module has its specific integration

### Category Constants
- `CATEGORIES` dict (main → subcategories) loaded from `shared/categories.json`
- `CATEGORY_DESCRIPTIONS` dict in `constants.py` is for Gemini prompts only — NOT moved to JSON (frontend doesn't need it)
- `TAXONOMY_COMPRESSED` string computed at import time after loading CATEGORIES (used for Gemini context)

### Rate Limiting (MANDATORY)
- **ALL Gemini API calls** must go through a `RateLimiter` obtained from
  `get_rate_limiter(...)` — never construct one ad hoc.
- `categorize_products.py` + `extract_features.py` share the `gemini-text` scope and the
  `TEXT_MODEL_RPM_LIMIT` / `TEXT_MODEL_TPM_LIMIT` budget (`GEMINI_RPM_LIMIT` /
  `GEMINI_TPM_LIMIT`, defaults 3850 / 3.85M). Same model, same account, so they share one
  rolling 60 s window.
- `embed_products.py`: `get_rate_limiter(2850, 1_000_000, scope="gemini-embedding")`.
- The window is persisted to `logs/rate-limiter/<scope>.json` and reloaded on the next
  process start: `run_pipeline.py` runs stages as subprocesses, so without it the extractor
  would reset the window and immediately burst past Gemini's per-minute quota.
- Wrap the SDK call in `call_with_retries(request, rate_limiter=..., estimated_tokens=...)`
  instead of calling `acquire` by hand. It retries 429/5xx with the server's `retryDelay`
  and lowers the limiter's budget to the reported `quotaValue` (a free-tier key is 15 RPM
  for `gemini-3.1-flash-lite`, not 3,850).
- A batch that still fails must stay **unsaved** (no `Разно`/0.0 fallback): the categorizer
  and extractor both leave failed rows out of the upsert so the next run retries them.

### Grouping (deterministic, no thresholds)
- `group_products.py` blocks rows by `(manufacturer_id, quantity_value, quantity_unit, is_fluid)`,
  then refines each block with `grouping_rules.py` (canonical variant, key aliases, unit folding,
  word-order/typo guards). Same-market rows merge only when their variants are compatible.
- There is no similarity threshold and no `GROUPING_THRESHOLDS`; embeddings are not read by the grouper.
- `--apply` truncates `groups`, rebuilds every group and rewrites `products.group_id`; rows without
  extracted features become singleton groups per `(market, raw name)`.
- `manufacturer_merges.py` maintains `manufacturers.merged_into` / aliases; the API follows those
  chains when resolving `/manufacturer?manufacturer_name=`.
- `product_name_aliases.py` proposes token-spelling aliases (`алпен`/`алпине` -> `алпин`) into the
  optional `product_name_token_aliases` table; `group_products.py` rewrites name tokens through it
  before blocking (no table = no change). The table is not created by the pipeline.

## Pipeline Commands

```bash
# Full pipeline (all stages) — NOTE: script mode only; `-m backend.data.run_pipeline`
# raises ModuleNotFoundError because run_pipeline.py imports `run_scrapers` unqualified
python backend/data/run_pipeline.py

# Individual stages
python -m backend.data.run_scrapers                # Stage 1: Scrape all markets
python -m backend.data.categorize_products         # Stage 2: Categorize via Gemini
python -m backend.data.extract_features            # Stage 3: Manufacturer / name / quantity
python -m backend.data.embed_products              # Stage 4: Generate embeddings
python -m backend.data.group_products --apply      # Stage 5: Rebuild deterministic groups

# Scraper options
python -m backend.data.run_scrapers --parallel 3   # Run 3 scrapers concurrently
python -m backend.data.run_scrapers --dry-run      # List scrapers without running

# Grouping preview (no writes)
python -m backend.data.group_products --dry-run
```

There are no `__init__.py` files in this tree — imports resolve through namespace packages and
`PYTHONPATH`, so always use `from backend.data.X import ...` from other modules.

`run_pipeline.py` passes `--parallel <number of scrapers>` to `run_scrapers.py`, i.e. the full pipeline
scrapes all markets concurrently.

Logs saved to `backend/data/logs/` with timestamps. Run summary as JSON in `run_summary-YYYYMMDD-HHMMSS.json`.

## Requirements Files

| File | Purpose | Environment |
|------|---------|-------------|
| `requirements.txt` (repo root) | Full dependencies: pipeline + API + dev tools | Local development |
| `requirements-api.txt` (repo root) | API-only, minimal deps (FastAPI, psycopg2, langchain-google-genai) | Docker backend service |
| `requirements-data.txt` (here) | Pipeline-only with pinned versions (reproducible data processing) | Data pipeline runs |

See root `AGENTS.md` for tech stack, Docker deployment, frontend conventions, and environment variables.
