# backend/data — Data Pipeline Module Guide

Data pipeline orchestration for CenaPlus: scrape → categorize → extract features → group (Layers 1–5 non-LLM).

## Module Responsibilities

| Module | Role |
|--------|------|
| `constants.py` | Category taxonomy (loaded from `shared/categories.json`), Gemini prompt descriptions (`CATEGORY_DESCRIPTIONS`), computed compressed taxonomy string (`TAXONOMY_COMPRESSED`) |
| `db_utils.py` | PostgreSQL connection, upsert/batch update helpers, in-stock status management |
| `categorize_products.py` | Two-stage Gemini 3.1 Flash-Lite categorization (main → sub), Pydantic `response_schema`, batched via shared `RateLimiter` + `call_with_retries` |
| `extract_features.py` | Gemini 3.1 Flash-Lite feature extraction (manufacturer / product_name / quantity / metadata) into `products`; deterministic canonicalisation in Python; skips `Разно`/`Останато` |
| `group_products.py` | 5-layer non-LLM grouping + 4-tier display titles (`reptil` → `bigshop` → synthesized `Brand – Product Size` → cleanest raw fallback). `--apply` truncates `groups` and rewrites every `group_id` |
| `grouping_rules.py` | Layers 1–4 deterministic/IR rules: unified `base_qty`, `singular_price` qty recovery, raw safety specs, variant allowlist, consonant-skeleton/prefix alignment, same-market differentiators, Block-IDF + MNN matching |
| `local_embeddings.py` | Layer 5 Ollama `bge-m3` client & `public.bge_text_embeddings` (`vector(1024)`) cache for guarded semantic merging inside multi-market Core Grocery blocks |
| `text_utils.py` | Shared `normalize_string()` (lowercase + Cyrillic transliteration + punctuation flattening) and `strip_quantity_tokens()` — used by `backend/api.py`, `extract_features.py`, `grouping_rules.py` |
| `RateLimiter.py` | Async token-aware rate limiter for Gemini API, tracks RPM/TPM per 60-sec window across subprocesses |
| `run_scrapers.py` | Orchestrator: auto-discovers `*_scraper.py` files, runs sequential or parallel (ThreadPoolExecutor), writes JSON run summaries to `logs/` |
| `run_pipeline.py` | Full pipeline runner: `run_scrapers.py` → `categorize_products.py` → `extract_features.py` → `group_products.py --apply` sequentially as subprocesses, stops on first failure |
| `scrapers/` | `scraper_base.py` (`ScrapedProduct` + `MarketScraper`), shared platform bases (`proverkanaceni_platform.py`, `zur_platform.py`), and 12 market scrapers |
| `tools/grouping_keys.py` | Maintenance CLI to derive/recompute `product_name_key`, `product_name_sorted`, `variant_key`, `variant` |
| `tools/manufacturer_merges.py` | Maintenance CLI (`propose`, `--csv`, `--apply`, `--pair`) writing `manufacturers.merged_into` |
| `tools/product_name_aliases.py` | Maintenance CLI (`--all`, `--report`, `--csv`, `--apply`) for `product_name_token_aliases` |

## Key Conventions

### Product timestamps
- `products.etl_loadtime` is written on INSERT only (first load); `last_updated` moves only when a
  **scraped** field (`name`, `description`, `price`, `singular_price`, `in_stock`, `image`, `link`,
  `market`) actually changed. Both are enforced by `db_utils.bulk_upsert_products_table` plus the
  `products_set_timestamps_trg` trigger in `init/00_schema.sql`.
- Unchanged re-scrapes write **nothing** (`ON CONFLICT ... WHERE row IS DISTINCT FROM excluded`).
- Pipeline stages must not touch `etl_loadtime` or `last_updated`. Both are naive `timestamp`
  columns holding Python local time while the DB runs `Etc/UTC` — never author from SQL `now()`.

### Rate Limiting (MANDATORY for Gemini)
- **ALL Gemini API calls** must go through `get_rate_limiter(...)` + `call_with_retries(...)`.
- `categorize_products.py` and `extract_features.py` share `TEXT_MODEL_SCOPE` (`gemini-text`),
  persisted to `logs/rate-limiter/<scope>.json` across `run_pipeline.py` subprocesses.
- Failed batches stay **unsaved** (no `Разно`/0.0 fallback) so the next run retries them.

### Grouping (5 non-LLM layers)
- **Layer 1:** Unified `(manufacturer_id, qty_kind, base_qty)` blocking (`g=ml`, drops `is_fluid` from block key), `singular_price` qty recovery, Zur/ERP text cleanup, physical variant allowlist + raw safety specs (`AA/AAA`, SPF, `%`, `45+/65+`), and `product_name_token_aliases`.
- **Layers 2–4:** Self-supervised same-market differentiator discovery, Block-IDF weighted matching with Cyrillic consonant-skeleton / prefix / stem alignment, price-ratio guards, and Mutual-Nearest-Neighbor non-transitive clustering.
- **Layer 5:** Optional Ollama `bge-m3` (`bge_text_embeddings` cache) strictly inside multi-market Core Grocery blocks requiring same `sub_category` and all Layer 1–4 hard vetoes (`cos >= 0.72`, `price_ratio <= 1.80`). Skips cleanly if Ollama is offline or `--no-embeddings` is passed.

## Pipeline Commands

```bash
# Full pipeline (both script and module mode work)
python backend/data/run_pipeline.py
python -m backend.data.run_pipeline

# Individual stages
python -m backend.data.run_scrapers [--parallel N] [--dry-run]
python -m backend.data.categorize_products
python -m backend.data.extract_features
python -m backend.data.group_products --dry-run [--no-embeddings] [--warm-cache]
python -m backend.data.group_products --apply

# Maintenance tools
python -m backend.data.tools.grouping_keys --dry-run
python -m backend.data.tools.manufacturer_merges --help
python -m backend.data.tools.product_name_aliases --all --apply
```

## Requirements Files

| File | Purpose | Environment |
|------|---------|-------------|
| `requirements.txt` (repo root) | Full dependencies: pipeline + API + dev | Local development |
| `requirements-api.txt` (repo root) | Minimal API-only deps (`fastapi`, `uvicorn`, `psycopg2-binary`, `pydantic`, `python-dotenv`, `cyrtranslit`) | Docker backend service |
| `backend/data/requirements-data.txt` | Pinned pipeline deps (`google-genai`, `numpy`, `requests`, `pdfplumber`, `lxml`, etc.) | Data pipeline runs |
