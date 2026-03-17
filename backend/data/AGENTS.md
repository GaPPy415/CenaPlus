# backend/data — Data Pipeline Module Guide

Data pipeline orchestration for CenaPlus: scrape → categorize → embed → group products.

## Module Responsibilities

| Module | Role |
|--------|------|
| `constants.py` | Category taxonomy (loaded from `shared/categories.json`), Gemini prompt descriptions (`CATEGORY_DESCRIPTIONS`), computed compressed taxonomy string (`TAXONOMY_COMPRESSED`), market kipper links |
| `db_utils.py` | PostgreSQL connection, upsert/batch update helpers, pgvector grouping functions (`group_products_by_category`), in-stock status management |
| `categorize_products.py` | Two-stage Gemini 2.0 Flash categorization (main → sub), Pydantic `response_schema` for structured output, batch processing via `RateLimiter` |
| `embed_products.py` | Gemini Embedding 001 generation (768-dim), name normalization via `normalize_name()`, L2 normalization, batch upsert to `name_embedding` column |
| `group_products.py` | Cross-market product grouping using pgvector cosine similarity (`<=>` operator), per-category thresholds (dict), default 0.95, one-per-market-per-group enforcement |
| `text_utils.py` | **Shared utilities:** `normalize_name()` (Cyrillic transliteration + alphabetical sort), `get_embeddings_client()` (langchain wrapper), `normalize_embedding()` (L2 norm) — used by both `embed_products.py` and `backend/api.py` search |
| `RateLimiter.py` | Async token-aware rate limiter for Gemini API, tracks RPM/TPM per 60-sec window, conservative defaults: 1900 RPM / 3.8M TPM |
| `run_scrapers.py` | Orchestrator: auto-discovers `*_scraper.py` files, runs sequential or parallel (ThreadPoolExecutor), generates JSON run summaries to `logs/` |
| `run_pipeline.py` | Full pipeline runner: executes `run_scrapers.py` → `categorize_products.py` → `embed_products.py` → `group_products.py` sequentially, stops on first failure |

## Key Conventions

### Embedding Normalization
- `normalize_name()` in `text_utils.py` MUST be used consistently in both:
  - `embed_products.py` (during initial embedding generation)
  - `backend/api.py` (during search query embedding)
- Ensures query embeddings match product embeddings for semantic search

### Gemini Clients (Mixed Usage)
- **`text_utils.py`** uses langchain wrapper: `GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-001")`
- **`categorize_products.py`** uses direct SDK: `google.genai.Client()` with Pydantic `response_schema` for structured JSON output
- Do NOT mix clients — each module has its specific integration

### Category Constants
- `CATEGORIES` dict (main → subcategories) loaded from `shared/categories.json`
- `CATEGORY_DESCRIPTIONS` dict in `constants.py` is for Gemini prompts only — NOT moved to JSON (frontend doesn't need it)
- `TAXONOMY_COMPRESSED` string computed at import time after loading CATEGORIES (used for Gemini context)

### Rate Limiting (MANDATORY)
- **ALL Gemini API calls** must go through `RateLimiter` instance
- `categorize_products.py`: `rate_limiter = RateLimiter(rpm_limit=1900, tpm_limit=3800000)`
- `embed_products.py`: `rate_limiter = RateLimiter(rpm_limit=2850, tpm_limit=1000000)`
- Call `await rate_limiter.acquire(estimated_tokens)` before API request to respect budget

### Grouping Thresholds
- Per-category thresholds defined in `GROUPING_THRESHOLDS` dict in `group_products.py`
- Default: 0.95 cosine similarity for unlisted categories
- Specialized categories (e.g., milk, water) may use higher thresholds (0.96–0.99)

## Pipeline Commands

```bash
# Full pipeline (all stages)
python -m backend.data.run_pipeline

# Individual stages
python -m backend.data.run_scrapers                # Stage 1: Scrape all markets
python -m backend.data.categorize_products         # Stage 2: Categorize via Gemini
python -m backend.data.embed_products              # Stage 3: Generate embeddings
python -m backend.data.group_products              # Stage 4: Group by similarity

# Scraper options
python -m backend.data.run_scrapers --parallel 3   # Run 3 scrapers concurrently
python -m backend.data.run_scrapers --dry-run      # List scrapers without running
```

Logs saved to `backend/data/logs/` with timestamps. Run summary as JSON in `run_summary-YYYYMMDD-HHMMSS.json`.

## Requirements Files

| File | Purpose | Environment |
|------|---------|-------------|
| `requirements.txt` | Full dependencies: pipeline + API + dev tools | Local development |
| `requirements-api.txt` | API-only, minimal deps (FastAPI, psycopg2, google-genai) | Docker backend service |
| `requirements-data.txt` | Pipeline-only with pinned versions (reproducible data processing) | Data pipeline runs |

See root `AGENTS.md` for tech stack, Docker deployment, frontend conventions, and environment variables.
