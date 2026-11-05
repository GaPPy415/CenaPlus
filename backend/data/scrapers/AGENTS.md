# Scrapers — Agent Documentation

Minimal contract guide for adding or debugging CenaPlus market scrapers.

---

## Scraper Contract

Every scraper is a **strategy** for one market, plugged into a shared **template method**.

| Piece | Role |
| ----- | ---- |
| `backend/data/scrapers/scraper_base.py` | `ScrapedProduct` dataclass + `MarketScraper` ABC. Owns dedupe, UUID reuse, timestamping, upsert, out-of-stock marking. |
| `{Market}_scraper.py` | One subclass implementing `fetch()`; runs itself under `__main__`. |

**Naming:** `{Market}_scraper.py` (e.g., `Reptil_scraper.py`, `Kam_scraper.py`).
`scraper_base.py` does **not** end in `_scraper.py`, so auto-discovery skips it.

**Auto-discovery:** `run_scrapers.py` finds all `*_scraper.py` files via `glob()` — no registration needed.

---

## What a scraper implements

```python
from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct


class ExampleScraper(MarketScraper):
    market = "example"          # must match products.market / MARKET_INFO / Tailwind token

    def fetch(self) -> list[ScrapedProduct]:
        return [
            ScrapedProduct(
                name="Млеко 1л",             # unescaped automatically
                price=59,                    # whole denars, int
                singular_price="59 ден/л",   # optional, human-readable unit price
                description="Млечни производи",  # optional, feeds categorization
                in_stock=True,
                image="https://…",           # optional
                link="https://…",            # optional
            )
        ]


if __name__ == "__main__":
    raise SystemExit(ExampleScraper().run())
```

`ScrapedProduct` fields map 1:1 to `products` columns; anything left `None` is written as `NULL`.
Duplicates are allowed — `fetch()` may return the same name from several pages/stores.

### Class attributes worth knowing

| Attribute | Default | Meaning |
| --------- | ------- | ------- |
| `market` | required | Value written to `products.market`. |
| `keep_in_stock_on_duplicate` | `True` | Duplicate name: keep `in_stock=True` if any record has it. The WooCommerce scrapers set `False` (later record wins outright). |
| `abort_on_empty` | `True` | A run that yields zero products exits 1 **without touching the DB** instead of marking the whole market out of stock. |

### What the base class does for you

1. `collect()` — html-unescapes/strips names, drops records without a name or price, merges duplicates
   (later record wins on price; optional fields backfill from the earlier record).
2. Opens one connection and looks up existing UUIDs by `(name, market)`.
3. Builds the upsert payload with `ETL_loadtime` and `last_updated`.
4. Upserts, then marks the market's rows missing from this run as out of stock.

`run()` returns `0` on success and `1` when `abort_on_empty` fires — `run_scrapers.py` records that as a
failed scraper.

---

## Timestamps

| Column | Meaning | Written when |
| ------ | ------- | ------------ |
| `etl_loadtime` | Row first entered the warehouse | INSERT only; preserved by `db_utils.bulk_upsert_products_table` (and the `products_set_timestamps_trg` trigger) on every later upsert |
| `last_updated` | A scraped field last actually changed | Upsert whose incoming row differs from the stored row; also when an unseen product is marked out of stock |

Both start equal for a brand-new row and are **naive local timestamps from Python** (`datetime.now()`),
while the DB session runs `Etc/UTC` — do not author them with SQL `now()`.

---

## Per-Scraper Methods

| Scraper | Market | Method |
|---------|--------|--------|
| `Reptil_scraper.py` | Reptil | WooCommerce Store API, `marketonline.mk` (paginated, concurrent) |
| `Bigshop_scraper.py` | BigShop | WooCommerce Store API, `bigshop.mk` (paginated, concurrent) |
| `Zito_scraper.py` | Жито Маркет | proverkanaceni.mk platform, `zito.proverkanaceni.mk`, 94 stores |
| `Kam_scraper.py` | КАМ | PDF price lists per store, `kam.com.mk` (uses `kam_pdf_utils.py`) |
| `Ramstore_scraper.py` | Рамстор | HTML per-store pages, 15 stores (`ramstore.com.mk`) + pandas, 3 stores in parallel |
| `Stokomak_scraper.py` | Стокомак | proverkanaceni.mk platform, `stokomak.proverkanaceni.mk`, 81 stores |
| `Vero_scraper.py` | Веро | Static paged HTML `pricelist.vero.com.mk`, 10 categories × 3 pages in parallel |
| `Dim_scraper.py` | ДИМ Маркет | ZUR platform: `dim.marketceni.mk` (iframe of `dim.mk/cenovnik`), 26 stores |
| `Zur_scraper.py` | Зур | ZUR platform: `ceni.zur.mk`, 21 stores |
| `Elida_scraper.py` | Елида | proverkanaceni.mk platform, `elida.proverkanaceni.mk`, 43 stores |
| `Spar_scraper.py` | SPAR | JSON API `api.prod.spar.mk/api/pricelists-web`, Skopje stores only (6 stores × 4 page workers) |
| `Kipper_scraper.py` | Киппер | WP REST store list (city term) + `admin-ajax.php` `get_products_data`, one request per store |

**Utilities:** `kam_pdf_utils.py` — `extract_name_price()` tries camelot first, falls back to pdfplumber
(camelot is imported lazily, so it is an optional dependency).

**Shared transports:** `zur_platform.py` — `ZurPlatformScraper` implements the whole ZUR price-list
platform (store selector + `admin-ajax.php` table). A market on that platform is three attributes
(`market`, `index_url`, `ajax_action`), as in `Dim_scraper.py` / `Zur_scraper.py`.

`proverkanaceni_platform.py` — `ProverkanaceniScraper` implements the `*.proverkanaceni.mk` platform
(store selector + 100-row HTML tables). A market is `market`, `base_url` and optional worker counts, as
in `Zito_scraper.py` / `Stokomak_scraper.py` / `Elida_scraper.py`.

`Spar_scraper.py` is API-only (no HTML): markets and 500-row product pages from
`api.prod.spar.mk/api/pricelists-web`. It also owns the ASCII→Cyrillic transliteration of the ERP's
names (`to_cyrillic`), which the other scrapers do not need.

The `market` value written by each scraper must match the key in `MARKET_INFO`
(`src/lib/categories.ts`) and the `--market-*` Tailwind tokens, otherwise the market filter hides the
products.

---

## Using `db_utils` directly

`MarketScraper.run()` is the normal path. Write to the warehouse by hand only for one-off scripts, and
still go through `save_products_to_products_table(conn, market, products_to_upsert, all_product_names)`
so the timestamp rules and out-of-stock marking stay intact. `bulk_upsert_products_table(conn, rows)`
accepts an optional `table=` for tests.

---

## Running Scrapers

```bash
python -m backend.data.run_scrapers              # Sequential
python -m backend.data.run_scrapers --parallel 3 # Parallel (3 workers)
python -m backend.data.run_scrapers --dry-run    # List only
```

Logs → `backend/data/logs/`, summary → `run_summary-*.json`

`run_scrapers.py` runs each scraper as a **separate process** (`python <script>`), with
`cwd=backend/data/scrapers/` and `PYTHONPATH=<repo root>:<scrapers dir>`. Consequences:

- Import project code absolutely (`from backend.data.db_utils import *`,
  `from backend.data.scrapers.scraper_base import MarketScraper`).
- Import sibling scraper utilities unqualified (`from kam_pdf_utils import extract_name_price`), with a
  fallback to the absolute form so `python -m backend.data.scrapers.Kam_scraper` also works.
- Each scraper must be runnable standalone: do the work under `if __name__ == "__main__":`.
- A failing scraper does not abort the run; `run_scrapers.py` exits 1 at the end and the summary JSON
  records per-scraper status. `run_pipeline.py` **does** stop on the first failing stage.

---

## Adding a New Scraper

1. Create `{Market}_scraper.py` in `backend/data/scrapers/`
2. Subclass `MarketScraper`, set `market`, implement `fetch()` returning `ScrapedProduct`s
3. Add `if __name__ == "__main__": raise SystemExit({Market}Scraper().run())`
4. Add the market key to `src/lib/categories.ts` (`MARKET_INFO`) and the Tailwind `--market-*` token
5. Verify with `--dry-run`, then `python backend/data/scrapers/{Market}_scraper.py` for a single run
6. If the site exposes a per-product unit price / category / promo text, map it to `singular_price` /
   `description` so categorization has something to work with

Auto-discovery finds all `*_scraper.py` files — no registry to update.

---

## Tests

```bash
python -m unittest backend.tests.test_scraper_base backend.tests.test_product_timestamps
# full suite (stdlib unittest, no test runner dependency):
python -m unittest backend.tests.test_scraper_base backend.tests.test_product_timestamps `
  backend.tests.test_out_of_stock backend.tests.test_rate_limiter backend.tests.test_categorize_failures `
  backend.tests.test_kipper_parsing backend.tests.test_ramstore_parallel backend.tests.test_spar_transliteration
```

`backend/tests/` uses stdlib `unittest` (pytest is not installed) and has no `__init__.py`, matching the
rest of the tree — pass module paths, not `discover -s backend/tests`.
