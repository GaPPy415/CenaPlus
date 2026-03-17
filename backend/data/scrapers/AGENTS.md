# Scrapers — Agent Documentation

Minimal contract guide for adding or debugging CenaPlus market scrapers.

---

## Scraper Contract

**Naming:** `{Market}_scraper.py` (e.g., `Reptil_scraper.py`, `Kam_scraper.py`)  
**Auto-discovery:** `run_scrapers.py` finds all `*_scraper.py` files via `glob()` — no registration needed  
**Persistence:** Call `save_products_to_products_table()` or `bulk_upsert_products_table()` from `db_utils`

---

## Required Product Fields

**Minimum required:**
- `name` (str) — Product name in Macedonian Cyrillic
- `price` (str or float) — Retail price
- `market` (str) — Market key (e.g., 'reptil', 'kam', 'zito')
- `link` (str) — Product URL
- `image` (str or None) — Product image URL

**Optional:** `singular_price`, `description`, `in_stock`, `existing_categories`

---

## Per-Scraper Methods

| Scraper | Market | Method |
|---------|--------|--------|
| `Reptil_scraper.py` | Reptil | WooCommerce REST API (paginated, concurrent) |
| `Zito_scraper.py` | Жито Маркет | WooCommerce REST API (async/await) |
| `Kam_scraper.py` | КАМ | PDF parsing (uses kam_pdf_utils.py) |
| `Ramstore_scraper.py` | Рамстор | Web scraping + pandas |
| `Stokomak_scraper.py` | Стокомак | HTML tables (BeautifulSoup, threaded) |
| `Vero_scraper.py` | Веро | HTML tables (ThreadPoolExecutor) |

**Utility:** `kam_pdf_utils.py` — PDF extraction helpers (camelot, Macedonian Cyrillic support)

---

## Common Pattern

```python
# 1. Connect
conn = connect_to_db()

# 2. Scrape products into list of dicts
products = []  # [name, price, market, link, image, ...]
product_names = set()
# ... market-specific logic ...

# 3. Save
save_products_to_products_table(conn, market='your_market', 
                                products_to_upsert=products, 
                                all_product_names=product_names)
```

Key: always include market key, required fields, product names for out-of-stock marking.

---

## Running Scrapers

```bash
python -m backend.data.run_scrapers              # Sequential
python -m backend.data.run_scrapers --parallel 3 # Parallel (3 workers)
python -m backend.data.run_scrapers --dry-run    # List only
```

Logs → `backend/data/logs/`, summary → `run_summary-*.json`

---

## Adding a New Scraper

1. Create `{Market}_scraper.py` in `backend/data/scrapers/`
2. Implement market-specific scraping
3. Build product dicts with required fields
4. Call `save_products_to_products_table()`
5. Verify with `--dry-run`, test with `--parallel 1`

Auto-discovery finds all `*_scraper.py` files — no registration needed.
