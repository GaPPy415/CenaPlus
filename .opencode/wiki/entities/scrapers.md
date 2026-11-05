---
title: Scrapers
tags: [scrapers, markets, entities]
sourceRefs: [backend/data/scrapers/]
lastReviewed: 2026-11-05
---

# Scrapers

Twelve markets, one file each, all subclasses of `MarketScraper` (see
[[concepts/scraper-strategy-pattern]]). `run_scrapers.py` discovers them by glob `*_scraper.py` and runs
each as a standalone subprocess with `cwd=backend/data/scrapers/` and
`PYTHONPATH=<root>:<scrapers/>`.

| File | Market key | Transport |
| ---- | ---------- | --------- |
| `Reptil_scraper.py` | `reptil` | WooCommerce Store API `marketonline.mk` — 58 paginated GETs, 6 workers |
| `Bigshop_scraper.py` | `bigshop` | WooCommerce Store API `bigshop.mk`; unit price scraped out of `price_html` by regex |
| `Zito_scraper.py` | `zito` | proverkanaceni platform: `zito.proverkanaceni.mk`, 94 stores, 100-row pages |
| `Stokomak_scraper.py` | `stokomak` | proverkanaceni platform: `stokomak.proverkanaceni.mk`, 81 stores |
| `Elida_scraper.py` | `elida` | proverkanaceni platform: `elida.proverkanaceni.mk`, 43 stores |
| `Spar_scraper.py` | `spar` | JSON API `api.prod.spar.mk/api/pricelists-web`; Skopje stores only (6 in parallel × 4 page workers), names transliterated |
| `Kipper_scraper.py` | `kipper` | WP REST store list (city term 563) + `admin-ajax.php` `get_products_data`, one request per store |
| `Vero_scraper.py` | `vero` | Paged static HTML `pricelist.vero.com.mk` (`N.html` until 404), 10 categories in parallel × 3 pages per batch |
| `Ramstore_scraper.py` | `ramstore` | 15 store pages parsed with `pandas.read_html`, 3 stores in parallel (each page ~7 MB) |
| `Kam_scraper.py` | `kam` | Daily PDF price lists `kam.com.mk/{y}/{m}/{d}/{n}.pdf`, 35 Skopje store ids; `kam_pdf_utils` (camelot → pdfplumber) |
| `Dim_scraper.py` | `dim` | ZUR platform: `dim.marketceni.mk` (iframe target of `dim.mk/cenovnik`), 26 stores |
| `Zur_scraper.py` | `zur` | ZUR platform: `ceni.zur.mk`, 21 stores |

## Site-specific quirks worth remembering

- **Zito**: rows whose price contains ≥2 dots (`0.01 ден`) are skipped as malformed. Price text is
  sliced with `int(price[0:-5])` to drop the trailing `" ден"`.
- **Zito/Stokomak/Elida**: all three are `ProverkanaceniScraper` subclasses (`proverkanaceni_platform.py`)
  — same PHP app, one store per `org` id from `select.form-select`, product table paginated with
  `?page=N&perPage=&search=&org=<id>`. `perPage` is **capped at 100 server-side**, so a 5k-product store is
  50 requests (pages and stores are both threaded). The product count is in a `<p>` ("Прикажи 1 - 4929 од
  4929 артикли"); pages = `count // 100 + 1`. Prices are `"299 ден."`; junk voucher rows (`"0.1 ден."`,
  `"0.01 ден."`) are skipped rather than stored as 0.
- **Zito's listing is not stable**: page 1 fetched twice returns different rows, and two consecutive
  sweeps of one store share only ~3,340 of ~4,000 names (union 4,605 vs the advertised 4,929). Stokomak's
  pagination is stable. This is pre-existing (the old scraper sampled the same way) and means zito stock
  flags flap between runs — a repeated-sweep union would converge but doubles the request count.
- **Zito/Stokomak** differ only in site URL and worker counts; keep them as three thin subclasses rather
  than one file, because the sites' data quality differs.
- **Spar**: `spar.mk/pricelist` is Next.js, but the data is a plain Laravel API —
  `GET https://api.prod.spar.mk/api/pricelists-web` (all stores, no auth) and
  `GET …/pricelists-web/{id}/products?page=&per_page=` (500 cap, stable pagination, 148 pages for a
  74k-product store). No server-side filter params work; `available` ("0"/"1") is per **store** while
  prices are chain-wide, so only the six `city == "Skopje"` stores are fetched and merged. ~89% of the
  catalogue is flagged unavailable in any given store, so the scraper keeps only products available in
  at least one Skopje store (`only_available` flag). Names are the ERP's ASCII Macedonian: the physical
  keyboard supplies `[`→Ш, `]`→Ќ, `\`→Ѓ, `^`→Ч, `@`→Ж, `{`→ш, `~`→ч, `` ` ``→ж plus `W`/`Q`/`X`→Њ/Љ/Џ
  inside words, then Latin→Cyrillic (`cyrtranslit`, `mk`). The `category` field is English and is kept
  as-is for categorization.
- **Kipper**: the store page's price table is **empty in the HTML** and filled by DataTables from
  `POST /wp-admin/admin-ajax.php` with `action=get_products_data&post_id=<store post id>` + DataTables
  paging (`draw`, `start`, `length`). `length` is not capped — 100000 returns a whole store (~2.2k rows,
  1.6 MB) in one request. Store posts come from `/mk/wp-json/wp/v2/posts?categories=563` (563 = the
  Скопје option of the marketet filter; it also returns a Radoviš and a Prilep store that are tagged
  Скопје). `product_name` is already Cyrillic (`product_name_alt` is the Latin twin), `product_price` is
  the selling price, `product_price_weight` + `product_type_alt` form the "1КГ=600 ден" unit price,
  `product_subgroup` is the category, and `product_status` "D" means available. All 51 Skopje stores
  share one ~2.2k-product catalogue, so 110k rows collapse to ~2.2k products.
- **Vero**: the index page is `index.html`; the first link and the last two are not product categories
  (`shop_links[1:-2]`). Category pages count up from `1.html` until a 404.
- **Ramstore**: discount price lives in `ЦЕНА СО ПОПУСТ` and needs `[:-3]` to drop the decimal part;
  without it, `ПРОДАЖНА ЦЕНА` is used. Everything on the page is treated as in stock.
- **Kam**: the PDF URL is date-stamped **today**, so a failed run cannot be retried with yesterday's
  list. `numbers_skopje` is the active store subset; `numbers_all_markets` and `stores` are unused.
  camelot is not installed locally, so pdfplumber is the effective parser.
- **Reptil/Bigshop** both trim any PHP warning prefix before `json.loads` (they differ slightly:
  Reptil tolerates a missing `X-WP-TotalPages`).
- **Dim / Zur**: `dim.mk/cenovnik` contains no table — it iframes `dim.marketceni.mk`, which is a
  front-end for `ceni.zur.mk`. `ceni.zur.mk` itself is the same app with its own store list. Both are
  `ZurPlatformScraper` subclasses in `zur_platform.py`; only `index_url` and the AJAX `action`
  (`load_dimceni_prices_data` vs `load_prices_data`) differ, plus the store ids. The price table comes
  from `POST /wp-admin/admin-ajax.php` with `action=…&org=<store>&page=&search=&limit=`. Two traps:
  (1) the UI's 100/250/500 page sizes are only defaults — any `limit` is honoured, so `limit=10000`
  returns a whole store in one request; (2) OFFSET pagination is non-deterministic (same request twice =
  different order, ~30% of rows duplicated and others dropped), so always use one big page and keep the
  paginated loop only as a fallback. Rows are `[code, name, selling price, unit price, description,
  availability, regular price, promo price+discount, promo type, promo window]`; the selling price
  already includes an active promo, the description column is a literal `/`, and every row is in stock.
  Rows with price `.00` (a free promo bag, "ТОРБА ПИНК СТАФ ГРАТИС") are dropped.

## Running one market

```bash
python -m backend.data.run_scrapers --dry-run      # list discovered scrapers
python -m backend.data.run_scrapers --parallel 3   # all markets
python backend/data/scrapers/Vero_scraper.py       # single market, standalone
```

Logs land in `backend/data/logs/` next to a `run_summary-*.json`.

See also [[entities/products-table]].
