# Wiki log

## [2026-11-03] ingest | Scraper strategy layer + product timestamp semantics

- Read all seven scrapers and `db_utils`; profiled the live `products` table over the host-side DB.
- Confirmed the suspected bug: `ETL_loadtime` and `last_updated` were written with the same
  `datetime.now()` on every upsert (97,683/117,054 rows still equal; the remaining 19,371 are exactly the
  rows touched by `mark_out_of_stock_products_table`).
- Introduced `backend/data/scrapers/scraper_base.py` (`ScrapedProduct`, `MarketScraper`) and migrated all
  seven scrapers to it; `db_utils.bulk_upsert_products_table` now preserves `ETL_loadtime` and only bumps
  `last_updated` on a real row change; `init/03_product_timestamps.sql` adds the durable trigger
  (applied to the dev DB).
- Found and fixed a timezone trap: DB session is `Etc/UTC`, app writes local time — the trigger must not
  author timestamps from SQL `now()`.
- Added `backend/tests/` (stdlib `unittest`, 19 tests) and docs updates in `AGENTS.md`,
  `backend/data/AGENTS.md`, `backend/data/scrapers/AGENTS.md`.

Pages created: [[concepts/scraper-strategy-pattern]], [[concepts/product-timestamps]],
[[entities/products-table]], [[entities/scrapers]].

## [2026-11-03] ingest | New market: ДИМ (dim)

- `dim.mk/cenovnik` iframes `dim.marketceni.mk` → `ceni.zur.mk`; the price table is an
  `admin-ajax.php` response (`action=load_dimceni_prices_data`), 26 stores.
- Found that its OFFSET pagination is non-deterministic; solved by requesting each store with
  `limit=10000` (one page, deterministic).
- Added `Dim_scraper.py` (first scraper written directly against the new `MarketScraper` base),
  market key `dim`, frontend colour `#a5a9ac` → `--market-dim: 206 4% 66%`.
- Dry run: 92,854 store rows → 6,200 unique products in ~18s.
- ~500 names carry per-store prices, so the merge iterates stores in id order (thread completion
  order made the winning price — and `last_updated` — flip between runs). Verified idempotent: second
  run produced an identical content hash and left `last_updated` untouched.
- Wrote 6,200 rows to `products` (market `dim`); categorization/embedding/grouping still pending, so the
  market is not yet visible in the API (the `grouped_products` view needs `group_id`).

Updated: [[entities/scrapers]].

## [2026-11-03] ingest | New market: Зур (zur)

- `ceni.zur.mk` turned out to be the same ZUR platform as Dim, only a different AJAX action
  (`load_prices_data`) and its own 21 stores — so the client moved out of `Dim_scraper.py` into
  `zur_platform.py` (`ZurPlatformScraper`); `Dim_scraper.py` is now a 3-attribute subclass.
- `Zur_scraper.py`: 108,098 store rows → **9,673 unique products**, ~23s. Colour `#fff200` →
  `--market-zur: 57 100% 50%`.
- Only rows dropped are the `.00` free-bag promo items.
- Verified: dim re-ran to the identical 6,200 products after the refactor; zur's second run produced
  the same content hash and left `last_updated` untouched.

Updated: [[entities/scrapers]].

## [2026-11-03] ingest | New market: Елида (elida)

- `elida.proverkanaceni.mk` is the same platform as `zito`/`stokomak`, so the client moved into
  `proverkanaceni_platform.py` (`ProverkanaceniScraper`); Zito, Stokomak and Elida are now thin
  subclasses.
- `perPage` is capped at 100 server-side; a 4.9k-product store is 50 requests, pages and stores both
  threaded. Zito's pagination turned out to be non-deterministic (two sweeps share only ~3.3k of ~4k
  names); Stokomak's is stable. Pre-existing, documented.
- `Elida_scraper.py`: 108k store rows → **9,612 unique products**, ~237s. Colour `#1a3a73` →
  `--market-elida: 218 63% 28%`. Second run left content and timestamps untouched.

Updated: [[entities/scrapers]].

## [2026-11-03] ingest | `db_utils` cleanup + query tuning

- Deleted dead code: `group_products_by_category_old` and the unused `load_products`.
- `load_products_to_categorize` no longer reads the whole table to validate categories in Python; the
  taxonomy check is an SQL `NOT EXISTS` over `unnest(main[], sub[])`.
- `bulk_upsert_products_table` now carries `ON CONFLICT ... WHERE <row> IS DISTINCT FROM <excluded>`, so
  an unchanged re-scrape writes nothing. Verified in production: re-scraping `dim` left `xmin`, content
  and timestamps untouched.
- `group_products_by_category` drops the per-product `SELECT group_id` (in-memory pending set) and
  batches neighbour assignment with `= ANY`. Verified identical partitioning against the old
  implementation on a scratch schema (22 groups, same memberships).
- `init/04_pipeline_indexes.sql` applied: groups nearest-match 6.7 ms → 0.6 ms per product.
- Added `backend/tests/test_out_of_stock.py` plus two `xmin` assertions; 26 Python tests pass.

New page: [[concepts/pipeline-query-tuning]].

## [2026-11-03] ingest | New market: SPAR (spar)

- `spar.mk/pricelist` is Next.js, but the data is an open Laravel API: `api.prod.spar.mk/api/pricelists-web`
  (all stores) + `/{id}/products?page=&per_page=` (500 cap, stable). No HTML parsed.
- 60 stores, only the 6 `city == "Skopje"` ones are scraped (filter on
  `Markets-module…__location` / API `city`). Prices are chain-wide, `available` is per store.
- ~89% of each store's 74k catalogue is unavailable, so only rows available in at least one Skopje
  store are ingested (`SparScraper.only_available`), 64,107 store rows → **19,739 unique products** in
  67s.
- Names are the ERP's ASCII Macedonian (keyboard symbols + Latin): `to_cyrillic()` reverses it and is
  covered by `backend/tests/test_spar_transliteration.py` (9 golden cases). Categories are English and
  kept as-is.
- Colour `#009541` → `--market-spar: 146 100% 29.2%`.

Updated: [[entities/scrapers]].

## [2026-11-03] ingest | Scraper API survey

- Probed every scraper's source for a direct data endpoint. Five already use one (reptil, bigshop,
  dim, zur, spar); zito/stokomak/elida (proverkanaceni PHP), vero (static HTML) and kam (PDF) do not.
- `stokomak.mk` / `vero.com.mk` expose WooCommerce Store APIs but with placeholder catalogues
  (3 demo products / 14 brands at price 0), not the real price data.
- Ramstore's WordPress exposes a custom `/price/v2` REST namespace — the only remaining candidate,
  to be verified (site is slow and times out under probing).

New page: [[concepts/scraper-api-survey]].

## [2026-11-03] ingest | Parallelism: Spar stores, Vero pages, Ramstore stores

- **Ramstore `/price/v2` is a dead end**: `/wp-json/price/v2` is only the namespace index; its three GET
  routes (`app_cron`, `app_cron1`, `app_cron2`) accept the connection and send nothing for 90 s — cron
  jobs, not query endpoints. Probing also knocked the site offline, so no further calls were made.
  Added `backend/data/scrapers/ramstore_api_probe.py` for a later re-check, and parallelised the HTML
  fetch instead (3 stores at a time, ordered merge; verified offline with
  `backend/tests/test_ramstore_parallel.py`). Live run still outstanding: server was down.
- **Spar**: stores now fetched in parallel (6 × 4 page workers): 70 s → **44 s** for the same 19,738
  products.
- **Vero**: pages of a category fetched in batches of 3 (10 categories in parallel). Whole market in
  **42.7 s**, 37,275 unique products; per-category output verified byte-identical to the sequential walk
  on three categories (9,695 / 15,877 / 22,117 rows). The DB holds 51,013 rows because it accumulates
  delisted products — the current site has fewer, and more of them are in stock (34,818 vs 31,973).

Updated: [[concepts/scraper-api-survey]], [[entities/scrapers]].

## [2026-11-03] ingest | New market: Kipper (kipper)

- `kipper.mk/mk/marketet/` is a WP store directory; the per-store price table is empty in the HTML and
  filled by DataTables from `admin-ajax.php` (`action=get_products_data`, `post_id`). `length=100000`
  returns a whole store in one request (~2.2k rows), so no pagination and no HTML parsing.
- Store list from `/mk/wp-json/wp/v2/posts?categories=563` (563 = Скопје in the page's city filter;
  51 posts, including a Radoviš and a Prilep store that are tagged Скопје).
- 110,508 store rows → **2,174 unique products in 30.5s**. All names already Cyrillic;
  `product_price_weight` + `product_type_alt` reproduce their "1КГ=600 ден" unit price.
- Colour `#ed1c24` → `--market-kipper: 358 85% 52%`.

Updated: [[entities/scrapers]], [[concepts/scraper-api-survey]].

## [2026-11-05] ingest | Product detail + manufacturer pages (API and React)

- `GET /product?g_id=<uuid>`: group metadata + `manufacturer_display_name`, price stats, and **every**
  offer (in and out of stock), UUID-validated, bound parameters. `/product` page renders hero, stat
  cards, available/unavailable sections and per-offer shop links; `ProductCard` links image, title
  and footer to it.
- `GET /manufacturer?manufacturer_name=`: resolves names through `normalize_string` → canonical →
  `manufacturer_aliases` → `merged_into` recursive CTE, then returns the same `grouped_products`
  payload so the card renders unchanged. **Alias matching was the missing piece**: `Битолско` only
  resolves through `manufacturer_aliases`, not `merged_into` (404 before the fix).
- `/manufacturer` page: accent-bordered brand header with stats, URL-backed paging, category chip on
  every card (`showCategory`), shared `Pagination` extracted from `SubCategoryView` (215 → 136 lines).
- Dev gotcha: Vite must proxy to `http://127.0.0.1:8000`; `localhost` resolves to `::1` while uvicorn
  binds IPv4, so every `/api` call 500s without reaching the backend.
- Verified with `tsc`, lint, 5 tests, production build, live endpoint calls and Playwright (chip →
  brand page, 21 Medela groups, page 2 = 9 cards, per_page=24 = 21 cards, 0 console errors).
- Pre-existing (not fixed): the global header overflows horizontally at 390px on every page.
- Fixed in passing: `src/index.css` had the Google Fonts `@import` after the `@tailwind` directives,
  so browsers dropped it and Inter/DM Sans never loaded. Moved it to the top; fonts now load.

New pages: [[entities/api-endpoints]], [[entities/frontend-views]],
[[concepts/manufacturer-resolution]].

## [2026-11-05] ingest | Docs refresh: live table sizes and twelve markets

- Re-measured the warehouse after the pipeline rework: **165,215 products** across **12 markets**
  (vero 51.3k, ramstore 24.6k, spar 19.9k, zito 18.6k, zur 9.7k, elida 9.6k, stokomak 8.0k, dim 6.2k,
  reptil 6.1k, kam 5.4k, bigshop 3.7k, kipper 2.2k) and **146,670 groups**.
- Corrected stale claims: `products-table` now lists the extraction/grouping columns and points at
  `init/00_schema.sql` (the schema is tracked now); `scrapers` says twelve markets, not seven.
- Root and per-directory guides were rewritten around deterministic grouping (`--apply` rebuilds
  `groups`), the shared rate-limiter scopes, and the real `run_pipeline` stage list.
- Repo hygiene: `__pycache__`, `*.tsbuildinfo`, `.playwright-mcp/`, `.sisyphus/` and local scratch are
  git-ignored; the applied manufacturer merges live in `.opencode/manual-merges.csv`.

## [2026-11-05] ingest | Product-name token aliases (S2.10 prototype)

- Reproduced the split with Milka Alpenmilch: `чоколадо алпен милк` (spar), `чоколадо алпин милк`
  (vero), `чоколадо алпине милк` (bigshop) are three groups, so no card compares three markets.
  `guarded_fuzzy_merge` misses it (`алпен`/`алпине` = 0.727 < 0.85).
- Full-name aliases failed (the same spelling maps to different canonicals at 80 g vs 250 g); moved to
  **token** aliases scoped per manufacturer (`product_name_token_aliases`), resolved by the grouper
  before blocking. Guards: one edit, ≥4 chars, same first letter, no digits, no form/packaging words.
- Measured with the real `build_components()` before/after, nothing written: Milka 40 aliases /
  179 rows / −21 groups (391 → 370, 65 rows now compare more markets); all manufacturers 5,130 aliases
  / 10,306 rows / −653 groups (75,562 → 74,909, 1,914 rows with a wider comparison).
- New page: [[concepts/product-name-aliases]]; regression suite
  `backend/tests/test_product_name_aliases.py` (6 tests, 64 total Python tests pass).
