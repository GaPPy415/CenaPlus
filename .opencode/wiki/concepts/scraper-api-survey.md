---
title: Scraper API survey
tags: [scrapers, api, performance, survey]
sourceRefs: [backend/data/scrapers/, backend/data/run_scrapers.py]
lastReviewed: 2026-11-03
---

# Scraper API survey

Which markets can be read as JSON instead of rendered HTML/PDF. Checked 2026-11-03 by probing
each source directly (no browser), looking for `wp-json`, `admin-ajax.php`, `/api/`, and JSON payloads.

| Market | Source | Direct API? |
| --- | --- | --- |
| `reptil` | `marketonline.mk` | ✅ WooCommerce Store API (already used) |
| `bigshop` | `bigshop.mk` | ✅ WooCommerce Store API (already used) |
| `dim` | `dim.marketceni.mk` | ✅ `admin-ajax.php` action `load_dimceni_prices_data` (already used) |
| `zur` | `ceni.zur.mk` | ✅ `admin-ajax.php` action `load_prices_data` (already used) |
| `spar` | `api.prod.spar.mk` | ✅ Laravel JSON (already used) |
| `kipper` | `kipper.mk` | ✅ `admin-ajax.php` action `get_products_data` + WP REST store list |
| `zito` | `zito.proverkanaceni.mk` | ❌ server-rendered PHP/IIS; GET form (`search`/`org`/`perPage`), no JS, no JSON route |
| `stokomak` | `stokomak.proverkanaceni.mk` | ❌ same platform as Zito |
| `elida` | `elida.proverkanaceni.mk` | ❌ same platform as Zito |
| `vero` | `pricelist.vero.com.mk` | ❌ static HTML pages (`N.html`) on Apache; no JSON route |
| `ramstore` | `ramstore.com.mk/marketi/...` | ❌ `/price/v2` only exposes cron triggers that never respond |
| `kam` | `kam.com.mk/{y}/{m}/{d}/{n}.pdf` | ❌ PDFs only; the market HTML page returns 0 bytes |

## Dead ends worth remembering

- `stokomak.mk` and `vero.com.mk` run WooCommerce with an open Store API
  (`/wp-json/wc/store/v1/products`) and return **Cyrillic** names — but they are not the catalogue:
  Stokomak has 3 demo products (`X-WP-Total: 3`) and Vero has 14 entries that are *brands* with
  `price: 0`. The real price data only exists on the price-list domains above.
- `kam.com.mk/price_markets/...` answers `200` with an empty body; only the PDF URLs work (and they
  need a browser-like `User-Agent`).
- The `*.proverkanaceni.mk` platform is a classic PHP form app: no scripts, no AJAX, `?format=json`
  and `/api` are dead ends (404 / same HTML). Only `perPage` and threading can speed it up.

## Ramstore: `/price/v2` is a cron namespace, not a data API

`https://ramstore.com.mk/wp-json/price/v2` returns only the namespace index. Its three GET routes —
`/app_cron`, `/app_cron1`, `/app_cron2` (optional `wpml_language` = `mk|sq|en`) — accepted the connection
and then sent **no response at all for 90 s**. They behave like scheduled price-sync jobs, not query
endpoints, so there is nothing to parse.

The normal store pages are the only source (~7 MB of HTML each, 15 stores). `Ramstore_scraper.py` now
fetches three of them at a time; keep it low, the server is fragile — a burst of probe requests took it
down for a while on 2026-11-03. `ramstore_api_probe.py` re-checks the routes if that ever changes.

`vero.com.mk` is a different story that looks similar at first glance: it ships a WooCommerce Store API
with real Cyrillic names, but only 14 *brand* entries at price 0 — the actual price list is the static
`pricelist.vero.com.mk` export, which has no JSON route and whose pages are now fetched in parallel
batches (10 categories × 3 pages).

## Related

- [[entities/scrapers]]
- [[concepts/scraper-strategy-pattern]]
