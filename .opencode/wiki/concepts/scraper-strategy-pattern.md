---
title: Scraper strategy pattern
tags: [scrapers, architecture, python, strategy-pattern]
sourceRefs: [backend/data/scrapers/scraper_base.py, backend/data/scrapers/AGENTS.md]
lastReviewed: 2026-11-03
---

# Scraper strategy pattern

Every market scraper does the same job in a different way: read a site, produce products. That is a
textbook strategy, so the market-specific part is isolated and the rest is shared.

```
backend/data/scrapers/
├── scraper_base.py        ScrapedProduct + MarketScraper (template method)
├── {Market}_scraper.py    one subclass per market + `__main__` entry point
└── kam_pdf_utils.py       helper used by Kam only
```

## Division of labour

| Layer | Owns |
| ----- | ---- |
| `MarketScraper.fetch()` (overridden) | HTTP/PDF/HTML work, parsing prices and stock, mapping site fields onto `ScrapedProduct` |
| `MarketScraper.collect()` | html-unescape + strip names, drop records without name/price, merge duplicates |
| `MarketScraper.build_rows()` | reuse the existing UUID for `(name, market)`, stamp `etl_loadtime`/`last_updated`, shape the upsert payload |
| `MarketScraper.run()` | orchestrate: fetch → collect → connect → lookup ids → upsert → mark unseen out of stock. Returns `0`/`1` |
| `db_utils.save_products_to_products_table()` | the SQL write + out-of-stock sweep (see [[concepts/product-timestamps]]) |

`ScrapedProduct` is a slots dataclass whose fields map 1:1 onto `products` columns:
`name`, `price`, `singular_price`, `description`, `in_stock`, `image`, `link`.
Markets without an image/link (all the HTML price-list ones) simply leave them `None`.

## Duplicate policy

One run can see the same name on several pages/stores. Two behaviours exist, because the original
scrapers disagreed:

- `keep_in_stock_on_duplicate = True` (default): later record wins on **price**, `in_stock=True` if any
  occurrence is in stock, absent optional fields backfill. Used by Zito, Stokomak, Vero, Ramstore, Kam.
- `keep_in_stock_on_duplicate = False`: the later record wins outright. Used by Reptil and Bigshop,
  whose WooCommerce payload is authoritative per category entry.

## Failure policy

`abort_on_empty = True` (default) makes a zero-product run exit `1` **without touching the database**.
Previously an empty or broken scrape would mark an entire market out of stock. Opt out per market with
`abort_on_empty = False`.

## Adding a market

1. `backend/data/scrapers/{Market}_scraper.py` with a `MarketScraper` subclass (`market = "..."`).
2. `if __name__ == "__main__": raise SystemExit({Market}Scraper().run())`.
3. Add the key to `MARKET_INFO` in `src/lib/categories.ts` and the Tailwind `--market-*` token.
4. Reuse an existing transport when possible: `Reptil`/`Bigshop` are the WooCommerce Store API
   template, `Zito`/`Stokomak`/`Elida` the `proverkanaceni.mk` template (`proverkanaceni_platform.py`),
   `Dim`/`Zur` the ZUR platform template (`zur_platform.py`), `Vero` the paged price-list template,
   `Kam` the PDF template.

`run_scrapers.py` globs `*_scraper.py`; there is no registry to update. `scraper_base.py` deliberately
does not match the glob.

See also [[entities/scrapers]], [[entities/products-table]].
