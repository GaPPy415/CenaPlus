---
title: products table
tags: [schema, postgres, products, warehouse, grouping]
sourceRefs: [init/00_schema.sql, backend/data/db_utils.py, backend/data/extract_features.py, backend/data/group_products.py]
lastReviewed: 2026-11-05
---

# `products` table

The single warehouse table every scraper writes into. The full schema now lives in
`init/00_schema.sql` (extensions, tables, constraints, indexes, timestamp trigger, `grouped_products`
view); host-side the live DB is `localhost:5454`, db `products_db` (see `.env`).

## Scraper-facing columns

| Column | Type | Notes |
| ------ | ---- | ----- |
| `id` | `uuid` PK | Generated in Python; reused for known `(name, market)` |
| `name` | `text` | HTML-unescaped, Macedonian Cyrillic |
| `price` | `integer` | Whole denars |
| `singular_price` | `text` | Unit price as the market prints it, e.g. `"128 ден/kg"` |
| `description` | `text` | Market category or promo blurb — the input to categorization |
| `in_stock` | `boolean` | False when a product is absent from the latest run |
| `image`, `link` | `text` | Only the two WooCommerce markets fill these |
| `market` | `text` | Must match `MARKET_INFO` / Tailwind `--market-*` |
| `etl_loadtime` | `timestamp` | First load, never rewritten — see [[concepts/product-timestamps]] |
| `last_updated` | `timestamp` | Last real change to a scraped field |

## Pipeline-owned columns

- categorization: `main_category`, `sub_category`, `confidence`, `categorized_at`
- embeddings: `name_embedding` (`vector(768)`, search fallback only)
- feature extraction: `manufacturer_id`, `product_name`, `product_name_key`, `product_name_sorted`,
  `quantity_value`, `quantity_unit`, `quantity_raw`, `is_fluid`, `variant_key`, `variant` (jsonb),
  `metadata` (jsonb), `features_confidence`, `features_version`, `features_extracted_at`,
  `grouping_keys_version`
- grouping: `group_id` → `groups.id`

## Keying

There is **no unique constraint on `(market, name)`** — only the `id` PK. Upserts therefore depend on a
Python-side `get_products_by_market()` name→id map. Renaming a product at the source creates a new row
and leaves the old one to be marked out of stock.

## Size (2026-11-05)

| Market | Rows |
| ------ | ---- |
| vero | 51,299 |
| ramstore | 24,606 |
| spar | 19,856 |
| zito | 18,591 |
| zur | 9,687 |
| elida | 9,617 |
| stokomak | 8,019 |
| dim | 6,215 |
| reptil | 6,071 |
| kam | 5,409 |
| bigshop | 3,667 |
| kipper | 2,178 |
| **total** | **165,215** |

`groups` holds 146,670 rows. Grouping is deterministic: a group is a block of
(manufacturer, canonical name, quantity, unit, fluid) refined by variant rules — embeddings are not
used to group any more (see [[concepts/manufacturer-resolution]] for the manufacturer side).

`grouped_products` is a view over `groups` ⨝ `products` filtered to `in_stock = true`, aggregating
per-group products as JSON ordered by price. The API reads the view; the frontend never sees
`etl_loadtime`/`last_updated`, and out-of-stock offers appear only on `/product`.

See also [[entities/scrapers]], [[concepts/product-timestamps]], [[entities/api-endpoints]].
