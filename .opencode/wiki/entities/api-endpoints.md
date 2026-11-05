---
title: FastAPI endpoints
tags: [api, fastapi, postgres, frontend]
sourceRefs: [backend/api.py, src/lib/api.ts, init/00_schema.sql]
lastReviewed: 2026-11-05
---

# FastAPI endpoints

`backend/api.py` is one module-level app. It opens the Postgres connection **at import time** and
sets `conn.autocommit = True`, so every read commits immediately and the shared connection never sits
`idle in transaction` (it used to hold `ACCESS SHARE` locks and block DDL). The Gemini client is
created lazily by `embeddings_client()`, so the service boots without `GOOGLE_API_KEY`; only
`/search/semantic` needs it.

| Endpoint | Returns |
| -------- | ------- |
| `GET /health` | `{"status": "healthy"}` |
| `GET /categories` | the taxonomy from `shared/categories.json` |
| `GET /search?q=` | `{data: GroupedProduct[]}` via pg_trgm over `groups.product_name` + `manufacturers.canonical_name`, similarity ≥ 0.8, no row cap |
| `GET /search/semantic?q=` | old 768-dim cosine path, ≥ 0.80, `LIMIT 15`; fallback only |
| `GET /product?g_id=<uuid>` | one group with **every** offer, in and out of stock, plus price stats |
| `GET /manufacturer?manufacturer_name=` | all groups of one manufacturer across categories |
| `GET /{main_category}/{sub_category}` | paginated `grouped_products` rows, optional repeated `market=` filter |

`page`/`per_page` are shared: `PerPage` is an `int` enum (12 / 24 / 36). Listings return
`{total, page, per_page, data}`.

## `GET /product`

`g_id` is parsed through `uuid.UUID` (400 on anything else) and used as a bound parameter (404 when
the group was rebuilt away). The group row joins `groups` → `manufacturers` and exposes
`manufacturer` (canonical, the link value) and `manufacturer_display_name` (what the UI prints).
Offers are ordered `in_stock DESC NULLS LAST, price ASC NULLS LAST`; `kratok_rok` is computed per
offer with `p.name ILIKE '%краток рок%'` and lifted to the group. Stats: `offers`, `markets`,
`in_stock`, `out_of_stock`, `cheapest_price`, `priciest_price`, `savings`.

## `GET /manufacturer`

Four SQL constants: `RESOLVE_MANUFACTURER_SQL`, `MANUFACTURER_GROUPS_SQL`, `MANUFACTURER_TOTAL_SQL`,
`MANUFACTURER_MARKETS_SQL`. The payload deliberately reuses the `grouped_products` shape, so
`ProductCard` renders unchanged:

```
{ manufacturer: {name, display_name, groups, categories, markets},
  total, page, per_page, data: GroupedProduct[] }
```

`groups`/`total` count only groups that have at least one in-stock offer (the `grouped_products` view
hides empty groups), which matches the length of the listing. `markets` counts distinct in-stock
markets. `per_page` is read through `int(getattr(per_page, "value", per_page))` so the handler stays
callable with a plain int from scripts.

Name resolution is the interesting part: see [[concepts/manufacturer-resolution]].

## Shared constraints

- Every query is parameterised; no string interpolation of user input.
- `grouped_products` exposes **in-stock offers only**; the detail endpoint is the one place
  out-of-stock rows appear.
- Sorting is stable (`ORDER BY group_name`, then `LIMIT/OFFSET`), unlike the scrapers' offset walks.

Frontend fetch layer: [[entities/frontend-views]].
