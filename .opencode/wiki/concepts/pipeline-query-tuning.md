---
title: Pipeline query tuning
tags: [postgres, performance, indexes, grouping, upsert]
sourceRefs: [backend/data/db_utils.py, init/00_schema.sql]
lastReviewed: 2026-11-05
---

# Pipeline query tuning

Measurements taken on the dev DB (142,539 products / 61,102 groups, ~765 MB, all hot pages cached).
`db_utils` was rewritten around these numbers; see [[concepts/product-timestamps]] for the write path.

## What was actually slow

| Query | Before | After | Fix |
| --- | --- | --- | --- |
| Groups nearest-match (per product in grouping) | 6.7 ms | 0.6 ms | `idx_groups_category` |
| Ungrouped fetch (once per sub-category) | 68.6 ms | 39.3 ms | `idx_products_grouping` |
| Similar-products scan (per product in grouping) | 54.7 ms | 43.6 ms | `idx_products_grouping` |
| Categorization validation | reads 117k rows into Python | SQL `NOT EXISTS` against the taxonomy | no Python round trip |
| Re-scrape of unchanged data | rewrites every row | writes nothing | `ON CONFLICT ... WHERE row IS DISTINCT FROM excluded` |

The grouping stage was O(n²) per sub-category: each product scanned the sub-category's ungrouped
neighbours. The indexes cut the constant, not the complexity. That similarity path has since been
replaced by the deterministic grouper (`group_products.py`); the measurements remain useful for the
embedding scan, which still filters `group_id IS NULL AND name_embedding IS NOT NULL`.

The indexes originally shipped as `init/04_pipeline_indexes.sql`; they now live in
`init/00_schema.sql`, the single idempotent schema file.

## Deliberately not optimised

- **Out-of-stock sweep.** The existing `SELECT id, name ... WHERE market AND in_stock` + Python set +
  `UPDATE ... WHERE id = ANY(...)` costs ~50 ms for the biggest market (`vero`, 32k in-stock rows). A
  single `UPDATE ... WHERE NOT name = ANY(array)` measured 147 ms, and a temp-table anti-join 97 ms —
  both slower, because the planner must scan products to evaluate the predicate. Left alone.
- **`products(market)` index.** `vero` is ~36% of the table, so the planner keeps choosing a sequential
  scan; the index would only add write overhead.
- **Pending-categorization index.** The predicate matches ~17% of rows; no better than the scan.

## Related

- [[entities/products-table]]
- [[concepts/product-timestamps]]
