---
title: Product timestamps
tags: [products, etl, timestamps, postgres, data-quality]
sourceRefs: [backend/data/db_utils.py, init/00_schema.sql, backend/tests/test_product_timestamps.py]
lastReviewed: 2026-11-05
---

# Product timestamps: `etl_loadtime` vs `last_updated`

Two timestamp columns on `products` that used to be the same value (see [[entities/products-table]]).

## Intended meaning

| Column | Meaning |
| ------ | ------- |
| `etl_loadtime` | When the row **first** entered the warehouse. Immutable after INSERT. |
| `last_updated` | When a **scraped** field last actually changed. |

Scraped fields watched by both enforcement layers: `name`, `description`, `price`, `singular_price`,
`in_stock`, `image`, `link`, `market`. Pipeline-owned columns (`main_category`, `sub_category`,
`confidence`, `categorized_at`, `name_embedding`, `group_id`, `product_name`, `quantity_*`, `metadata`)
must never move `last_updated` — categorization is not a price change.

`mark_out_of_stock_products_table` is the one non-upsert writer that legitimately bumps `last_updated`:
a product disappearing from the scrape is a stock change.

## What was wrong

Every scraper wrote the same `datetime.now()` into both columns on every upsert:

```python
'ETL_loadtime': now,
'last_updated': now,
```

Measured on the live DB before the fix: **97,683 of 117,054 rows had identical values**; the other
19,371 were exactly the rows `mark_out_of_stock_products_table` had touched (it updates only
`last_updated`). So `etl_loadtime` really meant "last full scrape of this market" and `last_updated`
carried no information — every row looked freshly updated every run.

## How it is enforced now

Two layers, deliberately redundant:

1. **Loader** — `db_utils.bulk_upsert_products_table` builds the `ON CONFLICT (id) DO UPDATE` clause so
   that `ETL_loadtime = target.ETL_loadtime` (first-seen wins) and
   `last_updated = CASE WHEN <watched row> IS DISTINCT FROM <EXCLUDED row> THEN EXCLUDED.last_updated ELSE target.last_updated END`.
   Row comparison is NULL-safe, so a `NULL` -> `NULL` field is not a change.
2. **Database** — `init/00_schema.sql` (originally applied as `init/03_product_timestamps.sql`) adds
   `DEFAULT now()`, `NOT NULL` and the `products_set_timestamps_trg` trigger for direct/ad-hoc writes.
   It forces `etl_loadtime` to the old value and suppresses `last_updated` when nothing watched changed.

## Timezone trap

Both columns are **naive** `timestamp` (no zone) filled with Python local time, but the Postgres session
runs `TimeZone = Etc/UTC` (verified: `now()` was one hour behind `datetime.now()`).

Consequence: the trigger must **never author these values itself**. It preserves the writer's timestamp
and only uses the DB clock as a fallback when a statement changed data without touching
`last_updated`. Authoring them from SQL `now()` makes new values silently jump an hour behind the rest
of the table — this actually happened during implementation and was caught by the end-to-end smoke test.

## Backfill status

Unrecoverable: for the ~97k rows where the two values were equal, the true first-seen time is gone.
They keep the last scrape timestamp until they are next written; from then on `etl_loadtime` freezes at
that value forever.

## Verification

`backend/tests/test_product_timestamps.py` (stdlib `unittest`) covers loader semantics on a scratch
table and trigger semantics on a temp table, and skips the trigger half when the migration is not
applied. End-to-end: a fake scraper run twice with identical data keeps both timestamps; a price change
or a stock flip moves only `last_updated`.

See also [[concepts/scraper-strategy-pattern]].
