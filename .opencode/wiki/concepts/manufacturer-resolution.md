---
title: Manufacturer name resolution
tags: [manufacturers, aliases, merging, api, gotcha]
sourceRefs: [backend/api.py, backend/data/text_utils.py, backend/data/extract_features.py, backend/data/manufacturer_merges.py]
lastReviewed: 2026-11-05
---

# Manufacturer name resolution

A shop writes "Битолско", another writes "Бимилк", a third "Bimilk". All three must land on the same
brand page, but `groups.manufacturer_id` points at one concrete `manufacturers` row. Resolution has
three layers, applied in this order.

## 1. Normalise the query

`normalize_string()` in `backend/data/text_utils.py` lowercases, maps `w`/`y`, transliterates Latin
to Cyrillic (`cyrtranslit`, `mk`), flattens punctuation and collapses whitespace. It must stay
identical to the rules used when products were extracted, otherwise names never match. So
`Битолско`, `битолско` and `BITOLSKO` all become `битолско`.

## 2. Match a row: canonical name first, alias second

`manufacturers.canonical_name` is the normalised key. `manufacturer_aliases(alias, manufacturer_id)`
holds the variants collected during extraction (`битолско`, `битолски`, `битолска`,
`бимилк моја кравица` → `бимилк`). A canonical hit wins over an alias hit — that priority is why the
recursive CTE carries a `priority` column and orders by `(priority, depth)`.

## 3. Follow `merged_into` to the surviving row

`manufacturer_merges.py` can merge two manufacturers by writing `merged_into` on the absorbed row.
The endpoint follows the chain recursively until `merged_into IS NULL`:

```sql
WITH RECURSIVE seed AS (
    SELECT m.id, m.canonical_name, m.display_name, m.merged_into, 0 AS priority
    FROM manufacturers m WHERE m.canonical_name = %s
    UNION
    SELECT m.id, m.canonical_name, m.display_name, m.merged_into, 1 AS priority
    FROM manufacturers m JOIN manufacturer_aliases a ON a.manufacturer_id = m.id
    WHERE a.alias = %s
), chain AS (
    SELECT *, 0 AS depth FROM seed
    UNION ALL
    SELECT m.id, m.canonical_name, m.display_name, m.merged_into, c.priority, c.depth + 1
    FROM manufacturers m JOIN chain c ON m.id = c.merged_into
    WHERE c.merged_into IS NOT NULL
)
SELECT id, canonical_name, display_name FROM chain
WHERE merged_into IS NULL ORDER BY priority, depth LIMIT 1;
```

Verified on the live DB: `Битолско` → `Бимилк` (alias, 144 groups / 3 categories / 12 markets),
`битол` → `Битола` (`merged_into`), `непостои-овој` → 404.

## Consequences worth remembering

- **Aliases are the common path, not `merged_into`.** The Bimilk family was recorded as aliases; the
  `merged_into` chain exists for the fuzzy-merge pass. An endpoint that only checks
  `canonical_name` + `merged_into` returns 404 for `Битолско` — this was a real bug, caught by
  testing the alias before shipping.
- **Groups live on the surviving row.** No group currently references a manufacturer with
  `merged_into IS NOT NULL` (checked: 0), so the listing does not need to aggregate over the chain.
  If a future merge leaves groups behind, the count queries must switch to `manufacturer_id = ANY(chain ids)`.
- **Counts are in-stock only.** `бимилк` owns 162 groups but the page reports 144, because 18 have no
  in-stock offer and are absent from `grouped_products`. This is intentional and matches the listing.
- `display_name` is what the UI shows; `canonical_name` is what links carry. `manufacturers` rows
  without a display name fall back to the canonical form.

Used by [[entities/api-endpoints]] and the manufacturer page in [[entities/frontend-views]].
