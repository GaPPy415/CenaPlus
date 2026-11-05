---
title: Product-name token aliases
tags: [grouping, names, aliases, data-quality]
sourceRefs: [backend/data/product_name_aliases.py, backend/data/group_products.py, backend/data/grouping_rules.py]
lastReviewed: 2026-11-05
---

# Product-name token aliases

Grouping is exact-match on `products.product_name_sorted` (manufacturer + canonical
name + quantity + unit + fluid). The rule is correct; the *input* is not. The LLM
extracts `product_name` per row, and shops spell the same brand differently:

| `product_name_key` | markets (250 g) |
| --- | --- |
| `чоколадо алпен милк` | spar |
| `чоколадо алпин милк` | vero |
| `чоколадо алпине милк` | bigshop |

Three spellings, three groups, no card compares spar + vero + bigshop. The existing
`guarded_fuzzy_merge` does not catch this: `алпен` vs `алпине` scores 0.727 on
`SequenceMatcher`, below its 0.85 token threshold.

## Design: token aliases, not full-name aliases

A full-name alias table was tried first and failed: the same spelling maps to
different canonicals at different sizes (80 g vs 250 g), so the proposals conflicted
and were dropped. The variant is a property of the **word**, so the table is:

```sql
product_name_token_aliases (
    manufacturer_id bigint,
    alias_token text,
    canonical_token text,
    PRIMARY KEY (manufacturer_id, alias_token)
)
```

`group_products.load_name_aliases()` loads the map (empty when the table is absent) and
rewrites each row's name tokens before blocking:

```python
tokens = row["product_name_key"].split()
replaced = [table.get(token, token) for token in tokens]
```

Canonical token = the one in the most rows (ties: most name keys, most markets,
lexicographic). For Milka that picks `алпин` (23 rows) over `алпине` (8) and `алпен` (2).

## Guards (why it is conservative)

`token_pair_ok()` accepts only:

- same first letter, both tokens ≥ 4 characters, no digits;
- one edit at most (Levenshtein), or a prefix abbreviation (shorter ≥ 5, length gap ≤ 3);
- a plural/derivation rule for pairs like `лешник`/`лешници` (same first 3, two edits);
- **no form/packaging words** (`бонбони`, `бонбониера`, `колач`, `колачиња`, `пакет`,
  `пакетче`, `стапче`, `стапчиња`, `снек`, `млеко`, `млечно`).

The first, similarity-based version proposed `малина → алпин`, `кејк → кекс`,
`млеко → млечно`, `барни → брауни`. Edit-distance + form stoplist removes all of them;
the regression suite is `backend/tests/test_product_name_aliases.py`.

## Measured effect (2026-11-05, live DB)

| Scope | Aliases | Rows rewritten | Rows with wider comparison | Groups |
| --- | --- | --- | --- | --- |
| Milka | 40 | 179 | 65 | 391 → 370 (−21) |
| All manufacturers | 5,130 | 10,306 | 1,914 | 75,562 → 74,909 (−653) |

The Milka 250 g family now groups spar + bigshop + vero into one card (4 rows, 3
markets); the packaging variant (`|кутија`) stays in its own group because the grouper
only joins a variant row to a cluster that already carries that key. The global gain is
modest (−0.9% of manufacturer-attached groups) but concentrated: 1,914 rows move into a
wider price comparison.

Projection method: rewrite every manufacturer-attached row with the proposed aliases,
then run the real `build_components()` on each block before and after; `groups_saved` is
the difference. Nothing is written.

## Limits / next step

- Same-token-count only: `чоколадо алпин` and `чоколадо алпин милк` are not merged
  (a subset name could be a different product).
- Form words are never aliased, so `бонбони`/`бонбониера` stay separate.
- The proposal CSV is reviewed before `--apply`; the table is not created by the
  pipeline.
- Applying aliases + `group_products.py --apply` is a full regroup (~146k groups); the
  prototype stops at the measurement.

See also [[entities/products-table]] and [[concepts/manufacturer-resolution]] (the same
alias idea one level up, for brand names).
