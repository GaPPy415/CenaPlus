# CenaPlus — Pipeline rework

## M1: Schema + LLM feature extraction (manufacturer / product_name / quantity / metadata) | status: completed
### T1.1: Lock design decisions (grilling) | status: completed
- [x] S1.1.1: Storage shape — new columns on `products` + `manufacturers`/`manufacturer_aliases`
- [x] S1.1.2: Identity key — manufacturer + product_name + quantity (+ is_fluid); name carries fat %/variant
- [x] S1.1.3: Quantity math — LLM returns pieces, Python canonicalises; g/kg/ml/l/pcs; promote at >=1000; 3 decimals; multipacks multiplied
- [x] S1.1.4: Fluids — LLM flags pourable liquids only, g<->ml at 1.0 density, dry-goods guard
- [x] S1.1.5: Manufacturer aliases table (bimilk / битолско mergeable as data)
- [x] S1.1.6: Deterministic Python normalisation, name loses quantity + brand
- [x] S1.1.7: Matching — Tier A exact, Tier B token-set equal, Tier C reviewed report
- [x] S1.1.8: Group ids — bigint identity + unique natural key (applied in M2)
- [x] S1.1.9: Model — `gemini-3.1-flash-lite` everywhere (verified 200; 2.5-flash-lite is 404, 3.5-lite is 40% pricier), `GEMINI_MODEL` / `--model` override
- [x] S1.1.10: `reasoning` removed from both LLM schemas and from `products` (column dropped) — output tokens were the expensive part

### T1.2: Apply schema changes | status: completed
- [x] S1.2.1: `init/01_features.sql` written and applied to the live DB (columns verified 10/10)
- [x] S1.2.2: Schema consolidated — the seven `init/*.sql` files are replaced by one idempotent `init/00_schema.sql` (extensions, 4 tables, constraints, 15 indexes, timestamp trigger, `grouped_products` view). Verified by building a scratch database from it: 4 tables, 1 view, 32/13 columns matching live, `uq_groups_identity` created on an empty table

### T1.3: Build `backend/data/extract_features.py` | status: completed
- [x] S1.3.1: Pydantic response schema (metadata as list of pairs — Developer API rejects free-form dicts)
- [x] S1.3.2: Prompt with canonicalisation rules + worked examples
- [x] S1.3.3: Batched + concurrent Gemini calls behind `RateLimiter` (32 / 32)
- [x] S1.3.4: Deterministic validators (units, dry goods, dimensions, multipack fallback, repeat collapse)
- [x] S1.3.5: DB writer (idempotent via `features_extracted_at`, `--limit/--sample/--ids/--dry-run/--reprocess`)
- [x] S1.3.6: Wired into `run_pipeline.py` after `categorize_products.py`

### T1.4: Verify | status: completed
- [x] S1.4.1: Guard unit checks (10/10 cases) + dry-run samples over 60+ real rows
- [x] S1.4.2: Exclude non-groupable rows from the queue — main category `Разно` + sub category `Останато` (22,572 rows, 21.5%)
- [x] S1.4.3: Full run completed — 130,259 of 165,215 products have features, 0 pending in the non-excluded queue; 70.6% got a manufacturer, 65.6% a quantity, 22% flagged fluid; 8,509 manufacturers (aliases still 1:1)

## M1b: Carry the rate-limit window across stages + stop storing failed categorizations | status: completed
### T1b.1: Share one rolling window between categorize and extract_features | status: completed
- [x] S1b.1.1: `get_rate_limiter()` singleton registry keyed by `(scope, rpm, tpm)` in `RateLimiter.py`
- [x] S1b.1.2: Persist the 60 s window to `logs/rate-limiter/<scope>.json` (atomic write, 1 s debounce, `atexit` flush)
- [x] S1b.1.3: Both text stages use `TEXT_MODEL_SCOPE` + `TEXT_MODEL_*_LIMIT`; `embed_products` gets its own `gemini-embedding` scope
- [x] S1b.1.4: `GEMINI_RPM_LIMIT` / `GEMINI_TPM_LIMIT` overrides; budget auto-tightens to the 429 `quotaValue`
### T1b.2: Failed categorizations are not stored as data | status: completed
- [x] S1b.2.1: `call_with_retries()` retries 429/5xx honouring `RetryInfo.retryDelay`; used by categorize + extract
- [x] S1b.2.2: Batch functions return `None` for dropped products and raise after retries (no `Разно`/0.0 fallback)
- [x] S1b.2.3: `save_categorizations_to_db` skips rows without `categorization`; CLI reports categorized vs failed
- [x] S1b.2.4: `backend/tests/test_rate_limiter.py` + `test_categorize_failures.py` (12 cases) pass; 57 backend tests pass
- [x] S1b.2.5: Live check: `categorize_products.py --limit 3` stored 3/3 high-confidence rows; `extract_features.py` resumed the same window (3 requests)

## M2: Deterministic grouping (replaces semantic grouping) | status: completed
- [x] S2.0: Measured on real data — Tier A: 103,789 keys (14,811 multi, 41,281 rows, avg 2.79); Tier B (sorted tokens) merges another 2,677 keys → 101,112 keys, 15,821 multi, 44,968 rows (avg 2.84)
- [x] S2.0b: **id type decided — keep UUID.** bigint would save only ~2 MB of a 1.1 GB database and no measurable lookup time, and would churn the frontend `group_id: string` type. Natural-key unique index does the real work either way
- [x] S2.1: Key columns implemented — `init/05_grouping_keys.sql` applied (`product_name_key`, `product_name_sorted`, `variant_key`, `grouping_keys_version` + `idx_products_group_key`; `groups` gained the matching natural-key columns; `uq_groups_identity` deferred until the cutover empties `groups`). Rules live in `backend/data/grouping_rules.py` (shared with the collision study); `backend/data/grouping_keys.py` derived all 130,259 rows in ~6.5 s (~20k rows/s)
- [x] S2.1a: **Measured result: 114,967 identity keys, only 11,151 multi-product (26,443 rows, avg 2.37), largest key 10 rows** → 20.3% coverage
- [x] S2.1b: Tier C + variant impact measured (`backend/data/tier_c_report.py`, `variant_impact.py`):
  - fuzzy merging is weak and unsafe: at 0.90 it adds only ~2.9k rows, and at 0.98 it merges `млечна формула 1` with `2`, `батерии аа` with `ааа`, `јогурт 0%` with `10%`
  - **the real lever is variant strictness**: strict `variant_key` 20.3% → presence-insensitive compatibility ("missing metadata key = unknown, only differing values split") **29.5%** (105,706 groups / 38,481 rows) → ignoring variants entirely 34.4%
  - structural ceiling: 91% of rows sit in blocks with ≥2 rows, but one block of **25,185 rows has neither manufacturer nor quantity**, so it can only match on exact names
- [x] S2.1c: Rule B implemented — `products.variant` jsonb (init/06) + `variants_compatible()` + `guarded_fuzzy_merge()`; keys recomputed at version 2
- [x] S2.1d: **Projection of the final algorithm (rule B + guarded fuzzy): 103,901 groups, 14,754 multi-product, 41,111 rows in multi = 31.6% coverage** (up from 20.3% strict; variant-ignored ceiling is 34.4%, so we recovered ~92% of the safe gain). 3,457 components still contain same-market duplicates — S2.3 splits those that are not legitimate duplicates, so final coverage will land slightly lower (~28–30%)
- [x] S2.1e: Guarded fuzzy verified against the dangerous pairs — merges `колба/колбас`, `концетрат/концентрат`; refuses `формула 1/2`, `батерии аа/ааа`, `јогурт 0%/10%` (numeric tokens block)
- [x] S2.1f: Unbranded block (25,401 rows, no brand/size — household/clothing/toys, not produce) stays singleton unless variant info allows a merge (decision)
- [x] S2.2: Backfill implemented in `backend/data/group_products.py` (`--dry-run` / `--apply`) and **applied**: groups truncated and rebuilt
- [x] S2.2a: Rows without features (34,957 incl. 12 new-scraper rows) got singleton groups per (market, raw name) — 0 products left ungrouped
- [x] S2.6: Group category = majority `(main_category, sub_category)` tuple among members, **written back onto every member** (tie ⇒ tuple of the member with the smallest `id`). Applied; verified 0 groups with mixed categories
- [x] S2.3: Same-market policy (decided from the collision study; implemented in `grouping_rules.classify()`):
  - **variant families** — differing values ⇒ *different products, never merge*: `dimensions`/`dimension`/`size`, `packaging`/`pack`/`pakuvanje`/`packing`/`paket`, `code`/`model`/`sku`/`barcode`, `accessory`, and form words in the name (`парче`/`парчиња`/`цело`/`рефус`/`пакет`)
  - `pack_count` is a **variant signal only** (differing 18/1 vs 42/1 ⇒ separate) — deliberately *not* part of the global identity key
  - **promo is not identity**: ignore the `promo` metadata key, strip fixed promo words (`гратис`/`промо`/`акција`/`подарок`) and `N+M` patterns, **and strip the tokens recorded in that row's `promo` value** (e.g. `со 2 чаши`)
  - **unit spelling is formatting**: fold `г/гр/g → g`, `мл/ml → ml`, `л/l → l`, `см/mm → cm` before comparing, so `250Г` vs `250 ГР` are the same name and merge *without* the price guard
  - **name-level variants**: build a signature from the raw name — number+unit (75М vs 100М), bought dimensions (24Х24СМ), standalone gender markers (м/ж) — differing signature ⇒ different product
  - **formatting noise** such as `(14)` vs `14п` gets no special rule; the ≥0.90 similarity path decides it
  - **merge** same-market rows when: identical normalised name, promo-only difference, word order, ≤1 char apart, or ≥0.90 similarity with a comparable price
  - **comparable price** = identical, *or* one row is out of stock; exact-normalised matches ignore price entirely
  - **exception:** `market='vero'` + `краток рок` anywhere in the bucket ⇒ always same group (1,044 rows; UI badge later)
  - every row still gets a `group_id`; display relies on the existing `WHERE in_stock = true` so the available row wins
  - measured result of these rules: 4,086 buckets are real variants (separate), 1,297 buckets merge across the 5 merge classes, **110 buckets / 222 rows remain for review** (`.opencode/collisions-leftover.csv`, generated locally, not committed)
- [x] S2.3a: Metadata canonicalisation before comparison — implemented in `backend/data/same_market_collisions.py` (`canonical_variant`, key aliases, value normalisation); the grouper will import that module rather than duplicate the rules
- [x] S2.3b: Collision study done — 5,493 same-market buckets / 16,105 rows classified: 4,086 variant (separate), 1,297 merge across 5 classes, **110 buckets / 222 rows residual** for review (`.opencode/collisions-leftover.csv`, generated locally, not committed)
- [x] S2.4: Tier C near-duplicate report done (`backend/data/tier_c_report.py`, `variant_impact.py`, `grouping_projection.py`); fuzzy merges are guarded rather than threshold-based
- [x] S2.5: `groups` key/type settled — uuid id kept (2 MB saving not worth the frontend churn), no unique natural-key index (compatible-variant groups can legitimately share a representative key; idempotency comes from TRUNCATE + rebuild). `pg_dump` taken before cutover
- [x] S2.7: Cutover done — `TRUNCATE groups`, `group_id` nulled, rebuilt, no `legacy_group_id` captured. Result: **147,038 groups / 12,476 multi-product / 30,653 rows in multi (23.5% of featured) / avg 1.12 / largest group 13 / max 10 markets / 0 ungrouped / 0 empty / 0 mixed-category groups**
- [x] S2.8: Fixed a transitivity bug found in production data — variant compatibility is not transitive, so union-find chained incompatible rows (a 66-row group mixed different dimensions). Replaced with fuzzy name merging + **greedy variant clustering**; largest group dropped 66 → 13
- [x] S2.9: Manufacturer merges applied — `backend/data/manufacturer_merges.py` (propose / `--csv` / `--apply`, plus manual `--pair`). 975 candidate pairs at ≥0.86; auto-applied the ≥0.95 band (81 pairs → 72 merged, 9 skipped because a chain had already absorbed the drop name), then the manual Bimilk family (`бимилк` ← `битолско`, `битолски`, `битолска`, `бимилк моја кравица`; left `битолска квасара`/`битолска млекара` alone as different companies). **8,509 → 8,433 manufacturers**, regrouped after each pass. Evidence it works: `бимилк` milk groups now span 7 markets (`млеко битолско 2.8% 1кг`), 6 markets (`Bimilk - млеко 250ml`), etc. Remaining 894 pairs at 0.86–0.95 sit in `.opencode/manufacturer-merges.csv` (generated locally, not committed; the applied manual pairs are in `.opencode/manual-merges.csv`)
- [x] S2.12: Manufacturer near-duplicate review tool (merge битолско/бимилк into one alias) — done: `manufacturer_aliases` holds `битолско`/`битолски`/`битолска`/`бимилк моја кравица` → `бимилк`, and `/manufacturer` resolves aliases
- [x] S2.11: Embeddings stay as fallback (decision) — kept: `name_embedding` / `embed_products.py` remain, used only by `/search/semantic`

## M3: Serve + display grouped products | status: completed
- [x] S3.1: API adaptation — `grouped_products` still drives listings; `/search` is pg_trgm over `groups.product_name` + `manufacturers.canonical_name`; `/product` exposes extracted fields (manufacturer + display name, quantity, `kratok_rok`); `/manufacturer` lists a brand across categories
- [x] S3.2: Frontend display of extracted fields — detail page shows manufacturer/size/`краток рок`; cards show canonical group name + main-category chip on manufacturer listings; all fetched via `src/lib/api.ts`

## M4: Product detail page (new feature) | status: completed
- [x] S4.1: `GET /product?g_id=<uuid>` — group metadata (manufacturer, size, fluid, `kratok_rok`), price stats (cheapest/priciest/savings, market counts) and **every** offer including out-of-stock, ordered in-stock first then by price. UUID validated (400 on malformed), 404 when the group is gone
- [x] S4.2: `/product` page (`src/pages/ProductDetail.tsx`): breadcrumb + back link, hero (image, title, manufacturer/size chips, `краток рок` badge), three stat cards, "Достапно во N продавници" and "Нема залиха во N продавници" sections, per-offer raw shop name + price + singular price + availability + last-updated, external shop links
- [x] S4.3: `ProductCard` links to the detail page (image, title, "Види детали" footer); cart controls and external offer links keep their own click targets
- [x] S4.4: UX/a11y per ui-ux-pro-max: skeleton loading (no spinner), error state with retry, 44px touch targets, focus-visible rings, availability conveyed by icon+text (not colour alone), tabular prices, dark-mode tokens, lazy images with alt text
- [x] S4.5: Verified `bunx tsc -b`, `bun run lint` (0 errors), `bun run test` (5 pass), `bun run build`; endpoint exercised against a real 11-offer / 10-market group

## M5: Manufacturer listing (new feature) | status: completed
- [x] S5.1: `GET /manufacturer?manufacturer_name=<name>&page=&per_page=` returns `{ manufacturer: {name, display_name, groups, categories, markets}, total, page, per_page, data }`; `data` is the same `grouped_products` payload as the subcategory listing, so `ProductCard` renders unchanged. Parameterised SQL; 404 for unknown names
- [x] S5.2: Name resolution = `normalize_string` → `manufacturers.canonical_name`, then `manufacturer_aliases.alias`, then the `merged_into` chain (recursive CTE, canonical match wins over alias). This is what makes `Битолско` resolve to `Бимилк` (alias), while `битол` resolves through `merged_into`
- [x] S5.3: `/manufacturer` page (`src/pages/ManufacturerView.tsx`): breadcrumb, accent-bordered header (`border-l-4 border-l-primary`, uppercase "Производител") with 3 stats, per-page selector, URL-backed `manufacturer_name`/`page`/`per_page`, skeleton loading, error+retry, empty state, cards with `showCategory`
- [x] S5.4: Extracted `src/components/Pagination.tsx` from `SubCategoryView` (215 → 136 lines) and reused it; `ProductCard` gained `showCategory` for the main-category chip
- [x] S5.5: `/product` now also returns `manufacturer_display_name`; the detail-page manufacturer chip is a `Link` to the manufacturer view (Store + ChevronRight icons, display name shown, canonical name in the URL)
- [x] S5.6: Verified `bunx tsc -b`, `bun run lint` (0 errors), `bun run test` (5 pass), `bun run build`; endpoint checked live (`бимилк` 144 groups/3 categories/12 markets, `Битолско` → `Бимилк`, unknown → 404); Playwright: chip click → manufacturer page, 21 Medela groups, 3 category chips, page 2 = 9 cards, per_page=24 = 21 cards, 0 console errors

## Backlog / flagged
- [x] Re-categorize the poisoned rows: verified on 2026-11-05 that the live DB has **0** rows with `confidence IS NULL` or `< 0.5` — the backlog is empty (a future extraction pass can still leave failed rows, which stay unsaved and retry).
- [x] `backend/api.py` idle-in-transaction: fixed with `conn.autocommit = True` at import, so read endpoints commit each statement and never hold `ACCESS SHARE`.
- [x] Mobile overflow fixed (2026-11-05): `Header` hides the wordmark below `sm`, hides breadcrumbs below `md`, and lets the search form flex (`w-full sm:w-48 lg:w-64`). Verified with Playwright at 320/360/390/414/640/768/1024/1280 on `/`, a subcategory listing, `/product`, `/manufacturer` and `/search`: `scrollWidth == clientWidth` everywhere, 0 console errors; mobile search still navigates to `/search?q=…`.
- [x] Frontend container rebuilt by the user, so the deployed image now includes `/product` and `/manufacturer`.
- [ ] S2.3c follow-up: decide what to do with descriptive metadata keys (`category`, `brand_line`, `type`) — every non-promo key counts as identity, so extraction inconsistency can split identical products; consider pruning description-only keys at extraction.
- [ ] S2.10 follow-up: extraction name-consistency across shops (`чоколадо алпен милк` / `алпин` / `алпине милк` are four different keys). Grouping is correct; the names are not consistent.
