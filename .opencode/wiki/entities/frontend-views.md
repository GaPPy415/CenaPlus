---
title: Frontend views and routing
tags: [react, routing, tanstack-query, ui, pagination]
sourceRefs: [src/App.tsx, src/pages/SubCategoryView.tsx, src/pages/ProductDetail.tsx, src/pages/ManufacturerView.tsx, src/components/ProductCard.tsx, src/components/Pagination.tsx, src/lib/api.ts]
lastReviewed: 2026-11-05
---

# Frontend views and routing

Routing is **query-param based**, not path based. `App.tsx` registers four routes:

| Route | Component | State |
| ----- | --------- | ----- |
| `/` | `Index` → `SubCategoryView` when `?category=&sub=` is present | local component state (`page`, `per_page`, market toggles) |
| `/search` | `SearchResults` | `?q=` |
| `/product` | `ProductDetail` | `?g_id=<uuid>` |
| `/manufacturer` | `ManufacturerView` | `?manufacturer_name=&page=&per_page=` |
| `*` | `NotFound` | — |

`src/lib/api.ts` is the only fetch layer (TanStack Query wraps it). Listings return
`{total, page, per_page, data}`, search returns `{data}`.

## Listing pages

`SubCategoryView` is the dense listing: market toggles, per-page selector, product grid, pagination.
Its page state is local, so changing page does not create a history entry.

`ManufacturerView` is the same grid with a deliberately different header, because the products span
every category:

- breadcrumb `Дома / Производител`
- header card with `border-l-4 border-l-primary`, an uppercase `Производител` label and three stats
  (производи / категории / продавници) from `manufacturer.{groups,categories,markets}`
- `ProductCard showCategory` renders the main-category chip above the title — the visual cue that
  this is a brand listing, not a subcategory
- `manufacturer_name`, `page` and `per_page` live in the URL so a brand page is linkable
- skeleton grid while loading, error state with retry, empty state

`Pagination` (`src/components/Pagination.tsx`) was extracted from `SubCategoryView` and is shared by
both listings: first/prev, numbered pages with ellipsis, next/last, and a go-to-page input. It
returns `null` for a single page, so callers need no guard.

## Detail page

`ProductDetail` reads `g_id`, renders breadcrumb + back link, a hero (image, title, manufacturer
chip, size chip, `краток рок` badge), three stat cards, then "Достапно во N продавници" and
"Нема залиха во N продавници" sections. Each offer row links to the shop in a new tab and shows the
raw listing name, price, unit price, availability (icon + text, not colour alone) and last-updated.

The manufacturer chip is a `Link` to `/manufacturer?manufacturer_name=<canonical>`; it prints
`manufacturer_display_name` and carries `manufacturer` (canonical) in the URL. Resolution rules:
[[concepts/manufacturer-resolution]].

## `ProductCard`

- image, title and a "Види детали" footer link to `/product?g_id=<uuid>`
- per-market rows are external `<a>`s; cart buttons stop propagation, so no nested links
- `showCategory` (optional) adds the main-category chip
- price rows show the cheapest offer in `price-best` styling; `ImageOff` replaces the old `📦` emoji

All copy is Macedonian Cyrillic. Touch targets are ≥44px, focus rings use `focus-visible`, and dark
mode comes from the same CSS variables as the rest of the app.

API shapes: [[entities/api-endpoints]].
