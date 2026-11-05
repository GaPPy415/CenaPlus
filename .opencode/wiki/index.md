---
title: CenaPlus wiki
tags: [index]
sourceRefs: [AGENTS.md]
lastReviewed: 2026-11-05
---

# CenaPlus wiki

Working notes on the CenaPlus pipeline (scrape → categorize → embed → group → FastAPI → React).
Start here, then follow the links.

## Concepts

| Page | Summary |
| ---- | ------- |
| [[concepts/scraper-strategy-pattern]] | `MarketScraper` template method: one `fetch()` per market, everything else shared. How to add a market. |
| [[concepts/product-timestamps]] | `etl_loadtime` vs `last_updated`: what each means, how it was broken, how it is enforced now. |
| [[concepts/pipeline-query-tuning]] | Measured query costs and the indexes that fixed them; what was deliberately left alone. |
| [[concepts/scraper-api-survey]] | Which markets expose a JSON API (and which dead ends look like one). |
| [[concepts/manufacturer-resolution]] | How a brand name resolves to one `manufacturers` row: normalise → canonical → alias → `merged_into` chain. |

## Entities

| Page | Summary |
| ---- | ------- |
| [[entities/products-table]] | The single `products` table every scraper writes into: columns, keying, market row counts. |
| [[entities/scrapers]] | The twelve `{Market}_scraper.py` strategies and what each site requires. |
| [[entities/api-endpoints]] | The FastAPI surface: `/search`, `/product`, `/manufacturer`, listing routes, and their SQL/safety notes. |
| [[entities/frontend-views]] | Routes, the three listing/detail pages, shared pagination and `ProductCard` behaviour. |

## External sources

_None yet._
