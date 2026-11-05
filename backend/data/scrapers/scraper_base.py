"""Shared scaffolding for every market scraper (strategy + template method).

A scraper has exactly one market-specific job: read one site and yield
:class:`ScrapedProduct` records. That is the *strategy* — subclass
:class:`MarketScraper` and implement :meth:`MarketScraper.fetch`.

Everything after scraping is identical for every market and lives in
:meth:`MarketScraper.run` (the *template method*):

1. de-duplicate by product name, applying the market's merge rule,
2. reuse the existing row UUID for known ``(market, name)`` pairs,
3. build the warehouse payload with correct ``etl_loadtime`` / ``last_updated``
   values,
4. upsert, then mark the rows missing from this run as out of stock.

Adding a market is therefore one file, ``{Market}_scraper.py``::

    class ExampleScraper(MarketScraper):
        market = "example"

        def fetch(self) -> list[ScrapedProduct]:
            ...
            return [ScrapedProduct(name=..., price=...)]

    if __name__ == "__main__":
        raise SystemExit(ExampleScraper().run())

``run_scrapers.py`` auto-discovers ``*_scraper.py`` and executes each file as a
standalone script, so keep the ``__main__`` block.
"""

from __future__ import annotations

import html
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Iterable

from backend.data.db_utils import (
    connect_to_db,
    get_products_by_market,
    save_products_to_products_table,
)


@dataclass(slots=True)
class ScrapedProduct:
    """One row as a market exposes it, before it reaches the warehouse.

    ``price`` is the whole-denar integer stored in ``products.price``.
    ``singular_price`` is the human-readable unit price shown by the market
    (e.g. ``"128 ден/kg"``). ``description`` holds whatever per-market text is
    useful for categorization (a category label, a promotion note, ...).
    """

    name: str
    price: int
    singular_price: str | None = None
    description: str | None = None
    in_stock: bool = True
    image: str | None = None
    link: str | None = None

    def __post_init__(self) -> None:
        # Names are written unescaped to the DB, and the existing-row lookup
        # compares against unescaped names, so normalize once here.
        self.name = html.unescape(self.name or "").strip()
        if self.price is not None:
            self.price = int(self.price)


class MarketScraper(ABC):
    """Base class for one market. Subclasses only implement :meth:`fetch`."""

    #: Value written to ``products.market``. Must match ``MARKET_INFO`` in
    #: ``src/lib/categories.ts`` and the ``--market-*`` Tailwind tokens.
    market: str = ""

    #: How to merge two records with the same name in one run. ``True`` (the
    #: default, used by the HTML price-list scrapers) keeps ``in_stock=True`` if
    #: any occurrence is in stock. ``False`` (WooCommerce scrapers) lets the
    #: later record win outright, matching their pre-refactor behaviour.
    keep_in_stock_on_duplicate: bool = True

    #: A run that yields zero products is almost always a broken parser or a
    #: downed site. With ``True`` the scraper returns a failure status and
    #: leaves the DB untouched instead of marking the whole market out of stock.
    abort_on_empty: bool = True

    @abstractmethod
    def fetch(self) -> Iterable[ScrapedProduct]:
        """Scrape the market and return its products (duplicates allowed)."""

    # ------------------------------------------------------------------ merge
    def merge(self, current: ScrapedProduct, incoming: ScrapedProduct) -> ScrapedProduct:
        """Combine two records for the same name; ``incoming`` wins on price."""
        if self.keep_in_stock_on_duplicate:
            in_stock = current.in_stock or incoming.in_stock
        else:
            in_stock = incoming.in_stock
        return replace(
            incoming,
            in_stock=in_stock,
            singular_price=incoming.singular_price or current.singular_price,
            description=incoming.description or current.description,
            image=incoming.image or current.image,
            link=incoming.link or current.link,
        )

    def collect(self) -> list[ScrapedProduct]:
        """Run :meth:`fetch` and collapse duplicates by name."""
        merged: dict[str, ScrapedProduct] = {}
        skipped = 0
        for product in self.fetch():
            if not product.name or product.price is None:
                skipped += 1
                continue
            current = merged.get(product.name)
            merged[product.name] = (
                product if current is None else self.merge(current, product)
            )
        if skipped:
            print(f"[{self.market}] skipped {skipped} records without a name or price")
        return list(merged.values())

    # ------------------------------------------------------------- warehouse
    def build_rows(
        self,
        products: list[ScrapedProduct],
        existing_ids: dict[tuple[str, str], str],
        now: datetime,
    ) -> list[dict]:
        """Build upsert payloads, reusing UUIDs and stamping both timestamps.

        ``etl_loadtime`` is the first-seen time: the loader keeps the stored
        value on conflict. ``last_updated`` is only bumped when a scraped field
        actually changes. Both start equal for a brand-new row.
        """
        rows = []
        for product in products:
            rows.append(
                {
                    "id": existing_ids.get((product.name, self.market))
                    or str(uuid.uuid4()),
                    "name": product.name,
                    "price": product.price,
                    "singular_price": product.singular_price,
                    "description": product.description,
                    "in_stock": product.in_stock,
                    "image": product.image,
                    "link": product.link,
                    "market": self.market,
                    "ETL_loadtime": now,
                    "last_updated": now,
                }
            )
        return rows

    def run(self) -> int:
        """Execute the market's strategy and persist the result. 0 = success."""
        if not self.market:
            raise ValueError(f"{type(self).__name__}.market is not set")
        start = time.time()
        print(f"[{self.market}] scraping")
        products = self.collect()

        if not products:
            message = f"[{self.market}] scrape returned no products"
            if self.abort_on_empty:
                print(f"{message}; aborting without touching the database")
                return 1
            print(f"{message}; continuing because abort_on_empty is False")

        print(f"[{self.market}] {len(products)} unique products in {round(time.time() - start, 2)}s")

        conn = connect_to_db()
        try:
            existing_ids = get_products_by_market(conn, self.market)
            now = datetime.now()
            rows = self.build_rows(products, existing_ids, now)
            save_products_to_products_table(
                conn, self.market, rows, {p.name for p in products}
            )
        finally:
            if not conn.closed:
                conn.close()

        print(f"[{self.market}] done in {round(time.time() - start, 2)}s")
        return 0
