"""Shared client for WooCommerce Store API markets (Reptil, BigShop)."""

from __future__ import annotations

import html
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from backend.data.scrapers.scraper_base import MarketScraper, ScrapedProduct


def parse_price(price_str: str | None, minor_unit: int) -> int:
    """Convert WooCommerce minor-unit price string (e.g. '3200') to whole denars."""
    try:
        return int(price_str or 0) // (10 ** minor_unit)
    except (ValueError, TypeError):
        return 0


def parse_optional_price(price_str: str | None, minor_unit: int) -> int | None:
    if not price_str:
        return None
    try:
        value = int(price_str) // (10 ** minor_unit)
        return value if value > 0 else None
    except (ValueError, TypeError):
        return None


def decode_wc_json(raw: str) -> list[dict]:
    """Parse a WooCommerce JSON list, stripping any leading PHP warnings."""
    starts = [idx for ch in ("{", "[") if (idx := raw.find(ch)) != -1]
    if not starts:
        raise ValueError("No JSON payload in WooCommerce response")
    payload = json.loads(raw[min(starts) :])
    if not isinstance(payload, list):
        raise ValueError(f"Expected WooCommerce product list, got {type(payload).__name__}")
    return payload


class WooCommerceScraper(MarketScraper):
    """Base scraper for markets exposing `/wp-json/wc/store/v1/products`."""

    base_url: str = ""
    per_page: int = 100
    max_workers: int = 6
    request_timeout: tuple[int, int] = (10, 30)
    empty_image: str | None = None
    # Last category entry wins, matching pre-refactor WooCommerce behavior.
    keep_in_stock_on_duplicate: bool = False

    def get_probe_url(self) -> str | None:
        return f"{self.base_url}?per_page=1&page=1" if self.base_url else None

    def parse_singular_price(self, price_html: str) -> str | None:
        return None

    def parse_product(self, item: dict) -> ScrapedProduct:
        prices = item.get("prices", {})
        minor_unit = prices.get("currency_minor_unit", 2)
        images = item.get("images") or []
        categories = [c["name"] for c in item.get("categories", [])]

        return ScrapedProduct(
            name=html.unescape(item.get("name", "")),
            # 'price' covers both regular and sale prices.
            price=parse_price(prices.get("price", "0"), minor_unit),
            regular_price=parse_optional_price(prices.get("regular_price"), minor_unit),
            singular_price=self.parse_singular_price(item.get("price_html", "")),
            description=str(categories) if categories else None,
            in_stock=bool(item.get("is_in_stock")),
            image=images[0]["src"] if images else self.empty_image,
            link=item.get("permalink"),
        )

    def fetch_page(self, page: int) -> list[dict]:
        url = f"{self.base_url}?per_page={self.per_page}&page={page}"
        print(f"Fetching page: {page}")
        response = requests.get(url, timeout=self.request_timeout)
        response.raise_for_status()
        return decode_wc_json(response.text)

    def fetch(self) -> list[ScrapedProduct]:
        if not self.base_url:
            raise ValueError(f"{type(self).__name__}.base_url is not set")

        url = f"{self.base_url}?per_page={self.per_page}&page=1"
        print("Fetching page: 1")
        response = requests.get(url, timeout=self.request_timeout)
        response.raise_for_status()
        total_pages = int(response.headers.get("X-WP-TotalPages", 1))
        print(f"Total pages to scrape: {total_pages}")

        page_results: dict[int, list[ScrapedProduct]] = {}
        try:
            page_results[1] = [self.parse_product(item) for item in decode_wc_json(response.text)]
        except Exception as exc:
            print(f"Error scraping page 1: {exc}")

        if total_pages > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self.fetch_page, page): page
                    for page in range(2, total_pages + 1)
                }
                for future in as_completed(futures):
                    page_num = futures[future]
                    try:
                        page_results[page_num] = [
                            self.parse_product(item) for item in future.result()
                        ]
                    except Exception as exc:
                        print(f"Error scraping page {page_num}: {exc}")

        products: list[ScrapedProduct] = []
        for page_num in sorted(page_results):
            products.extend(page_results[page_num])
        return products
