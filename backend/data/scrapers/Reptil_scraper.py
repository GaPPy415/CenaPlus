"""Reptil (marketonline.mk) — WooCommerce Store API."""

import requests
from bs4 import BeautifulSoup

from backend.data.scrapers.woocommerce_platform import WooCommerceScraper

MARKET_NAME = "reptil"
BASE_URL = "https://marketonline.mk/wp-json/wc/store/v1/products"
WP_PRODUCT_URL = "https://marketonline.mk/wp-json/wp/v2/product"


def parse_singular_price(price_html: str) -> str | None:
    """Extract the per-unit price string from price_html, e.g. '128 ден/kg'."""
    try:
        soup = BeautifulSoup(price_html, "html.parser")
        recalc = soup.find("span", class_="mcmp_recalc_price_row")
        if not recalc:
            return None
        bdi = recalc.find("bdi")
        suffix = recalc.find("span", class_="mcmp-recalc-price-suffix")
        if bdi:
            price_part = bdi.get_text(strip=True).replace("\xa0", " ")
            suffix_part = suffix.get_text(strip=True) if suffix else ""
            return f"{price_part}{suffix_part}"
    except Exception:
        pass
    return None


class ReptilScraper(WooCommerceScraper):
    market = MARKET_NAME
    base_url = BASE_URL
    request_timeout = (10, 30)

    def parse_singular_price(self, price_html: str) -> str | None:
        return parse_singular_price(price_html)

    def get_source_marker(self) -> str | None:
        response = requests.get(
            WP_PRODUCT_URL,
            params={"per_page": 1, "orderby": "modified", "order": "desc"},
            timeout=(10, 30),
        )
        response.raise_for_status()
        items = response.json()
        if not isinstance(items, list) or not items:
            return None
        modified = items[0].get("modified_gmt") or items[0].get("modified")
        total = response.headers.get("X-WP-Total", "")
        return f"{modified}|total={total}" if modified else None


if __name__ == "__main__":
    raise SystemExit(ReptilScraper().run())
