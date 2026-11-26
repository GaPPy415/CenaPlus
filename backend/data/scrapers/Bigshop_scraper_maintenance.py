"""BigShop (bigshop.mk) — WooCommerce Store API."""

import re

from backend.data.scrapers.woocommerce_platform import WooCommerceScraper

MARKET_NAME = "bigshop"
BASE_URL = "https://bigshop.mk/wp-json/wc/store/v1/products"


def parse_singular_price(price_html: str) -> str | None:
    """Legacy per-unit price parsing inherited from the original Zito scraper."""
    if price_html:
        parts = re.split(r"<[^>]+>", price_html)
        if len(parts) >= 13:
            # Reconstruct: index 9 + index 12 + "ден"
            return parts[9] + parts[12] + "ден"
    return None


class BigshopScraper(WooCommerceScraper):
    market = MARKET_NAME
    base_url = BASE_URL
    request_timeout = (10, 45)
    empty_image = ""

    def parse_singular_price(self, price_html: str) -> str | None:
        return parse_singular_price(price_html)


if __name__ == "__main__":
    raise SystemExit(BigshopScraper().run())
