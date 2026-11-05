"""Зур — price list on the ZUR platform (ceni.zur.mk).

Same app as `Dim_scraper.py`, different AJAX action and store list.
"""

from backend.data.scrapers.zur_platform import ZurPlatformScraper

MARKET_NAME = "zur"


class ZurScraper(ZurPlatformScraper):
    market = MARKET_NAME
    index_url = "https://ceni.zur.mk/"
    ajax_action = "load_prices_data"


if __name__ == "__main__":
    raise SystemExit(ZurScraper().run())
