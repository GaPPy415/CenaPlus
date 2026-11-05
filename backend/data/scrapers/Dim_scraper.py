"""ДИМ Маркети — price list on the ZUR platform.

`dim.mk/cenovnik` embeds `dim.marketceni.mk`, which is an instance of the
platform client in `zur_platform.py`; only the AJAX action differs.
"""

from backend.data.scrapers.zur_platform import ZurPlatformScraper

MARKET_NAME = "dim"


class DimScraper(ZurPlatformScraper):
    market = MARKET_NAME
    index_url = "https://dim.marketceni.mk/"
    ajax_action = "load_dimceni_prices_data"


if __name__ == "__main__":
    raise SystemExit(DimScraper().run())
