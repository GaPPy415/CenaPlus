"""Елида — price list on the proverkanaceni.mk platform."""

from backend.data.scrapers.proverkanaceni_platform import ProverkanaceniScraper

MARKET_NAME = "elida"


class ElidaScraper(ProverkanaceniScraper):
    market = MARKET_NAME
    base_url = "https://elida.proverkanaceni.mk/"
    market_workers = 8
    page_workers = 6


if __name__ == "__main__":
    raise SystemExit(ElidaScraper().run())
