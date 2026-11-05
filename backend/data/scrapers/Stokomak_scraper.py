"""Стокомак — price list on the proverkanaceni.mk platform."""

from backend.data.scrapers.proverkanaceni_platform import ProverkanaceniScraper

MARKET_NAME = "stokomak"


class StokomakScraper(ProverkanaceniScraper):
    market = MARKET_NAME
    base_url = "https://stokomak.proverkanaceni.mk/"
    market_workers = 10
    page_workers = 8


if __name__ == "__main__":
    raise SystemExit(StokomakScraper().run())
