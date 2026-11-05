"""Probe ramstore.mk's custom `price/v2` REST namespace.

`/wp-json/price/v2` is only the namespace index; it advertises three GET routes:
`/price/v2/app_cron`, `/price/v2/app_cron1`, `/price/v2/app_cron2` (plus the
optional `wpml_language` arg, `mk|sq|en`). On 2026-11-03 all three accepted the
connection and then sent no response at all for 90 s — they behave like long
cron jobs (price sync), not like query endpoints — while the normal store pages
returned ~7 MB of HTML.

That is why `Ramstore_scraper.py` still parses HTML. Run this script (one
request at a time, generous timeout) if the API is worth re-checking later; it
prints the status, content type and first bytes of each route.

    python backend/data/scrapers/ramstore_api_probe.py [--timeout 180]
"""

import argparse
import time

import requests

BASE = "https://ramstore.com.mk/wp-json/price/v2"
ROUTES = ("", "/app_cron", "/app_cron1", "/app_cron2")


def probe(path: str, timeout: int) -> None:
    url = BASE + path
    print(f"\nGET {url}?wpml_language=mk")
    started = time.time()
    try:
        response = requests.get(
            url,
            params={"wpml_language": "mk"},
            headers={"User-Agent": "Mozilla/5.0 CenaPlus/1.0"},
            timeout=(15, timeout),
            stream=True,
        )
        print(f"  status={response.status_code} type={response.headers.get('content-type')}")
        chunk = response.raw.read(1000, decode_content=True)
        response.close()
        print(f"  {round(time.time() - started, 1)}s, first bytes: {chunk[:500]!r}")
    except Exception as e:
        print(f"  ERR {type(e).__name__}: {e} after {round(time.time() - started, 1)}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=180, help="read timeout per route")
    args = parser.parse_args()
    for route in ROUTES:
        probe(route, args.timeout)
