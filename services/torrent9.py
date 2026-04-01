"""
services/torrent9.py
────────────────────
Torrent9 scraper. Asynchronous.

All title searches and detail page fetches run concurrently via asyncio.gather.
"""

import asyncio
import logging
import re
from urllib.parse import quote

import httpx
from selectolax.parser import HTMLParser

from settings import TORRENT9_BASE_URL

logger = logging.getLogger(__name__)

_HEADERS     = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
_SIZE_UNITS  = {"kb": 1_000, "mb": 1_000_000, "gb": 1_000_000_000, "tb": 1_000_000_000_000}
_MAX_RESULTS = 25


def _parse_size(raw: str) -> int:
    m = re.match(r"([\d.,]+)\s*([a-zA-Z]+)", raw.strip())
    if not m:
        return 0
    factor = _SIZE_UNITS.get(m.group(2).lower(), 0)
    return int(float(m.group(1).replace(",", ".")) * factor) if factor else 0


class Torrent9Client:
    __slots__ = ("_base", "_timeout", "client")

    def __init__(self, base_url: str = "", timeout: float = 6.0) -> None:
        self._base    = (base_url or TORRENT9_BASE_URL).rstrip("/")
        self._timeout = timeout
        self.client   = httpx.AsyncClient(
            headers=_HEADERS,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=4),
            timeout=timeout,
        )

    # ─────────────────────────────────────────────────────────────────────────

    async def get_streams(self, titles: list[str]) -> list[dict]:
        unique = list(dict.fromkeys(t for t in titles if t))
        if not unique:
            return []

        # Step 1: all title searches in parallel → unique detail URLs
        search_results = await asyncio.gather(*[self._search(t) for t in unique])
        seen:   set[str]  = set()
        detail: list[str] = []
        for urls in search_results:
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    detail.append(u)

        if not detail:
            return []

        # Step 2: all detail pages in parallel
        page_results = await asyncio.gather(*[self._parse_page(u) for u in detail])
        streams = [r for r in page_results if r]
        logger.info("Torrent9 │ %d stream(s) from %d page(s)", len(streams), len(detail))
        return streams

    # ─────────────────────────────────────────────────────────────────────────

    async def _search(self, query: str) -> list[str]:
        try:
            r = await self.client.get(f"{self._base}/recherche/{quote(query)}")
            if r.status_code != 200:
                return []
            dom = HTMLParser(r.text)
            return [
                self._base + a.attributes["href"]
                for a in dom.css("table tbody tr td a")
                if a.attributes.get("href", "").startswith("/detail/")
            ][:_MAX_RESULTS]
        except Exception as exc:
            logger.debug("Torrent9 │ search error q=%r: %s", query, exc)
            return []

    async def _parse_page(self, url: str) -> dict | None:
        try:
            r = await self.client.get(url)
            if r.status_code != 200:
                return None
            dom = HTMLParser(r.text)

            a_torrent = dom.css_first('a[href^="/get_torrents/"]')
            if not a_torrent:
                return None
            infohash = a_torrent.attributes["href"].split("/get_torrents/")[1].upper()

            name = ""
            strong = dom.css_first(".movie-information p strong")
            if strong:
                name = strong.text(strip=True)

            seeders = 0
            size    = 0
            for ul in dom.css(".movie-information ul"):
                label_el = ul.css_first("strong")
                if not label_el:
                    continue
                label = label_el.text(strip=True).lower()
                lis   = ul.css("li")
                if label == "seed":
                    for li in lis:
                        if "green" in li.attributes.get("style", ""):
                            m = re.search(r'\d+', li.text())
                            if m:
                                seeders = int(m.group())
                elif "poids" in label and len(lis) >= 3:
                    size = _parse_size(lis[2].text(strip=True))

            return {
                "torrent_name": name,
                "infohash":     infohash,
                "source":       "T9",
                "stream_type":  "torrent",
                "cached":       False,
                "size":         size,
                "seeders":      seeders,
            }
        except Exception as exc:
            logger.debug("Torrent9 │ parse error %s: %s", url, exc)
            return None

    def close(self) -> None:
        pass
