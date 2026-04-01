"""
services/torznab.py
───────────────────
Torznab client. Asynchronous.

Two search modes:
  search(query, categories)   – generic t=search (movies)
  search_tv(query, categories) – t=tvsearch (series)

Retry policy: up to 3 attempts on network errors or HTTP 5xx / 429.
"""

import asyncio
import logging
import xml.etree.ElementTree as ET

import httpx

from settings import TORZNAB_RESULT_LIMIT

logger = logging.getLogger(__name__)

_NS             = {"t": "http://torznab.com/schemas/2015/feed"}
_RETRY_ATTEMPTS = 3
_RETRY_DELAY    = 0.5


class Torznab:
    __slots__ = ("name", "url", "apikey", "client", "movie_cats", "series_cats")

    def __init__(
        self,
        name: str,
        url: str,
        apikey: str | None = None,
        movie_cats: list[int] | None = None,
        series_cats: list[int] | None = None,
    ) -> None:
        self.name        = name
        self.url         = url
        self.apikey      = apikey
        self.client      = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2, keepalive_expiry=30.0),
            timeout=5,
        )
        self.movie_cats  = movie_cats or []
        self.series_cats = series_cats or []

    # ─────────────────────────────────────────────────────────────────────────

    async def search(self, query: str, categories: list[int] | None = None, **kwargs) -> list[dict]:
        params: dict = {"t": "search", "limit": TORZNAB_RESULT_LIMIT}
        if self.apikey:
            params["apikey"] = self.apikey
        if query:
            params["q"] = query
        if categories:
            params["cat"] = ",".join(str(c) for c in categories)
        params.update(kwargs)
        logger.debug("Torznab │ [%s] search q=%r", self.name, query)
        results = await self._fetch(params)
        logger.info("Torznab │ [%s] %d result(s) for q=%r", self.name, len(results), query)
        return results

    async def search_tv(self, query: str, categories: list[int] | None = None) -> list[dict]:
        params: dict = {"t": "tvsearch", "limit": TORZNAB_RESULT_LIMIT}
        if self.apikey:
            params["apikey"] = self.apikey
        if query:
            params["q"] = query
        if categories:
            params["cat"] = ",".join(str(c) for c in categories)
        logger.debug("Torznab │ [%s] tvsearch q=%r", self.name, query)
        results = await self._fetch(params)
        logger.info("Torznab │ [%s] tvsearch %d result(s) q=%r", self.name, len(results), query)
        return results

    # ─────────────────────────────────────────────────────────────────────────

    async def _fetch(self, params: dict) -> list[dict]:
        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            try:
                r = await self.client.get(self.url, params=params)

                if r.status_code == 429 or r.status_code >= 500:
                    if attempt < _RETRY_ATTEMPTS:
                        delay = (
                            float(r.headers.get("Retry-After", _RETRY_DELAY))
                            if r.status_code == 429 else _RETRY_DELAY
                        )
                        logger.warning(
                            "Torznab │ [%s] HTTP %d (attempt %d/%d) – retrying in %.1fs",
                            self.name, r.status_code, attempt, _RETRY_ATTEMPTS, delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    logger.warning(
                        "Torznab │ [%s] HTTP %d after %d attempts",
                        self.name, r.status_code, _RETRY_ATTEMPTS,
                    )
                    return []

                r.raise_for_status()
                return self._parse(r.text)

            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                if attempt < _RETRY_ATTEMPTS:
                    logger.warning(
                        "Torznab │ [%s] %s (attempt %d/%d) – retrying in %.1fs",
                        self.name, type(exc).__name__, attempt, _RETRY_ATTEMPTS, _RETRY_DELAY,
                    )
                    await asyncio.sleep(_RETRY_DELAY)
                    continue
                logger.warning(
                    "Torznab │ [%s] failed after %d attempts: %s",
                    self.name, _RETRY_ATTEMPTS, exc,
                )
                return []
            except Exception as exc:
                logger.warning("Torznab │ [%s] request failed: %s", self.name, exc)
                return []
        return []

    # ─────────────────────────────────────────────────────────────────────────

    def _parse(self, xml_text: str) -> list[dict]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.error("Torznab │ [%s] XML parse error: %s", self.name, exc)
            return []

        results: list[dict] = []
        for item in root.findall(".//item"):
            guid = item.findtext("guid") or ""

            infohash: str | None = None
            seeders  = 0
            size_attr: str | None = None
            for attr in item.findall("t:attr", _NS):
                name = attr.get("name")
                if name == "infohash":
                    infohash = attr.get("value")
                elif name == "seeders":
                    v = attr.get("value")
                    seeders = int(v) if v and v.isdigit() else 0
                elif name == "size":
                    size_attr = attr.get("value")

            if not infohash and "btih:" in guid.lower():
                infohash = guid.lower().split("btih:")[-1].split("&")[0]

            if not infohash:
                continue

            try:
                size = int(size_attr or item.findtext("size") or 0)
            except (ValueError, TypeError):
                size = 0

            results.append({
                "title":       item.findtext("title"),
                "size":        size,
                "source":      self.name,
                "stream_type": "torrent",
                "seeders":     seeders,
                "infohash":    infohash.upper(),
            })

        return results

    def close(self) -> None:
        pass  # aclose() must be awaited; managers are long-lived
