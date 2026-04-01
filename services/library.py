"""
services/library.py
───────────────────
AllDebrid Library source. Asynchronous.

Fetches all Ready magnets from the user's AllDebrid library (v4.1 API).
Results are cached for _CACHE_TTL seconds (matches stream pipeline dedup window).
Streams are pre-cached (cached=True) and pass directly to ranking.
"""

import logging
import time

import httpx

from settings import ALLDEBRID_V41_BASE_URL

logger = logging.getLogger(__name__)

_READY_STATUS = "Ready"
_CACHE_TTL    = 15.0


class LibraryClient:
    __slots__ = ("api_key", "client", "_cache", "_cache_ts")

    def __init__(self, api_key: str) -> None:
        self.api_key   = api_key
        self.client    = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2, keepalive_expiry=30.0),
            timeout=15,
        )
        self._cache:    list[dict] | None = None
        self._cache_ts: float             = 0.0

    async def get_streams(self) -> list[dict]:
        now = time.monotonic()
        if self._cache is not None and (now - self._cache_ts) < _CACHE_TTL:
            logger.info("Library │ cache HIT (%d streams)", len(self._cache))
            return self._cache

        try:
            r = await self.client.post(
                f"{ALLDEBRID_V41_BASE_URL}/magnet/status",
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
            r.raise_for_status()
            body = r.json()
        except Exception as exc:
            logger.error("Library │ request failed: %s", exc)
            return self._cache or []

        if body.get("status") != "success":
            logger.warning("Library │ API error: %s", body.get("error", {}))
            return self._cache or []

        magnets = body.get("data", {}).get("magnets") or []
        streams = [self._to_stream(m) for m in magnets if m.get("status") == _READY_STATUS]

        logger.info("Library │ %d Ready magnet(s) out of %d", len(streams), len(magnets))
        self._cache    = streams
        self._cache_ts = now
        return streams

    def invalidate_cache(self) -> None:
        self._cache    = None
        self._cache_ts = 0.0

    @staticmethod
    def _to_stream(magnet: dict) -> dict:
        filename = magnet.get("filename") or ""
        size     = magnet.get("size") or 0
        h        = magnet.get("hash") or ""

        try:
            size = int(size)
        except (ValueError, TypeError):
            size = 0

        gb       = size / (1 << 30)
        size_fmt = f"{gb:.2f} GB" if gb >= 1 else f"{size >> 20} MB"

        return {
            "title":        filename,
            "torrent_name": filename,
            "infohash":     h.upper(),
            "ad_id":        magnet.get("id"),
            "source":       "Library",
            "stream_type":  "torrent",
            "cached":       True,
            "seeders":      0,
            "size":         size,
            "size_fmt":     size_fmt,
        }

    def close(self) -> None:
        pass
