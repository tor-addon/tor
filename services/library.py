"""
services/library.py
───────────────────
AllDebrid Library source client.
Synchronous – call via asyncio.to_thread().

Fetches all Ready magnets from the user's AllDebrid library (v4.1 API).
Each magnet is converted to a pipeline-compatible stream dict (stream_type=torrent,
cached=True) so it passes through the normal Filter → Rank pipeline.

Results are cached for 15 s (TTL) to match the stream pipeline dedup window.
Resolution works identically to a regular torrent: infohash → resolve_stream().
"""

import logging
import time

import requests

from settings import ALLDEBRID_V41_BASE_URL

logger = logging.getLogger(__name__)

_READY_STATUS = "Ready"
_CACHE_TTL    = 15.0   # seconds


class LibraryClient:
    __slots__ = ("api_key", "session", "_cache", "_cache_ts")

    def __init__(self, api_key: str) -> None:
        self.api_key   = api_key
        self.session   = requests.Session()
        self._cache:    list[dict] | None = None
        self._cache_ts: float             = 0.0

    def get_streams(self) -> list[dict]:
        """
        Returns pipeline-compatible stream dicts for all Ready magnets.
        Results are cached for 15 s (same window as the stream pipeline).
        """
        now = time.monotonic()
        if self._cache is not None and (now - self._cache_ts) < _CACHE_TTL:
            logger.info("Library │ cache HIT (%d streams)", len(self._cache))
            return self._cache

        try:
            r = self.session.post(
                f"{ALLDEBRID_V41_BASE_URL}/magnet/status",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=15,
            )
            r.raise_for_status()
            body = r.json()
        except Exception as exc:
            logger.error("Library │ request failed: %s", exc)
            return self._cache or []   # stale cache is better than nothing

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
        """Force next call to re-fetch (e.g. after the user uploads a new magnet)."""
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

        gb = size / (1 << 30)
        size_fmt = f"{gb:.2f} GB" if gb >= 1 else f"{size / (1 << 20):.0f} MB"

        return {
            "title":        filename,
            "torrent_name": filename,
            "infohash":     h.upper(),
            "source":       "Library",
            "stream_type":  "torrent",
            "cached":       True,
            "seeders":      0,
            "size":         size,
            "size_fmt":     size_fmt,
        }

    def close(self) -> None:
        self.session.close()