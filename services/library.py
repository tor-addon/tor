# Author: adam


"""
services/library.py
───────────────────
AllDebrid Library source client.
Synchronous – call via asyncio.to_thread().

Fetches all Ready magnets from the user's AllDebrid library (v4.1 API).
Each magnet is converted to a pipeline-compatible stream dict (stream_type=torrent,
cached=True) so it passes through the normal Filter → Rank pipeline.

Resolution works identically to a regular torrent: infohash → resolve_stream().
"""

import logging

import requests

from settings import ALLDEBRID_AGENT, ALLDEBRID_V41_BASE_URL

logger = logging.getLogger(__name__)

_READY_STATUS = "Ready"


class LibraryClient:
    __slots__ = ("api_key", "session")

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.session = requests.Session()

    def get_streams(self) -> list[dict]:
        """
        Returns pipeline-compatible stream dicts for all Ready magnets in the library.
        Up to ~7 000 entries per AllDebrid docs; typically <1 000.
        """
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
            return []

        if body.get("status") != "success":
            logger.warning("Library │ API error: %s", body.get("error", {}))
            return []

        magnets = body.get("data", {}).get("magnets") or []
        streams = [self._to_stream(m) for m in magnets if m.get("status") == _READY_STATUS]

        logger.info("Library │ %d Ready magnet(s) out of %d", len(streams), len(magnets))
        return streams

    # ─────────────────────────────────────────────────────────────────────────

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
            # Identity – PTT will enrich title/resolution/quality/languages in filtering
            "title":        filename,
            "torrent_name": filename,
            "infohash":     h.upper(),
            "source":       "Library",
            "stream_type":  "torrent",
            # Pre-marked cached: skip AllDebrid cache re-check
            "cached":       True,
            "seeders":      0,
            "size":         size,
            "size_fmt":     size_fmt,
        }

    def close(self) -> None:
        self.session.close()