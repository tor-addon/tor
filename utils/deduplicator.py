"""
utils/deduplicator.py
─────────────────────
Two-level deduplication per request (torrents only):

  Level 1 – infohash (exact match, lowercased)
  Level 2 – size_bytes
             Catches the same torrent from multiple Torznab sources.

Special cases:
  DDL streams (Movix, Wawacity) – bypass entirely (each source is unique/valuable).
  Library streams – never rejected; hashes registered to dedup Torznab dupes.
"""

import logging

logger = logging.getLogger(__name__)


class StreamDeduplicator:
    __slots__ = ("_seen_hashes", "_seen_sizes")

    def __init__(self) -> None:
        self._seen_hashes: set[str] = set()
        self._seen_sizes:  set[int] = set()

    def is_valid(self, stream: dict) -> bool:
        """
        Returns True if the stream should be kept.
        DDL streams bypass dedup entirely.
        Library streams always pass (but register their hash to dedup Torznab dupes).
        """
        is_library = stream.get("source") == "Library"
        is_ddl     = stream.get("stream_type") == "ddl"

        # DDL streams skip deduplication – each link is unique and valuable
        if is_ddl:
            return True

        infohash = stream.get("infohash") or stream.get("hash")

        # ── Level 1: infohash ─────────────────────────────────────────────────
        if infohash:
            h_key = str(infohash).lower()
            if h_key in self._seen_hashes:
                if is_library:
                    return True
                stream["invalid_reason"] = "Duplicate infohash"
                logger.debug("DEDUP L1 ✗ hash=%s", h_key[:16])
                return False
            self._seen_hashes.add(h_key)
        elif not is_library:
            stream["invalid_reason"] = "Missing infohash"
            logger.debug("DEDUP ✗ missing infohash title=%s", stream.get("torrent_name") or stream.get("title"))
            return False

        # ── Level 2: size-based (torrent only; Library skips) ─────────────────
        if not is_library:
            size = stream.get("size") or 0
            try:
                size = int(size)
            except (ValueError, TypeError):
                size = 0

            if size > 0:
                if size in self._seen_sizes:
                    stream["invalid_reason"] = "Duplicate content"
                    logger.debug("DEDUP L2 ✗ size=%d", size)
                    return False
                self._seen_sizes.add(size)

        stream.pop("invalid_reason", None)
        return True