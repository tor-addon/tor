"""
utils/deduplicator.py
─────────────────────
Infohash deduplication per request (torrents only).

Special cases:
  DDL streams (Movix, Wawacity) – bypass entirely (each link is unique).
  Library streams – always pass, but register their hash to drop Torznab dupes.
"""

import logging

logger = logging.getLogger(__name__)


class StreamDeduplicator:
    __slots__ = ("_seen_hashes",)

    def __init__(self) -> None:
        self._seen_hashes: set[str] = set()

    def is_valid(self, stream: dict) -> bool:
        """
        Returns True if the stream should be kept.
        DDL streams bypass dedup entirely.
        Library streams always pass (but register their hash to dedup Torznab dupes).
        """
        is_ddl = stream.get("stream_type") == "ddl"
        if is_ddl:
            return True

        is_library = stream.get("source") == "Library"
        infohash   = stream.get("infohash") or stream.get("hash")

        if infohash:
            h_key = str(infohash).lower()
            if h_key in self._seen_hashes:
                if is_library:
                    return True
                stream["invalid_reason"] = "Duplicate infohash"
                logger.debug("DEDUP ✗ hash=%s", h_key[:16])
                return False
            self._seen_hashes.add(h_key)
        elif not is_library:
            stream["invalid_reason"] = "Missing infohash"
            logger.debug("DEDUP ✗ missing infohash title=%s",
                         stream.get("torrent_name") or stream.get("title"))
            return False

        stream.pop("invalid_reason", None)
        return True
