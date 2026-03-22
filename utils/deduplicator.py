"""
utils/deduplicator.py
─────────────────────
Stateful deduplicator for a single get_streams() call.
One instance per request – never shared across requests.
"""

import logging

logger = logging.getLogger(__name__)


class StreamDeduplicator:
    __slots__ = ("_seen",)

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def is_valid(self, stream: dict) -> bool:
        """
        Returns True and registers the hash if this torrent is new.
        Sets stream['valid'] and 'invalid_reason' in-place.
        """
        infohash = stream.get("infohash") or stream.get("hash")

        if not infohash:
            stream["valid"] = False
            stream["invalid_reason"] = "Missing infohash"
            logger.debug("DEDUP  ✗ missing infohash – title=%s", stream.get("title"))
            return False

        key = str(infohash).lower()

        if key in self._seen:
            stream["valid"] = False
            stream["invalid_reason"] = "Duplicate infohash"
            logger.debug("DEDUP  ✗ duplicate – hash=%s", key)
            return False

        self._seen.add(key)
        stream["valid"] = True
        stream.pop("invalid_reason", None)
        return True