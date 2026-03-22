"""
utils/deduplicator.py
─────────────────────
Two-level deduplication per request:

  Level 1 – infohash (exact match, lowercased)
  Level 2 – (normalized_title, size_bytes)
             Catches the same torrent indexed by multiple sources
             with different or missing hashes.

'normalized_title' is built from BOTH the raw torrent name AND the
PTT-parsed title (whichever is available), both fully cleaned:
  NFKD → ASCII → lowercase → strip punctuation → collapse spaces
This matches how filtering.py builds its comparison keys.
"""

import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

_PUNCT_RE  = re.compile(r"[^a-z0-9\s]")
_SPACES_RE = re.compile(r"\s+")


def _clean(text: str) -> str:
    t = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    t = _PUNCT_RE.sub(" ", t)
    return _SPACES_RE.sub(" ", t).strip()


class StreamDeduplicator:
    __slots__ = ("_seen_hashes", "_seen_content")

    def __init__(self) -> None:
        self._seen_hashes:  set[str]   = set()   # level 1: infohash
        self._seen_content: set[tuple] = set()   # level 2: (title_key, size)

    def is_valid(self, stream: dict) -> bool:
        """
        Returns True if the stream is new (registers it).
        Sets stream['valid'] and 'invalid_reason' in-place on rejection.
        """
        infohash = stream.get("infohash") or stream.get("hash")

        # ── Level 1: infohash ─────────────────────────────────────────────────
        if infohash:
            h_key = str(infohash).lower()
            if h_key in self._seen_hashes:
                stream["valid"] = False
                stream["invalid_reason"] = "Duplicate infohash"
                logger.debug("DEDUP L1 ✗ hash=%s", h_key[:16])
                return False
        else:
            stream["valid"] = False
            stream["invalid_reason"] = "Missing infohash"
            logger.debug("DEDUP ✗ missing infohash title=%s", stream.get("title"))
            return False

        # ── Level 2: (normalized_title, size) ────────────────────────────────
        # Build the most informative title string available.
        # We use torrent_name if set (raw), else title field.
        raw_name    = stream.get("torrent_name") or stream.get("title") or ""
        parsed_name = stream.get("title") or ""

        # Clean both and pick the longer (more specific) result
        clean_raw    = _clean(raw_name)
        clean_parsed = _clean(parsed_name)
        title_key    = clean_raw if len(clean_raw) >= len(clean_parsed) else clean_parsed

        size = stream.get("size") or 0
        try:
            size = int(size)
        except (ValueError, TypeError):
            size = 0

        content_key = (title_key, size)

        if title_key and content_key in self._seen_content:
            stream["valid"] = False
            stream["invalid_reason"] = "Duplicate content"
            logger.debug("DEDUP L2 ✗ title=%s size=%d", title_key[:40], size)
            return False

        # ── Accept ────────────────────────────────────────────────────────────
        self._seen_hashes.add(h_key)
        if title_key:
            self._seen_content.add(content_key)

        stream["valid"] = True
        stream.pop("invalid_reason", None)
        return True