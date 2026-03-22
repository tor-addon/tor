"""
utils/filtering.py
──────────────────
Parses, enriches and validates raw stream dicts against TMDB metadata.

Two validation paths:
  - Torrent streams (stream_type == "torrent" or unset):
      PTT parsing, title fuzzy match, year/season/episode checks.
  - DDL streams (stream_type == "ddl", e.g. Movix):
      Skip PTT + title match (Movix ID was already validated against TMDB/IMDB).
      Only check language filter.
"""

import logging
import re
import unicodedata

from PTT import parse_title
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

_LANG_ALIASES: dict[str, str] = {
    "french": "vff",
    "French": "vff",
    "FRENCH": "vff",
    "VOF":    "vff",
}

_PUNCT_RE  = re.compile(r"[^a-z0-9\s]")
_SPACES_RE = re.compile(r"\s+")


def _clean(text: str) -> str:
    """NFKD → ASCII → lowercase → strip punctuation → collapse spaces."""
    t = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    t = _PUNCT_RE.sub(" ", t)
    return _SPACES_RE.sub(" ", t).strip()


class StreamFilter:
    __slots__ = (
        "media_type", "tmdb_year", "min_match",
        "target_season", "target_episode", "target_language",
        "vostfr", "remove_trash", "_tmdb_titles",
    )

    def __init__(
        self,
        tmdb_info: dict,
        min_match: float = 85.0,
        target_season: int | None = None,
        target_episode: int | None = None,
        target_language: str | None = None,
        vostfr: bool = False,
        remove_trash: bool = True,
    ) -> None:
        self.media_type      = tmdb_info.get("type")
        self.tmdb_year       = tmdb_info.get("year")
        self.min_match       = min_match
        self.target_season   = target_season
        self.target_episode  = target_episode
        self.target_language = target_language.lower() if target_language else None
        self.vostfr          = vostfr
        self.remove_trash    = remove_trash

        self._tmdb_titles: list[str] = [
            _clean(t) for t in tmdb_info.get("titles", []) if t
        ]

    # ─────────────────────────────────────────────────────────────────────────

    def is_valid(self, stream: dict) -> bool:
        """
        Validates and enriches a stream dict in-place.
        Returns True if valid, False otherwise (sets 'invalid_reason').
        """
        if stream.get("stream_type") == "ddl":
            return self._validate_ddl(stream)
        return self._validate_torrent(stream)

    # ─────────────────────────────────────────────────────────────────────────
    # DDL path (Movix) – structured data, skip PTT + title match
    # ─────────────────────────────────────────────────────────────────────────

    def _validate_ddl(self, stream: dict) -> bool:
        stream["valid"] = False

        # Language is the only meaningful filter – title/year already validated
        # at the Movix ID resolution step
        if self.target_language:
            langs = stream.get("languages") or []
            if self.target_language not in {str(l).lower() for l in langs}:
                stream["invalid_reason"] = "Language"
                return False

        stream["valid"] = True
        stream.pop("invalid_reason", None)
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Torrent path – full PTT parsing + all checks
    # ─────────────────────────────────────────────────────────────────────────

    def _validate_torrent(self, stream: dict) -> bool:
        stream["valid"] = False
        raw_title = stream.get("title") or stream.get("torrent_name", "")

        # Normalise French tokens before PTT
        mod_title = raw_title
        for src, dst in _LANG_ALIASES.items():
            if src in raw_title:
                mod_title = mod_title.replace(src, dst)

        parsed = parse_title(mod_title, translate_languages=False)
        stream.update(parsed)
        stream["torrent_name"] = raw_title
        stream["stream_type"]  = "torrent"

        # ── 1. Trash ─────────────────────────────────────────────────────────
        if self.remove_trash and stream.get("trash"):
            stream["invalid_reason"] = "Trash"
            return False

        # ── 2. Language ──────────────────────────────────────────────────────
        if self.target_language:
            langs: list = stream.get("languages") or []
            lang_set = {str(l).lower() for l in langs}
            has_target = self.target_language in lang_set

            # VOSTFR: accept if stream has FR subtitles even with foreign audio
            if not has_target and self.vostfr:
                subs = stream.get("subtitles") or []
                sub_set = {str(s).lower() for s in subs}
                has_target = self.target_language in sub_set

            if not has_target:
                stream["invalid_reason"] = "Language"
                return False

        # ── 3a. Movie – year ─────────────────────────────────────────────────
        if self.media_type == "movie":
            t_year = stream.get("year")
            if self.tmdb_year and t_year:
                try:
                    if abs(int(t_year) - int(self.tmdb_year)) > 1:
                        stream["invalid_reason"] = "Year"
                        return False
                except ValueError:
                    pass

        # ── 3b. Series – season / episode ────────────────────────────────────
        elif self.media_type == "series" and self.target_season:
            t_complete: bool = stream.get("complete", False)
            t_seasons:  list = stream.get("seasons", [])

            if not t_complete and self.target_season not in t_seasons:
                stream["invalid_reason"] = "Season"
                return False

            t_episodes: list = stream.get("episodes", [])
            if self.target_episode and t_episodes and not t_complete:
                if self.target_episode not in t_episodes:
                    stream["invalid_reason"] = "Episode"
                    return False

        # ── 4. Title fuzzy match (both PTT title and raw torrent name) ────────
        parsed_title  = _clean(stream.get("title", ""))
        torrent_clean = _clean(raw_title)
        best: float   = 0.0

        for tmdb_title in self._tmdb_titles:
            for candidate in (parsed_title, torrent_clean):
                score = fuzz.ratio(candidate, tmdb_title)
                if score > best:
                    best = score
                    if best == 100.0:
                        break
            if best == 100.0:
                break

        if best < self.min_match:
            stream["invalid_reason"] = f"Title:{best:.0f}%"
            return False

        # ── 5. Size formatting ───────────────────────────────────────────────
        try:
            b  = int(stream.get("size", 0))
            gb = b / (1 << 30)
            stream["size_fmt"] = f"{gb:.2f} GB" if gb >= 1 else f"{b / (1 << 20):.0f} MB"
        except (ValueError, TypeError):
            stream["size_fmt"] = str(stream.get("size", ""))

        stream["valid"] = True
        stream.pop("invalid_reason", None)
        return True