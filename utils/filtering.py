"""
utils/filtering.py
──────────────────
Parses, enriches and validates raw stream dicts against TMDB metadata.

Called AFTER deduplication so PTT is never invoked on duplicate streams.

All streams (torrents and DDL) go through the same validation path:
  PTT parse, multi/vostfr detection, title fuzzy match, year/season/episode.
  Movie: reject if PTT found seasons (TV series). complete=True OK.
  Series: complete=True (even with empty seasons) = full-series pack, accept.

DDL sources (Movix, Wawacity) pre-set: languages, year, seasons, episodes.
These survive the PTT merge — PTT won't overwrite non-empty lists with empty ones.

Title matching strategy (for cases where PTT stops early, e.g. "Mike & Nick & Alice"):
  1. PTT-parsed title (stream["title"] after parsing)
  2. Title portion extracted from torrent_name (text before the year)
  The torrent_name extraction handles titles that contain words PTT confuses with
  network names or other release tags (e.g. "Nick" → Nickelodeon).
  Both candidates are tried; the highest score wins.
  & is replaced with "and" in both TMDB titles and torrent names before matching.

At the end of validation: if quality is still None → default to "WEB-DL".

target_languages: list of "fr", "multi", "vostfr" – stream passes if ANY match.
"""

import logging
import re
import unicodedata

from PTT import parse_title
from rapidfuzz import fuzz

from settings import FRENCH_MULTI_SOURCES

logger = logging.getLogger(__name__)

_LANG_ALIASES: dict[str, str] = {
    "TRUEFRENCH": "vff",
    "truefrench": "vff",
    "TrueFrench": "vff",
    "french":     "vff",
    "French":     "vff",
    "FRENCH":     "vff",
    "VOF":        "vff",
}

_PUNCT_RE   = re.compile(r"[^a-z0-9\s]")
_SPACES_RE  = re.compile(r"\s+")
_YEAR_RE    = re.compile(r"\b(?:19|20)\d{2}\b")
_AMP_RE     = re.compile(r"\s*&\s*")

_NON_TV_EXT: frozenset[str] = frozenset({
    ".iso", ".vob", ".bdmv",
    ".rar", ".zip", ".7z", ".nzb",
    ".exe", ".bat",
})

# ── Resolution inference from tags (last-resort when PTT finds nothing) ────────
_RES_TAGS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'\bULTRA[.\s-]?HD\b', re.IGNORECASE), "2160p"),
    (re.compile(r'\bUHD\b',            re.IGNORECASE), "2160p"),
    (re.compile(r'\b4K\b',             re.IGNORECASE), "2160p"),
    (re.compile(r'\bFull[.\s-]?HD\b',  re.IGNORECASE), "1080p"),
    (re.compile(r'\bFHD\b',            re.IGNORECASE), "1080p"),
]


def _clean(text: str) -> str:
    """NFKD → ASCII → lowercase → strip punctuation → collapse spaces."""
    t = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii")
    t = t.lower()
    t = _PUNCT_RE.sub(" ", t)
    return _SPACES_RE.sub(" ", t).strip()


def _extract_torrent_title(torrent_name: str) -> str:
    """
    Extract the title portion from a torrent filename (text before the year).
    Replaces dots/underscores with spaces and & with 'and'.
    Returns empty string if no year found.
    """
    if not torrent_name:
        return ""
    tn = torrent_name.replace(".", " ").replace("_", " ")
    tn = _AMP_RE.sub(" and ", tn)
    m = _YEAR_RE.search(tn)
    return _clean(tn[:m.start()]) if m else _clean(tn)


class StreamFilter:
    __slots__ = (
        "media_type", "tmdb_year", "tmdb_year_min", "tmdb_year_max", "min_match",
        "target_season", "target_episode", "target_languages",
        "remove_trash", "remove_non_tv", "_tmdb_titles",
        "allowed_resolutions",
    )

    def __init__(
        self,
        tmdb_info:           dict,
        min_match:           float       = 85.0,
        target_season:       int | None  = None,
        target_episode:      int | None  = None,
        target_languages:    list[str] | None = None,
        remove_trash:        bool        = True,
        remove_non_tv:       bool        = True,
        allowed_resolutions: list[str] | None = None,
    ) -> None:
        self.media_type       = tmdb_info.get("type")
        self.tmdb_year        = tmdb_info.get("year")
        self.tmdb_year_min    = tmdb_info.get("year_min")
        self.tmdb_year_max    = tmdb_info.get("year_max")
        self.min_match        = min_match
        self.target_season    = target_season
        self.target_episode   = target_episode
        self.target_languages = [l.lower() for l in (target_languages or [])]
        self.remove_trash     = remove_trash
        self.remove_non_tv    = remove_non_tv
        # Replace & with "and" so "Mike & Nick" matches "Mike and Nick" in torrents
        self._tmdb_titles: list[str] = [
            _clean(_AMP_RE.sub(" and ", t))
            for t in tmdb_info.get("titles", []) if t
        ]
        self.allowed_resolutions: frozenset[str] = frozenset(
            r.lower() for r in (allowed_resolutions or [])
        )

    # ─────────────────────────────────────────────────────────────────────────

    def is_valid(self, stream: dict) -> bool:
        return self._validate(stream)

    def _lang_matches(self, lang_set: set[str], stream: dict) -> bool:
        if not self.target_languages:
            return True
        for tl in self.target_languages:
            if tl == "vostfr":
                if "vostfr" in lang_set:
                    return True
                subs = stream.get("subtitles") or []
                if "fr" in {str(s).lower() for s in subs}:
                    return True
            elif tl == "fr":
                if lang_set & {"fr", "vff", "vfq"}:
                    return True
            elif tl in lang_set:
                return True
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Unified validation path (torrents + DDL)
    # ─────────────────────────────────────────────────────────────────────────

    def _validate(self, stream: dict) -> bool:
        # Pre-set fields from DDL sources survive PTT merge
        saved_languages = stream.get("languages") or []
        saved_seasons   = stream.get("seasons") or []
        saved_episodes  = stream.get("episodes") or []

        # Use torrent_name for PTT input (more complete than an already-parsed title).
        # torrent_name is the raw filename from the indexer; title may have been truncated.
        raw_title    = stream.get("title") or stream.get("torrent_name", "")
        torrent_name = stream.get("torrent_name") or raw_title

        # Build PTT input: prefer torrent_name, replace & → and, apply language aliases
        mod_ptt = _AMP_RE.sub(" and ", torrent_name)
        for src, dst in _LANG_ALIASES.items():
            if src in mod_ptt:
                mod_ptt = mod_ptt.replace(src, dst)

        parsed = parse_title(mod_ptt, translate_languages=False)

        # Merge PTT results – don't overwrite with empty values
        for k, v in parsed.items():
            if v is not None and v != [] and v != {}:
                stream[k] = v

        # Normalize quality: "WEB" → "WEB-DL" (no distinction)
        if (stream.get("quality") or "").lower() == "web":
            stream["quality"] = "WEB-DL"

        # Restore pre-set seasons/episodes if PTT wiped them (DDL sources)
        if saved_seasons and not stream.get("seasons"):
            stream["seasons"] = saved_seasons
        if saved_episodes and not stream.get("episodes"):
            stream["episodes"] = saved_episodes

        # Preserve original torrent_name (PTT must not overwrite it)
        if torrent_name:
            stream["torrent_name"] = torrent_name
        if not stream.get("stream_type"):
            stream["stream_type"] = "torrent"

        # ── Enrich: Multi / VOSTFR detection ─────────────────────────────────
        name_lower = (torrent_name or raw_title).lower()
        langs      = list(stream.get("languages") or saved_languages or [])
        lang_lower = {l.lower() for l in langs}

        if "multi" in name_lower and "multi" not in lang_lower:
            langs.append("multi")
            source    = stream.get("source", "")
            has_french = source in FRENCH_MULTI_SOURCES or not {"fr", "vff", "vfq"}.isdisjoint(lang_lower)
            if has_french and "fr" not in lang_lower:
                langs.append("fr")
            stream["languages"] = langs
            lang_lower = {l.lower() for l in langs}

        if any(x in name_lower for x in ("vostfr", "vf+")) and "vostfr" not in lang_lower:
            if "multi" not in lang_lower:
                langs = [l for l in langs if l.lower() not in ("fr", "vff")]
            langs.append("vostfr")
            stream["languages"] = langs
            lang_lower = {l.lower() for l in langs}

        if not lang_lower and saved_languages:
            stream["languages"] = saved_languages
            lang_lower = {l.lower() for l in saved_languages}

        # ── 1. Trash ─────────────────────────────────────────────────────────
        if self.remove_trash and stream.get("trash"):
            stream["invalid_reason"] = "Trash"
            return False

        # ── 2. Non-TV format ─────────────────────────────────────────────────
        if self.remove_non_tv:
            dot = name_lower.rfind(".")
            if dot != -1 and name_lower[dot:] in _NON_TV_EXT:
                stream["invalid_reason"] = "NonTV"
                return False

        # ── 3. Language ──────────────────────────────────────────────────────
        if self.target_languages and not self._lang_matches(lang_lower, stream):
            stream["invalid_reason"] = "Language"
            return False

        # ── 4a. Movie ────────────────────────────────────────────────────────
        if self.media_type == "movie":
            if stream.get("seasons"):
                stream["invalid_reason"] = "Series"
                return False
            t_year = stream.get("year")
            if self.tmdb_year and t_year:
                try:
                    if abs(int(t_year) - int(self.tmdb_year)) > 1:
                        stream["invalid_reason"] = "Year"
                        return False
                except ValueError:
                    pass

        # ── 4b. Series ───────────────────────────────────────────────────────
        elif self.media_type == "series":
            if self.target_season:
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
            # Year range filter: stream year must fall within series air date range ±1
            if self.tmdb_year_min and stream.get("year"):
                try:
                    sy = int(stream["year"])
                    y_min = self.tmdb_year_min - 1
                    y_max = (self.tmdb_year_max or self.tmdb_year_min) + 1
                    if not (y_min <= sy <= y_max):
                        stream["invalid_reason"] = "Year"
                        return False
                except (ValueError, TypeError):
                    pass

        # ── 5. Title fuzzy match ─────────────────────────────────────────────
        # Candidate 1: PTT-parsed title (stream["title"] after PTT merge)
        # Candidate 2: Title extracted from torrent_name before the year
        #   This handles cases where PTT stops early (e.g. "Nick" parsed as Nickelodeon
        #   network → title becomes "Mike and" instead of "Mike and Nick and Nick and Alice")
        parsed_title = _clean(stream.get("title", ""))
        torrent_title = _extract_torrent_title(torrent_name)

        candidates = [c for c in (parsed_title, torrent_title) if c]
        if not candidates:
            candidates = [_clean(raw_title)]

        best: float = 0.0
        for tmdb_title in self._tmdb_titles:
            for candidate in candidates:
                score = fuzz.token_sort_ratio(candidate, tmdb_title)
                if score > best:
                    best = score
                    if best == 100.0:
                        break
            if best == 100.0:
                break

        if best < self.min_match:
            stream["invalid_reason"] = f"Title:{best:.0f}%"
            return False

        # ── 6. Size formatting ───────────────────────────────────────────────
        try:
            b  = int(stream.get("size") or 0)
            gb = b / (1 << 30)
            stream["size_fmt"] = f"{gb:.2f} GB" if gb >= 1 else f"{b >> 20} MB"
        except (ValueError, TypeError):
            stream["size_fmt"] = ""

        # ── 7. Resolution fallback (PTT found nothing) ───────────────────────
        if not stream.get("resolution"):
            for pattern, res in _RES_TAGS:
                if pattern.search(torrent_name or raw_title):
                    stream["resolution"] = res
                    break

        # ── 8. Resolution filter ─────────────────────────────────────────────
        if self.allowed_resolutions:
            raw_res = (stream.get("resolution") or "").lower()
            res_key = "2160p" if raw_res == "4k" else (raw_res or "?")
            if res_key not in self.allowed_resolutions:
                stream["invalid_reason"] = "Resolution"
                return False

        # ── 9. Default quality fallback ──────────────────────────────────────
        if not stream.get("quality"):
            stream["quality"] = "WEB-DL"

        stream.pop("invalid_reason", None)
        return True
