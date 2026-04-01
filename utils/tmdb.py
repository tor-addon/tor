"""
utils/tmdb.py
─────────────
TMDB wrapper. Asynchronous.

Two requests per lookup (results cached forever, bounded at 500):
  1. /find/{imdb_id}?external_source=imdb_id   → tmdb_id + type + original title
  2. /movie/{id}?append_to_response=translations
     or /tv/{id}?...  → EN + user-lang titles, series dates

Returns:
  {
    "titles":   [str, ...],   # original first, then EN + user-lang (case-insensitive dedup)
    "imdb_id":  str,
    "tmdb_id":  str,
    "type":     "movie"|"series",
    "year":     int|None,     # release year (movie) or first_air_year (series)
    "year_min": int|None,     # series: first_air_year
    "year_max": int|None,     # series: last_air_year (None if ongoing)
  }
"""

import logging
import re
import unicodedata

import httpx

from settings import TMDB_BASE_URL, TMDB_DEFAULT_KEY

logger = logging.getLogger(__name__)

_cache: dict[str, dict] = {}
_CACHE_MAX = 500

_RE_SPECIAL = re.compile(r"[:\-&]")


def _title_key(t: str) -> str:
    """Normalize title for deduplication: remove accents + special chars, lowercase."""
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return _RE_SPECIAL.sub("", t).lower()


_LANG_TO_ISO: dict[str, str] = {
    "fr": "fr", "en": "en", "de": "de", "es": "es",
    "it": "it", "pt": "pt", "ar": "ar", "ja": "ja",
    "ko": "ko", "zh": "zh",
}


class TMDBApi:
    def __init__(self, apikey: str | None = None) -> None:
        self._client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {apikey or TMDB_DEFAULT_KEY}",
                "Accept": "application/json",
            },
            timeout=10,
        )

    async def fetch_media_info(self, imdb_id: str, languages: list[str] | None = None) -> dict:
        if imdb_id in _cache:
            logger.info("TMDB │ cache HIT %s", imdb_id)
            return _cache[imdb_id]

        # ── Step 1: /find → tmdb_id + type + original title ──────────────────
        r = await self._client.get(
            f"{TMDB_BASE_URL}/find/{imdb_id}",
            params={"external_source": "imdb_id"},
        )
        r.raise_for_status()
        data = r.json()

        if data.get("movie_results"):
            item       = data["movie_results"][0]
            tmdb_id    = item["id"]
            media_type = "movie"
            title_orig = item.get("original_title") or ""
            date_raw   = item.get("release_date", "")
            year_min = year_max = None
        elif data.get("tv_results"):
            item       = data["tv_results"][0]
            tmdb_id    = item["id"]
            media_type = "series"
            title_orig = item.get("original_name") or ""
            date_raw   = item.get("first_air_date", "")
            year_min = year_max = None
        else:
            raise ValueError(f"No TMDB result for {imdb_id!r}")

        year: int | None = None
        try:
            year = int(date_raw.split("-")[0]) if date_raw else None
        except (ValueError, IndexError):
            pass
        if media_type == "series":
            year_min = year

        # ── Step 2: translations + series dates ───────────────────────────────
        extra_titles: list[str] = []
        try:
            endpoint = "movie" if media_type == "movie" else "tv"
            r2 = await self._client.get(
                f"{TMDB_BASE_URL}/{endpoint}/{tmdb_id}",
                params={"append_to_response": "translations"},
            )
            r2.raise_for_status()
            detail = r2.json()

            if media_type == "series":
                last_raw = detail.get("last_air_date") or ""
                try:
                    year_max = int(last_raw.split("-")[0]) if last_raw else None
                except (ValueError, IndexError):
                    year_max = None
                status = (detail.get("status") or "").lower()
                if status not in ("ended", "cancelled", "canceled"):
                    year_max = None

            title_key   = "name" if media_type == "series" else "title"
            target_isos = {"en"} | {_LANG_TO_ISO[lg] for lg in (languages or []) if lg in _LANG_TO_ISO}
            for t in (detail.get("translations") or {}).get("translations", []):
                if t.get("iso_639_1") in target_isos:
                    t_val = (t.get("data") or {}).get(title_key) or ""
                    if t_val:
                        extra_titles.append(t_val)

        except Exception as exc:
            logger.warning("TMDB │ detail request failed for %s: %s", imdb_id, exc)

        # ── Deduplicate: original first, then EN + user-lang (case-insensitive)
        seen_keys: set[str] = set()
        titles: list[str] = []
        for t in [title_orig] + extra_titles:
            key = _title_key(t) if t else ""
            if t and key not in seen_keys:
                seen_keys.add(key)
                titles.append(t)

        result = {
            "titles":   titles,
            "imdb_id":  imdb_id,
            "tmdb_id":  str(tmdb_id),
            "type":     media_type,
            "year":     year,
            "year_min": year_min,
            "year_max": year_max,
        }
        logger.info(
            "TMDB │ %s → type=%s  year=%s..%s  titles=%s",
            imdb_id, media_type, year_min or year, year_max, titles,
        )

        if len(_cache) >= _CACHE_MAX:
            _cache.pop(next(iter(_cache)))
        _cache[imdb_id] = result
        return result

    def close(self) -> None:
        pass
