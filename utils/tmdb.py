"""
utils/tmdb.py
─────────────
TMDB /find wrapper. Handles movies and TV series.
Synchronous – call via asyncio.to_thread().

Module-level cache: IMDB IDs are immutable → cache forever (bounded at 500 entries).
"""

import logging

import requests

from settings import TMDB_BASE_URL, TMDB_DEFAULT_KEY

logger = logging.getLogger(__name__)

# Shared across all StreamManager instances / requests
_cache: dict[str, dict] = {}
_CACHE_MAX = 500


class TMDBApi:
    def __init__(self, apikey: str | None = None) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {apikey or TMDB_DEFAULT_KEY}",
            "accept": "application/json",
        })

    def fetch_media_info(self, imdb_id: str) -> dict:
        """
        Returns:
            {"titles": [...], "imdb_id": ..., "tmdb_id": ..., "type": "movie"|"series", "year": int}
        Raises ValueError if no TMDB match.
        """
        if imdb_id in _cache:
            logger.info("TMDB │ cache HIT %s", imdb_id)
            return _cache[imdb_id]

        try:
            r = self.session.get(
                f"{TMDB_BASE_URL}/find/{imdb_id}",
                params={"external_source": "imdb_id", "language": "fr-FR"},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            logger.error("TMDB │ request failed for %s: %s", imdb_id, exc)
            raise

        if data.get("movie_results"):
            item       = data["movie_results"][0]
            title_fr   = item.get("title") or ""
            title_orig = item.get("original_title") or ""
            date_raw   = item.get("release_date", "")
            media_type = "movie"
        elif data.get("tv_results"):
            item       = data["tv_results"][0]
            title_fr   = item.get("name") or ""
            title_orig = item.get("original_name") or ""
            date_raw   = item.get("first_air_date", "")
            media_type = "series"
        else:
            raise ValueError(f"No TMDB result for {imdb_id!r}")

        titles: list[str] = [title_fr] if title_fr else []
        if title_orig and title_orig != title_fr:
            titles.append(title_orig)

        year: int | None = None
        try:
            year = int(date_raw.split("-")[0]) if date_raw else None
        except (ValueError, IndexError):
            pass

        result = {
            "titles":  titles,
            "imdb_id": imdb_id,
            "tmdb_id": str(item.get("id", "")),
            "type":    media_type,
            "year":    year,
        }
        logger.info("TMDB │ %s → type=%s  year=%s  titles=%s",
                    imdb_id, media_type, year, titles)

        if len(_cache) >= _CACHE_MAX:
            _cache.pop(next(iter(_cache)))
        _cache[imdb_id] = result
        return result

