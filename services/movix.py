"""
services/movix.py
─────────────────
Movix DDL source. Asynchronous.

Flow:
  1. find_id(titles, tmdb_id, imdb_id)   → parallel title search, validate by ID
  2. get_streams(movix_id, ...)           → fetch 1fichier links, normalize
  3. get_direct_link(stream_id)           → decode raw 1fichier URL

infohash = "" for all Movix streams; ddl_id carries the stream_id.
ID cache (module-level, 500 entries) avoids re-searching stable IDs.
"""

import asyncio
import base64
import json
import logging
from urllib.parse import quote_plus

import httpx

from settings import (
    MOVIX_DECODE_BASE_URL,
    MOVIX_ORIGIN,
    DARKIWORLD_API_BASE_URL,
    DDL_ALLOWED_HOSTS,
)

logger = logging.getLogger(__name__)

_ID_CACHE: dict[tuple[str, str], int | None] = {}
_ID_CACHE_MAX = 500

_LANG_MAP: dict[str, str] = {
    "french":           "fr",
    "french (canada)":  "fr",
    "truefrench":       "fr",
    "true french":      "fr",
    "français":         "fr",
    "vf":               "fr",
    "vff":              "fr",
    "vfq":              "fr",
    "english":          "en",
    "anglais":          "en",
    "spanish":          "es",
    "espagnol":         "es",
    "german":           "de",
    "allemand":         "de",
    "italian":          "it",
    "italien":          "it",
    "portuguese":       "pt",
    "portugais":        "pt",
    "japanese":         "ja",
    "korean":           "ko",
    "arabic":           "ar",
    "arab":             "ar",
    "arabe":            "ar",
}

_DARKIWORLD_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

_DECODE_HEADERS = {
    "origin":  MOVIX_ORIGIN,
    "referer": MOVIX_ORIGIN,
}


def _normalize_lang(name: str) -> str:
    return _LANG_MAP.get(name.lower(), name.lower())


def _episode_filter(episode: int) -> str:
    payload = [{"key": "episode", "value": str(episode), "operator": "="}]
    return base64.b64encode(json.dumps(payload).encode()).decode()


class MovixClient:
    __slots__ = ("_client_dw", "_client_dec")

    def __init__(self, base_url: str = "") -> None:
        self._client_dw = httpx.AsyncClient(
            headers=_DARKIWORLD_HEADERS,
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4, keepalive_expiry=30.0),
            timeout=8,
        )
        self._client_dec = httpx.AsyncClient(
            headers=_DECODE_HEADERS,
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2, keepalive_expiry=30.0),
            timeout=10,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 1. ID resolution
    # ─────────────────────────────────────────────────────────────────────────

    async def find_id(
        self,
        titles: list[str],
        tmdb_id: str | None = None,
        imdb_id: str | None = None,
    ) -> int | None:
        cache_key = (str(tmdb_id or ""), str(imdb_id or ""))
        if cache_key in _ID_CACHE:
            cached = _ID_CACHE[cache_key]
            logger.debug("Movix │ cache HIT id=%s tmdb=%s imdb=%s", cached, tmdb_id, imdb_id)
            return cached

        unique = list(dict.fromkeys(t for t in titles if t))
        if not unique:
            return None

        result: int | None = None

        if len(unique) == 1:
            result = await self._search_title(unique[0], tmdb_id, imdb_id)
            logger.info("Movix │ %s id=%s for %r", "found" if result else "no result", result, unique[0])
        else:
            # All titles in parallel – return first hit, cancel the rest
            tasks = [
                asyncio.create_task(self._search_title(t, tmdb_id, imdb_id))
                for t in unique
            ]
            for coro in asyncio.as_completed(tasks):
                try:
                    r = await coro
                    if r is not None and result is None:
                        result = r
                        logger.info("Movix │ found id=%d", r)
                except Exception:
                    pass
                if result is not None:
                    break
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if result is None:
                logger.info("Movix │ no ID found for titles=%s", unique)

        if len(_ID_CACHE) < _ID_CACHE_MAX:
            _ID_CACHE[cache_key] = result
        return result

    async def _search_title(
        self,
        title: str,
        tmdb_id: str | None,
        imdb_id: str | None,
    ) -> int | None:
        try:
            r = await self._client_dw.get(
                f"{DARKIWORLD_API_BASE_URL}/search/{quote_plus(title)}",
            )
            r.raise_for_status()
            results = r.json().get("results") or []
        except Exception as exc:
            logger.warning("Movix │ search error q=%r: %s", title, exc)
            return None

        for result in results:
            r_tmdb = str(result.get("tmdb_id") or "")
            r_imdb = str(result.get("imdb_id") or "")
            if (tmdb_id and r_tmdb == str(tmdb_id)) or (imdb_id and r_imdb == str(imdb_id)):
                return result.get("id")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Stream listing
    # ─────────────────────────────────────────────────────────────────────────

    async def get_streams(
        self,
        movix_id: int,
        media_title: str,
        is_serie: bool = False,
        season: int | None = None,
        episode: int | None = None,
    ) -> list[dict]:
        params: dict = {
            "perPage":  "20",
            "title_id": str(movix_id),
            "loader":   "linksdl",
            "season":   str(season) if season else "1",
            "filters":  "",
            "paginate": "preferLengthAware",
        }
        if is_serie and episode is not None:
            params["filters"] = _episode_filter(episode)

        for attempt in range(2):
            try:
                r = await self._client_dw.get(f"{DARKIWORLD_API_BASE_URL}/liens", params=params)
                if r.status_code == 401 and attempt == 0:
                    logger.warning("Movix │ 401 on get_streams id=%d, retrying…", movix_id)
                    continue
                r.raise_for_status()
                raw_streams = r.json().get("pagination", {}).get("data") or []
                break
            except Exception as exc:
                logger.warning("Movix │ get_streams error id=%d: %s", movix_id, exc)
                return []
        else:
            return []

        streams: list[dict] = []
        for raw in raw_streams:
            host      = raw.get("host", {})
            host_name = (host.get("name") if isinstance(host, dict) else host) or ""
            if host_name.lower() not in DDL_ALLOWED_HOSTS:
                continue
            stream = self._normalize(raw, media_title, is_serie, season)
            if stream:
                streams.append(stream)

        logger.info(
            "Movix │ %d stream(s)  movix_id=%d  s=%s e=%s",
            len(streams), movix_id, season, episode,
        )
        return streams

    def _normalize(
        self,
        raw: dict,
        media_title: str,
        is_serie: bool,
        season: int | None,
    ) -> dict | None:
        stream_id = raw.get("id")
        if not stream_id:
            return None

        lang_objects = raw.get("langues_compact") or []
        lang_names   = (
            [l.get("name", "") for l in lang_objects]
            if lang_objects
            else raw.get("languages") or []
        )
        languages = list({_normalize_lang(n) for n in lang_names if n})

        quality_raw = (
            (raw.get("qual") or {}).get("qual")
            or raw.get("quality")
            or raw.get("qualite")
            or ""
        )

        try:
            size_bytes = int(float(raw.get("taille", 0)))
        except (TypeError, ValueError):
            size_bytes = 0

        raw_season  = raw.get("saison")
        raw_episode = raw.get("episode")
        seasons  = [int(raw_season)]  if raw_season  else ([season] if season else [])
        episodes = [int(raw_episode)] if raw_episode else []

        return {
            "torrent_name": f"{media_title} {quality_raw}".strip() if quality_raw else media_title,
            "infohash":     "",
            "ddl_id":       stream_id,
            "source":       "Movix",
            "stream_type":  "ddl",
            "languages":    languages,
            "seasons":      seasons,
            "episodes":     episodes,
            "complete":     False,
            "size":         size_bytes,
            "cached":       True,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Link decoding
    # ─────────────────────────────────────────────────────────────────────────

    async def get_direct_link(self, stream_id: int) -> str | None:
        try:
            r = await self._client_dec.get(f"{MOVIX_DECODE_BASE_URL}/{stream_id}")
            r.raise_for_status()
            body = r.json()
        except Exception as exc:
            logger.error("Movix │ decode error stream_id=%d: %s", stream_id, exc)
            return None

        if not body.get("success"):
            logger.warning("Movix │ decode failed stream_id=%d: %s", stream_id, body)
            return None

        link = (body.get("embed_url") or {}).get("lien")
        if not link:
            logger.warning("Movix │ no 'lien' in response stream_id=%d", stream_id)
            return None

        logger.info("Movix │ decoded stream_id=%d → %s…", stream_id, link[:60])
        return link

    def close(self) -> None:
        pass
