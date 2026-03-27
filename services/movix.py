"""
services/movix.py
─────────────────
Movix DDL source client.
Synchronous – designed to be called via asyncio.to_thread().

Flow:
  1. find_id(titles, tmdb_id, imdb_id)   → search, validate via tmdb/imdb id
  2. get_streams(movix_id, ...)           → fetch 1fichier links, normalize
  3. get_direct_link(stream_id)           → decode raw 1fichier URL

Stream dicts are pipeline-compatible.
infohash = "movix_{id}" – synthetic key for deduplicator, carries the id.
"""

import base64
import json
import logging

import requests

from settings import (
    MOVIX_API_BASE_URL,
    MOVIX_DECODE_BASE_URL,
    MOVIX_ORIGIN,
    MOVIX_REFERER,
    DDL_ALLOWED_HOSTS,
)

logger = logging.getLogger(__name__)

# ── Language name → ISO 639-1 ──────────────────────────────────────────────────
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

_API_HEADERS = {
    "accept":          "application/json",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer":         "https://darkiworld2026.com/",
    "user-agent":      (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

_DECODE_HEADERS = {
    "origin":  MOVIX_ORIGIN,
    "referer": MOVIX_REFERER,
}


def _normalize_lang(name: str) -> str:
    return _LANG_MAP.get(name.lower(), name.lower())


def _episode_filter(episode: int) -> str:
    payload = [{"key": "episode", "value": str(episode), "operator": "="}]
    return base64.b64encode(json.dumps(payload).encode()).decode()


class MovixClient:
    __slots__ = ("session", "_api_base", "_decode_base")

    def __init__(self, base_url: str = "") -> None:
        self.session     = requests.Session()
        self.session.headers.update(_API_HEADERS)
        # Allow custom base URL from user config
        self._api_base   = base_url.rstrip("/") if base_url else MOVIX_API_BASE_URL
        self._decode_base = MOVIX_DECODE_BASE_URL

    # ─────────────────────────────────────────────────────────────────────────
    # 1. ID resolution
    # ─────────────────────────────────────────────────────────────────────────

    def find_id(
        self,
        titles: list[str],
        tmdb_id: str | None = None,
        imdb_id: str | None = None,
    ) -> int | None:
        """
        Search Movix for each title in order.
        Validates by tmdb_id OR imdb_id (at least one must match).
        Stops at first valid match.
        """
        for title in titles:
            movix_id = self._search_title(title, tmdb_id=tmdb_id, imdb_id=imdb_id)
            if movix_id is not None:
                logger.info("Movix │ found id=%d for title=%r", movix_id, title)
                return movix_id

        logger.info("Movix │ no ID found for titles=%s", titles)
        return None

    def _search_title(
        self,
        title: str,
        tmdb_id: str | None,
        imdb_id: str | None,
    ) -> int | None:
        query = title.replace(" ", "+")
        try:
            r = self.session.get(f"https://app.darkiworld2026.com/api/v1/titles?query={query}", timeout=8)
            r.raise_for_status()
            results = r.json().get('pagination').get("data") or []
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

    def get_streams(
        self,
        movix_id: int,
        media_title: str,
        is_serie: bool = False,
        season: int | None = None,
        episode: int | None = None,
    ) -> list[dict]:
        """
        Returns pipeline-compatible stream dicts for all 1fichier links.
        Series: always scoped to a specific episode (no complete seasons).
        """
        params: dict = {
            "perPage":  "100",
            "title_id": str(movix_id),
            "loader":   "linksdl",
            "season":   str(season) if season else "1",
            "filters":  "",
            "paginate": "preferLengthAware",
        }

        if is_serie and episode is not None:
            params["filters"] = _episode_filter(episode)

        try:
            r = self.session.get(f"{self._api_base}/liens", params=params, timeout=8)
            r.raise_for_status()
            raw_streams = r.json().get("pagination", {}).get("data") or []
        except Exception as exc:
            logger.warning("Movix │ get_streams error id=%d: %s", movix_id, exc)
            return []

        streams: list[dict] = []
        for raw in raw_streams:
            host = raw.get("host", {})
            host_name = (host.get("name") if isinstance(host, dict) else host) or ""
            if host_name.lower() not in DDL_ALLOWED_HOSTS:
                continue

            stream = self._normalize(raw, media_title, is_serie, season)
            if stream:
                streams.append(stream)

        logger.info(
            "Movix │ %d 1fichier stream(s)  movix_id=%d  s=%s e=%s",
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

        # ── Languages (pre-set; PTT fallback since API uses proper lang names) ─
        lang_objects = raw.get("langues_compact") or []
        lang_names   = (
            [l.get("name", "") for l in lang_objects]
            if lang_objects
            else raw.get("languages") or []
        )
        languages = list({_normalize_lang(n) for n in lang_names if n})

        # ── Quality string for torrent_name (PTT will parse it) ───────────────
        quality_raw = (
            (raw.get("qual") or {}).get("qual")
            or raw.get("quality")
            or raw.get("qualite")
            or ""
        )

        # ── Size (raw bytes from the API) ──────────────────────────────────────
        try:
            size_bytes = int(float(raw.get("taille", 0)))
        except (TypeError, ValueError):
            size_bytes = 0

        # ── Series metadata ────────────────────────────────────────────────────
        raw_season  = raw.get("saison")
        raw_episode = raw.get("episode")
        seasons  = [int(raw_season)]  if raw_season  else ([season] if season else [])
        episodes = [int(raw_episode)] if raw_episode else []

        return {
            # torrent_name includes quality string so PTT can parse resolution/quality
            "torrent_name": f"{media_title} {quality_raw}".strip() if quality_raw else media_title,
            "infohash":    f"movix_{stream_id}",
            "source":      "Movix",
            "stream_type": "ddl",
            "languages":   languages,
            "seasons":     seasons,
            "episodes":    episodes,
            "complete":    False,
            "size":        size_bytes,
            "cached":      True,
            "seeders":     0,
            "valid":       False,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Link decoding
    # ─────────────────────────────────────────────────────────────────────────

    def get_direct_link(self, stream_id: int) -> str | None:
        """
        Decode a Movix stream id → raw 1fichier URL.
        Pass the result to AllDebridClient.unlock_link() to get a streamable URL.
        """
        try:
            r = requests.get(
                f"{MOVIX_DECODE_BASE_URL}/{stream_id}",
                headers=_DECODE_HEADERS,
                timeout=10,
            )
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
        self.session.close()