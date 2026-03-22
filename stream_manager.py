"""
stream_manager.py
─────────────────
Async orchestrator.

Pipeline:
  TMDB → [Torznab × N + Movix] (parallel) → Dedup → Filter → Rank
       → AllDebrid cache (torrents only) → Sort

resolve_stream(stream, season, episode, year):
  torrent → AllDebrid magnet
  ddl     → Movix decode → 1fichier → AllDebrid unlock
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional

from services.alldebrid import AllDebridClient
from services.movix import MovixClient
from services.torznab import Torznab
from utils.deduplicator import StreamDeduplicator
from utils.filtering import StreamFilter
from utils.tmdb import TMDBApi
from utils import ranking
from settings import DEFAULT_LANGUAGE, DEFAULT_MIN_MATCH, DEFAULT_SEARCH_TIMEOUT, ALLDEBRID_UID_COOKIE

logger = logging.getLogger(__name__)


class StreamManager:
    def __init__(
        self,
        alldebrid_api_key: str,
        torznab_sources: list[dict],
        tmdb_api_key: str | None = None,
        language: str = DEFAULT_LANGUAGE,
        min_match: float = DEFAULT_MIN_MATCH,
        search_timeout: float = DEFAULT_SEARCH_TIMEOUT,
        enable_movix: bool = True,
    ) -> None:
        self._language       = language
        self._min_match      = min_match
        self._search_timeout = search_timeout
        self._enable_movix   = enable_movix

        self._tmdb    = TMDBApi(tmdb_api_key)
        self._ad      = AllDebridClient(alldebrid_api_key, uid_cookie=ALLDEBRID_UID_COOKIE)
        self._movix   = MovixClient() if enable_movix else None
        self._sources = [
            Torznab(s["name"], s["url"], s.get("apikey"))
            for s in torznab_sources
        ]

        logger.info(
            "StreamManager │ ready – %d Torznab source(s)  movix=%s  lang=%s  min_match=%.0f",
            len(self._sources), enable_movix, language, min_match,
        )

    # ─────────────────────────────────────────────────────────────────────────

    async def get_streams(
        self,
        imdb_id: str,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> list[dict]:
        t0 = time.perf_counter()
        logger.info("━━ [%s] start  s=%s e=%s", imdb_id, season, episode)

        # 1. TMDB
        try:
            tmdb_info = await asyncio.to_thread(self._tmdb.fetch_media_info, imdb_id)
        except Exception as exc:
            logger.error("[%s] TMDB failed: %s", imdb_id, exc)
            return []

        is_serie = tmdb_info["type"] == "series"

        # 2. All sources in parallel
        torznab_task = asyncio.create_task(self._search_torznab(imdb_id, tmdb_info["titles"]))
        movix_task   = asyncio.create_task(
            self._search_movix(imdb_id, tmdb_info, is_serie, season, episode)
        ) if self._enable_movix else asyncio.create_task(_empty())

        torznab_results, movix_results = await asyncio.gather(torznab_task, movix_task)

        raw = torznab_results + movix_results
        logger.info(
            "[%s] raw: %d total  (torznab=%d  movix=%d)",
            imdb_id, len(raw), len(torznab_results), len(movix_results),
        )

        if not raw:
            return []

        # 3. Dedup + Filter + Rank – single pass
        dedup = StreamDeduplicator()
        filt  = StreamFilter(
            tmdb_info,
            min_match=self._min_match,
            target_season=season,
            target_episode=episode,
            target_language=self._language,
        )

        valid: list[dict] = []
        n_dedup = 0
        reject_buckets: dict[str, list[str]] = defaultdict(list)

        for stream in raw:
            if not dedup.is_valid(stream):
                n_dedup += 1
                continue
            if not filt.is_valid(stream):
                reason = stream.get("invalid_reason", "Unknown")
                detail = _reject_detail(stream, reason)
                key    = reason.split(":")[0]
                bucket = reject_buckets[key]
                if detail and detail not in bucket and len(bucket) < 3:
                    bucket.append(detail)
                continue
            ranking.rank(stream)
            valid.append(stream)

        n_filter = len(raw) - n_dedup - len(valid)

        if reject_buckets:
            parts = []
            for reason, examples in reject_buckets.items():
                count  = sum(1 for s in raw if (s.get("invalid_reason") or "").startswith(reason))
                ex_str = f" ({', '.join(examples)})" if examples else ""
                parts.append(f"{reason} ×{count}{ex_str}")
            logger.info("[%s] rejected: %s", imdb_id, " | ".join(parts))

        logger.info(
            "[%s] filter: %d valid  (dedup=%d filter=%d) / %d",
            imdb_id, len(valid), n_dedup, n_filter, len(raw),
        )

        if not valid:
            return []

        # 4. AllDebrid cache check (torrent only)
        await asyncio.to_thread(self._ad.check_cache, valid)

        cached = [s for s in valid if s.get("cached")]
        logger.info("[%s] cached: %d / %d", imdb_id, len(cached), len(valid))

        if not cached:
            return []

        # 5. Sort
        result = ranking.sort_streams(cached)
        ms = (time.perf_counter() - t0) * 1000
        logger.info("━━ [%s] done %.0f ms – %d stream(s)", imdb_id, ms, len(result))
        return result

    # ─────────────────────────────────────────────────────────────────────────

    async def resolve_stream(
        self,
        stream: dict,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        year: Optional[int] = None,
    ) -> str | None:
        if stream.get("stream_type") == "ddl":
            return await self._resolve_ddl(stream)
        return await self._resolve_torrent(stream, season, episode, year)

    async def _resolve_torrent(self, stream, season, episode, year) -> str | None:
        return await asyncio.to_thread(
            self._ad.resolve_stream,
            stream["infohash"], season, episode, year,
        )

    async def _resolve_ddl(self, stream: dict) -> str | None:
        if not self._movix:
            logger.error("StreamManager │ Movix disabled, cannot resolve DDL")
            return None

        # id is encoded in infohash: "movix_{id}"
        try:
            stream_id = int(stream["infohash"].split("_", 1)[1])
        except (KeyError, IndexError, ValueError) as exc:
            logger.error("StreamManager │ cannot parse movix id from infohash: %s", exc)
            return None

        raw_link = await asyncio.to_thread(self._movix.get_direct_link, stream_id)
        if not raw_link:
            return None

        return await asyncio.to_thread(self._ad.unlock_link, raw_link)

    # ─────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._tmdb.close()
        self._ad.close()
        if self._movix:
            self._movix.close()
        for s in self._sources:
            s.close()
        logger.info("StreamManager │ closed")

    # ─────────────────────────────────────────────────────────────────────────

    async def _search_torznab(self, imdb_id: str, titles: list[str]) -> list[dict]:
        unique = list(dict.fromkeys(titles))

        async def _one(source: Torznab, title: str) -> list[dict]:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(source.search, title),
                    timeout=self._search_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning("Torznab │ [%s] TIMEOUT q=%r", source.name, title)
                return []
            except Exception as exc:
                logger.warning("Torznab │ [%s] ERROR q=%r → %s", source.name, title, exc)
                return []

        batches = await asyncio.gather(*[_one(s, t) for s in self._sources for t in unique])
        out: list[dict] = []
        for b in batches:
            out.extend(b)
        return out

    async def _search_movix(self, imdb_id, tmdb_info, is_serie, season, episode) -> list[dict]:
        if not self._movix:
            return []
        try:
            movix_id = await asyncio.wait_for(
                asyncio.to_thread(
                    self._movix.find_id,
                    tmdb_info["titles"],
                    tmdb_id=tmdb_info.get("tmdb_id"),
                    imdb_id=imdb_id,
                ),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Movix │ find_id TIMEOUT [%s]", imdb_id)
            return []
        except Exception as exc:
            logger.warning("Movix │ find_id ERROR [%s]: %s", imdb_id, exc)
            return []

        if not movix_id:
            return []

        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self._movix.get_streams,
                    movix_id,
                    tmdb_info["titles"][0],
                    is_serie,
                    season,
                    episode,
                ),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Movix │ get_streams TIMEOUT [%s]", imdb_id)
            return []
        except Exception as exc:
            logger.warning("Movix │ get_streams ERROR [%s]: %s", imdb_id, exc)
            return []


async def _empty() -> list:
    return []


def _reject_detail(stream: dict, reason: str) -> str:
    if reason == "Language":
        langs = stream.get("languages") or []
        return ", ".join(str(l) for l in langs) if langs else "none"
    if reason.startswith("Title:"):
        return reason.split(":", 1)[1]
    if reason == "Year":
        return str(stream.get("year", "?"))
    if reason == "Season":
        return str(stream.get("seasons", "?"))
    if reason == "Episode":
        return str(stream.get("episodes", "?"))
    return ""