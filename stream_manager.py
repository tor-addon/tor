"""
stream_manager.py
─────────────────
Async orchestrator.

Pipeline:
  TMDB → [Torznab × N + Movix + Wawacity + Library] (parallel)
       → Dedup (torrent-only; DDL bypass) → Filter → Rank
       → AllDebrid cache check (torrent-only, skips DDL/Library) → Sort

Dedup runs BEFORE Filter so PTT (~0.5 ms/stream) is never called on duplicates.
DDL streams (Movix, Wawacity) bypass deduplication entirely.

resolve_stream:
  torrent (Torznab/Library) → AllDebrid magnet → CDN URL
  ddl movix_*               → Movix decode → 1fichier → AllDebrid unlock
  ddl wawa_*                → concurrent AllDebrid redirector race → first success → unlock
"""

import asyncio
import base64
import itertools
import json
import logging
import time
import unicodedata
from collections import defaultdict

from services.alldebrid import AllDebridClient
from services.library import LibraryClient
from services.movix import MovixClient
from services.torznab import Torznab
from services.wawacity import WawacityClient
from utils.deduplicator import StreamDeduplicator
from utils.filtering import StreamFilter
from utils.tmdb import TMDBApi
from utils import ranking
from utils.ranking import LIBRARY_BONUS
from settings import DEFAULT_LANGUAGES, DEFAULT_MIN_MATCH, DEFAULT_SEARCH_TIMEOUT

logger = logging.getLogger(__name__)


class StreamManager:
    def __init__(
        self,
        alldebrid_api_key: str,
        torznab_sources: list[dict],
        tmdb_api_key: str | None = None,
        languages: list[str] | None = None,
        min_match: float = DEFAULT_MIN_MATCH,
        search_timeout: float = DEFAULT_SEARCH_TIMEOUT,
        enable_movix: bool = True,
        movix_url: str = "",
        enable_library: bool = False,
        library_priority: bool = False,
        remove_non_tv: bool = True,
        enable_wawacity: bool = False,
        wawacity_url: str = "",
    ) -> None:
        self._languages        = languages or list(DEFAULT_LANGUAGES)
        self._min_match        = min_match
        self._search_timeout   = search_timeout
        self._library_priority = library_priority
        self._remove_non_tv    = remove_non_tv

        self._tmdb      = TMDBApi(tmdb_api_key)
        self._ad        = AllDebridClient(alldebrid_api_key)
        self._movix     = MovixClient(movix_url) if enable_movix else None
        self._library   = LibraryClient(alldebrid_api_key) if enable_library else None
        self._wawacity  = WawacityClient(wawacity_url) if enable_wawacity else None
        self._sources   = [
            Torznab(s["name"], s["url"], s.get("apikey"))
            for s in torznab_sources
        ]

        logger.info(
            "StreamManager │ ready – %d Torznab  movix=%s  wawacity=%s  library=%s  langs=%s  min_match=%.0f",
            len(self._sources), enable_movix, enable_wawacity, enable_library,
            self._languages, min_match,
        )

    # ─────────────────────────────────────────────────────────────────────────

    async def get_streams(
        self,
        imdb_id: str,
        season: int | None = None,
        episode: int | None = None,
    ) -> list[dict]:
        t0 = time.perf_counter()
        logger.info("━━ [%s] start  s=%s e=%s", imdb_id, season, episode)

        try:
            tmdb_info = await asyncio.to_thread(self._tmdb.fetch_media_info, imdb_id)
        except Exception as exc:
            logger.error("[%s] TMDB failed: %s", imdb_id, exc)
            return []

        is_serie = tmdb_info["type"] == "series"

        # Strip accents once – torrent/DDL names rarely keep them
        search_titles = [_deaccent(t) for t in tmdb_info["titles"] if t]

        # All sources in parallel – coroutines directly, no create_task overhead
        torznab_r, movix_r, wawacity_r, library_r = await asyncio.gather(
            self._search_torznab(imdb_id, search_titles),
            self._search_movix(imdb_id, tmdb_info, search_titles, is_serie, season, episode) if self._movix else _empty(),
            self._search_wawacity(search_titles, is_serie, season, episode) if self._wawacity else _empty(),
            self._search_library() if self._library else _empty(),
        )

        # Library first: its hashes register first in dedup → Torznab dupes dropped
        raw = library_r + torznab_r + movix_r + wawacity_r
        logger.info(
            "[%s] raw: %d  (lib=%d  tz=%d  movix=%d  wawa=%d)",
            imdb_id, len(raw), len(library_r), len(torznab_r), len(movix_r), len(wawacity_r),
        )

        if not raw:
            return []

        # ── Dedup FIRST (before PTT in Filter) ────────────────────────────────
        dedup   = StreamDeduplicator()
        deduped: list[dict] = []
        n_dedup = 0
        for stream in raw:
            if dedup.is_valid(stream):
                deduped.append(stream)
            else:
                n_dedup += 1

        if not deduped:
            return []

        # ── Filter + Rank ─────────────────────────────────────────────────────
        filt = StreamFilter(
            tmdb_info,
            min_match=self._min_match,
            target_season=season,
            target_episode=episode,
            target_languages=self._languages,
            remove_non_tv=self._remove_non_tv,
        )

        valid: list[dict] = []
        reject_counts:   dict[str, int]         = {}
        reject_examples: dict[str, list[str]]   = defaultdict(list)

        for stream in deduped:
            if not filt.is_valid(stream):
                reason = stream.get("invalid_reason", "Unknown")
                key    = reason.split(":")[0]
                reject_counts[key] = reject_counts.get(key, 0) + 1
                detail = _reject_detail(stream, reason)
                bucket = reject_examples[key]
                if detail and detail not in bucket and len(bucket) < 3:
                    bucket.append(detail)
                continue
            ranking.rank(stream)
            if self._library_priority and stream.get("source") == "Library":
                stream["rank"] += LIBRARY_BONUS
            valid.append(stream)

        if reject_counts:
            parts = [
                f"{k} ×{c}" + (f" ({', '.join(reject_examples[k])})" if reject_examples[k] else "")
                for k, c in reject_counts.items()
            ]
            logger.info("[%s] rejected: %s", imdb_id, " | ".join(parts))

        logger.info(
            "[%s] filter: %d valid  (dedup=%d filter=%d) / %d",
            imdb_id, len(valid), n_dedup, len(deduped) - len(valid), len(raw),
        )

        if not valid:
            return []

        # ── AllDebrid cache check (torrent only; DDL/Library pre-cached) ──────
        await asyncio.to_thread(self._ad.check_cache, valid)
        cached = [s for s in valid if s.get("cached")]
        logger.info("[%s] cached: %d / %d", imdb_id, len(cached), len(valid))

        if not cached:
            return []

        result = ranking.sort_streams(cached)
        ms = (time.perf_counter() - t0) * 1000
        logger.info("━━ [%s] done %.0f ms – %d stream(s)", imdb_id, ms, len(result))
        return result

    # ─────────────────────────────────────────────────────────────────────────

    async def resolve_stream(
        self,
        stream: dict,
        season: int | None = None,
        episode: int | None = None,
        year: int | None = None,
    ) -> str | None:
        if stream.get("stream_type") == "ddl":
            return await self._resolve_ddl(stream)
        return await self._resolve_torrent(stream, season, episode, year)

    async def _resolve_torrent(self, stream, season, episode, year) -> str | None:
        delete_after = not stream.get("is_library", False)
        return await asyncio.to_thread(
            self._ad.resolve_stream,
            stream["infohash"], season, episode, year, delete_after,
        )

    async def _resolve_ddl(self, stream: dict) -> str | None:
        infohash = stream.get("infohash", "")
        if stream.get("di") is not None or infohash.startswith("movix_"):
            return await self._resolve_movix(stream)
        if stream.get("dl") or infohash.startswith("wawa_"):
            return await self._resolve_wawacity(stream)
        logger.error("StreamManager │ unknown DDL stream: infohash=%s", infohash[:20])
        return None

    async def _resolve_movix(self, stream: dict) -> str | None:
        if not self._movix:
            logger.error("StreamManager │ Movix disabled")
            return None
        # Prefer direct ddl_id field; fall back to parsing infohash (old tokens)
        stream_id = stream.get("di")
        if stream_id is None:
            try:
                stream_id = int(stream.get("infohash", "").split("_", 1)[1])
            except (IndexError, ValueError) as exc:
                logger.error("StreamManager │ invalid movix infohash: %s", exc)
                return None
        raw_link = await asyncio.to_thread(self._movix.get_direct_link, stream_id)
        if not raw_link:
            return None
        return await asyncio.to_thread(self._ad.unlock_link, raw_link)

    async def _resolve_wawacity(self, stream: dict) -> str | None:
        # Prefer direct ddl_links field; fall back to decoding infohash (old tokens)
        links = stream.get("dl")
        hosts = stream.get("dh") or []

        if not links:
            infohash = stream.get("infohash", "")
            try:
                b64  = infohash[5:]  # strip "wawa_"
                data = base64.urlsafe_b64decode(b64 + "==")
                try:
                    links = json.loads(data)
                    if not isinstance(links, list):
                        links = [str(links)]
                except (json.JSONDecodeError, UnicodeDecodeError):
                    links = [data.decode()]
            except Exception as exc:
                logger.error("StreamManager │ invalid wawa infohash: %s", exc)
                return None

        if not links:
            return None

        pairs = [(link, hosts[i] if i < len(hosts) else f"link_{i+1}")
                 for i, link in enumerate(links)]
        logger.info("Wawacity │ racing %d link(s): %s",
                    len(pairs), " | ".join(h for _, h in pairs))
        return await self._race_wawacity_links(pairs)

    async def _race_wawacity_links(self, pairs: list[tuple[str, str]]) -> str | None:
        """
        Run AllDebrid redirector concurrently on all (link, host) pairs.
        Each link gets up to 2 attempts. Returns the first successful CDN URL.
        """
        async def _try_link(link: str, host: str) -> tuple[str | None, str]:
            for attempt in range(1, 3):
                resolved = await asyncio.to_thread(self._ad.redirector_link, link)
                if not resolved:
                    if attempt < 2:
                        logger.debug("Wawacity │ %s – redirector failed, retrying…", host)
                    continue
                unlocked = await asyncio.to_thread(self._ad.unlock_link, resolved)
                if unlocked:
                    logger.info("Wawacity │ ✓ %s (attempt=%d)", host, attempt)
                    return unlocked, host
            logger.warning("Wawacity │ ✗ %s – all attempts failed", host)
            return None, host

        if len(pairs) == 1:
            url, _ = await _try_link(*pairs[0])
            return url

        tasks = [asyncio.create_task(_try_link(link, host)) for link, host in pairs]
        result: str | None = None
        try:
            for coro in asyncio.as_completed(tasks):
                url, host = await coro
                if url:
                    result = url
                    break
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass

        if not result:
            logger.warning("Wawacity │ all %d link(s) failed", len(pairs))
        return result

    # ─────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._ad.close()
        if self._movix:
            self._movix.close()
        if self._library:
            self._library.close()
        if self._wawacity:
            self._wawacity.close()
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
        return list(itertools.chain.from_iterable(batches))

    async def _search_movix(self, imdb_id, tmdb_info, search_titles, is_serie, season, episode) -> list[dict]:
        try:
            movix_id = await asyncio.wait_for(
                asyncio.to_thread(
                    self._movix.find_id,
                    search_titles,                  # deaccented for search query
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
                    movix_id, (tmdb_info["titles"] or [""])[0], is_serie, season, episode,
                ),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Movix │ get_streams TIMEOUT [%s]", imdb_id)
            return []
        except Exception as exc:
            logger.warning("Movix │ get_streams ERROR [%s]: %s", imdb_id, exc)
            return []

    async def _search_wawacity(self, titles, is_serie, season, episode) -> list[dict]:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(
                    self._wawacity.get_streams, titles, is_serie, season, episode,
                ),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Wawacity │ TIMEOUT")
            return []
        except Exception as exc:
            logger.warning("Wawacity │ ERROR: %s", exc)
            return []

    async def _search_library(self) -> list[dict]:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._library.get_streams),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Library │ TIMEOUT")
            return []
        except Exception as exc:
            logger.warning("Library │ ERROR: %s", exc)
            return []


async def _empty() -> list:
    return []


def _deaccent(text: str) -> str:
    """NFD decompose → drop combining diacritical marks → ASCII."""
    return unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("ascii")


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

