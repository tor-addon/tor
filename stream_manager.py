"""
stream_manager.py
─────────────────
Orchestrateur entièrement parallèle.

Architecture: chaque source a sa propre pipeline indépendante.
  run_source(name, coro):
      search → dedup → filter → rank → check_cache → collect cached → exit eval

Toutes les pipelines tournent SIMULTANÉMENT via asyncio.create_task.
Avec httpx.AsyncClient (async natif), t.cancel() interrompt réellement les
requêtes HTTP en cours — plus aucune requête après le exit condition.

resolve_stream:
  torrent (Torznab/Library/T9) → AllDebrid magnet → CDN URL
  ddl movix                    → Movix decode → 1fichier → AllDebrid unlock
  ddl wawacity                 → race AllDebrid redirector links → first success
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import unicodedata
from collections import defaultdict

from services.alldebrid  import AllDebridClient
from services.library    import LibraryClient
from services.movix      import MovixClient
from services.torznab    import Torznab
from services.torrent9   import Torrent9Client
from services.wawacity   import WawacityClient
from utils.deduplicator  import StreamDeduplicator
from utils.exit_condition import ExitConditionEvaluator
from utils.filtering     import StreamFilter
from utils.tmdb          import TMDBApi
from utils               import ranking
from utils.ranking       import LIBRARY_BONUS
from settings            import DEFAULT_LANGUAGES, DEFAULT_MIN_MATCH, DEFAULT_SEARCH_TIMEOUT

logger = logging.getLogger(__name__)
_AMP_RE = re.compile(r"\s*&\s*")


class StreamManager:
    def __init__(
        self,
        alldebrid_api_key:   str,
        torznab_sources:     list[dict],
        tmdb_api_key:        str | None  = None,
        languages:           list[str] | None = None,
        min_match:           float       = DEFAULT_MIN_MATCH,
        search_timeout:      float       = DEFAULT_SEARCH_TIMEOUT,
        enable_movix:        bool        = True,
        movix_url:           str         = "",
        enable_library:      bool        = False,
        library_priority:    bool        = False,
        remove_non_tv:       bool        = True,
        enable_wawacity:     bool        = False,
        wawacity_url:        str         = "",
        enable_torrent9:     bool        = True,
        torrent9_url:        str         = "",
        allowed_resolutions: list[str] | None = None,
        exit_condition:      str         = "",
    ) -> None:
        self._languages           = languages or list(DEFAULT_LANGUAGES)
        self._min_match           = min_match
        self._search_timeout      = search_timeout
        self._library_priority    = library_priority
        self._remove_non_tv       = remove_non_tv
        self._allowed_resolutions = allowed_resolutions or []
        self._exit_condition      = exit_condition.strip() if exit_condition else ""

        self._tmdb     = TMDBApi(tmdb_api_key)
        self._ad       = AllDebridClient(alldebrid_api_key)
        self._movix    = MovixClient(movix_url)               if enable_movix    else None
        self._library  = LibraryClient(alldebrid_api_key)     if enable_library  else None
        self._wawacity = WawacityClient(wawacity_url)         if enable_wawacity else None
        self._torrent9 = Torrent9Client(torrent9_url)         if enable_torrent9 else None
        self._sources  = [
            Torznab(s["name"], s["url"], s.get("apikey"),
                    movie_cats=s.get("movie_cats"), series_cats=s.get("series_cats"))
            for s in torznab_sources
        ]
        logger.info(
            "StreamManager │ ready – %d Torznab  movix=%s  wawa=%s  t9=%s  lib=%s  "
            "langs=%s  min_match=%.0f  exit=%r",
            len(self._sources), enable_movix, enable_wawacity, enable_torrent9, enable_library,
            self._languages, min_match, self._exit_condition or "—",
        )

    # ─────────────────────────────────────────────────────────────────────────

    async def get_streams(
        self,
        imdb_id: str,
        season:  int | None = None,
        episode: int | None = None,
    ) -> list[dict]:
        t0 = time.perf_counter()
        logger.info("━━ [%s] start  s=%s e=%s", imdb_id, season, episode)

        # ── TMDB ──────────────────────────────────────────────────────────────
        try:
            tmdb_info = await self._tmdb.fetch_media_info(imdb_id, self._languages)
        except Exception as exc:
            logger.error("[%s] TMDB failed: %s", imdb_id, exc)
            return []

        is_serie      = tmdb_info["type"] == "series"
        search_titles = [_deaccent(t) for t in tmdb_info["titles"] if t]

        # ── Shared state (accès sync → sérialisés par l'event loop) ──────────
        dedup    = StreamDeduplicator()
        filt     = StreamFilter(
            tmdb_info,
            min_match           = self._min_match,
            target_season       = season,
            target_episode      = episode,
            target_languages    = self._languages,
            remove_non_tv       = self._remove_non_tv,
            allowed_resolutions = self._allowed_resolutions,
        )
        results:           list[dict]           = []
        rejected_counts:   dict[str, int]       = {}
        rejected_examples: dict[str, list[str]] = defaultdict(list)
        exit_event = asyncio.Event()
        exit_eval  = ExitConditionEvaluator(self._exit_condition) if self._exit_condition else None

        # ── Pipeline par source ───────────────────────────────────────────────
        async def run_source(name: str, coro) -> None:
            try:
                raw: list[dict] = await coro
            except Exception as exc:
                logger.warning("[%s] %s error: %s", imdb_id, name, exc)
                return

            if not raw or exit_event.is_set():
                return

            deduped = [s for s in raw if dedup.is_valid(s)]
            if not deduped or exit_event.is_set():
                return

            valid: list[dict] = []
            for s in deduped:
                if filt.is_valid(s):
                    ranking.rank(s)
                    if self._library_priority and s.get("source") == "Library":
                        s["rank"] += LIBRARY_BONUS
                    valid.append(s)
                else:
                    reason = s.get("invalid_reason", "Unknown")
                    key    = reason.split(":")[0]
                    rejected_counts[key] = rejected_counts.get(key, 0) + 1
                    detail = _reject_detail(s, reason)
                    bucket = rejected_examples[key]
                    if detail and detail not in bucket and len(bucket) < 3:
                        bucket.append(detail)

            if not valid or exit_event.is_set():
                return

            # AllDebrid cache check – await interruptible via task cancellation
            await self._ad.check_cache(valid)

            if exit_event.is_set():
                return

            newly = [s for s in valid if s.get("cached")]
            if newly:
                results.extend(newly)
                logger.debug("[%s] %s: +%d cached  (valid=%d deduped=%d raw=%d)",
                             imdb_id, name, len(newly), len(valid), len(deduped), len(raw))

            if exit_eval and exit_eval.active and not exit_event.is_set():
                elapsed = time.perf_counter() - t0
                if exit_eval.evaluate(results, elapsed):
                    logger.info("[%s] exit condition met @ %.0f ms  streams=%d",
                                imdb_id, elapsed * 1000, len(results))
                    exit_event.set()

        # ── Lancement de toutes les pipelines ─────────────────────────────────
        unique_titles = list(dict.fromkeys(search_titles))
        source_list: list[tuple[str, object]] = []

        if self._library:
            source_list.append(("Library", self._library.get_streams()))
        for src in self._sources:
            cats = src.series_cats if is_serie else src.movie_cats
            for title in unique_titles:
                coro = (
                    self._one_torznab_tv(src, title, cats)
                    if is_serie
                    else self._one_torznab(src, title, cats)
                )
                source_list.append((f"Torznab[{src.name}]", coro))
        if self._movix:
            source_list.append(("Movix", self._search_movix(
                imdb_id, tmdb_info, search_titles, is_serie, season, episode,
            )))
        if self._wawacity:
            source_list.append(("Wawacity", self._wawacity.get_streams(
                search_titles, is_serie, season, episode,
            )))
        if self._torrent9:
            source_list.append(("Torrent9", self._torrent9.get_streams(search_titles)))

        if not source_list:
            return []

        tasks = [
            asyncio.create_task(run_source(name, coro), name=name)
            for name, coro in source_list
        ]

        if exit_eval and exit_eval.active:
            async def _exit_timer() -> None:
                while not exit_event.is_set():
                    await asyncio.sleep(0.25)
                    elapsed = time.perf_counter() - t0
                    if exit_eval.evaluate(results, elapsed):
                        logger.info("[%s] exit condition met @ %.0f ms  streams=%d (timer)",
                                    imdb_id, elapsed * 1000, len(results))
                        exit_event.set()

            async def _wait_all() -> None:
                await asyncio.gather(*tasks, return_exceptions=True)

            timer_task  = asyncio.create_task(_exit_timer())
            exit_waiter = asyncio.create_task(exit_event.wait())
            all_done    = asyncio.create_task(_wait_all())

            await asyncio.wait([exit_waiter, all_done], return_when=asyncio.FIRST_COMPLETED)

            timer_task.cancel()
            exit_waiter.cancel()
            if exit_event.is_set():
                # Arrêt brutal : task.cancel() interrompt réellement les await httpx
                # (CancelledError levé au prochain await dans chaque tâche, y compris
                # en plein milieu d'une requête réseau).
                all_done.cancel()
                for t in tasks:
                    if not t.done():
                        t.cancel()
            else:
                try:
                    await all_done
                except Exception:
                    pass
        else:
            await asyncio.gather(*tasks, return_exceptions=True)

        # ── Log rejets ────────────────────────────────────────────────────────
        if rejected_counts:
            parts = [
                f"{k} ×{c}" + (f" ({', '.join(rejected_examples[k])})" if rejected_examples[k] else "")
                for k, c in rejected_counts.items()
            ]
            logger.info("[%s] rejected: %s", imdb_id, " | ".join(parts))

        if not results:
            logger.info("━━ [%s] done %.0f ms – 0 streams", imdb_id, (time.perf_counter() - t0) * 1000)
            return []

        result = ranking.sort_streams(results)
        ms = (time.perf_counter() - t0) * 1000
        logger.info("━━ [%s] done %.0f ms – %d stream(s)", imdb_id, ms, len(result))
        return result

    # ─────────────────────────────────────────────────────────────────────────

    async def resolve_stream(
        self,
        stream:  dict,
        season:  int | None = None,
        episode: int | None = None,
        year:    int | None = None,
    ) -> str | None:
        if stream.get("stream_type") == "ddl":
            return await self._resolve_ddl(stream)
        return await self._resolve_torrent(stream, season, episode, year)

    async def _resolve_torrent(self, stream: dict, season, episode, year) -> str | None:
        ad_id = stream.get("ad_id")
        if stream.get("is_library") and ad_id is not None:
            return await self._ad.resolve_library_stream(ad_id, season, episode, year)
        return await self._ad.resolve_stream(stream["infohash"], season, episode, year)

    async def _resolve_ddl(self, stream: dict) -> str | None:
        if stream.get("di") is not None:
            return await self._resolve_movix(stream)
        if stream.get("dl"):
            return await self._resolve_wawacity(stream)
        logger.error("StreamManager │ unknown DDL stream (no di/dl): %s", stream.get("source"))
        return None

    async def _resolve_movix(self, stream: dict) -> str | None:
        if not self._movix:
            logger.error("StreamManager │ Movix disabled")
            return None
        stream_id = stream.get("di")
        if stream_id is None:
            logger.error("StreamManager │ Movix: no stream_id in token")
            return None
        raw_link = await self._movix.get_direct_link(stream_id)
        if not raw_link:
            return None
        return await self._ad.unlock_link(raw_link)

    async def _resolve_wawacity(self, stream: dict) -> str | None:
        links = stream.get("dl") or []
        hosts = stream.get("dh") or []
        if not links:
            logger.error("StreamManager │ Wawacity: no links in token")
            return None
        pairs = [(link, hosts[i] if i < len(hosts) else f"link_{i+1}")
                 for i, link in enumerate(links)]
        logger.info("Wawacity │ racing %d link(s): %s", len(pairs), " | ".join(h for _, h in pairs))
        return await self._race_wawacity_links(pairs)

    async def _race_wawacity_links(self, pairs: list[tuple[str, str]]) -> str | None:
        async def _try(link: str, host: str) -> tuple[str | None, str]:
            for attempt in range(1, 3):
                resolved = await self._ad.redirector_link(link)
                if not resolved:
                    if attempt < 2:
                        logger.debug("Wawacity │ %s retry…", host)
                    continue
                unlocked = await self._ad.unlock_link(resolved)
                if unlocked:
                    logger.info("Wawacity │ ✓ %s (attempt=%d)", host, attempt)
                    return unlocked, host
            logger.warning("Wawacity │ ✗ %s – all attempts failed", host)
            return None, host

        if len(pairs) == 1:
            url, _ = await _try(*pairs[0])
            return url

        tasks = [asyncio.create_task(_try(link, host)) for link, host in pairs]
        result: str | None = None
        try:
            for coro in asyncio.as_completed(tasks):
                url, _ = await coro
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
        # Services use AsyncClient – no sync close needed; long-lived managers stay open
        logger.info("StreamManager │ closed")

    # ─────────────────────────────────────────────────────────────────────────
    # Source search helpers (timeout + error isolation)
    # ─────────────────────────────────────────────────────────────────────────

    async def _one_torznab(self, source: Torznab, title: str, cats: list[int] | None) -> list[dict]:
        try:
            return await asyncio.wait_for(
                source.search(title, cats or None),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Torznab │ [%s] TIMEOUT q=%r", source.name, title)
            return []
        except Exception as exc:
            logger.warning("Torznab │ [%s] ERROR q=%r → %s", source.name, title, exc)
            return []

    async def _one_torznab_tv(self, source: Torznab, title: str, cats: list[int] | None) -> list[dict]:
        try:
            return await asyncio.wait_for(
                source.search_tv(title, cats or None),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Torznab │ [%s] tvsearch TIMEOUT q=%r", source.name, title)
            return []
        except Exception as exc:
            logger.warning("Torznab │ [%s] tvsearch ERROR q=%r → %s", source.name, title, exc)
            return []

    async def _search_movix(
        self, imdb_id: str, tmdb_info: dict, search_titles: list[str],
        is_serie: bool, season: int | None, episode: int | None,
    ) -> list[dict]:
        try:
            movix_id = await asyncio.wait_for(
                self._movix.find_id(
                    search_titles,
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

        media_title = (tmdb_info["titles"] or [""])[0]
        try:
            return await asyncio.wait_for(
                self._movix.get_streams(movix_id, media_title, is_serie, season, episode),
                timeout=self._search_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Movix │ get_streams TIMEOUT [%s]", imdb_id)
            return []
        except Exception as exc:
            logger.warning("Movix │ get_streams ERROR [%s]: %s", imdb_id, exc)
            return []


# ─────────────────────────────────────────────────────────────────────────────

def _deaccent(text: str) -> str:
    """NFD → ASCII. & → 'and' pour le matching de titre."""
    n = unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("ascii")
    return _AMP_RE.sub(" and ", n).strip()


def _reject_detail(stream: dict, reason: str) -> str:
    if reason == "Language":
        langs = stream.get("languages") or []
        return ", ".join(str(l) for l in langs) if langs else "none"
    if reason.startswith("Title:"):
        return reason.split(":", 1)[1]
    if reason in ("Year", "Season", "Episode"):
        key = {"Year": "year", "Season": "seasons", "Episode": "episodes"}[reason]
        return str(stream.get(key, "?"))
    return ""
