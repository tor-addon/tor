"""
services/alldebrid.py
─────────────────────
AllDebrid API client. Asynchronous.

  check_cache(torrents)         – batch instant-availability check
  resolve_stream(...)           – upload magnet → file tree → pick file → CDN URL → delete
  resolve_library_stream(...)   – existing library magnet → file tree → CDN URL (no upload/delete)
  unlock_link(link)             – any direct link (1fichier…) → streaming URL
  redirector_link(link)         – follow AllDebrid redirector → real link

Retry policy: ConnectError / TimeoutException retried up to _RETRY_ATTEMPTS times
with _RETRY_DELAY seconds. Logic errors (bad key, invalid hash…) are not retried.
"""

import asyncio
import logging

import httpx

from settings import ALLDEBRID_BASE_URL, ALLDEBRID_AGENT, ALLDEBRID_BATCH_SIZE
from utils.episode_selector import find_best_file

logger = logging.getLogger(__name__)

_RETRY_ATTEMPTS = 3
_RETRY_DELAY    = 0.5


async def _retry(coro_fn) -> dict:
    """Call coro_fn() up to _RETRY_ATTEMPTS times on network errors."""
    last_exc = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            return await coro_fn()
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            last_exc = exc
            if attempt < _RETRY_ATTEMPTS:
                logger.warning(
                    "AllDebrid │ network error (attempt %d/%d): %s – retrying in %.1fs",
                    attempt, _RETRY_ATTEMPTS, exc, _RETRY_DELAY,
                )
                await asyncio.sleep(_RETRY_DELAY)
    raise last_exc


class AllDebridClient:
    __slots__ = ("api_key", "client")

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.client  = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=40, max_keepalive_connections=8),
            timeout=15,
            follow_redirects=True,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Cache check
    # ─────────────────────────────────────────────────────────────────────────

    async def check_cache(self, torrents: list[dict]) -> list[dict]:
        """Sets cached=True/False in-place. DDL and pre-cached streams are skipped."""
        to_check = [
            t for t in torrents
            if t.get("stream_type") != "ddl" and not t.get("cached")
        ]
        if not to_check:
            return torrents

        hash_map: dict[str, list[dict]] = {}
        for t in to_check:
            h = str(t.get("infohash", "")).strip().lower()
            if h:
                hash_map.setdefault(h, []).append(t)

        unique = list(hash_map)
        logger.info("AllDebrid │ cache check: %d unique hashes", len(unique))

        for i in range(0, len(unique), ALLDEBRID_BATCH_SIZE):
            batch = unique[i : i + ALLDEBRID_BATCH_SIZE]
            try:
                await self._check_batch(batch, hash_map)
            except Exception as exc:
                logger.error("AllDebrid │ batch [%d:%d] failed: %s", i, i + ALLDEBRID_BATCH_SIZE, exc)
                _mark_not_cached(batch, hash_map)

        cached_count = sum(1 for t in torrents if t.get("cached"))
        logger.info("AllDebrid │ %d / %d cached", cached_count, len(torrents))
        return torrents

    # ─────────────────────────────────────────────────────────────────────────
    # Torrent resolution
    # ─────────────────────────────────────────────────────────────────────────

    async def resolve_stream(
        self,
        info_hash: str,
        season: int | None = None,
        episode: int | None = None,
        year: int | None = None,
    ) -> str | None:
        logger.info(
            "AllDebrid │ resolve  hash=%s…  s=%s e=%s year=%s",
            info_hash[:12], season, episode, year,
        )
        magnet_id: int | None = None
        try:
            magnet_id = await self._upload_magnet(info_hash)
            if magnet_id is None:
                return None
            raw_files = await self._fetch_files(magnet_id)
            if raw_files is None:
                return None
            flat = _flatten_tree(raw_files)
            logger.debug("AllDebrid │ %d file(s) in torrent", len(flat))
            best = find_best_file(flat, season=season, episode=episode, year=year)
            if best is None:
                logger.warning("AllDebrid │ no matching file found")
                return None
            logger.info("AllDebrid │ selected → %s (%.2f GB)", best["n"], best.get("s", 0) / 1e9)
            return await self._unlock(best["l"])
        finally:
            if magnet_id is not None:
                await self._delete_magnet(magnet_id)

    async def resolve_library_stream(
        self,
        magnet_id: int,
        season: int | None = None,
        episode: int | None = None,
        year: int | None = None,
    ) -> str | None:
        logger.info(
            "AllDebrid │ library resolve  id=%s  s=%s e=%s year=%s",
            magnet_id, season, episode, year,
        )
        raw_files = await self._fetch_files(magnet_id)
        if raw_files is None:
            return None
        flat = _flatten_tree(raw_files)
        logger.debug("AllDebrid │ %d file(s) in torrent", len(flat))
        best = find_best_file(flat, season=season, episode=episode, year=year)
        if best is None:
            logger.warning("AllDebrid │ no matching file found")
            return None
        logger.info("AllDebrid │ selected → %s (%.2f GB)", best["n"], best.get("s", 0) / 1e9)
        return await self._unlock(best["l"])

    # ─────────────────────────────────────────────────────────────────────────
    # DDL unlock
    # ─────────────────────────────────────────────────────────────────────────

    async def unlock_link(self, link: str) -> str | None:
        logger.info("AllDebrid │ unlock_link %s…", link[:60])
        return await self._unlock(link)

    async def redirector_link(self, link: str) -> str | None:
        logger.info("AllDebrid │ redirector %s…", link[:60])

        for attempt in range(1, 3):
            async def _call():
                r = await self.client.get(
                    f"{ALLDEBRID_BASE_URL}/link/redirector",
                    params={"link": link, "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
                )
                r.raise_for_status()
                return r.json()

            body = await _retry(_call)
            if body.get("status") != "success":
                err      = body.get("error") or {}
                err_code = err.get("code", "") if isinstance(err, dict) else str(err)
                err_msg  = err.get("message", "") if isinstance(err, dict) else str(err)
                is_redir = "REDIRECTOR_ERROR" in str(err_code) or "Could not extract" in str(err_msg)
                if is_redir and attempt < 2:
                    logger.warning("AllDebrid │ REDIRECTOR_ERROR attempt %d/2 – retrying in 1s…", attempt)
                    await asyncio.sleep(1.0)
                    continue
                logger.error("AllDebrid │ redirector failed: %s", err)
                return None
            links = body.get("data", {}).get("links") or []
            if not links:
                logger.error("AllDebrid │ redirector: no links for %s…", link[:60])
                return None
            resolved = links[0].get("link") if isinstance(links[0], dict) else str(links[0])
            logger.info("AllDebrid │ redirector → %s…", resolved[:60])
            return resolved
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _upload_magnet(self, info_hash: str) -> int | None:
        async def _call():
            r = await self.client.post(
                f"{ALLDEBRID_BASE_URL}/magnet/upload",
                data={"magnets[]": info_hash, "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
            )
            r.raise_for_status()
            return r.json()

        body = await _retry(_call)
        if body.get("status") != "success":
            logger.error("AllDebrid │ upload failed: %s", body.get("error"))
            return None
        magnets = body.get("data", {}).get("magnets") or []
        if not magnets:
            logger.error("AllDebrid │ upload: empty magnets in response")
            return None
        magnet_id = magnets[0].get("id")
        logger.debug("AllDebrid │ uploaded magnet id=%s", magnet_id)
        return magnet_id

    async def _fetch_files(self, magnet_id: int) -> list | None:
        async def _call():
            r = await self.client.post(
                f"{ALLDEBRID_BASE_URL}/magnet/files",
                data={"id[]": [magnet_id], "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
            )
            r.raise_for_status()
            return r.json()

        body = await _retry(_call)
        if body.get("status") != "success":
            logger.error("AllDebrid │ files fetch failed: %s", body.get("error"))
            return None
        magnets = body.get("data", {}).get("magnets") or []
        if not magnets:
            logger.error("AllDebrid │ files: empty magnets in response")
            return None
        return magnets[0].get("files") or []

    async def _unlock(self, link: str) -> str | None:
        async def _call():
            r = await self.client.get(
                f"{ALLDEBRID_BASE_URL}/link/unlock",
                params={"link": link, "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
            )
            r.raise_for_status()
            return r.json()

        body = await _retry(_call)
        if body.get("status") != "success":
            logger.error("AllDebrid │ unlock failed: %s", body.get("error"))
            return None
        url = body["data"]["link"]
        logger.info("AllDebrid │ unlocked → %s…", url[:60])
        return url

    async def _delete_magnet(self, magnet_id: int) -> None:
        try:
            await self.client.post(
                f"{ALLDEBRID_BASE_URL}/magnet/delete",
                data={"ids[]": [magnet_id], "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
                timeout=10,
            )
            logger.debug("AllDebrid │ deleted magnet id=%s", magnet_id)
        except Exception as exc:
            logger.warning("AllDebrid │ delete failed id=%s: %s", magnet_id, exc)

    async def _check_batch(self, batch: list[str], hash_map: dict[str, list[dict]]) -> None:
        payload = {"agent": ALLDEBRID_AGENT, "apikey": self.api_key, "magnets[]": batch}

        async def _call():
            r = await self.client.post(
                f"{ALLDEBRID_BASE_URL}/magnet/upload", data=payload
            )
            r.raise_for_status()
            return r.json()

        body = await _retry(_call)

        if body.get("status") != "success":
            logger.warning("AllDebrid │ API error: %s", body.get("error", {}))
            _mark_not_cached(batch, hash_map)
            return

        ids_to_delete: list[int] = []
        for m in body.get("data", {}).get("magnets", []):
            ad_hash  = str(m.get("hash") or m.get("magnet", "")).strip().lower()
            is_ready = bool(m.get("ready", False))
            if "id" in m:
                ids_to_delete.append(m["id"])
            if ad_hash in hash_map:
                _apply(hash_map[ad_hash], is_ready)
            else:
                for local_hash, objs in hash_map.items():
                    if local_hash in ad_hash:
                        _apply(objs, is_ready)
                        break

        if ids_to_delete:
            try:
                await self.client.post(
                    f"{ALLDEBRID_BASE_URL}/magnet/delete",
                    data={"agent": ALLDEBRID_AGENT, "apikey": self.api_key, "ids[]": ids_to_delete},
                    timeout=10,
                )
            except Exception as exc:
                logger.warning("AllDebrid │ delete error: %s", exc)

    def close(self) -> None:
        pass


# ─────────────────────────────────────────────────────────────────────────────

def _flatten_tree(entries: list, path: str = "") -> list[dict]:
    files = []
    for item in entries:
        name = item.get("n", "")
        if "l" in item:
            files.append({"n": name, "l": item["l"], "s": item.get("s", 0), "path": path})
        if "e" in item:
            files.extend(_flatten_tree(item["e"], f"{path}/{name}".strip("/")))
    return files


def _apply(objs: list[dict], is_ready: bool) -> None:
    for obj in objs:
        obj["cached"] = is_ready


def _mark_not_cached(batch: list[str], hash_map: dict[str, list[dict]]) -> None:
    for h in batch:
        for obj in hash_map.get(h, []):
            obj["cached"] = False
