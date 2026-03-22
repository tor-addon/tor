"""
services/alldebrid.py
─────────────────────
AllDebrid API client. Synchronous – call via asyncio.to_thread().

  check_cache(torrents)      – batch instant-availability check
  resolve_stream(...)        – torrent → file tree → direct URL
  unlock_link(link)          – any direct link (1fichier…) → streaming URL
"""

import logging

import requests

from settings import ALLDEBRID_BASE_URL, ALLDEBRID_AGENT, ALLDEBRID_BATCH_SIZE
from utils.episode_selector import find_best_file

logger = logging.getLogger(__name__)


class AllDebridClient:
    __slots__ = ("api_key", "session")

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.session = requests.Session()

    # ─────────────────────────────────────────────────────────────────────────
    # Cache check
    # ─────────────────────────────────────────────────────────────────────────

    def check_cache(self, torrents: list[dict]) -> list[dict]:
        """
        Sets cached=True/False in-place.
        DDL streams (stream_type=='ddl') are skipped – already cached=True.
        Uncached torrents get rank=0.
        """
        to_check = [t for t in torrents if t.get("stream_type") != "ddl"]
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
            batch = unique[i: i + ALLDEBRID_BATCH_SIZE]
            try:
                self._check_batch(batch, hash_map)
            except Exception as exc:
                logger.error("AllDebrid │ batch [%d:%d] failed: %s", i, i + ALLDEBRID_BATCH_SIZE, exc)
                _mark_not_cached(batch, hash_map)

        cached_count = sum(1 for t in torrents if t.get("cached"))
        logger.info("AllDebrid │ %d / %d cached", cached_count, len(torrents))
        return torrents

    # ─────────────────────────────────────────────────────────────────────────
    # Torrent resolution
    # ─────────────────────────────────────────────────────────────────────────

    def resolve_stream(
        self,
        info_hash: str,
        season: int | None = None,
        episode: int | None = None,
        year: int | None = None,
    ) -> str | None:
        """upload magnet → walk file tree → pick best file → unlock → URL"""
        logger.info(
            "AllDebrid │ resolve  hash=%s…  s=%s e=%s year=%s",
            info_hash[:12], season, episode, year,
        )
        magnet_id: int | None = None
        try:
            magnet_id = self._upload_magnet(info_hash)
            if magnet_id is None:
                return None

            raw_files = self._fetch_files(magnet_id)
            if raw_files is None:
                return None

            flat = _flatten_tree(raw_files)
            logger.debug("AllDebrid │ %d file(s) in torrent", len(flat))

            best = find_best_file(flat, season=season, episode=episode, year=year)
            if best is None:
                logger.warning("AllDebrid │ no matching file found")
                return None

            logger.info("AllDebrid │ selected → %s (%.2f GB)", best["n"], best.get("s", 0) / 1e9)
            return self._unlock(best["l"])
        finally:
            if magnet_id is not None:
                self._delete_magnet(magnet_id)

    # ─────────────────────────────────────────────────────────────────────────
    # DDL unlock
    # ─────────────────────────────────────────────────────────────────────────

    def unlock_link(self, link: str) -> str | None:
        """Unlock any direct link (1fichier…) → streamable URL."""
        logger.info("AllDebrid │ unlock_link %s…", link[:60])
        return self._unlock(link)

    # ─────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _upload_magnet(self, info_hash: str) -> int | None:
        r = self.session.post(
            f"{ALLDEBRID_BASE_URL}/magnet/upload",
            data={"magnets[]": info_hash, "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
            timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        if body.get("status") != "success":
            logger.error("AllDebrid │ upload failed: %s", body.get("error"))
            return None
        magnet_id = body["data"]["magnets"][0].get("id")
        logger.debug("AllDebrid │ uploaded magnet id=%s", magnet_id)
        return magnet_id

    def _fetch_files(self, magnet_id: int) -> list | None:
        r = self.session.post(
            f"{ALLDEBRID_BASE_URL}/magnet/files",
            data={"id[]": [magnet_id], "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
            timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        if body.get("status") != "success":
            logger.error("AllDebrid │ files fetch failed: %s", body.get("error"))
            return None
        return body["data"]["magnets"][0]["files"]

    def _unlock(self, link: str) -> str | None:
        r = self.session.get(
            f"{ALLDEBRID_BASE_URL}/link/unlock",
            params={"link": link, "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
            timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        if body.get("status") != "success":
            logger.error("AllDebrid │ unlock failed: %s", body.get("error"))
            return None
        url = body["data"]["link"]
        logger.info("AllDebrid │ unlocked → %s…", url[:60])
        return url

    def _delete_magnet(self, magnet_id: int) -> None:
        try:
            self.session.post(
                f"{ALLDEBRID_BASE_URL}/magnet/delete",
                data={"ids[]": [magnet_id], "apikey": self.api_key, "agent": ALLDEBRID_AGENT},
                timeout=10,
            )
            logger.debug("AllDebrid │ deleted magnet id=%s", magnet_id)
        except Exception as exc:
            logger.warning("AllDebrid │ delete failed id=%s: %s", magnet_id, exc)

    def _check_batch(self, batch: list[str], hash_map: dict[str, list[dict]]) -> None:
        payload = {"agent": ALLDEBRID_AGENT, "apikey": self.api_key, "magnets[]": batch}
        r = self.session.post(f"{ALLDEBRID_BASE_URL}/magnet/upload", data=payload, timeout=15)
        r.raise_for_status()
        body = r.json()

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
                self.session.post(
                    f"{ALLDEBRID_BASE_URL}/magnet/delete",
                    data={"agent": ALLDEBRID_AGENT, "apikey": self.api_key, "ids[]": ids_to_delete},
                    timeout=10,
                )
            except Exception as exc:
                logger.warning("AllDebrid │ delete error: %s", exc)

    def close(self) -> None:
        self.session.close()


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
        if not is_ready:
            obj["rank"] = 0


def _mark_not_cached(batch: list[str], hash_map: dict[str, list[dict]]) -> None:
    for h in batch:
        for obj in hash_map.get(h, []):
            obj["cached"] = False
            obj["rank"]   = 0