"""
services/torznab.py
───────────────────
Torznab client. Synchronous – call via asyncio.to_thread().
"""

import logging
import xml.etree.ElementTree as ET

import requests

from settings import TORZNAB_RESULT_LIMIT

logger = logging.getLogger(__name__)

_NS = {"t": "http://torznab.com/schemas/2015/feed"}


class Torznab:
    __slots__ = ("name", "url", "apikey", "session")

    def __init__(self, name: str, url: str, apikey: str | None = None) -> None:
        self.name    = name
        self.url     = url
        self.apikey  = apikey
        self.session = requests.Session()

    def search(self, query: str, categories: list[int] | None = None, **kwargs) -> list[dict]:
        params: dict = {"t": "search", "limit": TORZNAB_RESULT_LIMIT}
        if self.apikey:
            params["apikey"] = self.apikey
        if query:
            params["q"] = query
        if categories:
            params["cat"] = ",".join(str(c) for c in categories)
        params.update(kwargs)

        logger.debug("Torznab │ [%s] q=%r", self.name, query)
        try:
            r = self.session.get(self.url, params=params, timeout=5)
            r.raise_for_status()
        except Exception as exc:
            logger.warning("Torznab │ [%s] request failed: %s", self.name, exc)
            return []

        results = self._parse(r.text)
        logger.info("Torznab │ [%s] %d result(s) for q=%r", self.name, len(results), query)
        return results

    def _parse(self, xml_text: str) -> list[dict]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.error("Torznab │ [%s] XML parse error: %s", self.name, exc)
            return []

        results: list[dict] = []
        for item in root.findall(".//item"):
            guid = item.findtext("guid") or ""
            entry: dict = {
                "title":       item.findtext("title"),
                "size":        item.findtext("size"),
                "category":    item.findtext("category"),
                "source":      self.name,
                "stream_type": "torrent",
                "seeders":     0,
                "infohash":    None,
            }

            for attr in item.findall("t:attr", _NS):
                name = attr.get("name")
                if name == "seeders":
                    v = attr.get("value")
                    entry["seeders"] = int(v) if v and v.isdigit() else 0
                elif name == "infohash":
                    entry["infohash"] = attr.get("value")

            if not entry["infohash"] and "btih:" in guid.lower():
                entry["infohash"] = guid.lower().split("btih:")[-1].split("&")[0].upper()

            if entry["infohash"]:
                entry["infohash"] = entry["infohash"].upper()

            results.append(entry)

        return results

    def close(self) -> None:
        self.session.close()