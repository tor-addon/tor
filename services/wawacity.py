"""
services/wawacity.py
────────────────────
Wawacity DDL scraper. Asynchronous.

Flow:
  get_streams(titles, is_serie, season, episode) → pipeline-compatible stream dicts

All title searches and page fetches run concurrently via asyncio.gather.
Resolution at playback time: ddl_links → AllDebrid redirector → unlock → CDN URL.
"""

import asyncio
import logging
import re
from urllib.parse import quote_plus

import httpx
from selectolax.parser import HTMLParser

from settings import DDL_ALLOWED_HOSTS, WAWACITY_BASE_URL

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

_LANG_MAP: dict[str, list[str]] = {
    "vf":                     ["fr"],
    "vff":                    ["fr"],
    "vfq":                    ["fr"],
    "french":                 ["fr"],
    "truefrench":             ["fr"],
    "true french":            ["fr"],
    "vostfr":                 ["vostfr"],
    "vost":                   ["vostfr"],
    "multi":                  ["multi"],
    "multi (french)":         ["multi", "fr"],
    "multi(french)":          ["multi", "fr"],
    "multi (truefrench)":     ["multi", "fr"],
    "multi(truefrench)":      ["multi", "fr"],
    "vo":                     [],
}

_RE_EPISODE = re.compile(r'(?:épisode|episode)\s*(\d+)', re.IGNORECASE)
_RE_PARTIE  = re.compile(r'partie\s*\d+', re.IGNORECASE)
_RE_EXT     = re.compile(r'\.\w{2,4}$')
_RE_NAV     = re.compile(r'^[^»]*»\s*')

_SIZE_UNITS = {
    "o":  1, "ko": 1_000, "mo": 1_000_000,
    "go": 1_000_000_000, "to": 1_000_000_000_000,
}


def _map_lang(raw: str) -> list[str]:
    normalized = raw.lower().strip()
    if normalized in _LANG_MAP:
        return _LANG_MAP[normalized]
    if "multi" in normalized:
        return ["multi", "fr"]
    if "truefrench" in normalized or "french" in normalized:
        return ["fr"]
    if "vostfr" in normalized:
        return ["vostfr"]
    return ["fr"]


def _abs(base: str, path: str) -> str:
    if path.startswith("http"):
        return path
    return f"{base}/?{path[1:]}" if path.startswith("?") else f"{base}{path}"


def _parse_size(raw: str) -> int:
    m = re.match(r"([\d.,]+)\s*([a-zA-Z]+)", raw.strip())
    if not m:
        return 0
    factor = _SIZE_UNITS.get(m.group(2).lower(), 0)
    return int(float(m.group(1).replace(",", ".")) * factor) if factor else 0


class WawacityClient:
    __slots__ = ("_base", "_timeout", "client")

    def __init__(self, base_url: str = "", timeout: float = 6.0) -> None:
        self._base    = (base_url or WAWACITY_BASE_URL).rstrip("/")
        self._timeout = timeout
        self.client   = httpx.AsyncClient(
            headers=_HEADERS,
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            timeout=timeout,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    async def get_streams(
        self,
        titles: list[str],
        is_serie: bool,
        season: int | None = None,
        episode: int | None = None,
    ) -> list[dict]:
        if not titles:
            return []
        try:
            streams = (
                await self._get_episode(titles, season or 1, episode or 1)
                if is_serie
                else await self._get_movie(titles)
            )
        except Exception as exc:
            logger.error("Wawacity │ get_streams error: %s", exc)
            return []
        logger.info("Wawacity │ %d stream(s) extracted", len(streams))
        return streams

    # ─────────────────────────────────────────────────────────────────────────
    # HTTP helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _fetch(self, url: str) -> HTMLParser | None:
        try:
            r = await self.client.get(url)
            return HTMLParser(r.text) if r.status_code == 200 else None
        except Exception as e:
            logger.debug("Wawacity │ fetch error %s: %s", url[:60], e)
            return None

    async def _fetch_many(self, urls: set[str] | list[str]) -> list[HTMLParser | None]:
        urls_list = list(urls)
        if not urls_list:
            return []
        if len(urls_list) == 1:
            return [await self._fetch(urls_list[0])]
        return list(await asyncio.gather(*[self._fetch(u) for u in urls_list]))

    # ─────────────────────────────────────────────────────────────────────────
    # Search helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _search_first(self, query: str, kind: str) -> str | None:
        dom = await self._fetch(f"{self._base}/?p={kind}&search={quote_plus(query)}")
        if not dom:
            return None
        items = dom.css("#wa-mid-blocks .wa-post-detail-item")
        logger.info("Wawacity │ %d result(s) for q=%r", len(items), query)
        if not items:
            return None
        a = items[0].css_first(".wa-sub-block-title > a")
        return _abs(self._base, a.attributes["href"]) if a else None

    async def _find_all_urls(self, titles: list[str], kind: str) -> set[str]:
        unique = list(dict.fromkeys(t for t in titles if t))
        if not unique:
            return set()
        if len(unique) == 1:
            url = await self._search_first(unique[0], kind)
            return {url} if url else set()
        results = await asyncio.gather(*[self._search_first(t, kind) for t in unique])
        return {r for r in results if r}

    # ─────────────────────────────────────────────────────────────────────────
    # Page metadata helpers (synchronous – no I/O)
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _page_info(dom: HTMLParser) -> tuple[str, str, int, int | None]:
        """Returns (language, quality_raw, size_bytes, year)."""
        lang = qual = ""
        size = 0
        year = None
        for li in dom.css("ul.item-list li"):
            text = li.text(strip=True)
            b    = li.css_first("b")
            val  = b.text(strip=True) if b else text.split(":", 1)[-1].strip()
            tl   = text.lower()
            if tl.startswith("langue"):
                lang = val
            elif tl.startswith("qualit"):
                qual = val
            elif tl.startswith("taille"):
                size = _parse_size(val)
        for li in dom.css("ul.detail-list li"):
            span = li.css_first("span")
            if span and "ann" in span.text(strip=True).lower():
                b = li.css_first("b")
                if b:
                    m = re.search(r'\d{4}', b.text(strip=True))
                    if m:
                        year = int(m.group())
                break
        return lang, qual, size, year

    @staticmethod
    def _page_h1(dom: HTMLParser) -> str:
        h1 = dom.css_first("h1")
        if not h1:
            return ""
        return _RE_NAV.sub("", h1.text(strip=True)).strip()

    # ─────────────────────────────────────────────────────────────────────────
    # Link-row extraction (synchronous)
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_link_row(self, row) -> dict | None:
        host_td = row.css_first('td[width="120px"]')
        if not host_td:
            return None
        host = host_td.text(strip=True).lower()
        if host not in DDL_ALLOWED_HOSTS:
            return None

        size_td  = row.css_first('td[width="80px"]')
        row_size = _parse_size(size_td.text(strip=True)) if size_td else 0

        a = row.css_first("a.link")
        if not a:
            return None
        b_tag = a.css_first("b")
        if b_tag and _RE_PARTIE.search(b_tag.text(strip=True)):
            return None

        href = a.attributes.get("href", "")
        if not href:
            return None

        full_text = a.text(strip=True)
        b_text    = b_tag.text(strip=True) if b_tag else ""
        candidate = full_text.replace(b_text, "").strip()
        fn        = candidate if _RE_EXT.search(candidate) else ""

        return {"link": _abs(self._base, href), "host": host_td.text(strip=True), "filename": fn, "size": row_size}

    def _rows_to_stream(
        self,
        rows,
        h1_title: str,
        language: str,
        size: int,
        year: int | None,
        is_serie: bool,
        season: int | None,
        episode: int | None,
    ) -> dict | None:
        links:    list[str] = []
        hosts:    list[str] = []
        seen:     set[str]  = set()
        filename: str       = ""
        row_size: int       = 0

        for row in rows:
            info = self._extract_link_row(row)
            if not info or info["link"] in seen:
                continue
            seen.add(info["link"])
            links.append(info["link"])
            hosts.append(info["host"])
            if not filename and info["filename"]:
                filename = info["filename"]
            if not row_size and info["size"]:
                row_size = info["size"]

        if not links:
            return None

        stream: dict = {
            "torrent_name": filename or h1_title,
            "infohash":     "",
            "ddl_links":    links,
            "source":       "Wawacity",
            "stream_type":  "ddl",
            "cached":       True,
            "languages":    _map_lang(language),
            "year":         year,
            "size":         size if size > 0 else row_size,
            "hosts":        hosts,
        }
        if is_serie and season is not None:
            stream["seasons"]  = [season]
        if is_serie and episode is not None:
            stream["episodes"] = [episode]
        return stream

    # ─────────────────────────────────────────────────────────────────────────
    # Movie
    # ─────────────────────────────────────────────────────────────────────────

    async def _get_movie(self, titles: list[str]) -> list[dict]:
        result_urls = await self._find_all_urls(titles, "films")
        if not result_urls:
            return []

        result_list = list(result_urls)
        fetched: dict[str, HTMLParser] = {}
        for url, dom in zip(result_list, await self._fetch_many(result_list)):
            if dom:
                fetched[url] = dom

        if not fetched:
            return []

        variant_urls: set[str] = set(fetched.keys())
        for dom in fetched.values():
            for a in dom.css('a[href^="?p=film&id="]'):
                if a.css_first("button"):
                    variant_urls.add(_abs(self._base, a.attributes["href"]))

        new_urls   = variant_urls - set(fetched.keys())
        all_pages: list[HTMLParser] = list(fetched.values())
        for dom in await self._fetch_many(new_urls):
            if dom:
                all_pages.append(dom)

        out: list[dict] = []
        for page in all_pages:
            h1                = self._page_h1(page)
            lang, _, size, yr = self._page_info(page)
            rows              = page.css("#DDLLinks tr.link-row")
            s = self._rows_to_stream(rows, h1, lang, size, yr, False, None, None)
            if s:
                out.append(s)
        return out

    # ─────────────────────────────────────────────────────────────────────────
    # Episode
    # ─────────────────────────────────────────────────────────────────────────

    def _ep_streams_from_dom(self, dom: HTMLParser, season: int, episode: int) -> list[dict]:
        table = dom.css_first("#DDLLinks")
        if not table:
            return []

        h1                = self._page_h1(dom)
        lang, _, size, yr = self._page_info(dom)

        cur_ep, ep_rows = None, []
        for row in table.css("tr"):
            cls = row.attributes.get("class", "")
            if "title" in cls:
                m = _RE_EPISODE.search(row.text())
                cur_ep = int(m.group(1)) if m else None
            elif "link-row" in cls and cur_ep == episode:
                ep_rows.append(row)

        if not ep_rows:
            return []
        s = self._rows_to_stream(ep_rows, h1, lang, size, yr, True, season, episode)
        return [s] if s else []

    async def _get_episode(self, titles: list[str], season: int, episode: int) -> list[dict]:
        series_urls = await self._find_all_urls(titles, "series")
        if not series_urls:
            return []

        series_list = list(series_urls)
        fetched_series: dict[str, HTMLParser] = {}
        for url, dom in zip(series_list, await self._fetch_many(series_list)):
            if dom:
                fetched_series[url] = dom

        season_urls_needed: set[str] = set()
        season_doms: dict[str, HTMLParser] = {}

        for series_url, dom in fetched_series.items():
            h1 = dom.css_first("h1")
            if h1:
                m = re.search(r'Saison\s*(\d+)', h1.text(), re.IGNORECASE)
                if m and int(m.group(1)) != season:
                    for a in dom.css(".wa-sub-block ul.wa-post-list-ofLinks li a"):
                        if f"Saison {season}" in a.text():
                            season_urls_needed.add(_abs(self._base, a.attributes["href"]))
                            break
                    continue
            season_doms[series_url] = dom

        needed_list = list(season_urls_needed)
        for url, dom in zip(needed_list, await self._fetch_many(needed_list)):
            if dom:
                season_doms[url] = dom

        if not season_doms:
            return []

        lang_urls: set[str] = set(season_doms.keys())
        for dom in season_doms.values():
            for block in dom.css(".wa-sub-block"):
                t = block.css_first(".wa-sub-block-title")
                if t and "Autres langues" in t.text():
                    for a in block.css("ul.wa-post-list-ofLinks li a"):
                        lang_urls.add(_abs(self._base, a.attributes["href"]))

        out: list[dict] = []
        for dom in season_doms.values():
            out.extend(self._ep_streams_from_dom(dom, season, episode))

        for page in await self._fetch_many(lang_urls - set(season_doms.keys())):
            if page:
                out.extend(self._ep_streams_from_dom(page, season, episode))
        return out

    def close(self) -> None:
        pass
