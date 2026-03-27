"""
services/wawacity.py
─────────────────────
Wawacity DDL scraper. Synchronous – call via asyncio.to_thread().
Uses ThreadPoolExecutor internally for parallel page fetches.

Flow:
  get_streams(titles, is_serie, season, episode) → pipeline-compatible stream dicts
  Resolution at playback time (concurrent host race):
    infohash = "wawa_<b64url_json_list_of_links>"
    → AllDebrid.redirector_link(link) concurrently for all links
    → first success → AllDebrid.unlock_link(resolved) → CDN URL

Filtering: DDL streams go through the same torrent filtering path (PTT + all checks).
Pre-set only: languages (from page meta), year (scraped from HTML), seasons/episodes (series).
PTT parses quality/resolution from torrent_name (filename or full h1 title).
"""

import base64
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote_plus

import requests
from selectolax.parser import HTMLParser

from settings import DDL_ALLOWED_HOSTS, WAWACITY_BASE_URL

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ── Language → ISO codes ──────────────────────────────────────────────────────
_LANG_MAP: dict[str, list[str]] = {
    "vf":             ["fr"],
    "vff":            ["fr"],
    "vfq":            ["fr"],
    "french":         ["fr"],
    "truefrench":     ["fr"],
    "true french":    ["fr"],
    "vostfr":         ["vostfr"],
    "vost":           ["vostfr"],
    "multi":          ["multi", "fr"],
    "multi (french)": ["multi", "fr"],
    "multi(french)":  ["multi", "fr"],
    "multi (truefrench)": ["multi", "fr"],
    "multi(truefrench)":  ["multi", "fr"],
    "vo":             [],
}

_RE_EPISODE = re.compile(r'(?:épisode|episode)\s*(\d+)', re.IGNORECASE)
_RE_PARTIE  = re.compile(r'partie\s*\d+', re.IGNORECASE)
_RE_EXT     = re.compile(r'\.\w{2,4}$')
_RE_NAV     = re.compile(r'^[^»]*»\s*')     # strips "Films » " etc. from h1


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


def _encode_links(links: list[str]) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(links, separators=(",", ":")).encode()
    ).decode().rstrip("=")


def _abs(base: str, path: str) -> str:
    if path.startswith("http"):
        return path
    return f"{base}/?{path[1:]}" if path.startswith("?") else f"{base}{path}"


class WawacityClient:
    __slots__ = ("_base", "_timeout", "session")

    def __init__(self, base_url: str = "", timeout: float = 6.0) -> None:
        self._base    = (base_url or WAWACITY_BASE_URL).rstrip("/")
        self._timeout = timeout
        self.session  = requests.Session()
        self.session.headers.update(_HEADERS)

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def get_streams(
        self,
        titles: list[str],
        is_serie: bool,
        season: int | None = None,
        episode: int | None = None,
    ) -> list[dict]:
        title = titles[0] if titles else ""
        if not title:
            return []
        try:
            if is_serie:
                streams = self._get_episode(title, season or 1, episode or 1)
            else:
                streams = self._get_movie(title)
        except Exception as exc:
            logger.error("Wawacity │ get_streams error: %s", exc)
            return []

        logger.info("Wawacity │ %d stream(s) for %r", len(streams), title)
        return streams

    # ─────────────────────────────────────────────────────────────────────────
    # HTTP helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _fetch(self, url: str) -> HTMLParser | None:
        try:
            r = self.session.get(url, timeout=self._timeout)
            return HTMLParser(r.text) if r.status_code == 200 else None
        except Exception as e:
            logger.debug("Wawacity │ fetch error %s: %s", url[:60], e)
            return None

    def _fetch_many(self, urls: set[str]) -> dict[str, HTMLParser | None]:
        if len(urls) == 1:
            u = next(iter(urls))
            return {u: self._fetch(u)}
        with ThreadPoolExecutor(max_workers=min(len(urls), 8)) as pool:
            futs = {pool.submit(self._fetch, u): u for u in urls}
            return {futs[f]: f.result() for f in as_completed(futs)}

    # ─────────────────────────────────────────────────────────────────────────
    # Page metadata helpers
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
        # Year from detail-list
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
        """Return h1 text stripped of navigation prefix (e.g. 'Films » ')."""
        h1 = dom.css_first("h1")
        if not h1:
            return ""
        return _RE_NAV.sub("", h1.text(strip=True)).strip()

    # ─────────────────────────────────────────────────────────────────────────
    # Link-row extraction (movies + episodes)
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_link_row(self, row) -> dict | None:
        """
        Extract a single DDL link from a table row.
        Returns None if the row is not a valid, allowed, single-file link.
        """
        # Host cell
        td = row.css_first('td[width="120px"]') or row.css_first('td[width="80px"]')
        if not td:
            return None
        host = td.text(strip=True).lower()
        if host not in DDL_ALLOWED_HOSTS:
            return None

        a = row.css_first("a.link")
        if not a:
            return None

        # Skip multi-part file rows ("Partie N:")
        b_tag = a.css_first("b")
        if b_tag and _RE_PARTIE.search(b_tag.text(strip=True)):
            return None

        href = a.attributes.get("href", "")
        if not href:
            return None

        # Extract filename from link text (after the bold label)
        fn = ""
        full_text = a.text(strip=True)
        b_text    = b_tag.text(strip=True) if b_tag else ""
        candidate = full_text.replace(b_text, "").strip()
        if _RE_EXT.search(candidate):
            fn = candidate

        return {
            "link":     _abs(self._base, href),
            "host":     td.text(strip=True),
            "filename": fn,
        }

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
        """Build a single stream dict from a set of DDL table rows (one quality variant)."""
        links:    list[str] = []
        hosts:    list[str] = []
        filename: str       = ""

        for row in rows:
            info = self._extract_link_row(row)
            if not info:
                continue
            links.append(info["link"])
            hosts.append(info["host"])
            if not filename and info["filename"]:
                filename = info["filename"]

        if not links:
            return None

        # torrent_name: filename (ideal for PTT quality parsing) else full h1 title
        torrent_name = filename or h1_title

        stream: dict = {
            "torrent_name": torrent_name,
            "infohash":     f"wawa_{_encode_links(links)}",
            "source":       "Wawacity",
            "stream_type":  "ddl",
            "cached":       True,
            "languages":    _map_lang(language),
            "year":         year,
            "size":         size,
            "hosts":        hosts,
            "seeders":      0,
            "valid":        False,
        }

        if is_serie and season is not None:
            stream["seasons"]  = [season]
        if is_serie and episode is not None:
            stream["episodes"] = [episode]

        return stream

    # ─────────────────────────────────────────────────────────────────────────
    # Movie
    # ─────────────────────────────────────────────────────────────────────────

    def _get_movie(self, query: str) -> list[dict]:
        dom = self._fetch(f"{self._base}/?p=films&search={quote_plus(query)}")
        if not dom:
            return []
        first = dom.css_first("#wa-mid-blocks .wa-post-detail-item .wa-sub-block-title > a")
        if not first:
            return []

        base_url = _abs(self._base, first.attributes["href"])
        base_dom = self._fetch(base_url)
        if not base_dom:
            return []

        # Collect all quality-variant pages
        urls: set[str] = {base_url}
        for a in base_dom.css('a[href^="?p=film&id="]'):
            if a.css_first("button"):
                urls.add(_abs(self._base, a.attributes["href"]))

        pages = self._fetch_many(urls)
        out: list[dict] = []
        for page in pages.values():
            if not page:
                continue
            h1_title          = self._page_h1(page)
            lang, _, size, yr = self._page_info(page)
            rows              = page.css("#DDLLinks tr.link-row")
            s = self._rows_to_stream(rows, h1_title, lang, size, yr, False, None, None)
            if s:
                out.append(s)
        return out

    # ─────────────────────────────────────────────────────────────────────────
    # Episode
    # ─────────────────────────────────────────────────────────────────────────

    def _season_url(self, query: str, season: int) -> str | None:
        dom = self._fetch(f"{self._base}/?p=series&search={quote_plus(query)}")
        if not dom:
            return None
        first = dom.css_first("#wa-mid-blocks .wa-post-detail-item .wa-sub-block-title > a")
        if not first:
            return None
        url = _abs(self._base, first.attributes["href"])
        dom = self._fetch(url)
        if not dom:
            return None
        h1 = dom.css_first("h1")
        if h1:
            m = re.search(r'Saison\s*(\d+)', h1.text(), re.IGNORECASE)
            if m and int(m.group(1)) != season:
                for a in dom.css(".wa-sub-block ul.wa-post-list-ofLinks li a"):
                    if f"Saison {season}" in a.text():
                        return _abs(self._base, a.attributes["href"])
                return None
        return url

    def _ep_streams_from_dom(
        self,
        dom: HTMLParser,
        season: int,
        episode: int,
    ) -> list[dict]:
        table = dom.css_first("#DDLLinks")
        if not table:
            return []

        h1_title          = self._page_h1(dom)
        lang, _, size, yr = self._page_info(dom)

        # Collect rows belonging to the target episode
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

        s = self._rows_to_stream(ep_rows, h1_title, lang, size, yr, True, season, episode)
        return [s] if s else []

    def _get_episode(self, query: str, season: int, episode: int) -> list[dict]:
        season_url = self._season_url(query, season)
        if not season_url:
            return []
        dom = self._fetch(season_url)
        if not dom:
            return []

        # Collect all language-variant pages for the same season
        urls: set[str] = {season_url}
        for block in dom.css(".wa-sub-block"):
            t = block.css_first(".wa-sub-block-title")
            if t and "Autres langues" in t.text():
                for a in block.css("ul.wa-post-list-ofLinks li a"):
                    urls.add(_abs(self._base, a.attributes["href"]))

        pages = self._fetch_many(urls)
        out: list[dict] = []
        for page in pages.values():
            if page:
                out.extend(self._ep_streams_from_dom(page, season, episode))
        return out

    # ─────────────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self.session.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

_SIZE_UNITS = {
    "o":  1, "ko": 1_000, "mo": 1_000_000,
    "go": 1_000_000_000, "to": 1_000_000_000_000,
}


def _parse_size(raw: str) -> int:
    m = re.match(r"([\d.,]+)\s*([a-zA-Z]+)", raw.strip())
    if not m:
        return 0
    factor = _SIZE_UNITS.get(m.group(2).lower(), 0)
    return int(float(m.group(1).replace(",", ".")) * factor) if factor else 0