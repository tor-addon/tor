"""
wawa_debrid.py — Wawacity scraper + AllDebrid semaphore validator
"""

import asyncio
import json
import re
from urllib.parse import quote_plus

import httpx
from selectolax.parser import HTMLParser

# ── Config ────────────────────────────────────────────────────────────────────

BASE            = "https://www.wawacity.golf"
ALLDEBRID_TOKEN = "JxFva2FfhzjQWTUbvSr5"
ALLDEBRID_URL   = "https://api.alldebrid.com/v4/link/redirector"
CONCURRENCY     = 12
TIMEOUT         = 4.0

ALLOWED_HOSTERS: frozenset[str] = frozenset({"1fichier", "turbobit", "rapidgator", "alldebrid"})

_SIZE_UNITS = {
    "o": 8, "ko": 8_000, "mo": 8_000_000,
    "go": 8_000_000_000, "to": 8_000_000_000_000,
}

_RE_LINK_LABEL = re.compile(r'^Lien\s*\d+\s*:\s*', re.IGNORECASE)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_size(raw: str) -> int:
    m = re.match(r"([\d.,]+)\s*([a-zA-Z]+)", raw.strip())
    if not m:
        return -1
    factor = _SIZE_UNITS.get(m.group(2).lower(), -1)
    return int(float(m.group(1).replace(",", ".")) * factor) if factor != -1 else -1


def _abs(path: str) -> str:
    if path.startswith("http"):
        return path
    return f"{BASE}/?{path[1:]}" if path.startswith("?") else f"{BASE}{path}"


def _link_filename(a) -> str:
    """Extract filename from a link anchor (movies only). Returns '' for series watch/dl links."""
    raw = a.text(strip=True)
    cleaned = _RE_LINK_LABEL.sub("", raw).strip()
    # If it looks like a real filename (has extension), keep it; else empty
    return cleaned if re.search(r'\.\w{2,4}$', cleaned) else ""


# ── Scraper ───────────────────────────────────────────────────────────────────

class WawacityScraper:

    __slots__ = ("_c",)

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._c = client

    async def _fetch(self, url: str) -> HTMLParser | None:
        try:
            r = await self._c.get(url)
            return HTMLParser(r.text) if r.status_code == 200 else None
        except Exception:
            return None

    @staticmethod
    def _page_meta(dom: HTMLParser) -> tuple[str, str]:
        """Return (language, quality) from the file info block."""
        lang = qual = ""
        for li in dom.css("ul.item-list li"):
            text = li.text(strip=True).lower()
            b    = li.css_first("b")
            val  = b.text(strip=True) if b else ""
            if text.startswith("langue"):
                lang = val
            elif text.startswith("qualit"):
                qual = val
        return lang, qual

    @staticmethod
    def _size(dom: HTMLParser) -> int:
        for li in dom.css("ul.item-list li"):
            if li.text(strip=True).lower().startswith("taille"):
                b = li.css_first("b")
                return _parse_size(b.text(strip=True) if b else li.text(strip=True).split(":", 1)[-1])
        return -1

    @staticmethod
    def _title(dom: HTMLParser, prefix: str) -> str:
        h1 = dom.css_first("h1")
        return h1.text(strip=True).replace(prefix, "").strip() if h1 else "?"

    def _rows_to_streams(
        self,
        rows,
        title: str,
        size: int,
        language: str,
        quality: str,
    ) -> list[dict]:
        out = []
        for row in rows:
            td = row.css_first('td[width="120px"]')
            if not td or td.text(strip=True).lower() not in ALLOWED_HOSTERS:
                continue
            a = row.css_first("a.link")
            if not a:
                continue
            href = a.attributes.get("href", "")
            if not href:
                continue
            out.append({
                "title":    title,
                "size":     size,
                "language": language,
                "quality":  quality,
                "filename": _link_filename(a),
                "provider": td.text(strip=True),
                "link":     _abs(href),
            })
        return out

    # Movie ───────────────────────────────────────────────────────────────────

    async def get_movie(self, query: str) -> list[dict]:
        dom = await self._fetch(f"{BASE}/?p=films&search={quote_plus(query)}")
        if not dom:
            return []
        first = dom.css_first("#wa-mid-blocks .wa-post-detail-item .wa-sub-block-title > a")
        if not first:
            return []

        base_url = _abs(first.attributes["href"])
        base_dom = await self._fetch(base_url)
        if not base_dom:
            return []

        urls: set[str] = {base_url}
        for a in base_dom.css('a[href^="?p=film&id="]'):
            if a.css_first("button"):
                urls.add(_abs(a.attributes["href"]))

        pages = await asyncio.gather(*[self._fetch(u) for u in urls])
        out: list[dict] = []
        for page in pages:
            if not page:
                continue
            title        = self._title(page, "Films »")
            size         = self._size(page)
            language, quality = self._page_meta(page)
            rows         = page.css("#DDLLinks tr.link-row, #streamLinks tr.link-row")
            out.extend(self._rows_to_streams(rows, title, size, language, quality))
        return out

    # Episode ─────────────────────────────────────────────────────────────────

    async def _season_url(self, query: str, season: int) -> str | None:
        dom = await self._fetch(f"{BASE}/?p=series&search={quote_plus(query)}")
        if not dom:
            return None
        first = dom.css_first("#wa-mid-blocks .wa-post-detail-item .wa-sub-block-title > a")
        if not first:
            return None

        dom = await self._fetch(_abs(first.attributes["href"]))
        if not dom:
            return None

        h1 = dom.css_first("h1")
        if h1:
            m = re.search(r'Saison\s*(\d+)', h1.text(), re.IGNORECASE)
            if m and int(m.group(1)) != season:
                for a in dom.css(".wa-sub-block ul.wa-post-list-ofLinks li a"):
                    if f"Saison {season}" in a.text():
                        return _abs(a.attributes["href"])
                return None
        return _abs(first.attributes["href"])

    async def _ep_streams(self, url: str, episode: int) -> list[dict]:
        dom = await self._fetch(url)
        if not dom:
            return []
        table = dom.css_first("#DDLLinks")
        if not table:
            return []

        title            = self._title(dom, "Series »")
        language, quality = self._page_meta(dom)

        # For series, quality often lives only in the H1 suffix (e.g. "VF HD")
        # Fall back to parsing the H1 italic tag if quality is empty
        if not quality:
            h1 = dom.css_first("h1 i")
            if h1:
                quality = h1.text(strip=True).lstrip("- ").strip()

        cur_ep, ep_rows = None, []
        for row in table.css("tr"):
            cls = row.attributes.get("class", "")
            if "title" in cls:
                m = re.search(r'(?:Épisode|Episode)\s*(\d+)', row.text(), re.IGNORECASE)
                cur_ep = int(m.group(1)) if m else None
            elif "link-row" in cls and cur_ep == episode:
                ep_rows.append(row)

        if not ep_rows:
            return []
        td   = ep_rows[0].css_first('td[width="80px"]')
        size = _parse_size(td.text(strip=True)) if td else -1
        return self._rows_to_streams(ep_rows, title, size, language, quality)

    async def get_episode(self, query: str, season: int, episode: int) -> list[dict]:
        season_url = await self._season_url(query, season)
        if not season_url:
            return []
        dom = await self._fetch(season_url)
        if not dom:
            return []

        urls: set[str] = {season_url}
        for block in dom.css(".wa-sub-block"):
            t = block.css_first(".wa-sub-block-title")
            if t and "Autres langues/qualités" in t.text():
                for a in block.css("ul.wa-post-list-ofLinks li a"):
                    urls.add(_abs(a.attributes["href"]))

        results = await asyncio.gather(*[self._ep_streams(u, episode) for u in urls])
        return [s for sub in results for s in sub]


# ── AllDebrid semaphore validator ─────────────────────────────────────────────

async def _check(sem: asyncio.Semaphore, client: httpx.AsyncClient, stream: dict) -> dict:
    async with sem:
        try:
            r = await client.get(ALLDEBRID_URL, params={"link": stream["link"]}, timeout=TIMEOUT)
            data = r.json()
            if data.get("status") == "success":
                links = data.get("data", {}).get("links", [])
                if links:
                    return {**stream, "status": "valid", "resolved_links": links}
            return {**stream, "status": "invalid",
                    "error": data.get("error", {}).get("message", "no links")}
        except httpx.TimeoutException:
            return {**stream, "status": "timeout"}
        except Exception as e:
            return {**stream, "status": "error", "error": str(e)}


async def validate(streams: list[dict]) -> list[dict]:
    """Keep up to CONCURRENCY requests in-flight at all times — no sleep."""
    sem     = asyncio.Semaphore(CONCURRENCY)
    headers = {"Authorization": f"Bearer {ALLDEBRID_TOKEN}", "User-Agent": "Mozilla/5.0"}
    limits  = httpx.Limits(max_connections=CONCURRENCY, max_keepalive_connections=CONCURRENCY)
    async with httpx.AsyncClient(headers=headers, limits=limits, follow_redirects=True) as client:
        return list(await asyncio.gather(*[_check(sem, client, s) for s in streams]))


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    limits  = httpx.Limits(max_connections=30, max_keepalive_connections=20)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    async with httpx.AsyncClient(
        timeout=TIMEOUT, limits=limits, headers=headers, follow_redirects=True,
    ) as http:
        scraper = WawacityScraper(http)

        # ── Example: movie ────────────────────────────────────────────────────
        print("Scraping: Marty Supreme …")
        streams = await scraper.get_movie("Marty Supreme")

        # ── Example: episode (uncomment to use) ───────────────────────────────
        #print("Scraping: Outer Banks S01E04 …")
        #streams = await scraper.get_episode("Outer Banks", season=2, episode=6)

    print(f"{len(streams)} stream(s) found — validating (concurrency={CONCURRENCY}) …\n")
    print(streams)
    all_results = await validate(streams)
    valid       = [r for r in all_results if r.get("status") == "valid"]

    print(json.dumps({
        "total_found": len(streams),
        "total_valid": len(valid),
        "streams":     valid,
    }, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())