"""
utils/ranking.py
────────────────
Score hierarchy (gaps guarantee strict priority order):

    Resolution  800 000 / 400 000 / 200 000 / 100 000
    Quality     100 000 (remux) → 60 000 (bluray) → 40 000 (web) → 20 000 (hdtv) → −500 000 (cam)
    Pack        20 000   ← series packs between hdtv and web, always beats bare episode
    Size        × 50 pts/GiB  → max ~15 000 for a 300 GiB file  (never overrides quality gap)
    Seeders     capped at 50 pts  (micro tie-breaker only)

    Library     +1 000 000 when library_priority=True (pins Library results to top)

Correctness:
  • REMUX 2160p (900k) > BluRay 2160p (860k) regardless of size         ✓
  • BluRay 2160p 50 GB (862 500) > BluRay 2160p 6 GB (860 300)          ✓
  • BluRay 2160p (860k) > BluRay 1080p REMUX (500k)                     ✓
  • 300 GB WEBRip (55k) never beats a BluRay (60k) in same resolution   ✓
"""

import logging

logger = logging.getLogger(__name__)

_RESOLUTION: dict[str, int] = {
    "2160p": 800_000,
    "4k":    800_000,
    "1080p": 400_000,
    "720p":  200_000,
    "480p":  100_000,
}

_QUALITY: dict[str, int] = {
    "bluray remux": 100_000,
    "remux":        100_000,
    "bluray":        60_000,
    "web-dl":        40_000,
    "web":           40_000,
    "webrip":        40_000,
    "hdtv":          20_000,
    "cam":         -500_000,
}

_PACK_BONUS   = 20_000
_SIZE_MULT    = 50      # pts per GiB – max ~15 000 for 300 GiB
_SEEDERS_CAP  = 50
LIBRARY_BONUS = 1_000_000


def rank(stream: dict) -> dict:
    """Adds / updates stream['rank'] in-place and returns the dict."""
    score = 0

    res  = stream.get("resolution")
    qual = stream.get("quality")

    if res:
        score += _RESOLUTION.get(res.lower(), 0)
    if qual:
        score += _QUALITY.get(qual.lower(), 0)

    # Pack bonus: complete flag OR season pack (seasons set, no specific episode)
    if stream.get("complete") or (stream.get("seasons") and not stream.get("episodes")):
        score += _PACK_BONUS

    # Size: bitshift >> 30 is integer GiB division, faster than / 1_073_741_824
    try:
        score += (int(stream.get("size") or 0) >> 30) * _SIZE_MULT
    except (ValueError, TypeError):
        pass

    score += min(int(stream.get("seeders") or 0), _SEEDERS_CAP)

    stream["rank"] = score
    logger.debug(
        "RANK %7d  res=%-6s  qual=%-14s  %s",
        score,
        stream.get("resolution", "?"),
        stream.get("quality", "?"),
        (stream.get("torrent_name") or "?")[:60],
    )
    return stream


def sort_streams(streams: list[dict]) -> list[dict]:
    """Sorts in-place by rank descending. Returns the same list."""
    streams.sort(key=lambda s: s["rank"], reverse=True)
    return streams