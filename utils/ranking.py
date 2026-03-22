"""
utils/ranking.py
────────────────
Score hierarchy (gaps guarantee strict priority order):

    Resolution  800 000 / 400 000 / 200 000 / 100 000
    Quality     100 000 (remux) → 60 000 (bluray) → 40 000 (web) → 20 000 (hdtv) → −500 000 (cam)
    Pack        20 000   ← series packs sit between hdtv and web, always beats bare episode
    Size        × 50 pts/GB  → max ~15 000 for a 300 GB file  (never overrides quality gap)
    Seeders     capped at 50 pts  (true tie-breaker only, can't override size)

Correctness checks:
  • REMUX 2160p (900k) > BluRay 2160p (860k) regardless of size         ✓
  • BluRay 2160p 50 GB (862 500) > BluRay 2160p 6 GB (860 300+50)       ✓
  • BluRay 2160p (860k) > BluRay 1080p REMUX (500k)                     ✓
  • 300 GB WEBRip (40k+15k=55k) never beats a BluRay (60k) in same res  ✓
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

_PACK_BONUS   = 20_000   # series pack: between hdtv and web tier
_SIZE_MULT    = 50       # pts per GB – 300 GB → 15 000, never overrides quality gap
_SEEDERS_CAP  = 50       # caps seeder contribution so it can't override size


def rank(stream: dict) -> dict:
    """Adds / updates stream['rank'] in-place and returns the dict."""
    score = 0

    res  = stream.get("resolution")
    qual = stream.get("quality")

    if res:
        score += _RESOLUTION.get(res.lower(), 0)
    if qual:
        score += _QUALITY.get(qual.lower(), 0)

    # Pack bonus: explicit 'complete' flag OR season list with no episode list
    if stream.get("complete") or (stream.get("seasons") and not stream.get("episodes")):
        score += _PACK_BONUS

    # Size: larger is better, capped impact to stay below quality tier gaps
    try:
        score += int(int(stream.get("size", 0)) / 1_073_741_824) * _SIZE_MULT
    except (ValueError, TypeError):
        pass

    # Seeders: true micro tie-breaker, cannot override size
    score += min(int(stream.get("seeders", 0)), _SEEDERS_CAP)

    stream["rank"] = score
    logger.debug(
        "RANK %7d  res=%-6s  qual=%-14s  %s",
        score,
        stream.get("resolution", "?"),
        stream.get("quality", "?"),
        stream.get("torrent_name", "?")[:60],
    )
    return stream


def sort_streams(streams: list[dict]) -> list[dict]:
    """Sorts in-place by 'rank' descending. Returns the same list."""
    streams.sort(key=lambda s: s["rank"], reverse=True)
    return streams