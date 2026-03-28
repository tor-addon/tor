"""
utils/episode_selector.py
─────────────────────────
Finds the best matching video file inside a flattened AllDebrid torrent file tree.

Three selection strategies (tried in order):

  1. SINGLE  – only one video file → return directly, no parsing needed.
  2. EPISODE – parse each filename and match season + episode.
  3. YEAR    – for movie packs / trilogies: match by release year.

Fallback: return the largest video file.
"""

import logging
import re

from PTT import parse_title

logger = logging.getLogger(__name__)

_VIDEO_EXT: frozenset[str] = frozenset({
    ".mkv", ".mp4", ".avi", ".mov", ".ts", ".m2ts",
    ".wmv", ".flv", ".webm", ".mpg", ".mpeg", ".m4v",
})


def find_best_file(
    files: list[dict],
    season: int | None = None,
    episode: int | None = None,
    year: int | None = None,
) -> dict | None:
    videos = _filter_videos(files)
    if not videos:
        logger.warning("EpisodeSelector: no video files found in torrent")
        return None

    logger.debug("EpisodeSelector: %d video file(s) to inspect", len(videos))

    if len(videos) == 1:
        logger.debug("EpisodeSelector: single video fast-path → %s", videos[0]["n"])
        return videos[0]

    # Parse all files once – reused by both _match_episode and _match_year
    parsed_videos = [(f, parse_title(f["n"])) for f in videos]

    if season is not None and episode is not None:
        match = _match_episode(parsed_videos, season, episode)
        if match:
            logger.info("EpisodeSelector: S%02dE%02d → %s", season, episode, match["n"])
            return match

    if year is not None:
        match = _match_year(parsed_videos, year)
        if match:
            logger.info("EpisodeSelector: year=%d → %s", year, match["n"])
            return match

    best = max(videos, key=lambda f: f.get("s", 0))
    logger.warning("EpisodeSelector: no match, falling back to largest → %s", best["n"])
    return best


def _filter_videos(files: list[dict]) -> list[dict]:
    """Keep playable video files (O(1) extension lookup), skip samples."""
    result = []
    for f in files:
        name = f.get("n", "")
        dot  = name.rfind(".")
        if dot == -1 or name[dot:].lower() not in _VIDEO_EXT:
            continue
        if "sample" in name.lower():
            continue
        result.append(f)
    return result


def _natural_key(f: dict) -> list:
    """Natural sort key: numeric parts compared as ints (avoids '10' < '2')."""
    parts = re.split(r"(\d+)", f.get("n", ""))
    return [int(p) if p.isdigit() else p.lower() for p in parts]


def _match_episode(
    parsed_videos: list[tuple[dict, dict]],
    target_s: int,
    target_e: int,
) -> dict | None:
    season_candidates = []
    for f, parsed in parsed_videos:
        file_s = parsed.get("seasons") or []
        file_e = parsed.get("episodes") or []
        if target_s in file_s and target_e in file_e:
            return f
        if target_s in file_s and not file_e:
            season_candidates.append(f)

    if season_candidates:
        season_candidates.sort(key=_natural_key)
        idx = target_e - 1
        if 0 <= idx < len(season_candidates):
            return season_candidates[idx]
    return None


def _match_year(parsed_videos: list[tuple[dict, dict]], year: int) -> dict | None:
    for f, parsed in parsed_videos:
        if parsed.get("year") == year:
            return f
    return None