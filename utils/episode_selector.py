# Author: adam

"""
utils/episode_selector.py
─────────────────────────
Finds the best matching video file inside a flattened AllDebrid torrent file tree.

Three selection strategies (tried in order):

  1. SINGLE  – only one video file in the torrent → return it directly, no parsing needed.
  2. EPISODE – parse every video filename (with its folder path for context) and find
               the one whose season + episode match the target.
  3. YEAR    – for movie packs / trilogies: match by release year parsed from the filename.

Fallback: if nothing matches explicitly, return the largest video file (best-guess).
"""

import logging
import re

from PTT import parse_title

logger = logging.getLogger(__name__)

# All extensions considered playable video files
_VIDEO_EXT = frozenset({
    ".mkv", ".mp4", ".avi", ".mov", ".ts", ".m2ts",
    ".wmv", ".flv", ".webm", ".mpg", ".mpeg", ".m4v",
})


def find_best_file(
    files: list[dict],
    season: int | None = None,
    episode: int | None = None,
    year: int | None = None,
) -> dict | None:
    """
    Parameters
    ----------
    files   : flat list from AllDebridClient._flatten_tree()
              each dict has keys: n (name), l (link), s (size), path (folder)
    season  : target season number  (None for movies)
    episode : target episode number (None for movies)
    year    : release year hint (used for movie packs)

    Returns the matched file dict or None.
    """
    videos = _filter_videos(files)
    if not videos:
        logger.warning("EpisodeSelector: no video files found in torrent")
        return None

    logger.debug("EpisodeSelector: %d video file(s) to inspect", len(videos))

    # ── Strategy 1: single video → done ──────────────────────────────────────
    if len(videos) == 1:
        logger.debug("EpisodeSelector: single video fast-path → %s", videos[0]["n"])
        return videos[0]

    # ── Strategy 2: episode matching (series) ────────────────────────────────
    if season is not None and episode is not None:
        match = _match_episode(videos, season, episode)
        if match:
            logger.info(
                "EpisodeSelector: S%02dE%02d → %s",
                season, episode, match["n"],
            )
            return match

    # ── Strategy 3: year matching (movie packs / trilogies) ──────────────────
    if year is not None:
        match = _match_year(videos, year)
        if match:
            logger.info("EpisodeSelector: year=%d → %s", year, match["n"])
            return match

    # ── Fallback: largest video ───────────────────────────────────────────────
    best = max(videos, key=lambda f: f.get("s", 0))
    logger.warning(
        "EpisodeSelector: no match found, falling back to largest file → %s",
        best["n"],
    )
    return best


# ─────────────────────────────────────────────────────────────────────────────
# Internals
# ─────────────────────────────────────────────────────────────────────────────

def _filter_videos(files: list[dict]) -> list[dict]:
    """Keep only actual video files (by extension), skip samples."""
    result = []
    for f in files:
        name = f.get("n", "").lower()
        # Skip non-video and sample files
        if not any(name.endswith(ext) for ext in _VIDEO_EXT):
            continue
        if "sample" in name:
            continue
        result.append(f)
    return result


def _parse_file(f: dict) -> dict:
    """
    Parse a file dict using PTT.
    We combine folder path + filename for maximum context
    (e.g. "Breaking.Bad.S03/Breaking.Bad.S03E05.mkv" is richer than the filename alone).
    """
    context = f"{f['n']}".strip()
    return parse_title(context)


def _match_episode(videos: list[dict], target_s: int, target_e: int) -> dict | None:
    """
    Try to find a file matching exactly S[target_s]E[target_e].
    Falls back to a file that contains the target season if no episode list is found
    (e.g. a single-season pack where the episode is implicit from ordering).
    """
    season_candidates = []

    for f in videos:
        parsed     = _parse_file(f)
        file_s     = parsed.get("seasons") or []
        file_e     = parsed.get("episodes") or []

        # Exact season + episode match
        if target_s in file_s and target_e in file_e:
            return f

        # Collect season-only matches as fallback
        if target_s in file_s and not file_e:
            season_candidates.append(f)

    # Season-only fallback: sort by episode number guessed from filename ordering,
    # or by file name alphabetically (usually maps to episode order)
    if season_candidates:
        season_candidates.sort(key=lambda f: f.get("n", ""))
        # Try to pick the Nth file where N = target episode (1-indexed)
        idx = target_e - 1
        if 0 <= idx < len(season_candidates):
            return season_candidates[idx]

    return None


def _match_year(videos: list[dict], year: int) -> dict | None:
    """Match a video file whose parsed year equals the target year."""
    for f in videos:
        parsed = _parse_file(f)
        if parsed.get("year") == year:
            return f
    return None