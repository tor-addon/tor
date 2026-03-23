"""
router.py
─────────
All Stremio addon routes + config page.

Endpoints:
  GET  /                              → redirect to /configure
  GET  /configure                     → HTML config page
  GET  /{config}/manifest.json        → addon manifest
  GET  /{config}/stream/{type}/{id}   → stream list
  GET  /{config}/playback/{token}     → 307 redirect to resolved URL

Short-lived request dedup cache (TTL=15s):
  Stremio fires the same stream request 2-3 times in quick succession.
  We cache the pipeline result in memory for 15 s to avoid hammering AllDebrid.
  The cache is per-process and never persisted.
"""

import asyncio
import logging
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from config import (
    FORMAT_COMPACT,
    FORMAT_EPURE,
    UserConfig,
    decode_playback_token,
    encode_playback_token,
)
from settings import ADDON_DESCRIPTION, ADDON_ID, ADDON_LOGO, ADDON_NAME, ADDON_VERSION
from stream_manager import StreamManager

logger = logging.getLogger(__name__)
router = APIRouter()

# ── StreamManager pool ──────────────────────────────────────────────────────────
_managers: dict[str, StreamManager] = {}

def _get_manager(config: UserConfig) -> StreamManager:
    key = config.encode()
    if key not in _managers:
        _managers[key] = StreamManager(
            alldebrid_api_key=config.alldebrid_key,
            torznab_sources=config.torznab_sources,
            language=config.language,
            min_match=config.min_match,
            search_timeout=config.search_timeout,
            enable_movix=config.enable_movix,
        )
    return _managers[key]


# ── Short-lived dedup cache (Streams) ───────────────────────────────────────────
_CACHE_TTL = 15.0
_stream_cache: dict[tuple, tuple[float, list]] = {}

def _cache_get(key: tuple) -> list | None:
    entry = _stream_cache.get(key)
    if entry and (time.monotonic() - entry[0]) < _CACHE_TTL:
        return entry[1]
    _stream_cache.pop(key, None)
    return None

def _cache_set(key: tuple, streams: list) -> None:
    now = time.monotonic()
    expired = [k for k, (ts, _) in _stream_cache.items() if now - ts >= _CACHE_TTL]
    for k in expired:
        del _stream_cache[k]
    _stream_cache[key] = (now, streams)


# ── Resolved-URL cache (Playback) ───────────────────────────────────────────────
# Evite de re-résoudre pour chaque range request d'ExoPlayer.
# ExoPlayer sur Android TV frappe le même token 3-5x en quelques secondes.
_RESOLVED_URL_TTL = 45.0  # secondes — au-delà, on re-résout
_resolved_url_cache: dict[str, tuple[float, str]] = {}

def _resolved_cache_get(token: str) -> str | None:
    entry = _resolved_url_cache.get(token)
    if entry and (time.monotonic() - entry[0]) < _RESOLVED_URL_TTL:
        return entry[1]
    _resolved_url_cache.pop(token, None)
    return None

def _resolved_cache_set(token: str, url: str) -> None:
    now = time.monotonic()
    expired = [k for k, (ts, _) in _resolved_url_cache.items() if now - ts >= _RESOLVED_URL_TTL]
    for k in expired:
        del _resolved_url_cache[k]
    _resolved_url_cache[token] = (now, url)


# ── Routes ──────────────────────────────────────────────────────────────────────

@router.get("/")
async def root():
    return RedirectResponse("/configure")


@router.get("/configure", response_class=HTMLResponse)
async def configure_page():
    static_path = Path(__file__).parent / "static" / "configure.html"
    return static_path.read_text(encoding="utf-8")


@router.get("/{b64_config}/configure", response_class=HTMLResponse)
async def configure_preload(request: Request, b64_config: str):
    static_path = Path(__file__).parent / "static" / "configure.html"
    html = static_path.read_text(encoding="utf-8")
    html = html.replace(
        "// PRELOAD_CONFIG_PLACEHOLDER",
        f"const PRELOAD_CONFIG = {repr(b64_config)};",
    )
    return html


@router.get("/manifest.json")
async def manifest_root():
    return RedirectResponse("/configure")


@router.get("/{b64_config}/manifest.json")
async def manifest(request: Request, b64_config: str):
    base_url = str(request.base_url).rstrip("/")
    return JSONResponse(
        _build_manifest(configured=True, base_url=base_url, b64_config=b64_config),
        headers=_cors(),
    )


@router.get("/{b64_config}/stream/{media_type}/{stremio_id:path}")
async def stream(request: Request, b64_config: str, media_type: str, stremio_id: str):
    config = UserConfig.decode(b64_config)

    if not config.is_valid():
        return JSONResponse({"streams": []}, headers=_cors())

    stremio_id = stremio_id.removesuffix(".json")

    imdb_id = stremio_id
    season = None
    episode = None

    if ":" in stremio_id:
        parts = stremio_id.split(":")
        imdb_id = parts[0]
        try:
            season = int(parts[1])
            episode = int(parts[2])
        except (IndexError, ValueError):
            pass

    logger.info("stream  type=%s  id=%s  s=%s e=%s", media_type, imdb_id, season, episode)

    cache_key = (b64_config, imdb_id, season, episode)
    cached = _cache_get(cache_key)
    if cached is not None:
        logger.info("stream cache HIT for %s – skipping pipeline", imdb_id)
        base_url = str(request.base_url).rstrip("/")
        return JSONResponse(
            {
                "streams": [
                    _format_stream(s, b64_config, base_url, season, episode, config.display_format)
                    for s in cached
                ]
            },
            headers=_cors(),
        )

    manager = _get_manager(config)

    try:
        streams = await manager.get_streams(imdb_id, season=season, episode=episode)
    except Exception as exc:
        logger.error("stream pipeline error: %s", exc, exc_info=True)
        return JSONResponse({"streams": []}, headers=_cors())

    if not streams:
        return JSONResponse({"streams": []}, headers=_cors())

    _cache_set(cache_key, streams)

    base_url = str(request.base_url).rstrip("/")
    stremio_streams = [
        _format_stream(s, b64_config, base_url, season, episode, config.display_format)
        for s in streams
    ]

    logger.info("stream: returning %d stream(s)", len(stremio_streams))
    return JSONResponse({"streams": stremio_streams}, headers=_cors())


@router.get("/{b64_config}/playback/{token}")
async def playback(b64_config: str, token: str, request: Request):
    config = UserConfig.decode(b64_config)
    if not config.is_valid():
        raise HTTPException(status_code=400, detail="Invalid config")

    try:
        info = decode_playback_token(token)
    except Exception as exc:
        logger.error("playback: invalid token: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid token")

    # ── Cache hit : même token → même URL → ExoPlayer ne se perd pas ──────────
    cache_key = f"{b64_config}:{token}"
    cached_url = _resolved_cache_get(cache_key)
    if cached_url:
        logger.info("playback cache HIT → %s…", cached_url[:60])
        return RedirectResponse(
            cached_url,
            status_code=307,
            headers={**_cors(), "Accept-Ranges": "bytes", "Cache-Control": "no-store"},
        )

    stream_type = info.get("t", "torrent")
    infohash    = info.get("h", "")
    season      = info.get("s")
    episode     = info.get("e")
    year        = info.get("y")

    logger.info("playback  type=%s  hash=%s…  s=%s e=%s", stream_type, infohash[:12], season, episode)

    manager = _get_manager(config)
    stream_dict = {"stream_type": stream_type, "infohash": infohash}

    try:
        url = await manager.resolve_stream(stream_dict, season=season, episode=episode, year=year)
    except Exception as exc:
        logger.error("playback resolve error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Resolution failed")

    if not url:
        raise HTTPException(status_code=404, detail="Could not resolve stream")

    # ── Mise en cache avant redirect ───────────────────────────────────────────
    _resolved_cache_set(cache_key, url)

    logger.info("playback → %s…", url[:60])

    # 307 au lieu de 302 pour forcer le client à conserver les headers d'origine (Range)
    return RedirectResponse(
        url,
        status_code=307,
        headers={**_cors(), "Accept-Ranges": "bytes", "Cache-Control": "no-store"},
    )


# ── Helpers ─────────────────────────────────────────────────────────────────────

def _build_manifest(configured: bool = False, base_url: str = "", b64_config: str = "") -> dict:
    manifest = {
        "id":          ADDON_ID,
        "version":     ADDON_VERSION,
        "name":        ADDON_NAME,
        "description": ADDON_DESCRIPTION,
        "logo":        ADDON_LOGO,
        "resources":   ["stream"],
        "types":       ["movie", "series"],
        "idPrefixes":  ["tt"],
        "catalogs":    [],
        "behaviorHints": {
            "configurable":          True,
            "configurationRequired": not configured,
        },
    }
    if configured and base_url and b64_config:
        manifest["behaviorHints"]["configureUrl"] = f"{base_url}/{b64_config}/configure"
    return manifest


def _format_stream(
    stream: dict,
    b64_config: str,
    base_url: str,
    season: int | None,
    episode: int | None,
    display_format: str = FORMAT_EPURE,
) -> dict:
    stream_type = stream.get("stream_type", "torrent")
    infohash    = stream.get("infohash", "")
    year        = stream.get("year")

    token = encode_playback_token(
        stream_type=stream_type,
        infohash=infohash,
        season=season,
        episode=episode,
        year=year,
    )

    source       = stream.get("source", "?")
    resolution   = stream.get("resolution") or "?"
    quality      = stream.get("quality") or ""
    size_fmt     = stream.get("size_fmt") or ""     # human-readable for display
    langs        = stream.get("languages") or []
    lang_str     = " ".join(l.upper() for l in langs) if langs else ""
    torrent_name = stream.get("torrent_name") or stream.get("title") or ""
    hdr          = stream.get("hdr") or []
    audio        = stream.get("audio") or []

    try:
        size_bytes = int(stream.get("size") or 0)
    except (ValueError, TypeError):
        size_bytes = 0

    hdr_tags  = [h for h in hdr if h != "SDR"]
    hdr_str   = " ".join(hdr_tags) if hdr_tags else ""
    audio_str = " ".join(audio[:2]) if audio else ""

    dot = "🟣" if stream_type == "ddl" else "🔵"

    if display_format == FORMAT_EPURE:
        name = f"Tor\n{source}"

        line1_parts = [f"{dot} {resolution}"]
        if quality:
            line1_parts.append(quality)
        if hdr_str:
            line1_parts.append(hdr_str)
        line1 = " | ".join(line1_parts)

        line2_parts = []
        if lang_str:
            line2_parts.append(f"🌐 {lang_str}")
        if size_fmt:
            line2_parts.append(size_fmt)
        if audio_str:
            line2_parts.append(audio_str)
        line2 = " | ".join(line2_parts)

        tn = torrent_name if len(torrent_name) <= 60 else torrent_name[:57] + "…"
        line3 = f"🗂️ {tn}" if tn else ""

        parts = [line1]
        if line2:
            parts.append(line2)
        if line3:
            parts.append(line3)

        description = "\n".join(parts)

    else:  # FORMAT_COMPACT
        name = quality or resolution
        right_parts = [p for p in [resolution, size_fmt] if p]
        description = " • ".join(right_parts)

    behavior_hints: dict = {
        "notWebReady": True,
        "bingeGroup":  f"{ADDON_ID}-{resolution}",
    }

    if size_bytes and size_bytes > 0:
        behavior_hints["videoSize"] = size_bytes

    if torrent_name:
        fname = torrent_name if torrent_name.lower().endswith(".mkv") else torrent_name + ".mkv"
        behavior_hints["filename"] = fname

    return {
        "name":          name,
        "description":   description,
        "url":           f"{base_url}/{b64_config}/playback/{token}",
        "behaviorHints": behavior_hints,
    }

def _cors() -> dict:
    return {
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Headers": "*",
    }