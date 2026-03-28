"""
config.py
─────────
UserConfig encoded as base64url-JSON in the URL.
"""

import base64
import json
import logging
from dataclasses import dataclass, field

from settings import DEFAULT_LANGUAGES, DEFAULT_MIN_MATCH, DEFAULT_SEARCH_TIMEOUT

logger = logging.getLogger(__name__)

FORMAT_EPURE   = "epure"
FORMAT_COMPACT = "compact"


@dataclass
class UserConfig:
    alldebrid_key:     str        = ""
    languages:         list[str]  = field(default_factory=lambda: list(DEFAULT_LANGUAGES))
    min_match:         float      = DEFAULT_MIN_MATCH
    search_timeout:    float      = DEFAULT_SEARCH_TIMEOUT
    enable_movix:      bool       = True
    movix_url:         str        = ""
    display_format:    str        = FORMAT_EPURE
    torznab_sources:   list[dict] = field(default_factory=list)
    enable_library:    bool       = False
    library_priority:  bool       = False
    remove_non_tv:     bool       = True
    enable_wawacity:      bool       = False
    wawacity_url:         str        = ""
    allowed_resolutions:  list[str]  = field(default_factory=list)

    def encode(self) -> str:
        payload = {
            "ak": self.alldebrid_key,
            "lg": self.languages,
            "mm": self.min_match,
            "st": self.search_timeout,
            "mx": self.enable_movix,
            "mu": self.movix_url,
            "df": self.display_format,
            "tz": self.torznab_sources,
            "el": self.enable_library,
            "lp": self.library_priority,
            "nt": self.remove_non_tv,
            "ew": self.enable_wawacity,
            "wu": self.wawacity_url,
            "ar": self.allowed_resolutions,
        }
        raw = json.dumps(payload, separators=(",", ":"))
        return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")

    @classmethod
    def decode(cls, b64: str) -> "UserConfig":
        try:
            padding = "=" * (-len(b64) % 4)
            raw  = base64.urlsafe_b64decode(b64 + padding)
            data = json.loads(raw)

            # Backward compat: old "lg" was a string, "vo" was vostfr bool
            raw_lg = data.get("lg", DEFAULT_LANGUAGES)
            if isinstance(raw_lg, str):
                languages = [raw_lg]
                if data.get("vo", False):
                    languages.append("vostfr")
            else:
                languages = raw_lg

            return cls(
                alldebrid_key    = data.get("ak", ""),
                languages        = languages,
                min_match        = float(data.get("mm", DEFAULT_MIN_MATCH)),
                search_timeout   = float(data.get("st", DEFAULT_SEARCH_TIMEOUT)),
                enable_movix     = bool(data.get("mx", True)),
                movix_url        = data.get("mu", ""),
                display_format   = data.get("df", FORMAT_EPURE),
                torznab_sources  = data.get("tz", []),
                enable_library   = bool(data.get("el", False)),
                library_priority = bool(data.get("lp", False)),
                remove_non_tv    = bool(data.get("nt", True)),
                enable_wawacity     = bool(data.get("ew", False)),
                wawacity_url        = data.get("wu", ""),
                allowed_resolutions = data.get("ar", []),
            )
        except Exception as exc:
            logger.warning("Config decode error: %s – using defaults", exc)
            return cls()

    def is_valid(self) -> bool:
        return bool(self.alldebrid_key)


def encode_playback_token(
    stream_type: str,
    infohash: str,
    season: int | None = None,
    episode: int | None = None,
    year: int | None = None,
    is_library: bool = False,
    ad_id: int | None = None,
    ddl_id: int | None = None,
    ddl_links: list[str] | None = None,
    ddl_hosts: list[str] | None = None,
) -> str:
    payload: dict = {"t": stream_type}
    if infohash:               payload["h"]  = infohash
    if season     is not None: payload["s"]  = season
    if episode    is not None: payload["e"]  = episode
    if year       is not None: payload["y"]  = year
    if is_library:             payload["lb"] = 1
    if ad_id      is not None: payload["ai"] = ad_id
    if ddl_id     is not None: payload["di"] = ddl_id
    if ddl_links:              payload["dl"] = ddl_links
    if ddl_hosts:              payload["dh"] = ddl_hosts
    raw = json.dumps(payload, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_playback_token(token: str) -> dict:
    padding = "=" * (-len(token) % 4)
    raw  = base64.urlsafe_b64decode(token + padding)
    return json.loads(raw)