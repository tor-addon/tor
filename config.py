"""
config.py
─────────
UserConfig encoded as base64url-JSON in the URL.
"""

import base64
import json
import logging
from dataclasses import dataclass, field

from settings import DEFAULT_LANGUAGE, DEFAULT_MIN_MATCH, DEFAULT_SEARCH_TIMEOUT

logger = logging.getLogger(__name__)

FORMAT_EPURE   = "epure"
FORMAT_COMPACT = "compact"


@dataclass
class UserConfig:
    alldebrid_key:    str        = ""
    language:         str        = DEFAULT_LANGUAGE
    vostfr:           bool       = False
    min_match:        float      = DEFAULT_MIN_MATCH
    search_timeout:   float      = DEFAULT_SEARCH_TIMEOUT
    enable_movix:     bool       = True
    movix_url:        str        = ""
    display_format:   str        = FORMAT_EPURE
    torznab_sources:  list[dict] = field(default_factory=list)
    enable_library:   bool       = False   # search AllDebrid personal library
    library_priority: bool       = False   # pin library results to top (+1 000 000 rank)

    def encode(self) -> str:
        payload = {
            "ak": self.alldebrid_key,
            "lg": self.language,
            "vo": self.vostfr,
            "mm": self.min_match,
            "st": self.search_timeout,
            "mx": self.enable_movix,
            "mu": self.movix_url,
            "df": self.display_format,
            "tz": self.torznab_sources,
            "el": self.enable_library,
            "lp": self.library_priority,
        }
        raw = json.dumps(payload, separators=(",", ":"))
        return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")

    @classmethod
    def decode(cls, b64: str) -> "UserConfig":
        try:
            padding = "=" * (-len(b64) % 4)
            raw  = base64.urlsafe_b64decode(b64 + padding)
            data = json.loads(raw)
            return cls(
                alldebrid_key    = data.get("ak", ""),
                language         = data.get("lg", DEFAULT_LANGUAGE),
                vostfr           = bool(data.get("vo", False)),
                min_match        = float(data.get("mm", DEFAULT_MIN_MATCH)),
                search_timeout   = float(data.get("st", DEFAULT_SEARCH_TIMEOUT)),
                enable_movix     = bool(data.get("mx", True)),
                movix_url        = data.get("mu", ""),
                display_format   = data.get("df", FORMAT_EPURE),
                torznab_sources  = data.get("tz", []),
                enable_library   = bool(data.get("el", False)),
                library_priority = bool(data.get("lp", False)),
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
) -> str:
    payload = {"t": stream_type, "h": infohash}
    if season  is not None: payload["s"] = season
    if episode is not None: payload["e"] = episode
    if year    is not None: payload["y"] = year
    raw = json.dumps(payload, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_playback_token(token: str) -> dict:
    padding = "=" * (-len(token) % 4)
    raw  = base64.urlsafe_b64decode(token + padding)
    return json.loads(raw)