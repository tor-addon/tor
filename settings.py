import os

"""
settings.py
───────────
Single source of truth for all external URLs and global defaults.
"""

# ── AllDebrid ─────────────────────────────────────────────────────────────────
ALLDEBRID_BASE_URL = "https://api.alldebrid.com/v4"
ALLDEBRID_V41_BASE_URL = "https://api.alldebrid.com/v4.1"
ALLDEBRID_AGENT    = "Tor"
# Hosts supported by AllDebrid – shared by all DDL sources (Movix, Wawacity)
DDL_ALLOWED_HOSTS: frozenset[str] = frozenset({"1fichier", "turbobit", "rapidgator", "vidoza"})

# ── TMDB ──────────────────────────────────────────────────────────────────────
TMDB_BASE_URL    = "https://api.themoviedb.org/3"
TMDB_DEFAULT_KEY = (
    "eyJhbGciOiJIUzI1NiJ9"
    ".eyJhdWQiOiJlNTkxMmVmOWFhM2IxNzg2Zjk3ZTE1NWY1YmQ3ZjY1MSIsInN1YiI6IjY1M2NjNWUyZTg5NGE2MDBmZjE2N2FmYyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ"
    ".xrIXsMFJpI1o1j5g2QpQcFP1X3AfRjFA5FlBFO5Naw8"
)

# ── Movix ─────────────────────────────────────────────────────────────────────
MOVIX_API_BASE_URL    = "https://api.movix.blog/api"
MOVIX_DECODE_BASE_URL = MOVIX_API_BASE_URL + "/darkiworld/decode"
MOVIX_REFERER         = "https://movix.rodeo"
MOVIX_ORIGIN          = "https://movix.rodeo"

DARKIWORLD_API_BASE_URL = "https://darkiworld2026.com/api/v1"

# ── Wawacity ──────────────────────────────────────────────────────────────────
WAWACITY_BASE_URL = "https://www.wawacity.golf"

# ── Pipeline defaults ─────────────────────────────────────────────────────────
DEFAULT_LANGUAGES      = ["fr"]   # list – can include "fr", "multi", "vostfr"
DEFAULT_MIN_MATCH      = 75.0
DEFAULT_SEARCH_TIMEOUT = 5.0
TORZNAB_RESULT_LIMIT   = 100
ALLDEBRID_BATCH_SIZE   = 80

# ── Addon metadata ────────────────────────────────────────────────────────────
ADDON_ID          = "community.stremio-tor"
ADDON_NAME        = "Tor"
ADDON_VERSION     = "1.0.3"
ADDON_DESCRIPTION = "Stremio Optimized +5 Torrent Source, +2 DDL Source,  │ By Adam!"
ADDON_LOGO        = "https://images.icon-icons.com/2552/PNG/512/tor_alpha_browser_logo_icon_152957.png"
LOG_LEVEL         = os.environ.get("LOG_LEVEL", "INFO")