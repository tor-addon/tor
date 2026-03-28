"""
utils/logger.py
───────────────
Pastel-colored structured logger.
Call setup_logging() once at server startup.
Every module: logger = logging.getLogger(__name__)
"""

import logging
import sys

# Pastel ANSI palette (256-color)
_RESET   = "\033[0m"
_BOLD    = "\033[1m"
_MUTED   = "\033[38;5;245m"    # grey – timestamps, brackets
_LBLUE   = "\033[38;5;111m"    # soft blue – module name
_LGREEN  = "\033[38;5;156m"    # mint green – INFO
_LYELLOW = "\033[38;5;222m"    # butter yellow – WARNING
_LRED    = "\033[38;5;210m"    # salmon – ERROR
_BRED    = "\033[1;38;5;203m"  # bold coral – CRITICAL
_LGREY   = "\033[38;5;250m"    # light grey – DEBUG
_TEAL    = "\033[38;5;115m"    # teal – pipeline markers
_PEACH   = "\033[38;5;216m"    # peach – filter rejects

_LEVEL_COLORS = {
    "DEBUG":    _LGREY,
    "INFO":     _LGREEN,
    "WARNING":  _LYELLOW,
    "ERROR":    _LRED,
    "CRITICAL": _BRED,
}


class _ColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        color    = _LEVEL_COLORS.get(record.levelname, "")
        time_str = self.formatTime(record, self.datefmt)
        level    = f"{color}{record.levelname:<8}{_RESET}"
        name     = f"{_MUTED}[{_LBLUE}{record.name}{_MUTED}]{_RESET}"
        msg      = record.getMessage()

        # Pipeline start / end lines stand out
        if "━━" in msg:
            msg = f"{_BOLD}{_TEAL}{msg}{_RESET}"
        # Filter reject summary
        elif "filter rejected" in msg:
            msg = f"{_PEACH}{msg}{_RESET}"
        # Cached highlight
        elif "cached:" in msg:
            msg = msg.replace("cached:", f"{_LGREEN}cached:{_RESET}")
        # Warnings / timeouts
        elif record.levelno >= logging.WARNING:
            msg = f"{_LYELLOW}{msg}{_RESET}"

        return f"{_MUTED}{time_str}{_RESET} {level} {name} {msg}"


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_ColorFormatter(datefmt="%H:%M:%S"))
    root.addHandler(handler)