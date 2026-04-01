"""
utils/exit_condition.py
───────────────────────
Composable exit condition evaluator (SEL-inspired).

Syntax – boolean expressions with composable stream filters:

  Boolean    : and  or  not  ()
  Comparisons: >=  <=  >  <  =  ==

Stream filter functions (return a filtered list):
  torrent(streams)                  – torrent streams only
  ddl(streams)                      – DDL streams only
  resolution(streams, '>=1080p')    – filter by resolution (optional op + value)
  quality(streams, '>=bluray')      – filter by quality rank
  lang(streams, 'fr')               – filter by language

Aggregation:
  count(list)                       – length of a stream list

Named constants (usable directly in comparisons):
  streams  – streams en cache disponibles (count when compared)
  torrent  – torrent stream count
  ddl      – DDL stream count
  delay    – elapsed seconds

Resolution ladder : 480p < 576p < 720p < 1080p < 1440p < 2160p / 4k
Quality ladder    : cam < ts < hdtv < webrip < web-dl < bluray < remux

Examples:
  streams >= 5
  count(resolution(streams, '>=1080p')) >= 2
  count(lang(streams, 'fr')) >= 1 and delay >= 2
  streams >= 3 or delay >= 10
  not (delay < 2) and count(ddl(streams)) >= 1
"""
from __future__ import annotations

import re
import logging

logger = logging.getLogger(__name__)

# ── Rank tables ────────────────────────────────────────────────────────────────

_RES: dict[str, int] = {
    "480p": 480, "576p": 576, "720p": 720,
    "1080p": 1080, "1440p": 1440, "2160p": 2160, "4k": 2160,
}
_QUAL: dict[str, int] = {
    "cam": 0, "ts": 1, "hdtv": 2, "webrip": 3,
    "web-dl": 4, "bluray": 5, "remux": 6, "bluray remux": 6,
}

def _res_rank(v: str)  -> int: return _RES.get(v.lower().strip(), 0)
def _qual_rank(v: str) -> int: return _QUAL.get(v.lower().strip(), -1)

def _op(a, op: str, b) -> bool:
    if op in ("=", "=="): return a == b
    if op == ">=":        return a >= b
    if op == "<=":        return a <= b
    if op == ">":         return a > b
    if op == "<":         return a < b
    return False

# Parse filter expressions like ">=1080p", "1080p", ">bluray"
_FILT_RE = re.compile(r'^(>=|<=|>|<|=|==)?\s*(.+)$')


# ── Lexer ──────────────────────────────────────────────────────────────────────

_KW = frozenset({
    "and", "or", "not",
    "count", "torrent", "ddl",
    "resolution", "quality", "lang", "streams", "delay",
})


def _tokenize(src: str) -> list[tuple[str, object]]:
    tokens: list[tuple[str, object]] = []
    i = 0
    while i < len(src):
        c = src[i]
        if c.isspace():
            i += 1; continue
        two = src[i:i+2]
        if two in (">=", "<=", "=="):
            tokens.append(("OP", two)); i += 2; continue
        if c in "><=" :
            tokens.append(("OP", c)); i += 1; continue
        if c == "(":
            tokens.append(("LP", None)); i += 1; continue
        if c == ")":
            tokens.append(("RP", None)); i += 1; continue
        if c == ",":
            tokens.append(("CM", None)); i += 1; continue
        if c in "\"'":
            q = c; j = i + 1
            while j < len(src) and src[j] != q:
                j += 1
            tokens.append(("STR", src[i+1:j])); i = j + 1; continue
        if c.isdigit():
            j = i
            while j < len(src) and (src[j].isdigit() or src[j] == "."):
                j += 1
            if j < len(src) and src[j].lower() == "p":
                # resolution literal like 1080p → treat as string
                tokens.append(("STR", src[i:j] + "p")); i = j + 1
            else:
                tokens.append(("NUM", float(src[i:j]))); i = j
            continue
        if c.isalpha() or c == "_":
            j = i
            while j < len(src) and (src[j].isalnum() or src[j] in "_-"):
                j += 1
            word = src[i:j].lower()
            tokens.append(("KW" if word in _KW else "WORD", word))
            i = j; continue
        i += 1  # skip unknown character
    tokens.append(("EOF", None))
    return tokens


# ── Parser (recursive descent) ─────────────────────────────────────────────────
# AST node types (tuples):
#   ("and"|"or", left, right)
#   ("not", expr)
#   ("cmp", left, op_str, right)
#   ("call", func_name, [arg_nodes])
#   ("ident", name)
#   ("num", float_value)
#   ("str", str_value)

class _P:
    __slots__ = ("_t", "_i")

    def __init__(self, tokens: list) -> None:
        self._t = tokens
        self._i = 0

    def _peek(self):
        return self._t[self._i]

    def _next(self):
        t = self._t[self._i]; self._i += 1; return t

    def _match(self, typ: str, val=None) -> bool:
        t = self._t[self._i]
        if t[0] == typ and (val is None or t[1] == val):
            self._i += 1; return True
        return False

    def _expect(self, typ: str):
        t = self._next()
        if t[0] != typ:
            raise SyntaxError(f"Expected {typ}, got {t!r}")
        return t[1]

    def parse(self) -> tuple:
        node = self._or()
        if self._peek()[0] != "EOF":
            raise SyntaxError(f"Unexpected token: {self._peek()!r}")
        return node

    def _or(self) -> tuple:
        n = self._and()
        while self._match("KW", "or"):
            n = ("or", n, self._and())
        return n

    def _and(self) -> tuple:
        n = self._not()
        while self._match("KW", "and"):
            n = ("and", n, self._not())
        return n

    def _not(self) -> tuple:
        if self._match("KW", "not"):
            return ("not", self._not())
        return self._cmp()

    def _cmp(self) -> tuple:
        left = self._atom()
        if self._peek()[0] == "OP":
            op = self._next()[1]
            return ("cmp", left, op, self._atom())
        return left

    def _atom(self) -> tuple:
        t = self._peek()
        if t[0] == "LP":
            self._next()
            n = self._or()
            self._expect("RP")
            return n
        if t[0] == "NUM":
            self._next(); return ("num", t[1])
        if t[0] == "STR":
            self._next(); return ("str", t[1])
        if t[0] in ("KW", "WORD"):
            name = t[1]; self._next()
            if self._peek()[0] == "LP":
                self._next()  # consume "("
                args: list[tuple] = []
                if self._peek()[0] != "RP":
                    args.append(self._atom())
                    while self._match("CM"):
                        args.append(self._atom())
                self._expect("RP")
                return ("call", name, args)
            return ("ident", name)
        raise SyntaxError(f"Unexpected token: {t!r}")


# ── Evaluator ──────────────────────────────────────────────────────────────────

def _eval(node: tuple, streams: list[dict], delay: float):
    match node[0]:
        case "or":
            return _eval(node[1], streams, delay) or _eval(node[2], streams, delay)
        case "and":
            return _eval(node[1], streams, delay) and _eval(node[2], streams, delay)
        case "not":
            return not _eval(node[1], streams, delay)
        case "num":
            return node[1]
        case "str":
            return node[1]
        case "ident":
            match node[1]:
                case "delay":   return delay
                case "streams": return streams
                case "torrent": return [s for s in streams if s.get("stream_type") == "torrent"]
                case "ddl":     return [s for s in streams if s.get("stream_type") == "ddl"]
                case _:         return 0
        case "call":
            args = [_eval(a, streams, delay) for a in node[2]]
            return _apply(node[1], args, streams)
        case "cmp":
            left  = _eval(node[1], streams, delay)
            right = _eval(node[3], streams, delay)
            if isinstance(left,  list): left  = len(left)
            if isinstance(right, list): right = len(right)
            return _op(left, node[2], right)
        case _:
            raise ValueError(f"Unknown node type: {node[0]!r}")


def _apply(name: str, args: list, streams: list[dict]):
    """Execute a named filter or aggregation function."""

    def _lst(idx: int = 0) -> list[dict]:
        v = args[idx] if len(args) > idx else streams
        return v if isinstance(v, list) else streams

    def _sval(idx: int = 1) -> str:
        v = args[idx] if len(args) > idx else ""
        return str(v).strip("'\" ")

    match name:
        case "count":
            v = args[0] if args else streams
            return len(v) if isinstance(v, list) else int(v or 0)

        case "torrent":
            return [s for s in _lst() if s.get("stream_type") == "torrent"]

        case "ddl":
            return [s for s in _lst() if s.get("stream_type") == "ddl"]

        case "resolution":
            lst  = _lst()
            m    = _FILT_RE.match(_sval())
            if not m: return lst
            fop, val = (m.group(1) or "="), m.group(2)
            rank = _res_rank(val)
            return [s for s in lst if _op(_res_rank(s.get("resolution") or ""), fop, rank)]

        case "quality":
            lst  = _lst()
            m    = _FILT_RE.match(_sval())
            if not m: return lst
            fop, val = (m.group(1) or "="), m.group(2)
            rank = _qual_rank(val)
            return [s for s in lst if _op(_qual_rank(s.get("quality") or ""), fop, rank)]

        case "lang":
            lst    = _lst()
            target = _sval().lower()
            if target == "fr":
                return [s for s in lst
                        if {"fr", "vff", "vfq"} & {l.lower() for l in (s.get("languages") or [])}]
            return [s for s in lst
                    if target in {l.lower() for l in (s.get("languages") or [])}]

        case _:
            logger.debug("ExitCondition: unknown function %r", name)
            return []


# ── Public interface ───────────────────────────────────────────────────────────

class ExitConditionEvaluator:
    """
    Compiled exit condition. Immutable after __init__ – thread-safe.

    Usage:
        ec = ExitConditionEvaluator("streams >= 5")
        if ec.active and ec.evaluate(streams, elapsed_seconds):
            # stop searching
    """
    __slots__ = ("_node", "_src")

    def __init__(self, condition: str) -> None:
        self._src  = condition.strip() if condition else ""
        self._node = None
        if not self._src:
            return
        try:
            self._node = _P(_tokenize(self._src)).parse()
            logger.debug("ExitCondition compiled: %r", self._src)
        except Exception as exc:
            logger.warning("ExitCondition parse error (%r): %s – disabled", self._src, exc)

    @property
    def active(self) -> bool:
        return self._node is not None

    def evaluate(self, streams: list[dict], elapsed: float) -> bool:
        if self._node is None:
            return False
        try:
            return bool(_eval(self._node, streams, elapsed))
        except Exception as exc:
            logger.debug("ExitCondition eval error: %s", exc)
            return False
