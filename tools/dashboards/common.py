"""Shared helpers for the project dashboards: seeded randomness, time axes and
a small build-time syntax highlighter for the code samples."""

import datetime as dt
import html
import math
import random
import re


class Gen:
    """Seeded random source so every build produces the same sample data."""

    def __init__(self, seed):
        self.r = random.Random(seed)

    def gauss(self, sd=1.0, mu=0.0):
        return self.r.gauss(mu, sd)

    def jitter(self, sd):
        """Multiplicative noise around 1.0 (log-normal)."""
        return math.exp(self.r.gauss(0.0, sd))

    def uni(self, a, b):
        return self.r.uniform(a, b)

    def ri(self, a, b):
        return self.r.randint(a, b)

    def choice(self, seq):
        return self.r.choice(seq)

    def weighted(self, items, weights):
        return self.r.choices(items, weights=weights, k=1)[0]

    def chance(self, p):
        return self.r.random() < p

    def hexid(self, n=8):
        return "".join(self.r.choice("0123456789abcdef") for _ in range(n))


def bump(x, mu, sigma):
    return math.exp(-0.5 * ((x - mu) / sigma) ** 2)


def bump24(h, mu, sigma):
    """Gaussian bump on a 24-hour clock (wraps around midnight)."""
    d = abs(h - mu) % 24
    d = min(d, 24 - d)
    return math.exp(-0.5 * (d / sigma) ** 2)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def r0(v):
    return int(round(v))


def r1(v):
    return round(v, 1)


def r2(v):
    return round(v, 2)


def r3(v):
    return round(v, 3)


def r4(v):
    return round(v, 4)


def pct_change(new, old):
    return round((new - old) / old, 4) if old else 0.0


# ---------------------------------------------------------------- time axes
def five_min_labels():
    return ["%02d:%02d" % (i // 12, (i % 12) * 5) for i in range(288)]


def hour_labels(n=24):
    return ["%02d:00" % h for h in range(n)]


def day_list(end, n):
    """The n dates ending at `end` (inclusive), oldest first."""
    return [end - dt.timedelta(days=n - 1 - i) for i in range(n)]


def day_label(d):
    return d.strftime("%b ") + str(d.day)


def iso(d):
    return d.isoformat()


def quantile(values, q):
    s = sorted(values)
    if not s:
        return 0.0
    pos = (len(s) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


# ---------------------------------------------------------------- spec helpers
def status(s, text):
    return {"s": s, "t": text}


def col(key, label, fmt=None, **extra):
    c = {"key": key, "label": label}
    if fmt:
        c["fmt"] = fmt
    c.update(extra)
    return c


def esc(s):
    return html.escape(str(s), quote=True)


# ---------------------------------------------------------------- highlighter
_PY_KW = (
    "False None True and as assert async await break class continue def del elif else except "
    "finally for from global if import in is lambda nonlocal not or pass raise return try while with yield"
).split()
_SQL_KW = (
    "select from where group by order having join left right inner outer full cross on as and or not "
    "in is null case when then else end with union all distinct insert into values update set delete "
    "merge using matched create replace table view or alter add column primary key references "
    "partition over row_number rank asc desc limit qualify interval cast between like exists true false "
    "unique default if stream task warehouse schedule materialized incremental"
).split()
_HCL_KW = "resource module variable output locals provider data terraform for_each count true false null for in if".split()
_BASH_KW = "if then else fi for do done while in case esac function return export set local echo".split()
_YAML_KW = "true false null yes no".split()
_DOCKER_KW = "FROM AS RUN COPY ADD ARG ENV USER WORKDIR ENTRYPOINT CMD EXPOSE LABEL HEALTHCHECK".split()


def _kw(words, icase=False):
    body = r"\b(?:" + "|".join(sorted(map(re.escape, words), key=len, reverse=True)) + r")\b"
    return "(?i:" + body + ")" if icase else body


_NUM = r"\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b"
_DQ = r'"(?:\\.|[^"\\\n])*"'
_SQ = r"'(?:\\.|[^'\\\n])*'"

_LANGS = {
    "python": [
        ("c", r"#[^\n]*"),
        ("s", r'[rbfuRBFU]{0,2}"""[\s\S]*?"""|[rbfuRBFU]{0,2}\'\'\'[\s\S]*?\'\'\'|[rbfuRBFU]{0,2}' + _DQ + "|[rbfuRBFU]{0,2}" + _SQ),
        ("f", r"@[\w.]+"),
        ("k", _kw(_PY_KW)),
        ("n", _NUM),
        ("f", r"\b[A-Za-z_]\w*(?=\()"),
    ],
    "sql": [
        ("c", r"--[^\n]*"),
        ("p", r"\{\{|\}\}|\{%-?|-?%\}"),
        ("s", r"'(?:''|[^'\n])*'"),
        ("k", _kw(_SQL_KW, icase=True)),
        ("n", _NUM),
        ("f", r"\b[A-Za-z_]\w*(?=\()"),
    ],
    "yaml": [
        ("c", r"#[^\n]*"),
        ("s", _DQ + "|" + _SQ),
        ("p", r"[\w.\-/$]+(?=[ \t]*:(?:[ \t]|$))"),
        ("k", _kw(_YAML_KW)),
        ("n", _NUM),
    ],
    "hcl": [
        ("c", r"#[^\n]*|//[^\n]*"),
        ("s", _DQ),
        ("k", _kw(_HCL_KW)),
        ("p", r"\b[a-z_][\w-]*(?=[ \t]*=(?!=))"),
        ("n", _NUM),
        ("f", r"\b[a-z_]\w*(?=\()"),
    ],
    "json": [
        ("p", _DQ + r"(?=\s*:)"),
        ("s", _DQ),
        ("k", r"\b(?:true|false|null)\b"),
        ("n", r"-?" + _NUM),
    ],
    "docker": [
        ("c", r"#[^\n]*"),
        ("s", _DQ + "|" + _SQ),
        ("k", r"^[ \t]*(?:" + "|".join(_DOCKER_KW) + r")\b|\bAS\b"),
        ("p", r"--[\w-]+(?==)"),
        ("n", r"\$\{?\w+\}?"),
    ],
    "bash": [
        ("c", r"(?:^|(?<=\s))#[^\n]*"),
        ("s", _DQ + "|" + _SQ),
        ("k", _kw(_BASH_KW)),
        ("p", r"(?<=\s)--?[\w-]+"),
        ("n", _NUM),
    ],
}

_COMPILED = {}


def _compiled(lang):
    if lang not in _COMPILED:
        rules = _LANGS[lang]
        parts = ["(?P<g%d>%s)" % (i, pat) for i, (_, pat) in enumerate(rules)]
        _COMPILED[lang] = (re.compile("|".join(parts), re.M), [cls for cls, _ in rules])
    return _COMPILED[lang]


def _line_start_ok(text, start, allow_dash):
    """True when only whitespace (and optionally a YAML list dash) precedes `start` on its line."""
    line_start = text.rfind("\n", 0, start) + 1
    before = text[line_start:start].strip()
    return before == "" or (allow_dash and before == "-")


def highlight(code, lang):
    """Return the code as HTML: one <span class="ln"> per line, tokens wrapped in tok-* spans."""
    code = code.strip("\n")
    rx, classes = _compiled(lang)
    tokens = []  # (text, cls or None)
    pos = 0
    for m in rx.finditer(code):
        idx = int(m.lastgroup[1:])
        cls = classes[idx]
        if cls == "p" and lang in ("yaml", "hcl") and not _line_start_ok(code, m.start(), lang == "yaml"):
            cls = None
        if m.start() > pos:
            tokens.append((code[pos:m.start()], None))
        tokens.append((m.group(0), cls))
        pos = m.end()
    if pos < len(code):
        tokens.append((code[pos:], None))

    lines = [[]]
    for text, cls in tokens:
        pieces = text.split("\n")
        for j, piece in enumerate(pieces):
            if j:
                lines.append([])
            if piece:
                lines[-1].append('<span class="tok-%s">%s</span>' % (cls, esc(piece)) if cls else esc(piece))
    return "\n".join('<span class="ln">%s</span>' % "".join(parts) for parts in lines)
