"""Builds the project dashboards.

    python3 tools/dashboards/build.py

For each project module (p01_*.py ... p10_*.py) this writes, under /projects/<slug>/:
  index.html   the dashboard page
  data.json    the numbers the dashboard draws (KPIs, charts, tables)
  <name>.csv   the main sample dataset, offered as a download

It also rebuilds /projects/index.html and the three project tiles on the home page
(between the "tiles:start" and "tiles:end" comments in /index.html).

All numbers are generated sample data from a seeded random source, so a rebuild
produces identical files. Standard library only.
"""

import csv
import importlib
import io
import json
import os
import re
import sys

sys.dont_write_bytecode = True  # keep __pycache__ out of the published site

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)

from common import Gen, esc, highlight  # noqa: E402

VERSION = "20261007-8"
SITE = "https://www.sarunshrestha.com.np"
EMAIL = "sarun.shrestha.dev@gmail.com"
LINKEDIN = "https://www.linkedin.com/in/sarun2003"

MODULES = [
    "p01_streaming_lakehouse",
    "p02_medallion_lakehouse",
    "p03_cdc_pipeline",
    "p04_etl_orchestration_cicd",
    "p05_data_quality_observability",
    "p06_unified_streaming_batch",
    "p07_serverless_aws_pipeline",
    "p08_rag_data_pipeline",
    "p09_iac_data_platform",
    "p10_fraud_detection_streaming",
]
HOME_TILES = 3

# ------------------------------------------------------------------ icons
I_DOWNLOAD = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 4v11M7.5 10.5L12 15l4.5-4.5M5 19.5h14"/></svg>'
I_SAMPLE = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 1.8h4M6.6 1.8v4.1L2.7 12.6a1.1 1.1 0 0 0 1 1.6h8.6a1.1 1.1 0 0 0 1-1.6L9.4 5.9V1.8"/><path d="M4.6 9.6h6.8"/></svg>'
I_NOTE = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 11v5.5"/><circle cx="12" cy="7.6" r=".6" fill="currentColor"/></svg>'
I_ARROW = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M7 17L17 7M8 7h9v9"/></svg>'
I_TABLE = '<svg viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.3" aria-hidden="true"><rect x="1.5" y="2" width="9" height="8" rx="1.2"/><path d="M1.5 5h9M1.5 7.6h9M5 5v5"/></svg>'
I_COPY = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round" aria-hidden="true"><rect x="5.5" y="5.5" width="8" height="8" rx="1.6"/><path d="M10.5 5.5V3.6a1.1 1.1 0 0 0-1.1-1.1H3.6a1.1 1.1 0 0 0-1.1 1.1v5.8a1.1 1.1 0 0 0 1.1 1.1h1.9"/></svg>'
I_PREV = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M19 12H5M11 6l-6 6 6 6"/></svg>'


def md(text):
    """Escape, then turn `code` spans and **strong** into HTML."""
    out = esc(text)
    out = re.sub(r"`([^`]+)`", r"<code>\1</code>", out)
    out = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", out)
    return out


# ------------------------------------------------------------------ shared chrome
def head(title, description, path, image, extra_css=True):
    url = SITE + path
    css = '  <link rel="stylesheet" href="/assets/css/style.css?v=%s">\n' % VERSION
    if extra_css:
        css += '  <link rel="stylesheet" href="/assets/css/projects.css?v=%s">\n' % VERSION
    return """<!DOCTYPE html>
<html lang="en" class="no-js">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <meta name="description" content="{desc}">
  <meta name="author" content="Sarun Shrestha">
  <meta name="theme-color" content="#011013">
  <meta name="color-scheme" content="dark light">
  <link rel="canonical" href="{url}">

  <meta property="og:type" content="website">
  <meta property="og:url" content="{url}">
  <meta property="og:site_name" content="Sarun Shrestha">
  <meta property="og:title" content="{title}">
  <meta property="og:description" content="{desc}">
  <meta property="og:image" content="{site}{image}">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:image" content="{site}{image}">

  <link rel="icon" href="/assets/img/favicon.svg" type="image/svg+xml">
  <link rel="apple-touch-icon" href="/assets/img/apple-touch-icon.png">
  <link rel="preload" href="/assets/fonts/plus-jakarta-sans-latin-wght-normal.woff2" as="font" type="font/woff2" crossorigin>
{css}
  <script>
    /* No intro greeting on this page; fall back to the plain page if the main script never starts. */
    (function () {{
      var h = document.documentElement;
      h.classList.remove('no-js');
      h.classList.add('js', 'skip-loader');
      setTimeout(function () {{
        if (!h.classList.contains('app-ready')) {{ h.classList.remove('js'); h.classList.add('no-js'); }}
      }}, 6000);
    }})();
  </script>
</head>
""".format(title=esc(title), desc=esc(description), url=esc(url), site=SITE, image=esc(image), css=css.rstrip("\n"))


def nav(index_page):
    current = ' aria-current="page"' if index_page else ""
    return """  <a class="skip-link" href="#main">Skip to content</a>
  <div class="noise-overlay" aria-hidden="true"></div>

  <header class="ds-nav">
    <a class="ds-logo" href="/" aria-label="Sarun Shrestha, home">
      <span class="ds-logo-mark" aria-hidden="true">©</span>
      <span class="ds-logo-text">Sarun Shrestha</span>
    </a>
    <nav aria-label="Primary">
      <ul class="ds-nav-links">
        <li><a class="ds-nav-link" href="/#about">About</a></li>
        <li><a class="ds-nav-link is-active" href="/projects/"{cur}>Projects</a></li>
        <li><a class="ds-nav-link" href="/#experience">Experience</a></li>
        <li><a class="ds-nav-link" href="/#skills">Skills</a></li>
        <li><a class="ds-nav-link" href="/contact/">Contact</a></li>
        <li>
          <button class="menu-toggle" type="button" aria-expanded="false" aria-controls="side-menu">
            <span class="menu-toggle-icon" aria-hidden="true"></span>Menu
          </button>
        </li>
      </ul>
    </nav>
  </header>

  <div class="side-menu-overlay" data-menu-close></div>
  <aside class="side-menu" id="side-menu" role="dialog" aria-modal="true" aria-label="Menu">
    <button class="side-menu-close" type="button" data-menu-close>
      Close
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><path d="M5 5l14 14M19 5L5 19"/></svg>
    </button>
    <p class="side-menu-label">Navigation</p>
    <nav aria-label="Menu">
      <ul>
        <li><a class="side-menu-nav-link" href="/">Home</a></li>
        <li><a class="side-menu-nav-link" href="/#about">About</a></li>
        <li><a class="side-menu-nav-link is-active" href="/projects/"{cur}>Projects</a></li>
        <li><a class="side-menu-nav-link" href="/#experience">Experience</a></li>
        <li><a class="side-menu-nav-link" href="/#skills">Skills</a></li>
        <li><a class="side-menu-nav-link" href="/contact/">Contact</a></li>
      </ul>
    </nav>
    <div class="side-menu-socials">
      <p class="side-menu-label">Socials</p>
      <ul class="side-menu-social-links">
        <li><a href="{li}" target="_blank" rel="noopener">LinkedIn</a></li>
        <li><a href="mailto:{email}">Email</a></li>
      </ul>
    </div>
  </aside>
""".format(cur=current, li=LINKEDIN, email=EMAIL)


def footer(scripts, container=" dash-container"):
    tags = "\n".join('  <script src="%s" defer></script>' % s for s in scripts)
    return """
  <footer class="contact-page-footer">
    <div class="ds-container{container}">
      <div class="contact-footer-bottom">
        <div class="footer-meta-group">
          <div>
            <p class="footer-meta-label">Version</p>
            <p class="footer-meta-value"><span data-year>2026</span> © Edition</p>
          </div>
        </div>
        <div class="footer-meta-group">
          <div>
            <p class="footer-meta-label">Socials</p>
            <ul class="footer-socials">
              <li><a href="{li}" target="_blank" rel="noopener">LinkedIn</a></li>
              <li><a href="mailto:{email}">Email</a></li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  </footer>

{tags}
</body>
</html>
""".format(li=LINKEDIN, email=EMAIL, tags=tags, container=container)


def picture(slug, alt, width=1200, height=750, eager=False):
    loading = 'fetchpriority="high"' if eager else 'loading="lazy"'
    return (
        '<picture><source srcset="/assets/img/projects/{s}.webp" type="image/webp">'
        '<img src="/assets/img/projects/{s}.jpg" alt="{a}" width="{w}" height="{h}" {l} decoding="async"></picture>'
    ).format(s=slug, a=esc(alt), w=width, h=height, l=loading)


# ------------------------------------------------------------------ dashboard page
def panel_html(p, kinds):
    pid = "p-" + (p.get("chart") or p.get("table"))
    sub = '\n              <p class="dash-panel-sub">%s</p>' % md(p["sub"]) if p.get("sub") else ""
    if p.get("chart"):
        toggle = (
            '\n            <button class="dash-toggle" type="button" data-toggle aria-pressed="false" hidden>'
            + I_TABLE + "Table</button>"
        )
        if kinds.get(p["chart"]) in ("meters", "matrix"):  # HTML visuals size to their content
            body = '<div class="dash-chart dash-chart--html" data-chart="{c}"></div>'.format(c=p["chart"])
        else:
            body = '<div class="dash-chart" data-chart="{c}" style="height:{h}px"></div>'.format(c=p["chart"], h=p.get("h", 280))
    else:
        toggle = ""
        body = '<div class="dash-table-wrap" data-table="{t}"></div>'.format(t=p["table"])
    return """          <article class="dash-panel span-{span}" aria-labelledby="{pid}">
            <header class="dash-panel-head">
              <div>
                <h3 class="dash-panel-title" id="{pid}">{title}</h3>{sub}
              </div>{toggle}
            </header>
            {body}
          </article>""".format(span=p["span"], pid=pid, title=esc(p["title"]), sub=sub.replace("\n              ", "\n                "), toggle=toggle, body=body)


def model_card(m):
    cols = "\n".join(
        '              <div><dt><code>{n}</code><span class="model-type">{t}</span></dt><dd>{d}</dd></div>'.format(
            n=esc(c[0]), t=esc(c[1]), d=md(c[2]))
        for c in m["columns"])
    return """          <article class="model-card">
            <div class="model-card-head"><code>{name}</code><span>{kind}</span></div>
            <dl class="model-cols">
{cols}
            </dl>
          </article>""".format(name=esc(m["name"]), kind=esc(m["kind"]), cols=cols)


def code_block(slug, files):
    tabs, panels = [], []
    for i, f in enumerate(files):
        tid, pid = "tab-%s-%d" % (slug, i), "code-%s-%d" % (slug, i)
        sel = i == 0
        tabs.append(
            '<button class="code-tab" type="button" role="tab" id="{t}" aria-controls="{p}" aria-selected="{s}" tabindex="{ti}">{f}</button>'.format(
                t=tid, p=pid, s="true" if sel else "false", ti="0" if sel else "-1", f=esc(f["file"])))
        panels.append(
            '<div class="code-panel" role="tabpanel" id="{p}" aria-labelledby="{t}" tabindex="0"{h}>\n'
            '              <p class="code-panel-caption">{cap}</p>\n'
            '              <pre><code class="lang-{lang}">{code}</code></pre>\n'
            '            </div>'.format(p=pid, t=tid, h="" if sel else " hidden", cap=md(f["caption"]), lang=f["lang"],
                                       code=highlight(f["code"], f["lang"])))
    return """        <div class="code-block" data-code-block>
          <div class="code-bar">
            <div class="code-tabs" role="tablist" aria-label="Source files">
              {tabs}
            </div>
            <button class="code-copy" type="button" data-copy hidden>{icon}<span>Copy</span></button>
          </div>
          {panels}
        </div>""".format(tabs="\n              ".join(tabs), icon=I_COPY, panels="\n          ".join(panels))


def dashboard_page(mod, prev_mod, next_mod, csv_rows, csv_cols, kinds):
    m = mod.META
    path = "/projects/%s/" % m["slug"]
    title = "%s — Sarun Shrestha" % m["title"]
    out = [head(title, m["tagline"], path, "/assets/img/projects/%s.jpg" % m["slug"])]
    out.append('<body class="dash-page">\n')
    out.append(nav(index_page=False))

    stack = "".join("<li>%s</li>" % esc(s) for s in m["stack"])
    out.append("""
  <main id="main">
    <section class="dash-hero" aria-labelledby="dash-title">
      <div class="ds-container dash-container">
        <nav class="dash-crumbs" aria-label="Breadcrumb">
          <a href="/">Home</a><span aria-hidden="true">/</span><a href="/projects/">Projects</a><span aria-hidden="true">/</span><span aria-current="page">{short}</span>
        </nav>
        <p class="dash-eyebrow">Project {num} · {cat}</p>
        <h1 class="dash-title" id="dash-title">{title}</h1>
        <p class="dash-summary">{summary}</p>
        <ul class="dash-stack" aria-label="Tech stack">{stack}</ul>
        <div class="dash-meta">
          <span class="dash-badge">{i_sample}Sample data</span>
          <span class="dash-window">{window}</span>
          <a class="pill pill--sm" href="{csv}" download>{i_dl}Download data (CSV)</a>
        </div>
      </div>
    </section>

    <section class="dash-body" data-dashboard data-src="data.json" aria-label="Dashboard">
      <div class="ds-container dash-container">
        <noscript><p class="dash-noscript">The charts need JavaScript. The same numbers are in <a href="{csv}">{csv}</a> and <a href="data.json">data.json</a>.</p></noscript>
        <div class="dash-kpis" data-kpis></div>
        <div class="dash-grid">
{panels}
        </div>
      </div>
    </section>
""".format(short=esc(m["short"]), num=m["num"], cat=esc(m["category"]), title=esc(m["title"]), summary=md(m["summary"]),
           stack=stack, i_sample=I_SAMPLE, window=esc(m["window"]), csv=esc(m["csv"]), i_dl=I_DOWNLOAD,
           panels="\n".join(panel_html(p, kinds) for p in mod.PANELS)))

    # How it works
    flow = "\n".join(
        """          <li class="flow-step">
            <p class="flow-stage">{n:02d} · {stage}</p>
            <p class="flow-tech">{tech}</p>
            <p class="flow-detail">{detail}</p>
          </li>""".format(n=i + 1, stage=esc(s[0]), tech=esc(s[1]), detail=md(s[2]))
        for i, s in enumerate(mod.FLOW))
    steps = "\n".join("          <li><strong>%s</strong> %s</li>" % (esc(s[0]), md(s[1])) for s in mod.STEPS)
    out.append("""
    <section class="dash-section" aria-labelledby="how-title">
      <div class="ds-container dash-container">
        <div class="dash-section-head">
          <h2 class="dash-h2" id="how-title">How it works</h2>
          <p class="dash-section-note">{how_note}</p>
        </div>
        <ol class="flow">
{flow}
        </ol>
        <ul class="steps">
{steps}
        </ul>
      </div>
    </section>
""".format(how_note=md(m["how_note"]), flow=flow, steps=steps))

    # Data model
    cards = "\n".join(model_card(x) for x in mod.MODELS)
    out.append("""
    <section class="dash-section" aria-labelledby="model-title">
      <div class="ds-container dash-container">
        <div class="dash-section-head">
          <h2 class="dash-h2" id="model-title">Data model</h2>
          <p class="dash-section-note">{model_note}</p>
        </div>
        <div class="model-grid">
{cards}
        </div>
        <ul class="dataset-files" aria-label="Downloads">
          <li><a href="{csv}" download>{i_dl}<span><strong>{csv}</strong> {csv_desc} · {rows:,} rows, {cols} columns</span></a></li>
          <li><a href="data.json">{i_dl}<span><strong>data.json</strong> Everything this dashboard draws: KPIs, chart series and tables</span></a></li>
        </ul>
      </div>
    </section>
""".format(model_note=md(m["model_note"]), cards=cards, csv=esc(m["csv"]), i_dl=I_DOWNLOAD, csv_desc=esc(m["csv_desc"]),
           rows=csv_rows, cols=csv_cols))

    # Key code
    out.append("""
    <section class="dash-section" aria-labelledby="code-title">
      <div class="ds-container dash-container">
        <div class="dash-section-head">
          <h2 class="dash-h2" id="code-title">Key code</h2>
          <p class="dash-section-note">{code_note}</p>
        </div>
{code}
      </div>
    </section>
""".format(code_note=md(m["code_note"]), code=code_block(m["slug"], mod.CODE)))

    # Highlights + data note
    hl = "\n".join(
        """          <article class="highlight">
            <h3>{t}</h3>
            <p>{d}</p>
          </article>""".format(t=esc(h[0]), d=md(h[1])) for h in mod.HIGHLIGHTS)
    out.append("""
    <section class="dash-section" aria-labelledby="notes-title">
      <div class="ds-container dash-container">
        <div class="dash-section-head">
          <h2 class="dash-h2" id="notes-title">Engineering notes</h2>
        </div>
        <div class="highlights">
{hl}
        </div>
        <p class="data-note">{i_note}<span><strong>About the data.</strong> Every number on this page is sample data from a seeded generator written for this project. It is shaped to behave like a real workload, with daily peaks, late events and the odd incident, but it does not come from any employer or client. Download it as <a href="{csv}">{csv}</a> or read <a href="data.json">data.json</a>.</span></p>
      </div>
    </section>
""".format(hl=hl, i_note=I_NOTE, csv=esc(m["csv"])))

    # Pager
    out.append("""
    <div class="ds-container dash-container">
      <nav class="dash-pager" aria-label="More projects">
        <a href="/projects/{ps}/"><span>{i_prev} Previous · {pn}</span><strong>{pt}</strong></a>
        <a class="is-next" href="/projects/{ns}/"><span>Next · {nn} {i_next}</span><strong>{nt}</strong></a>
      </nav>
    </div>
  </main>
""".format(ps=prev_mod.META["slug"], pn=prev_mod.META["num"], pt=esc(prev_mod.META["title"]), i_prev=I_PREV,
           ns=next_mod.META["slug"], nn=next_mod.META["num"], nt=esc(next_mod.META["title"]),
           i_next=I_PREV.replace('d="M19 12H5M11 6l-6 6 6 6"', 'd="M5 12h14M13 6l6 6-6 6"')))

    out.append(footer([
        "/assets/vendor/echarts.custom.min.js?v=5.6.0",
        "/assets/js/dashboard.js?v=" + VERSION,
        "/assets/js/main.js?v=" + VERSION,
    ]))
    return "".join(out)


# ------------------------------------------------------------------ projects index
def index_page(mods):
    cards = []
    for i, mod in enumerate(mods):
        m = mod.META
        chips = "".join("<li>%s</li>" % esc(s) for s in m["stack"])
        cards.append("""          <li class="project-card">
            <div class="tile-image-block">{pic}</div>
            <div class="project-card-body">
              <p class="project-card-num">{num} · {cat}</p>
              <h2><a class="project-card-main" href="/projects/{slug}/"><span class="project-card-title">{title}</span></a></h2>
              <p>{tagline}</p>
              <ul aria-label="Tech stack">{chips}</ul>
              <span class="project-card-link" aria-hidden="true">Open dashboard {arrow}</span>
            </div>
          </li>""".format(pic=picture(m["slug"], "", eager=i < 2), num=m["num"], cat=esc(m["category"]), slug=m["slug"],
                          title=esc(m["title"]), tagline=esc(m["tagline"]), chips=chips, arrow=I_ARROW))

    out = [head("Projects — Sarun Shrestha",
                "Ten data engineering projects by Sarun Shrestha, each with a live dashboard: streaming, lakehouse, CDC, orchestration, data quality, serverless, RAG, infrastructure as code and fraud detection.",
                "/projects/", "/assets/img/projects/%s.jpg" % mods[0].META["slug"])]
    out.append('<body class="projects-page">\n')
    out.append(nav(index_page=True))
    out.append("""
  <main id="main">
    <section class="dash-hero projects-hero" aria-labelledby="projects-title">
      <div class="ds-container">
        <nav class="dash-crumbs" aria-label="Breadcrumb">
          <a href="/">Home</a><span aria-hidden="true">/</span><span aria-current="page">Projects</span>
        </nav>
        <p class="dash-eyebrow">Projects · {n}</p>
        <h1 class="dash-title" id="projects-title">Data platforms, built end&nbsp;to&nbsp;end</h1>
        <p class="dash-summary">Ten projects that cover the work I do every day: streaming and batch pipelines, lakehouses, change data capture, orchestration with CI/CD, data quality, serverless on AWS, retrieval pipelines for AI, infrastructure as code and real-time fraud detection. Each one opens as a dashboard with its data, architecture, data model and key code.</p>
        <div class="dash-meta">
          <span class="dash-badge">{i_sample}Sample data</span>
          <span class="dash-window">Every dashboard runs on generated sample data</span>
        </div>
      </div>
    </section>

    <section class="projects-list-section" aria-label="All projects">
      <div class="ds-container">
        <ul class="projects-grid">
{cards}
        </ul>
      </div>
    </section>
  </main>
""".format(n=len(mods), i_sample=I_SAMPLE, cards="\n".join(cards)))
    out.append(footer(["/assets/js/main.js?v=" + VERSION], container=""))
    return "".join(out)


# ------------------------------------------------------------------ home tiles
def home_tiles(mods):
    items = []
    for mod in mods[:HOME_TILES]:
        m = mod.META
        items.append("""          <li class="tile">
            <a class="tile-link" href="/projects/{slug}/">
              <div class="tile-image-block">{pic}</div>
              <div class="tile-meta">
                <div>
                  <h3 class="tile-title">{title}</h3>
                  <p class="tile-sub">{sub}</p>
                </div>
                <span class="tile-arrow" aria-hidden="true">{arrow}</span>
              </div>
            </a>
          </li>""".format(slug=m["slug"], pic=picture(m["slug"], ""), title=esc(m["title"]), sub=esc(m["tile_stack"]), arrow=I_ARROW))
    return """<!-- tiles:start (generated by tools/dashboards/build.py) -->
        <ul class="tiles-grid">
{items}
        </ul>
        <div class="more-work-row">
          <a class="pill pill--light pill--sm" href="/projects/">More projects<span class="pill-count" aria-hidden="true">{n}</span><span class="sr-only"> ({n} in total)</span></a>
        </div>
        <!-- tiles:end -->""".format(items="\n".join(items), n=len(mods))


# ------------------------------------------------------------------ main
def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def main():
    mods = [importlib.import_module(name) for name in MODULES if os.path.exists(os.path.join(HERE, name + ".py"))]
    for i, mod in enumerate(mods):
        m = mod.META
        data, header, rows = mod.build(Gen(m["seed"]))
        for p in mod.PANELS:  # the panel title doubles as the chart's accessible name
            if p.get("chart") and p["chart"] in data["charts"]:
                data["charts"][p["chart"]].setdefault("title", p["title"])
            if p.get("chart") and p["chart"] not in data["charts"]:
                raise SystemExit("%s: panel chart %r has no data" % (m["slug"], p["chart"]))
            if p.get("table") and p["table"] not in data["tables"]:
                raise SystemExit("%s: panel table %r has no data" % (m["slug"], p["table"]))
        data = {"slug": m["slug"], "window": m["window"], "sample": True, **data}

        base = os.path.join(ROOT, "projects", m["slug"])
        write(os.path.join(base, "data.json"), json.dumps(data, separators=(",", ":"), ensure_ascii=False))
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
        write(os.path.join(base, m["csv"]), buf.getvalue())
        prev_mod, next_mod = mods[i - 1], mods[(i + 1) % len(mods)]
        kinds = {k: v.get("kind") for k, v in data["charts"].items()}
        write(os.path.join(base, "index.html"), dashboard_page(mod, prev_mod, next_mod, len(rows), len(header), kinds))
        print("built %-32s %6.1f KB json  %5d csv rows" % (m["slug"], os.path.getsize(os.path.join(base, "data.json")) / 1024, len(rows)))

    write(os.path.join(ROOT, "projects", "index.html"), index_page(mods))

    home = os.path.join(ROOT, "index.html")
    with open(home, encoding="utf-8") as f:
        text = f.read()
    new, n = re.subn(r"<!-- tiles:start.*?<!-- tiles:end -->", lambda _: home_tiles(mods), text, flags=re.S)
    if n == 1 and new != text:
        write(home, new)
        print("updated home page tiles")


if __name__ == "__main__":
    main()
