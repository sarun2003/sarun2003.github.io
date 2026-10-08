# sarunshrestha.com.np

Personal site for Sarun Shrestha, Senior Data Engineer. Plain HTML, CSS and JavaScript with no build step, hosted on GitHub Pages.

## Files

```
index.html               the home page (all main text content lives here)
contact/index.html       contact page with the message form (www.sarunshrestha.com.np/contact/)
404.html                 "page not found" page
CNAME                    custom domain for GitHub Pages (www.sarunshrestha.com.np)
.nojekyll                tells GitHub Pages to serve files as-is
projects/index.html      all ten projects (www.sarunshrestha.com.np/projects/)
projects/<slug>/         one dashboard per project: index.html, data.json and a CSV download
assets/css/style.css     styles (colors and fonts are at the top, under "Tokens")
assets/css/projects.css  extra styles for the projects page and the dashboards
assets/js/main.js        intro greeting, hero pipeline animation, menu, accordion, cursor
assets/js/dashboard.js   draws the dashboards (KPIs, charts, tables, code tabs) from data.json
assets/vendor/           Apache ECharts 5.6.0 (charts only, Apache-2.0)
assets/fonts/            self-hosted fonts (Plus Jakarta Sans, JetBrains Mono)
assets/img/              favicon, touch icon, social preview image (og-image.png)
assets/img/projects/     dashboard pictures used on the home page and the projects page
tools/dashboards/        the script that builds every project page and its sample data
```

## Deploy

1. Copy everything in this folder (including `CNAME` and `.nojekyll`) into the root of the GitHub Pages repository and push to `main`.
2. In the repository, open **Settings → Pages**:
   - **Source:** Deploy from a branch, `main`, `/ (root)`
   - **Custom domain:** `www.sarunshrestha.com.np` → Save
   - Tick **Enforce HTTPS** once the certificate is ready (can take a few minutes).
3. If the domain doesn't load after that, check the DNS at your domain provider:
   - `www` → CNAME → your `<username>.github.io` address
   - the bare domain → A records `185.199.108.153`, `185.199.109.153`, `185.199.110.153`, `185.199.111.153`

## Contact form

The form on the contact page sends messages through [FormSubmit](https://formsubmit.co) (free, no account) to sarunshrestha03@gmail.com.

- **One-time activation:** the first message anyone sends triggers an "Activate Form" email from FormSubmit. Click it once; every message after that goes straight to your inbox. Until then, visitors see a link to email you directly instead.
- **Where the address lives:** `EMAIL` in `assets/js/main.js` (used by the form) and the form's `action` URL in `contact/index.html` (used only without JavaScript). After activation FormSubmit emails you a random string you can use in place of the address in both.
- **Topic chips:** edit the "What's this about?" options in `contact/index.html`.

## Projects and dashboards

Each project page is generated, so the ten pages stay consistent. Everything about a project (title, description, tech stack, the sample data generator, the charts and tables, architecture steps, data model, code samples and notes) lives in one file: `tools/dashboards/p01_streaming_lakehouse.py` through `p10_fraud_detection_streaming.py`.

To change a project, edit its file and rebuild:

```
python3 tools/dashboards/build.py
```

That rewrites `projects/<slug>/index.html`, `data.json` and the CSV for every project, rebuilds `projects/index.html`, and refreshes the three project tiles on the home page (between the `tiles:start` and `tiles:end` comments in `index.html`). It needs only Python 3 and produces identical files every time, because the sample data comes from a fixed random seed.

- **Which projects appear on the home page:** the first three in `MODULES` in `tools/dashboards/build.py`.
- **Dashboard pictures:** `assets/img/projects/<slug>.jpg` and `.webp` (1200×750) are screenshots of the top of each dashboard. Replace them if a dashboard changes a lot.
- **Sample data:** every dashboard is labelled "Sample data" and says so again at the bottom of the page. Keep that wording if you change the numbers.

## Common edits

All of these are in `index.html` unless noted.

- **Location / availability** (hero): search for `Located in the` and `Open to new opportunities`.
- **Change your photo:** replace `assets/img/sarun-portrait.jpg` and `.webp` (About section, 928×1152) and `assets/img/sarun-avatar.jpg` and `.webp` (round badges, square) with new files of the same names.
- **Footer clock time zone:** `timeZone: 'America/Chicago'` in `assets/js/main.js`.
- **Experience bullets, skills, certifications:** edit the lists directly; the layout adapts.
- **Social preview image:** `assets/img/og-image.png` (1200×630) is what LinkedIn and others show when the link is shared.

## After changing CSS or JavaScript

The HTML files load `style.css?v=...` and `main.js?v=...`. When you change either file, bump that number in `index.html`, `contact/index.html` and `404.html` so visitors' browsers fetch the new version instead of a saved copy. For the project pages, change `VERSION` at the top of `tools/dashboards/build.py` and rebuild.

## Preview locally

From this folder run `python3 -m http.server 8000` and open http://localhost:8000.

## Credits

Fonts: Plus Jakarta Sans, JetBrains Mono and Noto Sans Devanagari (subset), all under the SIL Open Font License 1.1.

Charts: [Apache ECharts](https://echarts.apache.org) 5.6.0, a custom build with only the chart types used here, under the Apache License 2.0 (see `assets/vendor/ECHARTS-LICENSE.txt` and `ECHARTS-NOTICE.txt`).
