# sarunshrestha.com.np

Personal site for Sarun Shrestha, Senior Data Engineer. Plain HTML, CSS and JavaScript with no build step, hosted on GitHub Pages.

## Files

```
index.html               the home page (all main text content lives here)
contact/index.html       contact page with the message form (www.sarunshrestha.com.np/contact/)
404.html                 "page not found" page
CNAME                    custom domain for GitHub Pages (www.sarunshrestha.com.np)
.nojekyll                tells GitHub Pages to serve files as-is
assets/css/style.css     styles (colors and fonts are at the top, under "Tokens")
assets/js/main.js        intro greeting, hero pipeline animation, menu, accordion, cursor
assets/fonts/            self-hosted fonts (Plus Jakarta Sans, JetBrains Mono)
assets/img/              favicon, touch icon, social preview image (og-image.png)
```

## Deploy

1. Copy everything in this folder (including `CNAME` and `.nojekyll`) into the root of the `sarun2003.github.io` repository and push to `main`.
2. In the repository, open **Settings → Pages**:
   - **Source:** Deploy from a branch, `main`, `/ (root)`
   - **Custom domain:** `www.sarunshrestha.com.np` → Save
   - Tick **Enforce HTTPS** once the certificate is ready (can take a few minutes).
3. If the domain doesn't load after that, check the DNS at your domain provider:
   - `www` → CNAME → `sarun2003.github.io`
   - the bare domain → A records `185.199.108.153`, `185.199.109.153`, `185.199.110.153`, `185.199.111.153`

## Contact form

The form on the contact page sends messages through [FormSubmit](https://formsubmit.co) (free, no account) to sarun.shrestha.dev@gmail.com.

- **One-time activation:** the first message anyone sends triggers an "Activate Form" email from FormSubmit. Click it once; every message after that goes straight to your inbox. Until then, visitors see a link to email you directly instead.
- **Hide your address (optional):** after activation FormSubmit emails you a random string. Replace the email in both `formsubmit.co/...` URLs in `contact/index.html` with it.
- **Topic chips:** edit the "What's this about?" options in `contact/index.html`.

## Common edits

All of these are in `index.html` unless noted.

- **Location / availability** (hero): search for `Located in the` and `Open to new opportunities`.
- **Add a project:** in the `Work` section there is a commented-out project block. Copy it, fill in the title, link and discipline, and delete the "Projects coming soon" item.
- **Change your photo:** replace `assets/img/sarun-portrait.jpg` and `.webp` (About section, 928×1152) and `assets/img/sarun-avatar.jpg` and `.webp` (round badges, square) with new files of the same names.
- **Footer clock time zone:** `timeZone: 'America/Chicago'` in `assets/js/main.js`.
- **Experience bullets, skills, certifications:** edit the lists directly; the layout adapts.
- **Social preview image:** `assets/img/og-image.png` (1200×630) is what LinkedIn and others show when the link is shared.

## Preview locally

From this folder run `python3 -m http.server 8000` and open http://localhost:8000.

## Credits

Fonts: Plus Jakarta Sans, JetBrains Mono and Noto Sans Devanagari (subset), all under the SIL Open Font License 1.1.
