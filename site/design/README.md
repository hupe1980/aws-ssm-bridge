# Design sources

Sources for generated assets in `static/`. Nothing here is served.

| Source | Generates | Command |
|---|---|---|
| `social-card.svg` | `static/social-card.png` | `just social-card` |

The Open Graph card is committed as a PNG because most link-preview scrapers
(Slack, Twitter/X, LinkedIn, iMessage) do not render SVG. Regenerate it after
changing the tagline or the palette, and keep it at 1200×630 — that is the
aspect ratio every one of those previews crops to.
