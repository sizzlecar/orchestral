# Orchestral brand assets

Editable SVG sources live here. The mark combines an underscore, a right-pointing
chevron, and a round period (`_ > .`) on a shared baseline. Preserve the spacing
and proportions in `symbol.svg`; use `_>.` as its plain-text terminal counterpart.

| Asset | Use |
| --- | --- |
| `symbol.svg` | Lime symbol on a transparent background |
| `logo-mark.svg` | Square mark with a hard shadow |
| `wordmark.svg` | Light wordmark for dark backgrounds |
| `favicon.svg` | Symbol on a lime square for small browser icons |
| `readme-banner.svg` | Responsive repository banner, 1280 × 360 |
| `social-card.svg` | Editable social preview, 1280 × 640 |
| `social-card.png` | GitHub/social preview export, opaque PNG under 1 MB |

The palette is charcoal `#1d2027`, cream `#f5f0cf`, lime `#83ff6b`, cyan
`#63dbea`, muted text `#a3a8b5`, and borders `#555b6b`. Use square corners,
solid offset shadows, subtle grids, and bold monospace type. Keep grids and
scan lines out of terminal conversation text.

Use **A runtime for reliable, interactive AI agents.** as the product description
and **Agents are the new processes. Orchestral is the runtime.** as the positioning
line. Describe the current product as an interactive agent with sessions, guarded
tools and several client interfaces. Multi-agent orchestration is the direction;
do not present multi-agent scheduling as an available feature.

Run `node scripts/export_brand.cjs` to regenerate the PNG with the repository's
Playwright dependency and installed Chrome. Set `PWA_SMOKE_CHANNEL=chromium`
to use Playwright Chromium. Run `node scripts/build_site.cjs` to copy the assets
and installers into the website output. Do not edit those generated copies.

The website uses the PNG through Open Graph metadata. GitHub's separate
repository social preview is configured under **Settings → Social preview**
by uploading `social-card.png`; committing the file does not change that setting.
