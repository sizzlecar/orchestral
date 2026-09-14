# Public Orchestral website

`orch.pandaailabs.com` is the public product and installation site. The existing
`orchestral.pandaailabs.com` remains a private Agent control endpoint protected by Cloudflare
Access. This Worker serves static public files and receives no model credentials.

Build and preview from the repository root with Node.js 22 and Wrangler 4.130.0:

```sh
node scripts/build_site.cjs
npx wrangler@4.130.0 dev --config deploy/cloudflare/orchestral-site/wrangler.jsonc
```

The build copies `assets/brand/` SVG/PNG files and the `scripts/install.sh` and
`scripts/install.ps1` installers into `public/`. Those generated copies are ignored;
change the source assets instead. After changing `assets/brand/social-card.svg`, run
`node scripts/export_brand.cjs` before building to refresh the social preview PNG.
Browser regression checks use the
pinned Playwright dependency in `scripts/package-lock.json`:

```sh
npm ci --prefix scripts --ignore-scripts
NODE_PATH="$PWD/scripts/node_modules" node scripts/site_smoke.cjs
```

The local check uses an installed Chrome by default; set `PWA_SMOKE_CHANNEL=chromium` after
installing Playwright Chromium if needed. It covers keyboard tabs, clipboard copying, mobile
widths, and both pre-release and published-release states without paid model calls.

Deploy using a Cloudflare account that owns the `pandaailabs.com` zone:

```sh
node scripts/build_site.cjs
npx wrangler@4.130.0 deploy --dry-run --config deploy/cloudflare/orchestral-site/wrangler.jsonc
npx wrangler@4.130.0 deploy --config deploy/cloudflare/orchestral-site/wrangler.jsonc
```

The Custom Domain configuration provisions routing and TLS. Verify the HTTPS homepage,
`/install.sh`, `/install.ps1`, and a missing path after deployment. Do not change the private
control domain or its Cloudflare Access policy when deploying this public site.

Before the first public release, the page explains source installation and marks binary
installers as pending. It checks public GitHub release metadata and switches to an available
state only when all advertised platform archives and checksum files exist. API failure leaves
the preparation state visible. Publish no credentials or local deployment configuration.
