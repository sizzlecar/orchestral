# Deployment assets

`cloudflare/orchestral-site/` contains the public website and installation entry point at
[orch.pandaailabs.com](https://orch.pandaailabs.com). See its [deployment guide](cloudflare/orchestral-site/README.md).

`cloudflare/orchestral-attachments/` contains the optional private R2 attachment Worker.
It has its own `package.json`, tests, and Wrangler configuration. It is separate from the
native CLI release and is only needed when configuring R2-backed attachments.

```sh
cd deploy/cloudflare/orchestral-attachments
npm ci
npm run check
npm test
```

Host startup, pairing, HTTPS, and gateway authentication are documented in the
[main README](../README.md#mobile-control-pwa). Configure the Worker for your own account
before using its deployment command. CLI archive preparation is documented in
[RELEASING.md](../RELEASING.md).
