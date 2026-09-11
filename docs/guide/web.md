# Browser and phone access

[Back to README](../../README.md)


`orchestral serve` starts the same Agent Host used by the TUI and serves an embedded,
installable mobile web app. The phone is a control client—not a second Agent runtime—so model,
Skill, MCP, workspace policy, approvals, and journals remain on the Host.

For local browser development:

```bash
orchestral serve --pair --backend google --model gemini-2.5-flash -C /path/to/workspace
```

For a phone, terminate HTTPS with a trusted reverse proxy or private-network relay and tell the
Host the browser-visible URL:

```bash
orchestral serve --pair \
  --public-url https://agent.example.com \
  --backend google --model gemini-2.5-flash \
  -C /path/to/workspace
```

Scan the printed QR code. Its fragment contains a one-time, short-lived pairing secret; after the
claim, the browser retains a device credential and the Host stores only its digest. The PWA can
start and continue Sessions, stream durable Run events with cursor-based reconnect, show bounded
Tool/file evidence and progress, resolve input and approval requests, steer or cancel a Run, and
revoke paired devices. API responses, credentials, and transcripts are excluded from the service
worker cache.

For an identity-aware reverse proxy, the Host can instead require and verify a signed RS256 JWT.
This mode does not use browser device pairing: the PWA restores the proxy session from its secure
cookie, while the Host independently verifies the assertion signature, issuer, audience, expiry,
and configured identity claims on every API request.

```bash
orchestral serve \
  --public-url https://agent.example.com \
  --access-jwt-issuer https://access.example.com \
  --access-jwt-jwks-url https://access.example.com/.well-known/jwks.json \
  --access-jwt-audience orchestral \
  --access-jwt-header X-Access-JWT \
  --access-jwt-required-claim email=owner@example.com \
  --backend google --model gemini-2.5-flash \
  -C /path/to/workspace
```

The contract is proxy-neutral: header name, issuer, JWKS endpoint, audience, and repeatable
`NAME=VALUE` claim constraints are deployment configuration. Cloudflare Access, oauth2-proxy, or
another gateway can provide the assertion. Once JWT mode is enabled, protected Host routes accept
only a valid gateway assertion; a stale device token cannot bypass the proxy identity policy.
The proxy must strip or overwrite the configured identity header, and the Host origin should stay
private or loopback-only so clients cannot bypass the proxy and inject their own assertion header.

The default listener is loopback-only. A direct trusted-LAN test can opt into cleartext explicitly
with `--listen 0.0.0.0:8765 --public-url http://HOST:8765 --allow-insecure-http`, but browsers
normally require trusted HTTPS for installation, service workers, notifications, and other PWA
features. Orchestral does not silently publish the Host or upload Agent state; the HTTPS proxy or
relay remains an explicit deployment choice. Device and Session metadata defaults to
`~/.config/orchestral/remote-control.json` on Unix.
