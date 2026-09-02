#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_ROOT="${REPO_ROOT}/web/orchestral-web"
DX_OUTPUT="${REPO_ROOT}/target/dx/orchestral-web/release/web/public"
DIST="${WEB_ROOT}/dist"

command -v dx >/dev/null || {
  echo "Dioxus CLI is required: cargo install dioxus-cli --version 0.7.9 --locked" >&2
  exit 1
}
command -v rsync >/dev/null || {
  echo "rsync is required to refresh the embedded web distribution" >&2
  exit 1
}

cd "${WEB_ROOT}"
# Dioxus fingerprints release assets but does not remove fingerprints from a
# previous build. Clean this generated target so the embedded binary cannot
# retain dead JavaScript or WASM bundles.
rm -rf "${DX_OUTPUT}"
dx build --web --release --debug-symbols=false --cargo-args="--locked"

# Bind the service-worker cache/version to the generated JavaScript bundle.
# The worker source therefore changes on every application release and can
# atomically move already-installed PWAs to the matching WASM shell.
BUNDLE_ID="$(sed -n 's/.*orchestral-web-\([A-Za-z0-9_-]*\)\.js.*/\1/p' "${DX_OUTPUT}/index.html" | head -1)"
if [[ -z "${BUNDLE_ID}" ]]; then
  echo "Unable to find the fingerprinted web bundle in generated index.html" >&2
  exit 1
fi
sed -i '' "s/__ORCHESTRAL_BUILD_ID__/${BUNDLE_ID}/g" "${DX_OUTPUT}/sw.js"

mkdir -p "${DIST}"
rsync --archive --delete "${DX_OUTPUT}/" "${DIST}/"
echo "Dioxus distribution refreshed at ${DIST}"
