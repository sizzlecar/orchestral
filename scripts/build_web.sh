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

mkdir -p "${DIST}"
rsync --archive --delete "${DX_OUTPUT}/" "${DIST}/"
echo "Dioxus distribution refreshed at ${DIST}"
