#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
TARGET="${1:?usage: package_release.sh TARGET}"
case "${TARGET}" in
  x86_64-unknown-linux-gnu|aarch64-apple-darwin|x86_64-apple-darwin) ;;
  *) echo "Unsupported release target: ${TARGET}" >&2; exit 1 ;;
esac

VERSION="$(cargo metadata --locked --offline --format-version 1 --no-deps |
  python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "orchestral-cli"))')"
TARGET_DIRECTORY="$(cargo metadata --locked --offline --format-version 1 --no-deps |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
cargo build --locked --release -p orchestral-cli --target "${TARGET}"
BINARY="${TARGET_DIRECTORY}/${TARGET}/release/orchestral"
test "$("${BINARY}" --version)" = "orchestral ${VERSION}"
"${BINARY}" --help >/dev/null
"${BINARY}" serve --help >/dev/null

ARCHIVE="orchestral-v${VERSION}-${TARGET}"
OUTPUT="${TARGET_DIRECTORY}/release-artifacts"
STAGING="$(mktemp -d "${TMPDIR:-/tmp}/orchestral-package.XXXXXX")"
trap 'rm -rf "${STAGING}"' EXIT
mkdir -p "${STAGING}/${ARCHIVE}/configs" "${OUTPUT}"
cp "${BINARY}" "${STAGING}/${ARCHIVE}/orchestral"
cp LICENSE README.md README.zh-CN.md CHANGELOG.md RELEASING.md "${STAGING}/${ARCHIVE}/"
cp configs/orchestral.cli.yaml "${STAGING}/${ARCHIVE}/configs/"
COPYFILE_DISABLE=1 tar -czf "${OUTPUT}/${ARCHIVE}.tar.gz" -C "${STAGING}" "${ARCHIVE}"
(cd "${OUTPUT}" && LC_ALL=C shasum -a 256 "${ARCHIVE}.tar.gz" > "${ARCHIVE}.tar.gz.sha256")
echo "Packaged ${OUTPUT}/${ARCHIVE}.tar.gz"
