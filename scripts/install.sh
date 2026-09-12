#!/bin/sh
# Install an official Orchestral release into a user-owned directory.
set -eu

fail() { printf 'Orchestral install: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'HELP'
Install Orchestral for macOS or Linux.

Usage: sh install.sh [--version VERSION] [--dir DIRECTORY] [--no-modify-path]

  --version VERSION   Install a specific release, such as 0.3.0.
  --dir DIRECTORY     Installation directory (default: ~/.local/bin).
  --no-modify-path    Leave shell startup files unchanged.

Run the installer again to upgrade, or select an older version to roll back.
Your configuration and conversations are preserved.
HELP
}
version=''
install_dir="${HOME}/.local/bin"
modify_path=1
while [ "$#" -gt 0 ]; do
  case "$1" in
    --version) [ "$#" -ge 2 ] || fail '--version needs a value'; version=${2#v}; shift 2 ;;
    --dir) [ "$#" -ge 2 ] || fail '--dir needs a value'; install_dir=$2; shift 2 ;;
    --no-modify-path) modify_path=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) fail "Unknown option: $1. Use --help for usage." ;;
  esac
done
for program in curl tar mktemp; do
  command -v "$program" >/dev/null 2>&1 || fail "$program is required. Install it with your system package manager and retry."
done
case "$(uname -s):$(uname -m)" in
  Darwin:arm64|Darwin:aarch64) target=aarch64-apple-darwin ;;
  Darwin:x86_64) target=x86_64-apple-darwin ;;
  Linux:x86_64|Linux:amd64)
    target=x86_64-unknown-linux-gnu
    command -v getconf >/dev/null 2>&1 || fail 'This release requires glibc 2.35 or newer.'
    glibc=$(getconf GNU_LIBC_VERSION 2>/dev/null) || fail 'This release requires glibc; musl/Alpine binaries are not available.'
    glibc=${glibc#glibc }
    major=${glibc%%.*}; minor=${glibc#*.}; minor=${minor%%.*}
    if [ "$major" -lt 2 ] || { [ "$major" -eq 2 ] && [ "$minor" -lt 35 ]; }; then
      fail 'This release requires glibc 2.35 or newer. Build from source on this system.'
    fi
    ;;
  *) fail 'No release binary is available for this platform. See https://orch.pandaailabs.com for supported platforms.' ;;
esac
release_origin=https://github.com/sizzlecar/orchestral/releases
if [ -z "$version" ]; then
  version=$(curl --proto '=https' --tlsv1.2 -fsSL --connect-timeout 15 --max-time 60 "$release_origin/latest/download/version.txt") ||
    fail 'Could not find a release for this platform. Check your connection and available platforms at https://orch.pandaailabs.com/#install.'
fi
printf '%s\n' "$version" | LC_ALL=C grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || fail 'Version must be MAJOR.MINOR.PATCH, for example 0.3.0.'
archive="orchestral-v${version}-${target}"
asset="${archive}.tar.gz"
base_url="${ORCHESTRAL_RELEASE_BASE_URL:-$release_origin/download/v$version}"
# Explicit mirrors are useful for offline/internal distribution. HTTP is allowed
# only for loopback test servers; normal downloads always require HTTPS.
case "$base_url" in
  https://*) protocol='=https' ;;
  http://127.0.0.1:*|http://localhost:*) protocol='=http,https' ;;
  *) fail 'Release mirror must use HTTPS (loopback HTTP is allowed for local testing).' ;;
esac
scratch=$(mktemp -d "${TMPDIR:-/tmp}/orchestral-install.XXXXXX")
staged=''
cleanup() { rm -rf "$scratch"; if [ -n "$staged" ]; then rm -f "$staged"; fi; }
trap cleanup EXIT HUP INT TERM
printf 'Downloading Orchestral %s for %s…\n' "$version" "$target"
for filename in "$asset" "$asset.sha256"; do
  curl --proto "$protocol" --tlsv1.2 -fsSL --connect-timeout 15 --max-time 300 --retry 2 \
    "$base_url/$filename" -o "$scratch/$filename" || fail "Could not download $filename. Check the version, connection and release assets. Your current installation is unchanged."
done
expected=$(awk 'NR == 1 { print $1 }' "$scratch/$asset.sha256")
printf '%s\n' "$expected" | LC_ALL=C grep -Eq '^[[:xdigit:]]{64}$' || fail 'The checksum file is invalid. Your current installation is unchanged.'
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$scratch/$asset" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
  actual=$(LC_ALL=C shasum -a 256 "$scratch/$asset" | awk '{print $1}')
else
  fail 'A SHA-256 utility is required (sha256sum or shasum).'
fi
[ "$(printf '%s' "$expected" | tr 'A-F' 'a-f')" = "$actual" ] || fail 'Checksum mismatch. Nothing was installed; download again from the official release.'
printf 'Checksum verified.\n'
# Extract only the expected executable, never arbitrary archive paths.
tar -xzf "$scratch/$asset" -C "$scratch" "$archive/orchestral" || fail 'Release archive is missing its executable.'
[ -f "$scratch/$archive/orchestral" ] && [ ! -L "$scratch/$archive/orchestral" ] || fail 'Release executable is invalid.'
chmod 755 "$scratch/$archive/orchestral"
[ "$("$scratch/$archive/orchestral" --version)" = "orchestral $version" ] || fail 'Downloaded executable did not report the expected version.'
[ -n "$install_dir" ] || fail 'Installation directory must not be empty.'
case "$install_dir" in /*) ;; *) install_dir="$(pwd)/$install_dir" ;; esac
mkdir -p "$install_dir" || fail "Cannot create installation directory: $install_dir"
staged=$(mktemp "$install_dir/.orchestral-install.XXXXXX")
cp "$scratch/$archive/orchestral" "$staged"
chmod 755 "$staged"
mv -f "$staged" "$install_dir/orchestral"
staged=''
printf 'Installed Orchestral %s at %s/orchestral\n' "$version" "$install_dir"
case ":${PATH:-}:" in
  *":$install_dir:"*) printf '\nStart in your project directory:\n  orchestral\n' ;;
  *)
    if [ "$modify_path" -eq 1 ]; then
      # Quote arbitrary paths as shell data, including spaces and apostrophes.
      quoted=$(printf '%s' "$install_dir" | sed "s/'/'\\\"'\\\"'/g")
      case "${SHELL:-}" in
        */zsh) profile="${ZDOTDIR:-$HOME}/.zshrc" ;;
        */bash) profile="$HOME/.bashrc" ;;
        *) profile="$HOME/.profile" ;;
      esac
      case "${SHELL:-}" in
        */fish)
          profile="$HOME/.config/fish/conf.d/orchestral.fish"
          mkdir -p "$(dirname "$profile")"
          line="fish_add_path -- '$quoted'"
          ;;
        *) line="export PATH='$quoted':\"\$PATH\"" ;;
      esac
      if [ ! -f "$profile" ] || ! grep -Fqx "$line" "$profile"; then
        printf '\n# Orchestral\n%s\n' "$line" >> "$profile"
      fi
      printf 'Added the installation directory to %s.\n' "$profile"
      printf '\nOpen a new terminal, enter your project directory, then run:\n  orchestral\n'
    else
      printf '\nAdd %s to PATH, or run:\n  "%s/orchestral"\n' "$install_dir" "$install_dir"
    fi
    ;;
esac
printf '\nSetup and local models: https://orch.pandaailabs.com/#quickstart\n'
