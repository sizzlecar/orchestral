# Releasing Orchestral

Releases distribute the `orchestral` CLI with its embedded PWA. The workspace is currently
pre-1.0. Agent Protocol v1 is a wire contract version, separate from the package version.

## Prepare a version

1. Update `[workspace.package].version` and versioned internal dependencies in `Cargo.toml`.
   Examples and protocol testkits have independent, unpublished package versions.
2. Refresh `Cargo.lock` without upgrading unrelated dependencies, and add a matching
   `## [VERSION]` section to `CHANGELOG.md`. Include CLI, SDK, configuration, and recovery
   compatibility changes. Keep both README translations consistent.
3. Rebuild the PWA with `bash scripts/build_web.sh` and include the generated `dist/` changes.
   Dioxus CLI 0.7.9, `rsync`, and the `wasm32-unknown-unknown` target are required.
4. Review the diff for local configuration, credentials, test outputs, and stale examples.
   `scripts/check_agent_surface.sh` rejects retired tracked entry points;
   `scripts/check_workspace.sh` checks resolved core production/build dependency paths,
   including workspace inheritance, renamed dependencies, and target-specific dependencies.
   Test-only plugin dependencies are allowed.

## Validate

```sh
bash scripts/check_workspace.sh
bash scripts/check_agent_surface.sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets
cargo test --locked --workspace --doc
cargo check --locked -p orchestral-web --target wasm32-unknown-unknown --features web
uv run --locked --project testing/orchestral-harbor \
  python -m unittest discover -s testing/orchestral-harbor/tests -v
```

Run the [PWA browser checks](web/orchestral-web/README.md#browser-regression-smoke) against
the rebuilt bundle, including service-worker and narrow-screen cases. CI repeats Rust checks
on Linux, macOS and Windows, builds and tests the PWA and public website on Ubuntu 24.04 (the pinned Dioxus binary requires glibc 2.39), and runs Linux Harbor process tests. CLI release binaries still build on Ubuntu 22.04. Onboarding smoke uses a local HTTP fixture and makes no paid API calls.
The repository toolchain comes from `rust-toolchain.toml`.

When disk space is limited, set `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, and
`CARGO_PROFILE_TEST_DEBUG=0`, as CI does. Keep the same settings across checks to reuse builds.

Live model, native Codex, and local MCP tests require their documented environment and explicit
`--ignored` selection. The **Live Model Smoke** workflow is manual and may incur model charges.
Record which optional checks and manual terminal/device acceptance checks actually ran.

CI also verifies the crates.io packages with Cargo's workspace publication dry run:

```sh
cargo run --locked -p orchestral-release -- prepare --output target/release-plan
cargo run --locked -p orchestral-release -- stage-web
cargo publish --workspace --locked --allow-dirty --dry-run
```

Run these commands after building and testing the PWA. `stage-web` copies that bundle into
the ignored `apps/orchestral-cli/web-dist/` packaging input; it is not a second committed
distribution. The CLI package includes this directory, so `cargo install` does not depend on
paths outside the crate. Normal workspace builds use the canonical `web/orchestral-web/dist/`.
Remove the staged directory before returning to PWA development to use fresh workspace assets.
The dry run compiles the packaged sources and checks the publishable dependency graph without
uploading anything. Protocol testkits, examples, web source, and the release helper stay unpublished.

## Build archives

Run on a machine matching the selected target; packaging executes the resulting binary:

```sh
bash scripts/package_release.sh aarch64-apple-darwin
```

| Target | Release runner | Archive |
| --- | --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 22.04 | `orchestral-vVERSION-x86_64-unknown-linux-gnu.tar.gz` |
| `aarch64-apple-darwin` | macOS 14 | `orchestral-vVERSION-aarch64-apple-darwin.tar.gz` |
| `x86_64-apple-darwin` | macOS 15 Intel | `orchestral-vVERSION-x86_64-apple-darwin.tar.gz` |
| `x86_64-pc-windows-msvc` | Windows 2022 | `orchestral-vVERSION-x86_64-pc-windows-msvc.zip` |

On Windows, run `./scripts/package_release.ps1` from PowerShell. Archives and matching
`.sha256` files are written to `release-artifacts/` inside the Cargo target directory. The script
checks `--version`, root help, and `serve --help` without model calls. It includes the executable,
license, README translations, changelog, release instructions, and default configuration. Linux archives need glibc
2.35+; sandboxed commands additionally need bubblewrap and usable unprivileged namespaces.
macOS binaries are not Developer ID notarized. Windows binaries are not Authenticode signed.
Native Windows shell commands require exact Host approval; WSL provides the Linux sandbox path.
Native Windows MCP connections use Streamable HTTP; local stdio MCP requires the WSL sandbox.
Windows persists file contents before replacement; Unix additionally syncs parent directories.

## Installers

The public entry point is [orch.pandaailabs.com](https://orch.pandaailabs.com).
`orchestral.pandaailabs.com` remains the existing private control service.

For a specific version or installation directory, download the script, inspect it, then run:

```sh
curl -fsSL https://orch.pandaailabs.com/install.sh -o install.sh
sh install.sh --version 0.3.0 --dir "$HOME/.local/bin" --no-modify-path
```

```powershell
Invoke-WebRequest https://orch.pandaailabs.com/install.ps1 -OutFile install.ps1
& ./install.ps1 -Version 0.3.0 -InstallDir "$env:LOCALAPPDATA\Orchestral\bin" -NoModifyPath
```

If your PowerShell execution policy does not permit downloaded scripts, inspect the script
and use the website's `irm ... | iex` command. The installer does not change execution policy.

Default directories are `~/.local/bin` on Unix and `%LOCALAPPDATA%\Orchestral\bin` on Windows.
Omit the version to use the latest public release; rerun with an earlier version to roll back.
The scripts resolve `version.txt` and download archives directly from the public GitHub Release;
the static website does not need an asset proxy.
Checksums and `--version` are verified before replacing the current executable. A failed
download or hash check leaves the current binary unchanged. During release preparation,
no public assets exist and the installer reports that status instead of claiming success.

To uninstall, remove only the installed `orchestral` / `orchestral.exe`. Remove the Orchestral
PATH entry or marked shell startup line if it was added. Keep `.orchestral/` directories to
retain configuration and conversation history. Remove them separately only if you want to
delete that data.

`ORCHESTRAL_RELEASE_BASE_URL` selects an explicit release-asset mirror; the PowerShell
installer also accepts `-ReleaseBaseUrl`. Use HTTPS; HTTP is accepted only on loopback for
local installer tests. Version selection and hash checks still apply to mirrors.

## Public website

The website is a separate Cloudflare static-assets Worker; it does not host Agent sessions
or handle model credentials. See [deployment instructions](deploy/cloudflare/orchestral-site/README.md).
Always run `node scripts/build_site.cjs` before preview or deployment to copy the canonical
installers from `scripts/`. Never edit generated copies in `public/`.

## Tag and publish

After the release commit is reviewed and merged, create and push its matching annotated tag:

```sh
git tag -a v0.3.0 -m "Orchestral v0.3.0"
git push origin v0.3.0
```

The **Release** workflow verifies the tag/version/notes, runs the reusable CI workflow, and
packages each native target with the PWA built and browser-tested in that same run. Only after
all builds succeed does it create a **draft** GitHub Release with the archives and checksums.
Review that draft and publish it from GitHub. Existing releases are not overwritten.

For a rehearsal, run **Release** with `workflow_dispatch` on a branch. This runs validation and
uploads downloadable workflow artifacts without creating a tag or a GitHub Release. A failed
CI job or missing artifact prevents the draft step from running.

The draft also contains the verified `.crate` files, their tested PWA archive, `release.json`
(version, source commit, package inventory), `version.txt`, and a Homebrew formula generated from
the native archive checksums. Keep these artifacts together when reviewing the release.

## Cargo and Homebrew publication

The **Release distribution** workflow starts when a stable GitHub Release is published. It can
also be dispatched with an existing public tag to retry an interrupted distribution. Its workflow
must already be on the default branch before publishing the release.

Before the first distribution, initialize the public `sizzlecar/homebrew-orchestral` repository
with a default branch, and configure these repository secrets through the normal GitHub settings:

- `CARGO_TOKEN`: crates.io publication rights for the workspace packages.
- `RELEASE_GITHUB_TOKEN`: write access to `sizzlecar/homebrew-orchestral`.

Missing credentials fail the relevant job; they do not silently skip a channel. No credentials are
stored in release artifacts. Cargo uses its normal registry authentication and dependency ordering.
The helper checks the public index: already published versions must be unyanked and have exactly
the reviewed archive checksum. It regenerates only missing packages, compares their bytes to the
verified draft artifacts, then publishes them. `--no-verify` at this final upload stage reuses the
earlier full package verification; it does not substitute for that CI check. Source commit, version,
and package inventory must match the reviewed release.

Crates.io publication is not atomic. If a later package fails after dependencies were uploaded,
retry the same reviewed release. A mismatching existing version is an error, not something the
workflow overwrites. The Homebrew job separately requires the tag to remain the latest stable
release, so replaying an older release cannot downgrade the tap.

After publication, the workflow installs the public crate on Linux, installs and tests the tap on
Linux and both supported macOS architectures, and runs the public-asset installers on all four
archive targets. Its final result requires all publication and installation jobs to succeed.
These are install/version/help checks; they do not claim live model or sandbox capability on every
host. Website deployment, live agent validation, and Host operations remain separate.

Once those channels are actually published, users can install the release with:

```sh
cargo install orchestral-cli --version 0.3.0 --locked
brew install sizzlecar/orchestral/orchestral
```

Before the first 0.3.0 publication, do not advertise these as available 0.3.0 binaries. The public
registry already contains the older 0.1.0 and 0.2.0 packages; the absence of a GitHub Release does
not mean crates.io is empty.
