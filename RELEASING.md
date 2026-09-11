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

This process does not run `cargo publish`, deploy a Host, or restart an existing service.
