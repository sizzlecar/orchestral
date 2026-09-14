use crate::{checksum, validate_version};
use anyhow::{ensure, Context, Result};
use std::fs;
use std::path::Path;

fn archive_hash(assets: &Path, version: &str, target: &str) -> Result<String> {
    let file = format!("orchestral-v{version}-{target}.tar.gz");
    let sidecar = fs::read_to_string(assets.join(format!("{file}.sha256")))?;
    let fields: Vec<_> = sidecar.split_whitespace().collect();
    ensure!(
        fields.len() == 2 && fields[1].trim_start_matches('*') == file,
        "checksum filename does not match {file}"
    );
    let actual = checksum(&assets.join(&file))?;
    ensure!(
        fields[0].eq_ignore_ascii_case(&actual),
        "archive checksum mismatch: {file}"
    );
    Ok(actual)
}

pub(super) fn render(version: &str, assets: &Path) -> Result<String> {
    validate_version(version)?;
    let arm = archive_hash(assets, version, "aarch64-apple-darwin").context("macOS ARM archive")?;
    let intel =
        archive_hash(assets, version, "x86_64-apple-darwin").context("macOS Intel archive")?;
    let linux =
        archive_hash(assets, version, "x86_64-unknown-linux-gnu").context("Linux archive")?;
    Ok(format!(
        r##"class Orchestral < Formula
  desc "Runtime for reliable, interactive AI agents"
  homepage "https://orch.pandaailabs.com"
  version "{version}"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/sizzlecar/orchestral/releases/download/v{version}/orchestral-v{version}-aarch64-apple-darwin.tar.gz"
      sha256 "{arm}"
    end
    on_intel do
      url "https://github.com/sizzlecar/orchestral/releases/download/v{version}/orchestral-v{version}-x86_64-apple-darwin.tar.gz"
      sha256 "{intel}"
    end
  end

  on_linux do
    depends_on arch: :x86_64
    depends_on "bubblewrap"
    url "https://github.com/sizzlecar/orchestral/releases/download/v{version}/orchestral-v{version}-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "{linux}"
  end

  def install
    if OS.linux?
      libc = Utils.safe_popen_read("getconf", "GNU_LIBC_VERSION").strip
      match = libc.match(/\Aglibc (\d+\.\d+)/)
      odie "This binary requires glibc 2.35 or newer; build from source on this host." unless match && Version.new(match[1]) >= Version.new("2.35")
    end
    bin.install "orchestral"
    pkgshare.install "configs"
  end

  test do
    assert_equal "orchestral #{{version}}", shell_output("#{{bin}}/orchestral --version").strip
    assert_match "serve", shell_output("#{{bin}}/orchestral serve --help")
  end
end
"##
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formula_uses_only_matching_actual_archive_checksums() {
        let dir = tempfile::tempdir().unwrap();
        for target in [
            "aarch64-apple-darwin",
            "x86_64-apple-darwin",
            "x86_64-unknown-linux-gnu",
        ] {
            let file = format!("orchestral-v1.2.3-{target}.tar.gz");
            fs::write(dir.path().join(&file), target).unwrap();
            fs::write(
                dir.path().join(format!("{file}.sha256")),
                format!("{}  {file}\n", checksum(&dir.path().join(&file)).unwrap()),
            )
            .unwrap();
        }
        let formula = render("1.2.3", dir.path()).unwrap();
        assert!(formula.contains("sizzlecar/orchestral/releases/download/v1.2.3"));
        fs::write(
            dir.path()
                .join("orchestral-v1.2.3-aarch64-apple-darwin.tar.gz"),
            "changed",
        )
        .unwrap();
        assert!(render("1.2.3", dir.path()).is_err());
    }
}
