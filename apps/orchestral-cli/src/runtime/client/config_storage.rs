//! Generated configuration is immutable by content and published atomically.
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;
use orchestral_core::agent_protocol::wire::Digest;

pub(super) fn publish(directory: &Path, suffix: &str, content: &str) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(directory)
        .with_context(|| format!("create configuration directory '{}'", directory.display()))?;
    let path = directory.join(format!("{}{suffix}", Digest::sha256(content)));
    if fs::read_to_string(&path).ok().as_deref() == Some(content) {
        return Ok(path);
    }
    let temporary = directory.join(format!(".orch-config-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> std::io::Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.with_context(|| format!("publish generated configuration '{}'", path.display()))?;
    Ok(path)
}
