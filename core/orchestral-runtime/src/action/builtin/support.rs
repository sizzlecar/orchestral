use std::collections::{HashMap, HashSet};

use tokio::io::AsyncReadExt;

/// Build the exact process environment authorized by the Host.
///
/// The ambient environment is never inherited wholesale. Sandbox-owned values
/// are appended after the allowlisted Host values.
pub(super) fn build_allowlisted_env(
    allowlist: &HashSet<String>,
    sandbox_environment: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut environment = HashMap::new();
    for name in allowlist {
        if let Ok(value) = std::env::var(name) {
            environment.insert(name.clone(), value);
        }
    }
    environment.extend(sandbox_environment.clone());
    environment
}

pub(super) async fn read_stream_limited<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut buffer = [0_u8; 8192];
    let mut kept = Vec::new();
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(kept.len());
        let copy = remaining.min(read);
        kept.extend_from_slice(&buffer[..copy]);
        truncated |= copy < read;
    }
    Ok((kept, truncated))
}

pub(super) fn truncate_utf8_lossy(bytes: &[u8], max_bytes: usize) -> (String, bool, usize) {
    let total = bytes.len();
    let kept = bytes.get(..max_bytes.min(total)).unwrap_or(bytes);
    (
        String::from_utf8_lossy(kept).into_owned(),
        total > max_bytes,
        total,
    )
}

pub(super) fn stderr_preview(stderr: &str, max_chars: usize) -> String {
    let compact = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.is_empty() {
        return "<empty>".to_owned();
    }
    if compact.chars().count() <= max_chars {
        return compact;
    }
    format!("{}...", compact.chars().take(max_chars).collect::<String>())
}
