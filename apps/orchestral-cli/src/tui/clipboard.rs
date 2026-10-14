//! Image and clipboard paste support for the TUI composer.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use orchestral_core::agent_protocol::wire::Content;

const MAX_IMAGE_BYTES: usize = 20 * 1024 * 1024;
const CLIPBOARD_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PastedImage {
    pub media_type: String,
    pub data_base64: String,
    pub byte_size: usize,
    pub dimensions: Option<(u32, u32)>,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImageAttachment {
    pub id: usize,
    pub marker: String,
    pub media_type: String,
    pub data_base64: String,
    pub byte_size: usize,
    pub dimensions: Option<(u32, u32)>,
    pub label: String,
}

impl ImageAttachment {
    pub(crate) fn summary(&self) -> String {
        let format = self
            .media_type
            .strip_prefix("image/")
            .unwrap_or(&self.media_type)
            .to_ascii_uppercase();
        let size = format_byte_size(self.byte_size);
        match self.dimensions {
            Some((width, height)) if width > 0 && height > 0 => {
                format!("{format} {width}×{height} · {size}")
            }
            _ => format!("{format} · {size}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClipboardPaste {
    Images(Vec<PastedImage>),
    Text(String),
}

pub(crate) fn submission_contents(text: &str, images: &[ImageAttachment]) -> Vec<Content> {
    let mut contents = Vec::with_capacity(1 + images.len());
    if !text.trim().is_empty() {
        contents.push(Content::text(text.to_owned()));
    }
    for image in images {
        contents.push(Content::inline_image(
            image.media_type.clone(),
            image.data_base64.clone(),
        ));
    }
    contents
}

pub(crate) fn decode_image_bytes(
    bytes: &[u8],
    label: impl Into<String>,
) -> Result<PastedImage, String> {
    let label = label.into();
    if bytes.is_empty() {
        return Err(format!("Image '{label}' is empty"));
    }
    if bytes.len() > MAX_IMAGE_BYTES {
        return Err(format!(
            "Image '{label}' exceeds the 20 MiB limit ({})",
            format_byte_size(bytes.len())
        ));
    }
    let (media_type, dimensions) = inspect_image_header(bytes).ok_or_else(|| {
        format!("Unsupported or corrupt image '{label}'; expected PNG, JPEG, WebP, or GIF")
    })?;
    Ok(PastedImage {
        media_type: media_type.to_owned(),
        data_base64: BASE64_STANDARD.encode(bytes),
        byte_size: bytes.len(),
        dimensions,
        label,
    })
}

fn inspect_image_header(bytes: &[u8]) -> Option<(&'static str, Option<(u32, u32)>)> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        let dims = if bytes.len() >= 24 && &bytes[12..16] == b"IHDR" {
            let width = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
            let height = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
            Some((width, height))
        } else {
            None
        };
        return Some(("image/png", dims));
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some(("image/jpeg", jpeg_dimensions(bytes)));
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        let dims = if bytes.len() >= 10 {
            let width = u32::from(u16::from_le_bytes([bytes[6], bytes[7]]));
            let height = u32::from(u16::from_le_bytes([bytes[8], bytes[9]]));
            Some((width, height))
        } else {
            None
        };
        return Some(("image/gif", dims));
    }
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some(("image/webp", webp_dimensions(bytes)));
    }
    None
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    let mut index = 2;
    while index + 4 <= bytes.len() {
        if bytes[index] != 0xFF {
            index += 1;
            continue;
        }
        while index < bytes.len() && bytes[index] == 0xFF {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        let marker = bytes[index];
        index += 1;
        if marker == 0xD8 || marker == 0xD9 || (0xD0..=0xD7).contains(&marker) || marker == 0x01 {
            continue;
        }
        if index + 2 > bytes.len() {
            break;
        }
        let length = usize::from(u16::from_be_bytes([bytes[index], bytes[index + 1]]));
        if length < 2 || index + length > bytes.len() {
            break;
        }
        if matches!(
            marker,
            0xC0..=0xC3 | 0xC5..=0xC7 | 0xC9..=0xCB | 0xCD..=0xCF
        ) && length >= 7
        {
            let height = u32::from(u16::from_be_bytes([bytes[index + 3], bytes[index + 4]]));
            let width = u32::from(u16::from_be_bytes([bytes[index + 5], bytes[index + 6]]));
            return Some((width, height));
        }
        index += length;
    }
    None
}

fn webp_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 30 {
        return None;
    }
    match &bytes[12..16] {
        b"VP8X" => {
            let width = 1 + u32::from_le_bytes([bytes[24], bytes[25], bytes[26], 0]);
            let height = 1 + u32::from_le_bytes([bytes[27], bytes[28], bytes[29], 0]);
            Some((width, height))
        }
        b"VP8L" if bytes[20] == 0x2f => {
            let bits = u32::from_le_bytes([bytes[21], bytes[22], bytes[23], bytes[24]]);
            let width = (bits & 0x3FFF) + 1;
            let height = ((bits >> 14) & 0x3FFF) + 1;
            Some((width, height))
        }
        b"VP8 " if bytes[23..26] == [0x9d, 0x01, 0x2a] => {
            let width = u32::from(u16::from_le_bytes([bytes[26], bytes[27]]) & 0x3FFF);
            let height = u32::from(u16::from_le_bytes([bytes[28], bytes[29]]) & 0x3FFF);
            Some((width, height))
        }
        _ => None,
    }
}

/// Inspects bracketed-paste text for either a `data:image/...;base64,...` URI
/// or one or more local image file paths (supporting shell escaping, quotes,
/// and `file://` URLs). Returns `Ok(None)` when the text is ordinary text.
pub(crate) fn extract_pasted_images(
    text: &str,
    workspace_root: &str,
) -> Result<Option<Vec<PastedImage>>, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let Some(rest) = trimmed.strip_prefix("data:image/") {
        let Some((format_and_params, encoded)) = rest.split_once(',') else {
            return Err("Invalid data:image URI; missing base64 payload".to_owned());
        };
        if !format_and_params.ends_with(";base64") {
            return Err("Unsupported data:image URI; expected ';base64' encoding".to_owned());
        }
        let clean: String = encoded
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect();
        let bytes = BASE64_STANDARD
            .decode(clean)
            .map_err(|error| format!("Invalid base64 image data: {error}"))?;
        let image = decode_image_bytes(&bytes, "clipboard")?;
        return Ok(Some(vec![image]));
    }

    let candidates = parse_image_path_candidates(trimmed, workspace_root);
    if candidates.is_empty() {
        return Ok(None);
    }
    let mut images = Vec::with_capacity(candidates.len());
    for path in candidates {
        let label = path
            .file_name()
            .unwrap_or(path.as_os_str())
            .to_string_lossy()
            .into_owned();
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("Could not read image '{}': {error}", path.display()))?;
        images.push(decode_image_bytes(&bytes, label)?);
    }
    Ok(Some(images))
}

fn parse_image_path_candidates(trimmed: &str, workspace_root: &str) -> Vec<PathBuf> {
    if let Some(single) = resolve_existing_image_path(trimmed, workspace_root) {
        return vec![single];
    }
    let tokens = split_shell_tokens(trimmed);
    if tokens.is_empty() {
        return Vec::new();
    }
    let mut resolved = Vec::with_capacity(tokens.len());
    for token in tokens {
        let Some(path) = resolve_existing_image_path(&token, workspace_root) else {
            return Vec::new();
        };
        resolved.push(path);
    }
    resolved
}

fn resolve_existing_image_path(raw: &str, workspace_root: &str) -> Option<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let unquoted = trimmed
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(trimmed);

    for candidate_str in [
        normalize_path_string(unquoted),
        normalize_path_string(&unescape_shell_backslashes(unquoted)),
    ] {
        if !has_supported_image_extension(&candidate_str) {
            continue;
        }
        let candidate = PathBuf::from(&candidate_str);
        let resolved = if candidate.is_absolute() {
            candidate
        } else if !workspace_root.is_empty() {
            Path::new(workspace_root).join(&candidate)
        } else {
            candidate
        };
        if resolved.is_file() {
            return Some(resolved);
        }
    }
    None
}

fn normalize_path_string(input: &str) -> String {
    if let Some(rest) = input.strip_prefix("file://") {
        let path_part = rest.strip_prefix("localhost").unwrap_or(rest);
        let decoded = percent_decode(path_part);
        #[cfg(windows)]
        if decoded.starts_with('/') && decoded.as_bytes().get(2) == Some(&b':') {
            return decoded[1..].to_owned();
        }
        return decoded;
    }
    if let Some(rest) = input.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            return Path::new(&home).join(rest).to_string_lossy().into_owned();
        }
    }
    input.to_owned()
}

fn unescape_shell_backslashes(input: &str) -> String {
    #[cfg(windows)]
    {
        input.to_owned()
    }
    #[cfg(not(windows))]
    {
        let mut out = String::with_capacity(input.len());
        let mut chars = input.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    out.push(next);
                    chars.next();
                    continue;
                }
            }
            out.push(ch);
        }
        out
    }
}

fn split_shell_tokens(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match quote {
            Some(q) if ch == q => {
                quote = None;
            }
            Some('"') if ch == '\\' => {
                if let Some(&next) = chars.peek() {
                    if matches!(next, '"' | '\\' | '$' | '`') {
                        current.push(next);
                        chars.next();
                        continue;
                    }
                }
                current.push(ch);
            }
            Some(_) => current.push(ch),
            None => match ch {
                '\'' | '"' => quote = Some(ch),
                '\\' if !cfg!(windows) => {
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                }
                c if c.is_whitespace() => {
                    if !current.is_empty() {
                        tokens.push(std::mem::take(&mut current));
                    }
                }
                _ => current.push(ch),
            },
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn has_supported_image_extension(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "png" | "jpg" | "jpeg" | "webp" | "gif"
            )
        })
}

pub(crate) async fn read_system_clipboard(workspace_root: &str) -> Result<ClipboardPaste, String> {
    if let Some(override_image) = std::env::var_os("ORCHESTRAL_TUI_CLIPBOARD_IMAGE") {
        let raw = override_image.to_string_lossy();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("No image or text on clipboard".to_owned());
        }
        if let Some(images) = extract_pasted_images(trimmed, workspace_root)? {
            return Ok(ClipboardPaste::Images(images));
        }
        let path = PathBuf::from(trimmed);
        let resolved = if path.is_absolute() || workspace_root.is_empty() {
            path
        } else {
            Path::new(workspace_root).join(path)
        };
        let label = resolved
            .file_name()
            .unwrap_or(resolved.as_os_str())
            .to_string_lossy()
            .into_owned();
        let bytes = std::fs::read(&resolved).map_err(|error| {
            format!(
                "Could not read clipboard image '{}': {error}",
                resolved.display()
            )
        })?;
        return Ok(ClipboardPaste::Images(vec![decode_image_bytes(
            &bytes, label,
        )?]));
    }
    if let Some(override_text) = std::env::var_os("ORCHESTRAL_TUI_CLIPBOARD_TEXT") {
        let text = override_text.to_string_lossy().into_owned();
        if text.is_empty() {
            return Err("No image or text on clipboard".to_owned());
        }
        if let Some(images) = extract_pasted_images(&text, workspace_root)? {
            return Ok(ClipboardPaste::Images(images));
        }
        return Ok(ClipboardPaste::Text(text));
    }

    if cfg!(target_os = "macos") {
        return read_macos_clipboard(workspace_root).await;
    }
    if cfg!(target_os = "windows") {
        return read_windows_clipboard(workspace_root).await;
    }
    read_linux_clipboard(workspace_root).await
}

async fn read_macos_clipboard(workspace_root: &str) -> Result<ClipboardPaste, String> {
    const JXA_SCRIPT: &str = r#"
ObjC.import('AppKit');
ObjC.import('Foundation');
const pb = $.NSPasteboard.generalPasteboard;
let data = pb.dataForType($.NSPasteboardTypePNG);
if (data.isNil()) {
    const tiff = pb.dataForType($.NSPasteboardTypeTIFF);
    if (!tiff.isNil()) {
        const rep = $.NSBitmapImageRep.imageRepWithData(tiff);
        if (!rep.isNil()) {
            data = rep.representationUsingTypeProperties($.NSBitmapImageFileTypePNG, $({}));
        }
    }
}
if (!data.isNil()) {
    'IMAGE_BASE64:' + data.base64EncodedStringWithOptions(0).js;
} else {
    const urls = pb.readObjectsForClassesOptions($([$.NSURL.class]), $({}));
    let paths = [];
    if (!urls.isNil() && urls.count > 0) {
        for (let i = 0; i < urls.count; i++) {
            const url = urls.objectAtIndex(i);
            if (url.isFileURL) {
                paths.push(url.path.js);
            }
        }
    }
    if (paths.length > 0) {
        'FILES:' + JSON.stringify(paths);
    } else {
        const str = pb.stringForType($.NSPasteboardTypeString);
        if (!str.isNil()) {
            'TEXT:' + str.js;
        } else {
            'EMPTY';
        }
    }
}
"#;
    if let Ok(output) =
        run_clipboard_command("osascript", &["-l", "JavaScript", "-e", JXA_SCRIPT]).await
    {
        let stdout = String::from_utf8_lossy(&output);
        let trimmed = stdout.trim_end_matches(&['\r', '\n'][..]);
        if let Some(b64) = trimmed.strip_prefix("IMAGE_BASE64:") {
            let clean: String = b64.chars().filter(|c| !c.is_ascii_whitespace()).collect();
            let bytes = BASE64_STANDARD
                .decode(clean)
                .map_err(|error| format!("Invalid clipboard image data: {error}"))?;
            return Ok(ClipboardPaste::Images(vec![decode_image_bytes(
                &bytes,
                "clipboard.png",
            )?]));
        }
        if let Some(files_json) = trimmed.strip_prefix("FILES:") {
            if let Ok(paths) = serde_json::from_str::<Vec<String>>(files_json) {
                let mut images = Vec::new();
                for path_str in &paths {
                    if let Some(resolved) = resolve_existing_image_path(path_str, workspace_root) {
                        let label = resolved
                            .file_name()
                            .unwrap_or(resolved.as_os_str())
                            .to_string_lossy()
                            .into_owned();
                        let bytes = std::fs::read(&resolved).map_err(|error| {
                            format!(
                                "Could not read clipboard image '{}': {error}",
                                resolved.display()
                            )
                        })?;
                        images.push(decode_image_bytes(&bytes, label)?);
                    }
                }
                if !images.is_empty() {
                    return Ok(ClipboardPaste::Images(images));
                }
                if !paths.is_empty() {
                    return Ok(ClipboardPaste::Text(paths.join(" ")));
                }
            }
        }
        if let Some(text) = trimmed.strip_prefix("TEXT:") {
            if !text.is_empty() {
                if let Some(images) = extract_pasted_images(text, workspace_root)? {
                    return Ok(ClipboardPaste::Images(images));
                }
                return Ok(ClipboardPaste::Text(text.to_owned()));
            }
        }
    }
    if let Ok(output) = run_clipboard_command("pbpaste", &[]).await {
        let text = String::from_utf8_lossy(&output).into_owned();
        if !text.is_empty() {
            if let Some(images) = extract_pasted_images(&text, workspace_root)? {
                return Ok(ClipboardPaste::Images(images));
            }
            return Ok(ClipboardPaste::Text(text));
        }
    }
    Err("No image or text found on clipboard".to_owned())
}

async fn read_linux_clipboard(workspace_root: &str) -> Result<ClipboardPaste, String> {
    if let Ok(types_out) = run_clipboard_command("wl-paste", &["--list-types"]).await {
        let types = String::from_utf8_lossy(&types_out);
        for mime in ["image/png", "image/jpeg", "image/webp", "image/gif"] {
            if types.lines().any(|line| line.trim() == mime) {
                if let Ok(bytes) = run_clipboard_command("wl-paste", &["--type", mime]).await {
                    if !bytes.is_empty() {
                        return Ok(ClipboardPaste::Images(vec![decode_image_bytes(
                            &bytes,
                            "clipboard",
                        )?]));
                    }
                }
            }
        }
        if let Ok(bytes) = run_clipboard_command("wl-paste", &["--no-newline"]).await {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            if !text.is_empty() {
                if let Some(images) = extract_pasted_images(&text, workspace_root)? {
                    return Ok(ClipboardPaste::Images(images));
                }
                return Ok(ClipboardPaste::Text(text));
            }
        }
    }
    if let Ok(targets_out) =
        run_clipboard_command("xclip", &["-selection", "clipboard", "-t", "TARGETS", "-o"]).await
    {
        let targets = String::from_utf8_lossy(&targets_out);
        for mime in ["image/png", "image/jpeg", "image/webp", "image/gif"] {
            if targets.lines().any(|line| line.trim() == mime) {
                if let Ok(bytes) =
                    run_clipboard_command("xclip", &["-selection", "clipboard", "-t", mime, "-o"])
                        .await
                {
                    if !bytes.is_empty() {
                        return Ok(ClipboardPaste::Images(vec![decode_image_bytes(
                            &bytes,
                            "clipboard",
                        )?]));
                    }
                }
            }
        }
        if let Ok(bytes) = run_clipboard_command("xclip", &["-selection", "clipboard", "-o"]).await
        {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            if !text.is_empty() {
                if let Some(images) = extract_pasted_images(&text, workspace_root)? {
                    return Ok(ClipboardPaste::Images(images));
                }
                return Ok(ClipboardPaste::Text(text));
            }
        }
    }
    Err("No image or text found on clipboard".to_owned())
}

async fn read_windows_clipboard(workspace_root: &str) -> Result<ClipboardPaste, String> {
    const PS_SCRIPT: &str = r#"
Add-Type -AssemblyName System.Windows.Forms;
Add-Type -AssemblyName System.Drawing;
$img = [System.Windows.Forms.Clipboard]::GetImage();
if ($null -ne $img) {
    $ms = New-Object System.IO.MemoryStream;
    $img.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png);
    Write-Output ('IMAGE_BASE64:' + [Convert]::ToBase64String($ms.ToArray()));
} elseif ([System.Windows.Forms.Clipboard]::ContainsFileDropList()) {
    $files = [System.Windows.Forms.Clipboard]::GetFileDropList();
    Write-Output ('FILES:' + ($files | ConvertTo-Json -Compress));
} elseif ([System.Windows.Forms.Clipboard]::ContainsText()) {
    Write-Output ('TEXT:' + [System.Windows.Forms.Clipboard]::GetText());
}
"#;
    if let Ok(output) = run_clipboard_command(
        "powershell",
        &["-NoProfile", "-NonInteractive", "-Command", PS_SCRIPT],
    )
    .await
    {
        let stdout = String::from_utf8_lossy(&output);
        let trimmed = stdout.trim_end_matches(&['\r', '\n'][..]);
        if let Some(b64) = trimmed.strip_prefix("IMAGE_BASE64:") {
            let clean: String = b64.chars().filter(|c| !c.is_ascii_whitespace()).collect();
            let bytes = BASE64_STANDARD
                .decode(clean)
                .map_err(|error| format!("Invalid clipboard image data: {error}"))?;
            return Ok(ClipboardPaste::Images(vec![decode_image_bytes(
                &bytes,
                "clipboard.png",
            )?]));
        }
        if let Some(text) = trimmed.strip_prefix("TEXT:") {
            if !text.is_empty() {
                if let Some(images) = extract_pasted_images(text, workspace_root)? {
                    return Ok(ClipboardPaste::Images(images));
                }
                return Ok(ClipboardPaste::Text(text.to_owned()));
            }
        }
    }
    Err("No image or text found on clipboard".to_owned())
}

async fn run_clipboard_command(program: &str, args: &[&str]) -> Result<Vec<u8>, String> {
    let child = tokio::process::Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| error.to_string())?;
    let output = tokio::time::timeout(CLIPBOARD_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| format!("Clipboard command '{program}' timed out"))?
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "Clipboard command '{program}' exited with {}",
            output.status
        ));
    }
    Ok(output.stdout)
}

fn format_byte_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_BY_ONE_PNG: &[u8] = &[
        0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D',
        b'R', 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f,
        0x15, 0xc4, 0x89, 0x00, 0x00, 0x00, 0x0a, b'I', b'D', b'A', b'T', 0x78, 0x9c, 0x63, 0x00,
        0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, b'I',
        b'E', b'N', b'D', 0xae, 0x42, 0x60, 0x82,
    ];

    #[test]
    fn decodes_png_dimensions_and_data_uri() {
        let b64 = BASE64_STANDARD.encode(ONE_BY_ONE_PNG);
        let uri = format!("data:image/png;base64,{b64}");
        let images = extract_pasted_images(&uri, "")
            .expect("valid data URI")
            .expect("detected as image");
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].media_type, "image/png");
        assert_eq!(images[0].dimensions, Some((1, 1)));
        assert_eq!(images[0].data_base64, b64);
    }

    #[test]
    fn resolves_escaped_and_file_url_image_paths_while_leaving_prose_untouched() {
        let temp = std::env::temp_dir().join(format!(
            "orchestral-tui-clipboard-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&temp).unwrap();
        let spaced = temp.join("Screen Shot 1.png");
        std::fs::write(&spaced, ONE_BY_ONE_PNG).unwrap();

        let escaped = spaced.to_string_lossy().replace(' ', "\\ ");
        let from_escaped = extract_pasted_images(&escaped, "")
            .expect("valid escaped path")
            .expect("detected image path");
        assert_eq!(from_escaped.len(), 1);
        assert_eq!(from_escaped[0].media_type, "image/png");

        let file_url = format!("file://{}", spaced.to_string_lossy().replace(' ', "%20"));
        let from_url = extract_pasted_images(&file_url, "")
            .expect("valid file URL")
            .expect("detected image URL");
        assert_eq!(from_url.len(), 1);

        assert!(
            extract_pasted_images("ordinary text with /missing/image.png", "")
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_dir_all(&temp);
    }
}
