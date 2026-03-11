use std::sync::{Arc, OnceLock};

use orchestral_core::config::ObservabilityConfig;

pub(super) fn init_tracing_if_needed(
    tracing_init: &OnceLock<()>,
    observability: &ObservabilityConfig,
) {
    tracing_init.get_or_init(|| {
        let silent_tui_logs = std::env::var("ORCHESTRAL_TUI_SILENT_LOGS")
            .map(|v| v == "1")
            .unwrap_or(false);
        let log_file_path = std::env::var("ORCHESTRAL_LOG_FILE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| observability.log_file.clone());
        let file_writer = log_file_path.as_deref().and_then(create_log_writer);
        let fallback_level = match observability.log_level.trim().to_ascii_lowercase().as_str() {
            "trace" => "trace",
            "debug" => "debug",
            "info" => "info",
            "warn" => "warn",
            "error" => "error",
            _ => "info",
        };

        let make_filter = || {
            tracing_subscriber::EnvFilter::try_from_default_env()
                .or_else(|_| tracing_subscriber::EnvFilter::try_new(fallback_level))
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        };

        match (observability.traces_enabled, silent_tui_logs, file_writer) {
            (true, _, Some(writer)) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_ansi(false)
                    .with_writer(writer)
                    .with_span_events(
                        tracing_subscriber::fmt::format::FmtSpan::NEW
                            | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                    )
                    .try_init();
            }
            (true, true, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_writer(std::io::sink)
                    .with_span_events(
                        tracing_subscriber::fmt::format::FmtSpan::NEW
                            | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                    )
                    .try_init();
            }
            (true, false, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_span_events(
                        tracing_subscriber::fmt::format::FmtSpan::NEW
                            | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                    )
                    .try_init();
            }
            (false, _, Some(writer)) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_ansi(false)
                    .with_writer(writer)
                    .try_init();
            }
            (false, true, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_writer(std::io::sink)
                    .try_init();
            }
            (false, false, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .try_init();
            }
        }

        tracing::info!(
            log_level = %observability.log_level,
            traces_enabled = observability.traces_enabled,
            log_file = log_file_path.as_deref().unwrap_or("(stdout)"),
            "tracing initialized"
        );
    });
}

fn create_log_writer(path: &str) -> Option<SharedFileMakeWriter> {
    use std::fs::{create_dir_all, OpenOptions};
    use std::path::Path;

    let file_path = Path::new(path);
    if let Some(parent) = file_path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(err) = create_dir_all(parent) {
                eprintln!(
                    "failed to create log directory '{}': {}",
                    parent.display(),
                    err
                );
                return None;
            }
        }
    }
    let file = match OpenOptions::new().create(true).append(true).open(file_path) {
        Ok(f) => f,
        Err(err) => {
            eprintln!("failed to open log file '{}': {}", file_path.display(), err);
            return None;
        }
    };
    Some(SharedFileMakeWriter::new(file))
}

#[derive(Clone)]
struct SharedFileMakeWriter {
    file: Arc<std::sync::Mutex<std::fs::File>>,
}

impl SharedFileMakeWriter {
    fn new(file: std::fs::File) -> Self {
        Self {
            file: Arc::new(std::sync::Mutex::new(file)),
        }
    }
}

struct SharedFileWriter {
    file: Arc<std::sync::Mutex<std::fs::File>>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedFileMakeWriter {
    type Writer = SharedFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedFileWriter {
            file: self.file.clone(),
        }
    }
}

impl std::io::Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("log file mutex poisoned"))?;
        std::io::Write::write(&mut *file, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("log file mutex poisoned"))?;
        std::io::Write::flush(&mut *file)
    }
}
