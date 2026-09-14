use anyhow::Context;
use clap::Args;
use orchestral_model_openai::{discover_models, OpenAiEndpoint};
use serde_json::json;

use crate::agent::AgentRunOptions;
use crate::runtime::client::inspect_runtime_config;

#[derive(Debug, Args)]
pub(crate) struct DoctorCommand {
    /// Query the selected OpenAI-compatible server's model list. No generation is sent.
    #[arg(long)]
    check_connection: bool,
    /// Print a report suitable for scripts. Credential values are always omitted.
    #[arg(long)]
    json: bool,
}

impl DoctorCommand {
    pub(crate) async fn run(self, options: AgentRunOptions) -> anyhow::Result<()> {
        let (config, source) = inspect_runtime_config(
            options.config,
            &options.model_overrides,
            options.credential_file.as_deref(),
        )?;
        let profile = config
            .agent
            .model_profile
            .as_deref()
            .and_then(|name| config.providers.get_model(name));
        let backend_name = config
            .agent
            .backend
            .as_deref()
            .or_else(|| profile.as_ref().map(|profile| profile.backend.as_str()))
            .or(config.providers.default_backend.as_deref())
            .context("no model backend configured")?;
        let backend = config
            .providers
            .get_backend(backend_name)
            .context("configured model backend was not found")?;
        let compatible = matches!(
            backend.kind.to_ascii_lowercase().as_str(),
            "openai" | "openrouter" | "deepseek" | "groq" | "xai" | "mistral"
        );
        let endpoint = backend.endpoint.as_deref().or(match backend.kind.as_str() {
            "openai" => Some("https://api.openai.com/v1"),
            "deepseek" => Some("https://api.deepseek.com/v1"),
            "gemini" | "google" => Some("https://generativelanguage.googleapis.com/v1beta"),
            _ => None,
        });
        let authentication = if backend.get_config::<String>("auth").as_deref() == Some("none") {
            "none (no credentials sent)"
        } else if (compatible && crate::openai_connection::api_key(&backend).is_ok())
            || (!compatible && backend.resolve_api_key().is_ok())
        {
            "API key configured (value hidden)"
        } else if !compatible
            && crate::google_auth::has_google_credentials(options.credential_file.as_deref())
        {
            "Google credentials available"
        } else {
            "missing"
        };
        let mut problems = Vec::new();
        if authentication == "missing" {
            problems.push("Configure your provider key, or use --base-url http://127.0.0.1:8000/v1 for a local server.".to_owned());
        }
        let sandbox = sandbox_status();
        if config.tools.exec.enabled && config.tools.exec.sandboxed_execution_enabled && !sandbox.1
        {
            problems.push(sandbox.0.to_owned());
        }
        let mut models = None;
        if self.check_connection {
            if compatible {
                let result = async {
                    let endpoint =
                        OpenAiEndpoint::parse(endpoint.context("API base URL is required")?)?;
                    let key = crate::openai_connection::api_key(&backend)?;
                    discover_models(&endpoint, &key)
                        .await
                        .map_err(anyhow::Error::msg)
                }
                .await;
                match result {
                    Ok(found) => models = Some(found),
                    Err(error) => problems.push(error.to_string()),
                }
            } else {
                problems.push("--check-connection currently supports OpenAI-compatible servers; the selected provider uses a different protocol.".to_owned());
            }
        }
        let cwd = options.cwd.unwrap_or(std::env::current_dir()?);
        let report = json!({
            "version": env!("CARGO_PKG_VERSION"),
            "platform": format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            "workspace": cwd,
            "config": source.map(|path| path.display().to_string()).unwrap_or_else(|| "built-in defaults (no file written)".to_owned()),
            "backend": backend.name,
            "model": config.agent.model.or_else(|| profile.map(|profile| profile.model)).unwrap_or_else(|| "auto (discover when connecting)".to_owned()),
            "endpoint": endpoint.map(redact_url),
            "authentication": authentication,
            "sandbox": sandbox.0,
            "codex": program_available(if cfg!(windows) { "codex.exe" } else { "codex" }),
            "connection_checked": self.check_connection,
            "available_models": models,
            "problems": problems,
        });
        if self.json {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            println!("Orchestral {}", env!("CARGO_PKG_VERSION"));
            for (title, key) in [
                ("Platform", "platform"),
                ("Workspace", "workspace"),
                ("Configuration", "config"),
                ("Provider", "backend"),
                ("Model", "model"),
                ("API URL", "endpoint"),
                ("Authentication", "authentication"),
                ("Command sandbox", "sandbox"),
            ] {
                println!(
                    "{title}: {}",
                    report[key].as_str().unwrap_or("not configured")
                );
            }
            println!(
                "Codex: {}",
                if report["codex"] == true {
                    "found"
                } else {
                    "not found (optional)"
                }
            );
            if let Some(models) = report["available_models"].as_array() {
                println!(
                    "Server models: {}",
                    models
                        .iter()
                        .filter_map(|model| model.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            for problem in &problems {
                println!("Next: {problem}");
            }
            if problems.is_empty() {
                println!(
                    "{}",
                    if self.check_connection {
                        "Configuration and model-list checks passed. No generation was sent."
                    } else {
                        "Local configuration checks passed. No network request was sent."
                    }
                );
            }
        }
        if !problems.is_empty() {
            anyhow::bail!("diagnostics found {} issue(s)", problems.len());
        }
        Ok(())
    }
}

fn redact_url(raw: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(raw) else {
        return "invalid URL (value hidden)".to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn program_available(name: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|directory| directory.join(name).is_file())
    })
}

fn sandbox_status() -> (&'static str, bool) {
    if cfg!(target_os = "macos") {
        (
            "macOS Seatbelt",
            std::path::Path::new("/usr/bin/sandbox-exec").is_file(),
        )
    } else if cfg!(target_os = "linux") {
        if program_available("bwrap") {
            (
                "bubblewrap found; namespace availability is checked when executing",
                true,
            )
        } else {
            ("bubblewrap missing; install it with your package manager (Ubuntu: sudo apt-get install bubblewrap)", false)
        }
    } else {
        ("Native command sandbox unavailable; use WSL for sandboxed commands or explicitly approved Host execution", false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_diagnostics_remove_credentials_and_query_values() {
        assert_eq!(
            redact_url("https://alice:secret@example.com/v1?token=secret#secret"),
            "https://example.com/v1"
        );
        assert!(!redact_url("secret").contains("secret"));
    }
}
