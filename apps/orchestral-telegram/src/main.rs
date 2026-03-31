//! Orchestral Telegram Bot Adapter
//!
//! Connects Orchestral's SDK to Telegram via Bot API long polling.
//! Messages are serialized through a channel to avoid concurrent interaction conflicts.
//!
//! ## Environment Variables
//!
//! Required:
//! - `TELEGRAM_BOT_TOKEN` — Telegram bot token from @BotFather
//! - One LLM provider key (see below)
//!
//! LLM Configuration (all optional, with defaults):
//! - `ORCHESTRAL_BACKEND` — LLM backend: google, openai, anthropic, openrouter (default: google)
//! - `ORCHESTRAL_MODEL` — Model name (default: gemini-2.5-flash)
//! - `ORCHESTRAL_MAX_ITERATIONS` — Agent loop max iterations (default: 6)
//!
//! Provider keys (set the one matching your backend):
//! - `GOOGLE_API_KEY` — for google backend
//! - `OPENAI_API_KEY` — for openai backend
//! - `ANTHROPIC_API_KEY` — for anthropic backend
//! - `OPENROUTER_API_KEY` — for openrouter backend
//!
//! Optional:
//! - `TELEGRAM_PROXY` — SOCKS5/HTTP proxy for Telegram API (e.g., socks5://127.0.0.1:41808)
//! - `ORCHESTRAL_CONFIG` — Path to orchestral YAML config (default: configs/orchestral.cli.runtime.override.yaml)
//!
//! ## Example
//!
//! ```bash
//! export TELEGRAM_BOT_TOKEN="123456:ABC-DEF..."
//! export GOOGLE_API_KEY="AIza..."
//! cargo run -p orchestral-telegram
//! ```

mod config;
mod telegram;

use std::sync::Arc;

use tracing::{error, info, warn};

use orchestral::Orchestral;

use config::BotConfig;
use telegram::TelegramClient;

struct ChatMessage {
    chat_id: i64,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = BotConfig::from_env().map_err(|e| format!("Config error: {}", e))?;

    info!("Starting Orchestral Telegram bot...");
    info!(
        backend = %cfg.backend,
        model = %cfg.model,
        max_iterations = cfg.max_iterations,
        config = %cfg.config_path,
        "Configuration"
    );

    let app = Orchestral::builder()
        .planner_backend(&cfg.backend)
        .planner_model(&cfg.model)
        .max_planner_iterations(cfg.max_iterations)
        .config_path(&cfg.config_path)
        .build()
        .await
        .map_err(|e| format!("Failed to build Orchestral: {}", e))?;

    let app = Arc::new(app);
    let client = TelegramClient::new(&cfg.bot_token);

    let me = client.get_me().await?;
    info!("Bot connected: @{} ({})", me.username, me.first_name);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ChatMessage>(32);

    let worker_app = app.clone();
    let worker_client = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let _ = worker_client.send_chat_action(msg.chat_id, "typing").await;

            // Subscribe to events for progress feedback
            let mut event_rx = worker_app.orchestrator.thread_runtime.subscribe_events();
            let progress_client = worker_client.clone();
            let progress_chat_id = msg.chat_id;
            let progress_handle = tokio::spawn(async move {
                let mut steps: Vec<String> = Vec::new();
                let mut progress_msg_id: Option<i64> = None;
                let mut typing_interval = tokio::time::interval(std::time::Duration::from_secs(4));
                loop {
                    tokio::select! {
                        _ = typing_interval.tick() => {
                            let _ = progress_client.send_chat_action(progress_chat_id, "typing").await;
                        }
                        event = event_rx.recv() => {
                            let Ok(event) = event else { continue };
                            let step_label = match &event {
                                orchestral::core::store::Event::SystemTrace { payload, .. } => {
                                    let category = payload.get("category").and_then(|v| v.as_str());
                                    let event_type = payload.get("event_type").and_then(|v| v.as_str());
                                    let phase = payload.get("phase").and_then(|v| v.as_str());
                                    let metadata = payload.get("metadata");
                                    match (category, event_type, phase) {
                                        (Some("runtime_lifecycle"), Some("planning_started"), _) => {
                                            let iteration = metadata
                                                .and_then(|m| m.get("iteration"))
                                                .and_then(|v| v.as_u64())
                                                .unwrap_or(1);
                                            if iteration <= 1 {
                                                Some("\u{1f4ad} Thinking".to_string())
                                            } else {
                                                Some(format!("\u{1f4ad} Thinking (round {})", iteration))
                                            }
                                        }
                                        (Some("execution_progress"), _, Some("step_started")) => {
                                            let action = payload
                                                .get("action")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("action");
                                            let label = match action {
                                                a if a.contains("codex") => "\u{1f916} Asking Codex".to_string(),
                                                a if a.contains("sls") || a.contains("log") => "\u{1f50d} Querying logs".to_string(),
                                                "shell" => "\u{2699}\u{fe0f} Running command".to_string(),
                                                "file_read" => "\u{1f4c4} Reading file".to_string(),
                                                "file_write" => "\u{270f}\u{fe0f} Writing file".to_string(),
                                                "skill_activate" => "\u{1f4e6} Loading skill".to_string(),
                                                "tool_lookup" => "\u{1f50e} Looking up tool".to_string(),
                                                "http" => "\u{1f310} HTTP request".to_string(),
                                                _ => format!("\u{25b6}\u{fe0f} {}", action),
                                            };
                                            Some(label)
                                        }
                                        _ => None,
                                    }
                                }
                                _ => None,
                            };
                            if let Some(label) = step_label {
                                // Dedupe consecutive identical labels
                                if steps.last().map(|s| s.as_str()) == Some(label.as_str()) {
                                    continue;
                                }
                                steps.push(label);
                                let progress_text = steps.join(" \u{2192} ");
                                // Edit existing progress message or send a new one
                                if let Some(mid) = progress_msg_id {
                                    let _ = progress_client.edit_message(progress_chat_id, mid, &progress_text).await;
                                } else if let Ok(sent) = progress_client.send_message(progress_chat_id, &progress_text).await {
                                    progress_msg_id = Some(sent.message_id);
                                }
                            }
                        }
                    }
                }
            });

            match worker_app.run(&msg.text).await {
                Ok(result) => {
                    progress_handle.abort();
                    let reply = match result.status.as_str() {
                        "completed" => clean_response(&result.message),
                        "rejected" => "I'm busy, please wait a moment.".to_string(),
                        _ => format!("{}\n\n[{}]", clean_response(&result.message), result.status),
                    };
                    if let Err(e) = worker_client.send_message(msg.chat_id, &reply).await {
                        error!(chat_id = msg.chat_id, error = %e, "Failed to send reply");
                    }
                }
                Err(e) => {
                    progress_handle.abort();
                    let reply = format!("Error: {}", e);
                    let _ = worker_client.send_message(msg.chat_id, &reply).await;
                }
            }
        }
    });

    let mut offset: i64 = 0;
    info!("Listening for messages...");

    loop {
        let updates = match client.get_updates(offset, 30).await {
            Ok(updates) => updates,
            Err(e) => {
                error!("Failed to get updates: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        for update in updates {
            offset = update.update_id + 1;

            let Some(message) = update.message else {
                continue;
            };
            let Some(text) = &message.text else {
                continue;
            };

            let chat_id = message.chat.id;
            let user = message
                .from
                .as_ref()
                .map(|u| u.first_name.as_str())
                .unwrap_or("unknown");

            info!(chat_id, user, text = text.as_str(), "Received message");

            if text == "/start" {
                let _ = client
                    .send_message(chat_id, "Hi! I'm an Orchestral bot. Send me any task.")
                    .await;
                continue;
            }

            if tx
                .try_send(ChatMessage {
                    chat_id,
                    text: text.clone(),
                })
                .is_err()
            {
                warn!(chat_id, "Message queue full, dropping message");
                let _ = client
                    .send_message(chat_id, "I'm busy, please try again in a moment.")
                    .await;
            }
        }
    }
}

/// Clean up raw response text from MCP/action output.
/// Extracts actual text content from JSON envelopes and strips artifacts.
fn clean_response(raw: &str) -> String {
    let trimmed = raw.trim();

    // Try to extract text from MCP-style JSON: {"content":[{"text":"..."}]}
    if trimmed.contains(r#""content":[{"text":"#) || trimmed.contains(r#""content": [{"text":"#) {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(text) = parsed
                .get("content")
                .and_then(|c| c.as_array())
                .and_then(|arr| arr.first())
                .and_then(|item| item.get("text"))
                .and_then(|t| t.as_str())
            {
                return text.to_string();
            }
        }
    }

    // Try to extract from inline JSON embedded in text
    if let Some(json_start) = trimmed.find(r#"{"content":[{"text":"#) {
        let json_part = &trimmed[json_start..];
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_part) {
            let prefix = trimmed[..json_start].trim();
            if let Some(text) = parsed
                .get("content")
                .and_then(|c| c.as_array())
                .and_then(|arr| arr.first())
                .and_then(|item| item.get("text"))
                .and_then(|t| t.as_str())
            {
                if prefix.is_empty() {
                    return text.to_string();
                }
                return format!("{}\n\n{}", prefix, text);
            }
        }
    }

    trimmed.to_string()
}
