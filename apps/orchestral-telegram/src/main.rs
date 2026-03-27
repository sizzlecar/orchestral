//! Orchestral Telegram Bot Adapter
//!
//! Connects Orchestral's SDK to Telegram via Bot API long polling.
//! Messages are serialized through a channel to avoid concurrent interaction conflicts.
//!
//! ```bash
//! export TELEGRAM_BOT_TOKEN="123456:ABC-DEF..."
//! export OPENROUTER_API_KEY="sk-or-..."
//! cargo run -p orchestral-telegram
//! ```

mod telegram;

use std::sync::Arc;

use tracing::{error, info, warn};

use orchestral::Orchestral;

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

    let bot_token =
        std::env::var("TELEGRAM_BOT_TOKEN").expect("TELEGRAM_BOT_TOKEN env var required");

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/orchestral.cli.runtime.override.yaml".to_string());

    info!("Starting Orchestral Telegram bot...");
    info!("Config: {}", config_path);

    let app = Orchestral::builder()
        .planner_backend("google")
        .planner_model("gemini-3.1-pro-preview")
        .max_planner_iterations(6)
        .config_path(&config_path)
        .build()
        .await
        .map_err(|e| format!("Failed to build Orchestral: {}", e))?;

    let app = Arc::new(app);
    let client = TelegramClient::new(&bot_token);

    // Verify bot token
    let me = client.get_me().await?;
    info!("Bot connected: @{} ({})", me.username, me.first_name);

    // Message processing channel — serializes requests to avoid interaction conflicts
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ChatMessage>(32);

    // Worker: processes messages one at a time
    let worker_app = app.clone();
    let worker_client = client.clone();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let _ = worker_client.send_chat_action(msg.chat_id, "typing").await;

            match worker_app.run(&msg.text).await {
                Ok(result) => {
                    let reply = match result.status.as_str() {
                        "completed" => result.message.clone(),
                        "rejected" => "I'm busy, please wait a moment.".to_string(),
                        _ => format!("{}\n\n[{}]", result.message, result.status),
                    };
                    if let Err(e) = worker_client.send_message(msg.chat_id, &reply).await {
                        error!(chat_id = msg.chat_id, error = %e, "Failed to send reply");
                    }
                }
                Err(e) => {
                    let reply = format!("Error: {}", e);
                    let _ = worker_client.send_message(msg.chat_id, &reply).await;
                }
            }
        }
    });

    // Polling loop: receives messages and sends to worker channel
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

            // Handle /start command
            if text == "/start" {
                let _ = client
                    .send_message(chat_id, "Hi! I'm an Orchestral bot. Send me any task.")
                    .await;
                continue;
            }

            // Queue message for processing
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
