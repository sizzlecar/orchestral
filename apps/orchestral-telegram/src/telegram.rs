//! Minimal Telegram Bot API client (long polling).

use serde::Deserialize;

#[derive(Clone)]
pub struct TelegramClient {
    token: String,
    http: reqwest::Client,
}

impl TelegramClient {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            http: reqwest::Client::new(),
        }
    }

    fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{}", self.token, method)
    }

    pub async fn get_me(&self) -> Result<User, TelegramError> {
        let resp: ApiResponse<User> = self
            .http
            .get(self.api_url("getMe"))
            .send()
            .await?
            .json()
            .await?;
        resp.into_result()
    }

    pub async fn get_updates(
        &self,
        offset: i64,
        timeout: u64,
    ) -> Result<Vec<Update>, TelegramError> {
        let resp: ApiResponse<Vec<Update>> = self
            .http
            .post(self.api_url("getUpdates"))
            .json(&serde_json::json!({
                "offset": offset,
                "timeout": timeout,
                "allowed_updates": ["message"]
            }))
            .send()
            .await?
            .json()
            .await?;
        resp.into_result()
    }

    pub async fn send_message(&self, chat_id: i64, text: &str) -> Result<Message, TelegramError> {
        // Truncate if too long for Telegram (4096 char limit)
        let text = if text.chars().count() > 4000 {
            let truncated: String = text.chars().take(3950).collect();
            format!("{}...\n\n_(truncated)_", truncated)
        } else {
            text.to_string()
        };

        let resp: ApiResponse<Message> = self
            .http
            .post(self.api_url("sendMessage"))
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown"
            }))
            .send()
            .await?
            .json()
            .await?;
        resp.into_result()
    }

    pub async fn send_chat_action(
        &self,
        chat_id: i64,
        action: &str,
    ) -> Result<bool, TelegramError> {
        let resp: ApiResponse<bool> = self
            .http
            .post(self.api_url("sendChatAction"))
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "action": action
            }))
            .send()
            .await?
            .json()
            .await?;
        resp.into_result()
    }
}

// --- Telegram API types ---

#[derive(Debug, Deserialize)]
pub struct ApiResponse<T> {
    pub ok: bool,
    pub result: Option<T>,
    pub description: Option<String>,
}

impl<T> ApiResponse<T> {
    fn into_result(self) -> Result<T, TelegramError> {
        if self.ok {
            self.result
                .ok_or_else(|| TelegramError::Api("ok=true but result is null".to_string()))
        } else {
            Err(TelegramError::Api(
                self.description
                    .unwrap_or_else(|| "unknown error".to_string()),
            ))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Message {
    pub message_id: i64,
    pub chat: Chat,
    pub from: Option<User>,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Chat {
    pub id: i64,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(rename = "type")]
    pub chat_type: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct User {
    pub id: i64,
    pub first_name: String,
    #[serde(default)]
    pub username: String,
}

#[derive(Debug)]
pub enum TelegramError {
    Http(reqwest::Error),
    Api(String),
}

impl std::fmt::Display for TelegramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(e) => write!(f, "HTTP error: {}", e),
            Self::Api(msg) => write!(f, "Telegram API error: {}", msg),
        }
    }
}

impl std::error::Error for TelegramError {}

impl From<reqwest::Error> for TelegramError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}
