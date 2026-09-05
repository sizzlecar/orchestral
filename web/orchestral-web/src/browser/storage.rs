use std::collections::BTreeMap;

use rexie::{ObjectStore, Rexie, TransactionMode};
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

use crate::model::{OutboxEntry, UploadedArtifact};

const DATABASE_NAME: &str = "orchestral-pwa";
const DATABASE_VERSION: u32 = 2;
const SECRETS_STORE: &str = "secrets";
const OUTBOX_STORE: &str = "outbox";
const TOKEN_KEY: &str = "device-token";
const PREFERENCES_KEY: &str = "orchestral.preferences.v1";
const DRAFTS_KEY: &str = "orchestral.drafts.v1";

pub type ComposerDrafts = BTreeMap<String, (String, Vec<UploadedArtifact>)>;

/// Drafts survive refresh in this tab without being shared with another tab.
pub fn load_drafts() -> ComposerDrafts {
    web_sys::window()
        .and_then(|window| window.session_storage().ok().flatten())
        .and_then(|storage| storage.get_item(DRAFTS_KEY).ok().flatten())
        .and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

pub fn save_drafts(drafts: &ComposerDrafts) -> Result<(), String> {
    let storage = web_sys::window()
        .and_then(|window| window.session_storage().ok().flatten())
        .ok_or_else(|| "浏览器无法保存草稿，请先复制草稿再刷新".to_owned())?;
    let value = serde_json::to_string(drafts).map_err(|error| error.to_string())?;
    storage
        .set_item(DRAFTS_KEY, &value)
        .map_err(|_| "草稿保存失败，请先复制草稿再刷新".to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Preferences {
    #[serde(default = "default_theme")]
    pub theme: String,
    #[serde(default)]
    pub notifications: bool,
    #[serde(default)]
    pub device_name: String,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            theme: default_theme(),
            notifications: false,
            device_name: String::new(),
        }
    }
}

fn default_theme() -> String {
    "auto".to_owned()
}

async fn database() -> Result<Rexie, String> {
    Rexie::builder(DATABASE_NAME)
        .version(DATABASE_VERSION)
        .add_object_store(ObjectStore::new(SECRETS_STORE))
        .add_object_store(ObjectStore::new(OUTBOX_STORE))
        .build()
        .await
        .map_err(|error| error.to_string())
}

pub async fn load_token() -> Result<Option<String>, String> {
    let database = database().await?;
    let transaction = database
        .transaction(&[SECRETS_STORE], TransactionMode::ReadOnly)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(SECRETS_STORE)
        .map_err(|error| error.to_string())?;
    let value = store
        .get(JsValue::from_str(TOKEN_KEY))
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .done()
        .await
        .map_err(|error| error.to_string())?;
    Ok(value.and_then(|value| value.as_string()))
}

pub async fn save_token(token: &str) -> Result<(), String> {
    let database = database().await?;
    let transaction = database
        .transaction(&[SECRETS_STORE], TransactionMode::ReadWrite)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(SECRETS_STORE)
        .map_err(|error| error.to_string())?;
    store
        .put(
            &JsValue::from_str(token),
            Some(&JsValue::from_str(TOKEN_KEY)),
        )
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .done()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub async fn clear_token() -> Result<(), String> {
    let database = database().await?;
    let transaction = database
        .transaction(&[SECRETS_STORE], TransactionMode::ReadWrite)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(SECRETS_STORE)
        .map_err(|error| error.to_string())?;
    store
        .delete(JsValue::from_str(TOKEN_KEY))
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .done()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub async fn save_outbox(entry: &OutboxEntry) -> Result<(), String> {
    let database = database().await?;
    let transaction = database
        .transaction(&[OUTBOX_STORE], TransactionMode::ReadWrite)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(OUTBOX_STORE)
        .map_err(|error| error.to_string())?;
    let value = serde_wasm_bindgen::to_value(entry).map_err(|error| error.to_string())?;
    store
        .put(&value, Some(&JsValue::from_str(&entry.id)))
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .done()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub async fn load_outbox() -> Result<Vec<OutboxEntry>, String> {
    let database = database().await?;
    let transaction = database
        .transaction(&[OUTBOX_STORE], TransactionMode::ReadOnly)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(OUTBOX_STORE)
        .map_err(|error| error.to_string())?;
    let values = store
        .get_all(None, None)
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .done()
        .await
        .map_err(|error| error.to_string())?;
    let mut entries = values
        .into_iter()
        .map(|value| serde_wasm_bindgen::from_value(value).map_err(|error| error.to_string()))
        .collect::<Result<Vec<OutboxEntry>, String>>()?;
    entries.sort_by_key(|entry| (entry.created_at_unix_ms, entry.id.clone()));
    Ok(entries)
}

pub async fn delete_outbox(id: &str) -> Result<(), String> {
    let database = database().await?;
    let transaction = database
        .transaction(&[OUTBOX_STORE], TransactionMode::ReadWrite)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(OUTBOX_STORE)
        .map_err(|error| error.to_string())?;
    store
        .delete(JsValue::from_str(id))
        .await
        .map_err(|error| error.to_string())?;
    transaction
        .done()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub fn load_preferences() -> Preferences {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return Preferences::default();
    };
    storage
        .get_item(PREFERENCES_KEY)
        .ok()
        .flatten()
        .and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

pub fn save_preferences(preferences: &Preferences) {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return;
    };
    if let Ok(value) = serde_json::to_string(preferences) {
        let _ = storage.set_item(PREFERENCES_KEY, &value);
    }
}
