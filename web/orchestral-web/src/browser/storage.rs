use rexie::{ObjectStore, Rexie, TransactionMode};
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

const DATABASE_NAME: &str = "orchestral-pwa";
const STORE_NAME: &str = "secrets";
const TOKEN_KEY: &str = "device-token";
const PREFERENCES_KEY: &str = "orchestral.preferences.v1";

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
        .version(1)
        .add_object_store(ObjectStore::new(STORE_NAME))
        .build()
        .await
        .map_err(|error| error.to_string())
}

pub async fn load_token() -> Result<Option<String>, String> {
    let database = database().await?;
    let transaction = database
        .transaction(&[STORE_NAME], TransactionMode::ReadOnly)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(STORE_NAME)
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
        .transaction(&[STORE_NAME], TransactionMode::ReadWrite)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(STORE_NAME)
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
        .transaction(&[STORE_NAME], TransactionMode::ReadWrite)
        .map_err(|error| error.to_string())?;
    let store = transaction
        .store(STORE_NAME)
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
