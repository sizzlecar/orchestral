use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;

const STATE_VERSION: u32 = 1;
const DEVICE_TOKEN_PREFIX: &str = "orch_device_";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceView {
    pub id: String,
    pub name: String,
    pub created_at_unix_ms: i64,
    pub last_seen_at_unix_ms: i64,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionView {
    pub id: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(default)]
    pub run_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevicePrincipal {
    pub device_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PairingClaim {
    pub token: String,
    pub device: DeviceView,
}

#[derive(Debug, Clone)]
pub struct PairingTicket {
    secret: String,
    secret_digest: String,
    pub expires_at_unix_ms: i64,
}

impl PairingTicket {
    pub fn issue(ttl_ms: i64) -> anyhow::Result<Self> {
        if ttl_ms <= 0 {
            bail!("pairing ticket TTL must be positive");
        }
        let secret = random_secret(32)?;
        Ok(Self {
            secret_digest: secret_digest(&secret),
            secret,
            expires_at_unix_ms: now_unix_ms().saturating_add(ttl_ms),
        })
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeviceRecord {
    id: String,
    name: String,
    token_digest: String,
    created_at_unix_ms: i64,
    last_seen_at_unix_ms: i64,
    #[serde(default)]
    revoked_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteStateFile {
    version: u32,
    owner_id: String,
    #[serde(default)]
    devices: Vec<DeviceRecord>,
}

impl RemoteStateFile {
    fn fresh() -> Self {
        Self {
            version: STATE_VERSION,
            owner_id: format!("owner-{}", uuid::Uuid::new_v4()),
            devices: Vec::new(),
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.version != STATE_VERSION {
            bail!(
                "unsupported remote-control state version {}; expected {STATE_VERSION}",
                self.version
            );
        }
        if self.owner_id.trim().is_empty() {
            bail!("remote-control state owner is empty");
        }
        for device in &self.devices {
            if device.id.trim().is_empty()
                || device.name.trim().is_empty()
                || device.token_digest.len() != 64
            {
                bail!("remote-control state contains an invalid device");
            }
        }
        Ok(())
    }
}

struct RegistryState {
    durable: RemoteStateFile,
    pairing: Option<PairingTicket>,
}

#[derive(Clone)]
pub struct RemoteRegistry {
    path: Option<PathBuf>,
    state: Arc<Mutex<RegistryState>>,
}

impl RemoteRegistry {
    pub fn in_memory(pairing: Option<PairingTicket>) -> Self {
        Self {
            path: None,
            state: Arc::new(Mutex::new(RegistryState {
                durable: RemoteStateFile::fresh(),
                pairing,
            })),
        }
    }

    pub fn open(path: impl Into<PathBuf>, pairing: Option<PairingTicket>) -> anyhow::Result<Self> {
        let path = path.into();
        let existed = path.exists();
        let durable = if existed {
            let bytes = std::fs::read(&path)
                .with_context(|| format!("read remote-control state '{}'", path.display()))?;
            let state: RemoteStateFile = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode remote-control state '{}'", path.display()))?;
            state.validate()?;
            state
        } else {
            RemoteStateFile::fresh()
        };
        // Version 1 previously mixed Session/Run indexes into the device
        // credential file. Unknown legacy fields are intentionally discarded
        // and the sanitized authentication-only state is persisted here.
        if existed {
            persist_state(&path, &durable)?;
        }
        Ok(Self {
            path: Some(path),
            state: Arc::new(Mutex::new(RegistryState { durable, pairing })),
        })
    }

    pub async fn claim_pairing(
        &self,
        secret: &str,
        device_name: &str,
    ) -> anyhow::Result<PairingClaim> {
        let device_name = normalize_device_name(device_name)?;
        let mut state = self.state.lock().await;
        let ticket = state
            .pairing
            .as_ref()
            .context("pairing ticket is unavailable or was already claimed")?;
        if ticket.expires_at_unix_ms < now_unix_ms() {
            bail!("pairing ticket has expired");
        }
        if !constant_time_eq(
            ticket.secret_digest.as_bytes(),
            secret_digest(secret).as_bytes(),
        ) {
            bail!("pairing secret is invalid");
        }

        let raw_token = random_secret(32)?;
        let device_id = format!("device-{}", uuid::Uuid::new_v4());
        let token = format!("{DEVICE_TOKEN_PREFIX}{device_id}.{raw_token}");
        let timestamp = now_unix_ms();
        let record = DeviceRecord {
            id: device_id.clone(),
            name: device_name,
            token_digest: secret_digest(&token),
            created_at_unix_ms: timestamp,
            last_seen_at_unix_ms: timestamp,
            revoked_at_unix_ms: None,
        };
        state.durable.devices.push(record.clone());
        state.pairing = None;
        self.persist_locked(&state.durable)?;
        Ok(PairingClaim {
            token,
            device: device_view(&record, Some(&device_id)),
        })
    }

    pub async fn authenticate(&self, token: &str) -> anyhow::Result<DevicePrincipal> {
        let Some((device_id, _)) = parse_device_token(token) else {
            bail!("device token is malformed");
        };
        let token_digest = secret_digest(token);
        let mut state = self.state.lock().await;
        let record = state
            .durable
            .devices
            .iter_mut()
            .find(|record| record.id == device_id && record.revoked_at_unix_ms.is_none())
            .context("device token is unknown or revoked")?;
        if !constant_time_eq(record.token_digest.as_bytes(), token_digest.as_bytes()) {
            bail!("device token is invalid");
        }
        let timestamp = now_unix_ms();
        if timestamp.saturating_sub(record.last_seen_at_unix_ms) >= 60_000 {
            record.last_seen_at_unix_ms = timestamp;
            self.persist_locked(&state.durable)?;
        }
        Ok(DevicePrincipal {
            device_id: device_id.to_owned(),
        })
    }

    pub async fn devices(&self, current_device_id: &str) -> Vec<DeviceView> {
        let state = self.state.lock().await;
        state
            .durable
            .devices
            .iter()
            .filter(|record| record.revoked_at_unix_ms.is_none())
            .map(|record| device_view(record, Some(current_device_id)))
            .collect()
    }

    pub async fn active_device_count(&self) -> usize {
        self.state
            .lock()
            .await
            .durable
            .devices
            .iter()
            .filter(|record| record.revoked_at_unix_ms.is_none())
            .count()
    }

    pub async fn revoke_device(&self, device_id: &str) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let record = state
            .durable
            .devices
            .iter_mut()
            .find(|record| record.id == device_id && record.revoked_at_unix_ms.is_none())
            .context("device was not found")?;
        record.revoked_at_unix_ms = Some(now_unix_ms());
        self.persist_locked(&state.durable)
    }

    fn persist_locked(&self, state: &RemoteStateFile) -> anyhow::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        persist_state(path, state)
    }
}

fn normalize_device_name(name: &str) -> anyhow::Result<String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 80 || name.chars().any(char::is_control) {
        bail!("device name must contain 1 to 80 printable characters");
    }
    Ok(name.to_owned())
}

fn device_view(record: &DeviceRecord, current_device_id: Option<&str>) -> DeviceView {
    DeviceView {
        id: record.id.clone(),
        name: record.name.clone(),
        created_at_unix_ms: record.created_at_unix_ms,
        last_seen_at_unix_ms: record.last_seen_at_unix_ms,
        current: current_device_id.is_some_and(|current| current == record.id),
    }
}

fn parse_device_token(token: &str) -> Option<(&str, &str)> {
    let token = token.strip_prefix(DEVICE_TOKEN_PREFIX)?;
    let (device_id, secret) = token.split_once('.')?;
    if device_id.is_empty() || secret.len() < 32 || secret.contains('.') {
        return None;
    }
    Some((device_id, secret))
}

fn random_secret(bytes: usize) -> anyhow::Result<String> {
    let mut value = vec![0_u8; bytes];
    getrandom::fill(&mut value)
        .map_err(|error| anyhow::anyhow!("generate remote-control secret: {error}"))?;
    Ok(URL_SAFE_NO_PAD.encode(value))
}

fn secret_digest(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn now_unix_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn persist_state(path: &Path, state: &RemoteStateFile) -> anyhow::Result<()> {
    state.validate()?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .context("remote-control state path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create remote-control directory '{}'", parent.display()))?;
    set_private_directory_permissions(parent)?;
    let temporary = parent.join(format!(".remote-control-{}.tmp", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(state).context("encode remote-control state")?;
    let write_result = (|| {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok::<(), std::io::Error>(())
    })();
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(error)
            .with_context(|| format!("persist remote-control state '{}'", path.display()));
    }
    Ok(())
}

fn set_private_directory_permissions(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("secure remote-control directory '{}'", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pairing_is_one_time_and_revocation_is_immediate() {
        let ticket = PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::in_memory(Some(ticket));

        let claim = registry
            .claim_pairing(&secret, "Alice's phone")
            .await
            .unwrap();
        let principal = registry.authenticate(&claim.token).await.unwrap();
        assert_eq!(principal.device_id, claim.device.id);
        assert!(registry
            .claim_pairing(&secret, "second phone")
            .await
            .is_err());

        registry.revoke_device(&principal.device_id).await.unwrap();
        assert!(registry.authenticate(&claim.token).await.is_err());
    }

    #[tokio::test]
    async fn persisted_remote_state_contains_only_authentication_data() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-remote-auth-state-test-{}",
            uuid::Uuid::new_v4()
        ));
        let path = root.join("state.json");
        let ticket = PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::open(&path, Some(ticket)).unwrap();
        registry.claim_pairing(&secret, "Phone").await.unwrap();

        let persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(persisted.get("devices").is_some());
        assert!(persisted.get("sessions").is_none());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_session_index_is_discarded_when_auth_state_opens() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-remote-legacy-state-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("state.json");
        std::fs::write(
            &path,
            serde_json::json!({
                "version": 1,
                "owner_id": "owner-legacy",
                "devices": [],
                "sessions": [{
                    "id": "dangling-session",
                    "created_at_unix_ms": 1,
                    "updated_at_unix_ms": 1,
                    "run_ids": ["missing-run"]
                }]
            })
            .to_string(),
        )
        .unwrap();

        RemoteRegistry::open(&path, None).unwrap();
        let sanitized: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(sanitized.get("sessions").is_none());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn persisted_tokens_are_hashed_and_reloadable() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-remote-state-test-{}",
            uuid::Uuid::new_v4()
        ));
        let path = root.join("state.json");
        let ticket = PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::open(&path, Some(ticket)).unwrap();
        let claim = registry.claim_pairing(&secret, "Phone").await.unwrap();

        let persisted = std::fs::read_to_string(&path).unwrap();
        assert!(!persisted.contains(&claim.token));
        let reloaded = RemoteRegistry::open(&path, None).unwrap();
        assert!(reloaded.authenticate(&claim.token).await.is_ok());

        std::fs::remove_dir_all(root).unwrap();
    }
}
