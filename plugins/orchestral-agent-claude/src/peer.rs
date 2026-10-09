//! Explicit peer messaging to an existing Claude process. This is not a user
//! input channel. Native journal identities establish initial delivery;
//! authenticated receipts report held, refused and subsequently released input.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use orchestral_core::agent_connector::{
    AgentConnectorError, AgentConnectorErrorCode, AgentSessionActionOutcome, AgentSessionActivity,
    AgentSessionActivityId, AgentSessionActivityKind, AgentSessionActivityStatus,
    AgentSessionDetail, AgentSessionTextInput, AgentSessionTurn, AgentSessionTurnId,
    AgentSessionTurnStatus,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, Content};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{ClaudeCodeConfig, ClaudeCodeConnector, NativeSession};

pub(crate) const INPUT_ACTION: &str = "claude.send-to-owner";

#[derive(Serialize, Deserialize)]
struct Dispatch {
    session_id: AgentSessionId,
    input: AgentSessionTextInput,
    status: Option<String>,
    #[serde(default)]
    created_at_unix_ms: Option<i64>,
}

fn dispatch_root(config: &ClaudeCodeConfig) -> Result<PathBuf, AgentConnectorError> {
    Ok(config
        .session_store_dir
        .parent()
        .ok_or_else(|| AgentConnectorError::invalid("missing Claude state root"))?
        .join("dispatch"))
}

pub(crate) fn dispatch_paths(
    config: &ClaudeCodeConfig,
    session: &AgentSessionId,
) -> Result<Vec<PathBuf>, AgentConnectorError> {
    let mut paths = Vec::new();
    for path in crate::entries(&dispatch_root(config)?)? {
        if path.extension().and_then(|value| value.to_str()) == Some("json") {
            let dispatch: Dispatch = crate::read_json(&path)?;
            if dispatch.session_id == *session {
                paths.push(path);
            }
        }
    }
    Ok(paths)
}

fn save(path: &Path, dispatch: &Dispatch) -> Result<(), AgentConnectorError> {
    use std::io::Write;
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary).map_err(crate::io_error)?;
        file.write_all(&serde_json::to_vec(dispatch).map_err(|_| unknown())?)
            .map_err(crate::io_error)?;
        file.sync_all().map_err(crate::io_error)?;
        std::fs::rename(&temporary, path).map_err(crate::io_error)?;
        std::fs::File::open(path.parent().ok_or_else(unknown)?)
            .and_then(|directory| directory.sync_all())
            .map_err(crate::io_error)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

fn unresolved(dispatch: &Dispatch) -> bool {
    matches!(
        dispatch.status.as_deref(),
        None | Some("unknown" | "submitted" | "held")
    )
}

/// A durable Host submission remains visible while the native owner is busy.
/// This records submission, not delivery or execution. Native echoes own the
/// same client identity and replace this projection once they are observed.
pub(crate) fn overlay_pending(
    config: &ClaudeCodeConfig,
    sessions: &mut BTreeMap<String, AgentSessionDetail>,
) -> Result<(), AgentConnectorError> {
    let mut pending = Vec::new();
    for path in crate::entries(&dispatch_root(config)?)? {
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let dispatch: Dispatch = crate::read_json(&path)?;
        if unresolved(&dispatch) {
            pending.push(dispatch);
        }
    }
    pending.sort_by_key(|dispatch| {
        (
            dispatch.created_at_unix_ms,
            dispatch.input.submission_id.clone(),
        )
    });
    for dispatch in pending {
        let Some(detail) = sessions.get_mut(dispatch.session_id.as_str()) else {
            continue;
        };
        if detail
            .turns
            .iter()
            .flat_map(|turn| &turn.activities)
            .any(|activity| {
                activity.details.get("clientId").and_then(Value::as_str)
                    == Some(&dispatch.input.submission_id)
            })
        {
            continue;
        }
        let id = &dispatch.input.submission_id;
        detail.turns.push(AgentSessionTurn {
            turn_id: AgentSessionTurnId::new(format!("claude-submission:{id}")),
            status: AgentSessionTurnStatus::Pending, failure: None,
            activities: vec![AgentSessionActivity {
                activity_id: AgentSessionActivityId::new(format!("claude-submission:{id}")),
                occurred_at_unix_ms: dispatch.created_at_unix_ms,
                kind: AgentSessionActivityKind::UserMessage,
                status: AgentSessionActivityStatus::Pending, title: None,
                content: vec![Content::text(dispatch.input.text)],
                details: json!({"clientId":id,"phase":"deferred","delivery_status":dispatch.status.as_deref().unwrap_or("unknown")}),
            }],
        });
    }
    Ok(())
}

/// Reconcile durable claims without sending anything. One journal pass per
/// session covers all unresolved submissions, including claims from older Hosts.
pub(crate) fn reconcile(config: &ClaudeCodeConfig) -> Result<(), AgentConnectorError> {
    let mut journals = BTreeMap::new();
    for path in crate::entries(&dispatch_root(config)?)? {
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let mut dispatch: Dispatch = crate::read_json(&path)?;
        if !unresolved(&dispatch) {
            continue;
        }
        let session = dispatch.session_id.as_str();
        if !journals.contains_key(session) {
            let mut journal =
                crate::delivery::JournalConfirmation::new(&config.config_dir, session)?;
            journal.poll()?;
            journals.insert(session.to_owned(), journal);
        }
        if journals[session].contains(&dispatch.input.submission_id) {
            dispatch.status = Some("delivered".to_owned());
            save(&path, &dispatch)?;
        }
    }
    Ok(())
}

fn unknown() -> AgentConnectorError {
    AgentConnectorError::new(
        AgentConnectorErrorCode::OutcomeUnknown,
        "无法确认终端是否收到这条消息；为避免重复执行，不会自动再次发送。请查看原会话。",
        false,
    )
}

fn outcome(status: &str) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
    let message = match status {
        "delivered" => "跨会话消息已送达；Claude 会将其作为同伴请求处理，附加来源说明。",
        "held" => "跨会话消息正在等待原终端允许接收；它不是用户输入。",
        "submitted" => "消息已提交，等待 Claude 会话记录确认；请勿重复发送。",
        "denied" | "expired" | "refused" | "dropped" => {
            return Err(AgentConnectorError::new(
                AgentConnectorErrorCode::LeaseConflict,
                format!("原 Claude 终端未接收消息：{status}"),
                false,
            ))
        }
        _ => return Err(unknown()),
    };
    let mut result = AgentSessionActionOutcome::completed();
    result.content = vec![Content::text(message)];
    result.details = json!({"delivery_status":status});
    Ok(result)
}

pub(crate) async fn submit(
    connector: &ClaudeCodeConnector,
    session_id: &AgentSessionId,
    input: AgentSessionTextInput,
) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
    let _gate = connector.peer_gate.lock().await;
    let root = dispatch_root(&connector.config)?;
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(crate::io_error)?;
    let identity = format!("{:x}", Sha256::digest(input.submission_id.as_bytes()));
    let path = root.join(format!("{identity}.json"));
    let previous = match tokio::fs::read(&path).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(crate::io_error(error)),
    };
    if let Some(bytes) = previous {
        let mut old: Dispatch = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
        if old.session_id != *session_id || old.input != input {
            return Err(AgentConnectorError::new(
                AgentConnectorErrorCode::LeaseConflict,
                "submission identity belongs to different input",
                false,
            ));
        }
        if unresolved(&old) {
            let config = connector.config.config_dir.clone();
            let session = session_id.as_str().to_owned();
            let submission = input.submission_id.clone();
            let delivered = tokio::task::spawn_blocking(move || {
                let mut journal = crate::delivery::JournalConfirmation::new(&config, &session)?;
                journal.poll()?;
                Ok::<_, AgentConnectorError>(journal.contains(&submission))
            })
            .await
            .map_err(|_| unknown())??;
            if delivered {
                old.status = Some("delivered".to_owned());
                let path = path.clone();
                old = tokio::task::spawn_blocking(move || {
                    save(&path, &old)?;
                    Ok::<_, AgentConnectorError>(old)
                })
                .await
                .map_err(|_| unknown())??;
            }
        }
        return old
            .status
            .as_deref()
            .map(outcome)
            .unwrap_or_else(|| Err(unknown()));
    }
    let config_root = connector.config.config_dir.clone();
    let owners = tokio::task::spawn_blocking(move || crate::native_sessions(&config_root))
        .await
        .map_err(|error| AgentConnectorError::protocol(error.to_string()))??;
    let owner = owners
        .into_iter()
        .find(|owner| owner.session_id == session_id.as_str())
        .ok_or_else(|| {
            AgentConnectorError::new(
                AgentConnectorErrorCode::LeaseConflict,
                "原 Claude 进程已退出，请刷新会话后重新发送。",
                false,
            )
        })?;
    let target = owner
        .messaging_socket_path
        .as_ref()
        .ok_or_else(|| AgentConnectorError::unsupported("Claude owner has no messaging socket"))?;
    if owner.peer_protocol != Some(1) {
        return Err(AgentConnectorError::unsupported(
            "unsupported Claude peer protocol",
        ));
    }
    let inbox = connector
        .peer
        .get_or_try_init(|| PeerInbox::bind(&connector.config.config_dir, target))
        .await?;
    inbox.validate_input(session_id, &input)?;
    // Authenticate before claiming, so disk sync cannot exceed the native
    // first-line deadline. Authentication sends no conversation input.
    let prepared = inbox.prepare(&connector.config.config_dir, &owner).await?;
    let mut dispatch = Dispatch {
        session_id: session_id.clone(),
        input,
        status: None,
        created_at_unix_ms: Some(chrono::Utc::now().timestamp_millis()),
    };
    let bytes = serde_json::to_vec(&dispatch)
        .map_err(|error| AgentConnectorError::protocol(error.to_string()))?;
    let claim_path = path.clone();
    let claim_root = root.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(claim_path).map_err(crate::io_error)?;
        file.write_all(&bytes).map_err(crate::io_error)?;
        file.sync_all().map_err(crate::io_error)?;
        std::fs::File::open(claim_root)
            .and_then(|directory| directory.sync_all())
            .map_err(crate::io_error)
    })
    .await
    .map_err(|_| unknown())??;
    let receipt = inbox
        .send(
            prepared,
            &connector.config.config_dir,
            session_id,
            &dispatch.input,
        )
        .await;
    dispatch.status = Some(receipt.unwrap_or_else(|_| "unknown".to_owned()));
    dispatch = tokio::task::spawn_blocking(move || {
        save(&path, &dispatch)?;
        Ok::<_, AgentConnectorError>(dispatch)
    })
    .await
    .map_err(|_| unknown())??;
    outcome(dispatch.status.as_deref().unwrap_or("unknown"))
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::{oneshot, Mutex};
    type Pending = Arc<Mutex<BTreeMap<String, oneshot::Sender<String>>>>;
    pub(crate) struct Prepared {
        stream: UnixStream,
    }
    pub(crate) struct PeerInbox {
        path: PathBuf,
        key: PathBuf,
        pending: Pending,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for PeerInbox {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(&self.key);
        }
    }
    fn hash(path: &Path) -> String {
        format!("{:x}", Sha256::digest(path.to_string_lossy().as_bytes()))
    }
    fn secret(path: &Path) -> Result<String, AgentConnectorError> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(crate::io_error)?;
        let metadata = file.metadata().map_err(crate::io_error)?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
            || metadata.nlink() != 1
            || metadata.len() > 4096
        {
            return Err(AgentConnectorError::invalid(
                "Claude peer key must be a private file owned by this user",
            ));
        }
        let mut bytes = Vec::new();
        file.by_ref()
            .take(4097)
            .read_to_end(&mut bytes)
            .map_err(crate::io_error)?;
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| AgentConnectorError::invalid("invalid Claude peer key"))?;
        value
            .get("peerToken")
            .and_then(Value::as_str)
            .filter(|token| token.len() == 32 && token.bytes().all(|c| c.is_ascii_hexdigit()))
            .map(str::to_owned)
            .ok_or_else(|| AgentConnectorError::invalid("invalid Claude peer token"))
    }
    fn validate_socket(path: &Path, pid: u32) -> Result<(), AgentConnectorError> {
        use std::os::unix::fs::FileTypeExt;
        if !path.is_absolute()
            || path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            || path.file_name().and_then(|v| v.to_str()) != Some(&format!("{pid}.sock"))
        {
            return Err(AgentConnectorError::invalid(
                "unexpected Claude peer address",
            ));
        }
        let metadata = std::fs::symlink_metadata(path).map_err(crate::io_error)?;
        if !metadata.file_type().is_socket()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            return Err(AgentConnectorError::invalid(
                "Claude peer socket must be private and owned by this user",
            ));
        }
        Ok(())
    }
    impl PeerInbox {
        fn frames(
            &self,
            id: &str,
            session: &AgentSessionId,
            input: &AgentSessionTextInput,
        ) -> String {
            format!(
                "{}\n",
                json!({"type":"user","session_id":session.as_str(),"uuid":input.submission_id,"msg_id":id,"from":format!("uds:{}",self.path.display()),"priority":"next","message":{"role":"user","content":input.text}})
            )
        }
        pub(crate) fn validate_input(
            &self,
            session: &AgentSessionId,
            input: &AgentSessionTextInput,
        ) -> Result<(), AgentConnectorError> {
            if self
                .frames(&uuid::Uuid::nil().to_string(), session, input)
                .len()
                > 1_000_000
            {
                return Err(AgentConnectorError::invalid(
                    "Claude message exceeds its native size limit",
                ));
            }
            Ok(())
        }
        pub(crate) async fn bind(
            config: &Path,
            target: &Path,
        ) -> Result<Arc<Self>, AgentConnectorError> {
            let parent = target
                .parent()
                .ok_or_else(|| AgentConnectorError::invalid("missing socket directory"))?;
            let metadata = std::fs::metadata(parent).map_err(crate::io_error)?;
            if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
                return Err(AgentConnectorError::invalid(
                    "Claude socket directory must be private",
                ));
            }
            let path = parent.join(format!("{}.sock", std::process::id()));
            let listener = UnixListener::bind(&path).map_err(crate::io_error)?;
            let key = config.join("sessions").join(format!(
                "{}.{:}.key",
                std::process::id(),
                hash(&path)
            ));
            let token = uuid::Uuid::new_v4().simple().to_string();
            use std::io::Write;
            let publish = (|| {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                    .map_err(crate::io_error)?;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(&key)
                    .map_err(crate::io_error)?;
                if let Err(error) =
                    file.write_all(json!({"peerToken":token}).to_string().as_bytes())
                {
                    let _ = std::fs::remove_file(&key);
                    return Err(crate::io_error(error));
                }
                Ok(())
            })();
            if let Err(error) = publish {
                let _ = std::fs::remove_file(&path);
                return Err(error);
            }
            let pending: Pending = Arc::default();
            let incoming = pending.clone();
            let task = tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    if stream
                        .peer_cred()
                        .map_or(true, |cred| cred.uid() != unsafe { libc::geteuid() })
                    {
                        continue;
                    }
                    let incoming = incoming.clone();
                    let token = token.clone();
                    tokio::spawn(async move {
                        let read = async {
                            let mut reader = BufReader::new(stream);
                            let mut authenticated = false;
                            loop {
                                let mut line = Vec::new();
                                use tokio::io::AsyncReadExt;
                                if (&mut reader)
                                    .take(65537)
                                    .read_until(b'\n', &mut line)
                                    .await
                                    .ok()?
                                    == 0
                                    || line.len() > 65536
                                {
                                    break;
                                }
                                let value: Value = serde_json::from_slice(&line).ok()?;
                                if !authenticated {
                                    authenticated = value.get("type").and_then(Value::as_str)
                                        == Some("auth")
                                        && value.get("token").and_then(Value::as_str)
                                            == Some(&token);
                                    if !authenticated {
                                        break;
                                    }
                                    continue;
                                }
                                if value.get("type").and_then(Value::as_str) != Some("control")
                                    || value.get("action").and_then(Value::as_str)
                                        != Some("peer_message_status")
                                {
                                    continue;
                                }
                                let id = value.get("orig_msg_id").and_then(Value::as_str)?;
                                let status = value.get("status").and_then(Value::as_str)?;
                                if let Some(sender) = incoming.lock().await.remove(id) {
                                    let _ = sender.send(status.to_owned());
                                }
                            }
                            Some(())
                        };
                        let _ = tokio::time::timeout(Duration::from_secs(5), read).await;
                    });
                }
            });
            Ok(Arc::new(Self {
                path,
                key,
                pending,
                task,
            }))
        }
        pub(crate) async fn prepare(
            &self,
            config: &Path,
            owner: &NativeSession,
        ) -> Result<Prepared, AgentConnectorError> {
            let target = owner
                .messaging_socket_path
                .clone()
                .ok_or_else(|| AgentConnectorError::invalid("missing peer socket"))?;
            validate_socket(&target, owner.pid)?;
            let key = config
                .join("sessions")
                .join(format!("{}.{:}.key", owner.pid, hash(&target)));
            let token = tokio::task::spawn_blocking(move || secret(&key))
                .await
                .map_err(|error| AgentConnectorError::protocol(error.to_string()))??;
            let mut stream =
                tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(target))
                    .await
                    .map_err(|_| {
                        AgentConnectorError::new(
                            AgentConnectorErrorCode::Unavailable,
                            "Claude peer connection timed out",
                            true,
                        )
                    })?
                    .map_err(crate::io_error)?;
            tokio::time::timeout(
                Duration::from_secs(3),
                stream.write_all(format!("{}\n", json!({"type":"auth","token":token})).as_bytes()),
            )
            .await
            .map_err(|_| {
                crate::io_error(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Claude peer authentication timed out",
                ))
            })?
            .map_err(crate::io_error)?;
            Ok(Prepared { stream })
        }
        pub(crate) async fn send(
            &self,
            mut prepared: Prepared,
            config: &Path,
            session: &AgentSessionId,
            input: &AgentSessionTextInput,
        ) -> Result<String, AgentConnectorError> {
            let (sender, receiver) = oneshot::channel();
            let id = uuid::Uuid::new_v4().to_string();
            self.pending.lock().await.insert(id.clone(), sender);
            let frames = self.frames(&id, session, input);
            let result = async {
                if frames.len() > 1_000_000 {
                    return Err(AgentConnectorError::invalid(
                        "Claude message exceeds its native size limit",
                    ));
                }
                tokio::time::timeout(Duration::from_secs(3), async {
                    prepared
                        .stream
                        .write_all(frames.as_bytes())
                        .await
                        .map_err(|_| unknown())?;
                    prepared.stream.shutdown().await.map_err(|_| unknown())
                })
                .await
                .map_err(|_| unknown())??;
                let mut journal =
                    crate::delivery::JournalConfirmation::new(config, session.as_str())?;
                let confirmation = async {
                    loop {
                        journal = tokio::task::spawn_blocking(move || {
                            journal.poll()?;
                            Ok::<_, AgentConnectorError>(journal)
                        })
                        .await
                        .map_err(|_| unknown())??;
                        if journal.contains(&input.submission_id) {
                            return Ok("delivered".to_owned());
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                };
                tokio::time::timeout(Duration::from_secs(2), async {
                    tokio::select! {
                        receipt = receiver => receipt.map_err(|_| unknown()),
                        delivered = confirmation => delivered,
                    }
                })
                .await
                .unwrap_or_else(|_| Ok("submitted".to_owned()))
            }
            .await;
            self.pending.lock().await.remove(&id);
            result
        }
    }
    use std::os::unix::fs::PermissionsExt;
}
#[cfg(unix)]
pub(crate) use unix::PeerInbox;

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::ClaudeCodeConfig;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::process::{Child, Command};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    async fn fixture() -> (
        tempfile::TempDir,
        Process,
        ClaudeCodeConnector,
        UnixListener,
        AgentSessionId,
    ) {
        let root = tempfile::Builder::new()
            .prefix("oc-peer-")
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let process = Process(Command::new("sleep").arg("30").spawn().unwrap());
        let pid = process.0.id();
        let config = root.path().join("native");
        std::fs::create_dir_all(config.join("sessions")).unwrap();
        let socket = root.path().join(format!("{pid}.sock"));
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let session = AgentSessionId::new(uuid::Uuid::new_v4().to_string());
        std::fs::write(config.join("sessions").join(format!("{pid}.json")),serde_json::to_vec(&json!({"pid":pid,"sessionId":session.as_str(),"cwd":root.path(),"status":"idle","peerProtocol":1,"messagingSocketPath":socket})).unwrap()).unwrap();
        let key = config.join("sessions").join(format!(
            "{pid}.{:x}.key",
            Sha256::digest(socket.to_string_lossy().as_bytes())
        ));
        std::fs::write(&key, json!({"peerToken":"a".repeat(32)}).to_string()).unwrap();
        std::fs::set_permissions(key, std::fs::Permissions::from_mode(0o600)).unwrap();
        let connector = ClaudeCodeConnector::new(ClaudeCodeConfig {
            executable: "unused".into(),
            config_dir: config,
            session_store_dir: root.path().join("host/sessions"),
            request_timeout: std::time::Duration::from_secs(1),
        });
        (root, process, connector, listener, session)
    }
    async fn receive(listener: &UnixListener) -> Value {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut auth = String::new();
        reader.read_line(&mut auth).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&auth).unwrap()["token"],
            "a".repeat(32)
        );
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }
    async fn receipt(config: &Path, frame: &Value, status: &str, wrong_auth: bool) {
        let path = frame["from"]
            .as_str()
            .unwrap()
            .strip_prefix("uds:")
            .unwrap();
        let pid = std::process::id();
        let key = config
            .join("sessions")
            .join(format!("{pid}.{:x}.key", Sha256::digest(path.as_bytes())));
        let token = serde_json::from_slice::<Value>(&std::fs::read(key).unwrap()).unwrap()
            ["peerToken"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut stream = UnixStream::connect(path).await.unwrap();
        let auth = if wrong_auth { "invalid" } else { &token };
        stream.write_all(format!("{}\n{}\n{}\n",json!({"type":"auth","token":auth}),json!({"type":"control","action":"peer_message_status","orig_msg_id":"unrelated","status":"delivered"}),json!({"type":"control","action":"peer_message_status","orig_msg_id":frame["msg_id"],"status":status})).as_bytes()).await.unwrap();
    }
    fn journal_input(
        config: &Path,
        session: &AgentSessionId,
        input: &AgentSessionTextInput,
        busy: bool,
    ) {
        use std::io::Write;
        let project = config.join("projects/project");
        std::fs::create_dir_all(&project).unwrap();
        let origin = json!({"kind":"peer"});
        let record = if busy {
            json!({"type":"attachment","sessionId":session.as_str(),"uuid":uuid::Uuid::new_v4(),"attachment":{"type":"queued_command","source_uuid":input.submission_id,"origin":origin}})
        } else {
            json!({"type":"user","sessionId":session.as_str(),"uuid":input.submission_id,"origin":origin,"message":{"role":"user","content":input.text}})
        };
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(project.join(format!("{}.jsonl", session.as_str())))
            .unwrap();
        writeln!(file, "{record}").unwrap();
    }
    #[tokio::test]
    async fn initial_idle_and_busy_delivery_need_no_receipt_and_never_resend() {
        let (_root, _process, connector, listener, session) = fixture().await;
        for busy in [false, true] {
            let input = AgentSessionTextInput {
                submission_id: uuid::Uuid::new_v4().to_string(),
                text: "continue the analysis".to_owned(),
            };
            let sending = submit(&connector, &session, input.clone());
            let owner = async {
                let frame = receive(&listener).await;
                assert_eq!(frame["uuid"], input.submission_id);
                journal_input(&connector.config.config_dir, &session, &input, busy);
                // Native initial acceptance deliberately emits no receipt.
            };
            let (result, ()) = tokio::join!(sending, owner);
            assert_eq!(result.unwrap().details["delivery_status"], "delivered");
            assert_eq!(
                submit(&connector, &session, input).await.unwrap().details["delivery_status"],
                "delivered"
            );
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn successful_socket_write_is_visible_as_submitted_until_native_confirmation() {
        use orchestral_core::agent_connector::AgentConnector;
        let (_root, _process, connector, listener, session) = fixture().await;
        let input = AgentSessionTextInput {
            submission_id: uuid::Uuid::new_v4().to_string(),
            text: "work continues later".to_owned(),
        };
        let (result, frame) = tokio::join!(
            submit(&connector, &session, input.clone()),
            receive(&listener)
        );
        assert_eq!(frame["uuid"], input.submission_id);
        assert_eq!(result.unwrap().details["delivery_status"], "submitted");
        let detail = connector.read_session(&session).await.unwrap();
        let message = &detail.turns.last().unwrap().activities[0];
        assert_eq!(message.status, AgentSessionActivityStatus::Pending);
        assert_eq!(message.details["clientId"], input.submission_id);
        assert_eq!(
            submit(&connector, &session, input.clone())
                .await
                .unwrap()
                .details["delivery_status"],
            "submitted"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
        journal_input(&connector.config.config_dir, &session, &input, false);
        let detail = connector.read_session(&session).await.unwrap();
        assert_eq!(detail.turns.len(), 1);
        assert_eq!(
            detail.turns[0].activities[0].status,
            AgentSessionActivityStatus::Completed
        );
        assert_eq!(
            submit(&connector, &session, input).await.unwrap().details["delivery_status"],
            "delivered"
        );
    }
    #[tokio::test]
    async fn unconfirmed_delivery_is_reconciled_after_restart_without_sending() {
        let (root, _process, connector, listener, session) = fixture().await;
        let input = AgentSessionTextInput {
            submission_id: uuid::Uuid::new_v4().to_string(),
            text: "already delivered before restart".to_owned(),
        };
        let path = root.path().join("host/dispatch").join(format!(
            "{:x}.json",
            Sha256::digest(input.submission_id.as_bytes())
        ));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        for status in [None, Some("unknown"), Some("held")] {
            save(
                &path,
                &Dispatch {
                    session_id: session.clone(),
                    input: input.clone(),
                    status: status.map(str::to_owned),
                    created_at_unix_ms: None,
                },
            )
            .unwrap();
            journal_input(&connector.config.config_dir, &session, &input, true);
            reconcile(&connector.config).unwrap();
            let saved: Dispatch = crate::read_json(&path).unwrap();
            assert_eq!(saved.status.as_deref(), Some("delivered"));
        }
        let config = connector.config.clone();
        drop(connector);
        let restarted = ClaudeCodeConnector::new(config);
        assert_eq!(
            submit(&restarted, &session, input).await.unwrap().details["delivery_status"],
            "delivered"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn existing_owner_receipt_is_correlated_and_restart_replays_without_resending() {
        let (_root, _process, connector, listener, session) = fixture().await;
        let config = connector.config.clone();
        let expected = session.clone();
        let server = tokio::spawn(async move {
            let frame = receive(&listener).await;
            assert_eq!(frame["session_id"], expected.as_str());
            assert_eq!(frame["message"]["content"], "continue the analysis");
            receipt(&config.config_dir, &frame, "denied", true).await;
            receipt(&config.config_dir, &frame, "delivered", false).await;
        });
        let input = AgentSessionTextInput {
            submission_id: uuid::Uuid::new_v4().to_string(),
            text: "continue the analysis".to_owned(),
        };
        assert_eq!(
            submit(&connector, &session, input.clone())
                .await
                .unwrap()
                .details["delivery_status"],
            "delivered"
        );
        server.await.unwrap();
        let saved = connector.config.clone();
        drop(connector);
        let restarted = ClaudeCodeConnector::new(saved);
        assert_eq!(
            submit(&restarted, &session, input.clone())
                .await
                .unwrap()
                .details["delivery_status"],
            "delivered"
        );
        let changed = AgentSessionTextInput {
            text: "different".to_owned(),
            ..input
        };
        assert_eq!(
            submit(&restarted, &session, changed)
                .await
                .unwrap_err()
                .code,
            AgentConnectorErrorCode::LeaseConflict
        );
    }
    #[tokio::test]
    async fn held_input_is_reported_as_held_and_uncertain_claims_are_never_dispatched() {
        let (root, _process, connector, listener, session) = fixture().await;
        let config = connector.config.config_dir.clone();
        let server = tokio::spawn(async move {
            let frame = receive(&listener).await;
            receipt(&config, &frame, "held", false).await;
        });
        let input = AgentSessionTextInput {
            submission_id: uuid::Uuid::new_v4().to_string(),
            text: "follow up".to_owned(),
        };
        assert_eq!(
            submit(&connector, &session, input).await.unwrap().details["delivery_status"],
            "held"
        );
        server.await.unwrap();
        let pending = AgentSessionTextInput {
            submission_id: uuid::Uuid::new_v4().to_string(),
            text: "pending before restart".to_owned(),
        };
        let path = root.path().join("host/dispatch").join(format!(
            "{:x}.json",
            Sha256::digest(pending.submission_id.as_bytes())
        ));
        std::fs::write(
            path,
            serde_json::to_vec(&Dispatch {
                session_id: session.clone(),
                input: pending.clone(),
                status: None,
                created_at_unix_ms: None,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            submit(&connector, &session, pending)
                .await
                .unwrap_err()
                .code,
            AgentConnectorErrorCode::OutcomeUnknown
        );
    }
    #[tokio::test]
    async fn native_terminal_keeps_composer_input_and_discloses_peer_origin() {
        use orchestral_core::agent_connector::{
            AgentConnector, AgentSessionListQuery, AgentSessionState,
        };
        let (_root, _process, connector, _listener, _session) = fixture().await;
        let page = connector
            .list_sessions(AgentSessionListQuery::default())
            .await
            .unwrap();
        assert_eq!(
            page.sessions[0].input_action.as_ref().unwrap().as_str(),
            INPUT_ACTION
        );
        assert_eq!(page.sessions[0].state, AgentSessionState::Idle);
        let descriptor = connector.describe();
        assert!(descriptor.actions[0].input_channel);
        assert_eq!(descriptor.actions[0].action_id.as_str(), INPUT_ACTION);
        assert!(descriptor.actions[0].description.contains("同伴请求"));
        let error = outcome("refused").unwrap_err();
        assert_eq!(error.code, AgentConnectorErrorCode::LeaseConflict);
    }
}

#[cfg(not(unix))]
pub(crate) struct PeerInbox;
#[cfg(not(unix))]
impl PeerInbox {
    pub(crate) fn validate_input(
        &self,
        _: &AgentSessionId,
        _: &AgentSessionTextInput,
    ) -> Result<(), AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "live Claude input requires Unix",
        ))
    }
    pub(crate) async fn bind(
        _: &std::path::Path,
        _: &std::path::Path,
    ) -> Result<Arc<Self>, AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "live Claude input currently requires a Unix socket",
        ))
    }
    pub(crate) async fn prepare(
        &self,
        _: &std::path::Path,
        _: &NativeSession,
    ) -> Result<(), AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "live Claude input requires Unix",
        ))
    }
    pub(crate) async fn send(
        &self,
        _: (),
        _: &std::path::Path,
        _: &AgentSessionId,
        _: &AgentSessionTextInput,
    ) -> Result<String, AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "live Claude input requires Unix",
        ))
    }
}
