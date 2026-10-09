//! PermissionRequest hooks bridge the native permission UI to the Host's
//! session-scoped request contract. Only an explicit user decision is returned.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestral_core::agent_connector::{
    AgentConnectorError, AgentConnectorErrorCode, AgentSessionRequestResolution,
    ResolveAgentSessionRequest,
};
use orchestral_core::agent_protocol::wire::{
    AgentSessionId, ApprovalDecision, Digest, PendingRequest, PendingRequestPayload, RequestId,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{oneshot, watch};

use crate::ClaudeCodeConnector;

const MAX_FRAME: u64 = 1_048_576;
const HOOK_TIMEOUT: u64 = 3600;

#[derive(Deserialize)]
struct HookCall {
    request_id: String,
    input: HookInput,
}

#[derive(Deserialize)]
struct HookInput {
    session_id: String,
    hook_event_name: String,
    tool_name: String,
    tool_input: Value,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Receipt {
    Waiting,
    Delivered,
    Closed,
}

struct Entry {
    session_id: AgentSessionId,
    request: PendingRequest,
    sender: Option<oneshot::Sender<ApprovalDecision>>,
    decision: Option<ApprovalDecision>,
    receipt: watch::Sender<Receipt>,
}

#[derive(Default)]
pub(crate) struct NativeApprovals {
    entries: Mutex<BTreeMap<RequestId, Entry>>,
}

fn closed() -> AgentConnectorError {
    AgentConnectorError::new(
        AgentConnectorErrorCode::NotFound,
        "Claude 审批已结束或由另一端处理，请刷新会话。",
        false,
    )
}

impl NativeApprovals {
    pub(crate) fn pending(&self, session_id: &AgentSessionId) -> Vec<PendingRequest> {
        crate::lock(&self.entries)
            .values()
            .filter(|entry| {
                entry.session_id == *session_id
                    && entry.sender.is_some()
                    && *entry.receipt.borrow() == Receipt::Waiting
            })
            .map(|entry| entry.request.clone())
            .collect()
    }

    fn open(
        &self,
        call: &HookCall,
    ) -> Result<oneshot::Receiver<ApprovalDecision>, AgentConnectorError> {
        let id = RequestId::new(call.request_id.clone());
        let mut entries = crate::lock(&self.entries);
        if entries.contains_key(&id) {
            return Err(AgentConnectorError::invalid(
                "duplicate native permission identity",
            ));
        }
        if entries.len() >= 512 {
            entries.retain(|_, entry| *entry.receipt.borrow() == Receipt::Waiting);
        }
        if entries.len() >= 512 {
            return Err(AgentConnectorError::new(
                AgentConnectorErrorCode::Busy,
                "too many pending native permissions",
                false,
            ));
        }
        let digest = serde_json::to_vec(&json!({"session_id":call.input.session_id,
            "tool_name":call.input.tool_name,"tool_input":call.input.tool_input}))
        .map_err(|error| AgentConnectorError::protocol(error.to_string()))?;
        let request = PendingRequest {
            request_id: id.clone(),
            blocking: true,
            payload: PendingRequestPayload::Approval {
                operation_digest: Digest::sha256(digest),
                requested_scope: vec!["external_side_effect".to_owned()],
                session_approval_scope: None,
                reason: format!("{}: {}", call.input.tool_name, call.input.tool_input),
            },
        };
        let (sender, receiver) = oneshot::channel();
        let (receipt, _) = watch::channel(Receipt::Waiting);
        entries.insert(
            id,
            Entry {
                session_id: AgentSessionId::new(&call.input.session_id),
                request,
                sender: Some(sender),
                decision: None,
                receipt,
            },
        );
        Ok(receiver)
    }

    fn finish(&self, id: &str, delivered: bool) {
        if let Some(entry) = crate::lock(&self.entries).get_mut(&RequestId::new(id)) {
            entry.sender = None;
            entry.receipt.send_replace(if delivered {
                Receipt::Delivered
            } else {
                Receipt::Closed
            });
        }
    }

    pub(crate) async fn resolve(
        &self,
        request: ResolveAgentSessionRequest,
    ) -> Result<(), AgentConnectorError> {
        let AgentSessionRequestResolution::Approval { decision } = request.response else {
            return Err(AgentConnectorError::invalid(
                "native permission requires an approval decision",
            ));
        };
        let mut receipt = {
            let mut entries = crate::lock(&self.entries);
            let entry = entries.get_mut(&request.request_id).ok_or_else(closed)?;
            if entry.session_id != request.session_id {
                return Err(closed());
            }
            if entry.decision.is_some_and(|old| old != decision) {
                return Err(AgentConnectorError::new(
                    AgentConnectorErrorCode::LeaseConflict,
                    "this approval already has a different decision",
                    false,
                ));
            }
            if *entry.receipt.borrow() == Receipt::Closed {
                return Err(closed());
            }
            if let Some(sender) = entry.sender.take() {
                entry.decision = Some(decision);
                sender.send(decision).map_err(|_| closed())?;
            }
            entry.receipt.subscribe()
        };
        let wait = async {
            loop {
                let state = *receipt.borrow_and_update();
                match state {
                    Receipt::Delivered => return Ok(()),
                    Receipt::Closed => return Err(closed()),
                    Receipt::Waiting => receipt.changed().await.map_err(|_| closed())?,
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(3), wait)
            .await
            .map_err(|_| {
                AgentConnectorError::new(
                    AgentConnectorErrorCode::OutcomeUnknown,
                    "审批决定已提交，但未确认原进程接收；请刷新会话，不会自动再次批准。",
                    false,
                )
            })?
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    use std::sync::Weak;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};

    pub(crate) struct ApprovalBridge {
        pub(crate) path: PathBuf,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for ApprovalBridge {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_file(&self.path);
        }
    }

    impl ApprovalBridge {
        pub(crate) async fn bind(
            connector: &Arc<ClaudeCodeConnector>,
        ) -> Result<Arc<Self>, AgentConnectorError> {
            let root = connector
                .config
                .session_store_dir
                .parent()
                .ok_or_else(|| AgentConnectorError::invalid("missing Claude state root"))?;
            std::fs::create_dir_all(root).map_err(crate::io_error)?;
            let metadata = std::fs::symlink_metadata(root).map_err(crate::io_error)?;
            if !metadata.is_dir() || metadata.uid() != unsafe { libc::geteuid() } {
                return Err(AgentConnectorError::invalid(
                    "Claude approval directory must be owned by this user",
                ));
            }
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))
                .map_err(crate::io_error)?;
            let path = root.join("approval.sock");
            if let Ok(metadata) = std::fs::symlink_metadata(&path) {
                if !metadata.file_type().is_socket() || metadata.uid() != unsafe { libc::geteuid() }
                {
                    return Err(AgentConnectorError::invalid(
                        "unexpected approval socket file",
                    ));
                }
                if UnixStream::connect(&path).await.is_ok() {
                    return Err(AgentConnectorError::new(
                        AgentConnectorErrorCode::Busy,
                        "Claude approval bridge is already running",
                        false,
                    ));
                }
                std::fs::remove_file(&path).map_err(crate::io_error)?;
            }
            let listener = UnixListener::bind(&path).map_err(crate::io_error)?;
            if let Err(error) =
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            {
                let _ = std::fs::remove_file(&path);
                return Err(crate::io_error(error));
            }
            let weak = Arc::downgrade(connector);
            let task = tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    if stream.peer_cred().map_or(true, |credentials| {
                        credentials.uid() != unsafe { libc::geteuid() }
                    }) {
                        continue;
                    }
                    let weak = weak.clone();
                    tokio::spawn(async move {
                        let _ = handle(stream, weak).await;
                    });
                }
            });
            Ok(Arc::new(Self { path, task }))
        }
    }

    async fn line<R: tokio::io::AsyncRead + Unpin>(
        reader: &mut BufReader<R>,
    ) -> Result<Vec<u8>, AgentConnectorError> {
        let mut bytes = Vec::new();
        (&mut *reader)
            .take(MAX_FRAME + 1)
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(crate::io_error)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_FRAME {
            return Err(AgentConnectorError::invalid("invalid native hook frame"));
        }
        Ok(bytes)
    }

    async fn handle(
        stream: UnixStream,
        weak: Weak<ClaudeCodeConnector>,
    ) -> Result<(), AgentConnectorError> {
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let bytes = tokio::time::timeout(Duration::from_secs(5), line(&mut reader))
            .await
            .map_err(|_| closed())??;
        let call: HookCall = serde_json::from_slice(&bytes)
            .map_err(|error| AgentConnectorError::invalid(error.to_string()))?;
        uuid::Uuid::parse_str(&call.request_id)
            .map_err(|_| AgentConnectorError::invalid("invalid permission identity"))?;
        crate::validate_session_id(&call.input.session_id)?;
        if call.input.hook_event_name != "PermissionRequest"
            || call.input.tool_name.trim().is_empty()
            || call.input.tool_name.len() > 256
            || !call.input.tool_input.is_object()
        {
            return Err(AgentConnectorError::invalid(
                "invalid PermissionRequest hook input",
            ));
        }
        let connector = weak.upgrade().ok_or_else(closed)?;
        let session = AgentSessionId::new(&call.input.session_id);
        // SDK-owned Runs already have their own control_request approval path.
        // A hook must never create a second decision authority for them.
        let owned = connector
            .provider_state()
            .session_state(&session)
            .is_some_and(|state| state != crate::AgentSessionState::Idle);
        if owned || !connector.native_owner(&session).await? {
            write.write_all(b"{}\n").await.map_err(crate::io_error)?;
            return Ok(());
        }
        let receiver = connector.native_approvals.open(&call)?;
        let mut cancelled = [0u8; 1];
        let decision = tokio::select! {
            decision = receiver => decision.ok(),
            _ = reader.read(&mut cancelled) => None,
            _ = tokio::time::sleep(Duration::from_secs(HOOK_TIMEOUT - 30)) => None,
        };
        let handed = async {
            let Some(decision) = decision else {
                return Ok(false);
            };
            let decision = match decision {
                ApprovalDecision::Allow => {
                    // An unchanged input is not a hook rewrite. Echoing it as
                    // updatedInput makes Claude re-check explicit ask rules and
                    // can reopen the very permission the user just answered.
                    json!({"behavior":"allow"})
                }
                ApprovalDecision::Deny => {
                    json!({"behavior":"deny","message":"用户在 Orchestral PWA 中拒绝了此操作"})
                }
                _ => {
                    return Err(AgentConnectorError::unsupported(
                        "unsupported approval decision",
                    ))
                }
            };
            let response = json!({"request_id":call.request_id,"output":{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":decision}}});
            write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .map_err(crate::io_error)?;
            let acknowledgement: Value =
                serde_json::from_slice(&line(&mut reader).await?).map_err(|_| closed())?;
            Ok(acknowledgement.get("received").and_then(Value::as_str) == Some(&call.request_id))
        };
        let delivered = tokio::time::timeout(Duration::from_secs(2), handed)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false);
        connector
            .native_approvals
            .finish(&call.request_id, delivered);
        Ok(())
    }

    pub(crate) async fn run_hook(socket: &Path) -> Result<(), AgentConnectorError> {
        use std::io::Write;
        let mut input = Vec::new();
        tokio::io::stdin()
            .take(MAX_FRAME + 1)
            .read_to_end(&mut input)
            .await
            .map_err(crate::io_error)?;
        if input.len() as u64 > MAX_FRAME {
            return Err(AgentConnectorError::invalid(
                "native permission input exceeds size limit",
            ));
        }
        let input: Value = serde_json::from_slice(&input)
            .map_err(|error| AgentConnectorError::invalid(error.to_string()))?;
        let call = json!({"request_id":uuid::Uuid::new_v4().to_string(),"input":input});
        let stream = tokio::time::timeout(Duration::from_secs(2), UnixStream::connect(socket))
            .await
            .map_err(|_| closed())?
            .map_err(crate::io_error)?;
        let (read, mut write) = stream.into_split();
        write
            .write_all(format!("{call}\n").as_bytes())
            .await
            .map_err(crate::io_error)?;
        let mut reader = BufReader::new(read);
        let reply = tokio::time::timeout(Duration::from_secs(HOOK_TIMEOUT), line(&mut reader))
            .await
            .map_err(|_| closed())??;
        let reply: Value = serde_json::from_slice(&reply).map_err(|_| closed())?;
        let output = reply.get("output").cloned().unwrap_or_else(|| json!({}));
        {
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{output}").map_err(crate::io_error)?;
            stdout.flush().map_err(crate::io_error)?;
        }
        if let Some(id) = reply.get("request_id").and_then(Value::as_str) {
            let _ = write
                .write_all(format!("{}\n", json!({"received":id})).as_bytes())
                .await;
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::ClaudeCodeConfig;
    use orchestral_core::agent_connector::AgentConnector;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    async fn fixture() -> (tempfile::TempDir, Arc<ClaudeCodeConnector>, AgentSessionId) {
        let root = tempfile::Builder::new()
            .prefix("oc-approval-")
            .tempdir_in("/tmp")
            .unwrap();
        let native = root.path().join("native");
        std::fs::create_dir_all(native.join("sessions")).unwrap();
        let id = AgentSessionId::new(uuid::Uuid::new_v4().to_string());
        std::fs::write(native.join("sessions").join(format!("{}.json",std::process::id())),
            json!({"pid":std::process::id(),"sessionId":id.as_str(),"cwd":root.path(),"status":"idle"}).to_string()).unwrap();
        std::fs::write(native.join("settings.json"), json!({"env":{"FIXTURE_ENV":"preserved"},"permissions":{"deny":["Bash(secret)"]},"hooks":{"PermissionRequest":[{"matcher":"Edit","hooks":[{"type":"command","command":"existing-hook"}]}]}}).to_string()).unwrap();
        let connector = Arc::new(ClaudeCodeConnector::new(ClaudeCodeConfig {
            executable: "unused".into(),
            config_dir: native,
            session_store_dir: root.path().join("host/sessions"),
            request_timeout: Duration::from_secs(1),
        }));
        connector
            .enable_native_approvals(Path::new("/fixture/path with spaces/orchestral"))
            .await
            .unwrap();
        (root, connector, id)
    }

    async fn pending(connector: &ClaudeCodeConnector, session: &AgentSessionId) -> PendingRequest {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(request) = connector
                    .read_session(session)
                    .await
                    .unwrap()
                    .pending_requests
                    .first()
                {
                    return request.clone();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }

    async fn connect(connector: &ClaudeCodeConnector, session: &AgentSessionId) -> UnixStream {
        let mut stream = UnixStream::connect(&connector.approval_bridge.get().unwrap().path)
            .await
            .unwrap();
        let call = json!({"request_id":uuid::Uuid::new_v4().to_string(),"input":{
            "session_id":session.as_str(),"hook_event_name":"PermissionRequest", "tool_name":"Bash",
            "tool_input":{"command":"cargo check"}, "permission_suggestions":[]}});
        stream
            .write_all(format!("{call}\n").as_bytes())
            .await
            .unwrap();
        stream
    }

    #[tokio::test]
    async fn native_permissions_are_session_bound_and_duplicate_decisions_cannot_flip() {
        let (_root, connector, session) = fixture().await;
        assert!(connector.describe().capabilities.resolve_requests);
        for decision in [ApprovalDecision::Allow, ApprovalDecision::Deny] {
            let stream = connect(&connector, &session).await;
            let request = pending(&connector, &session).await;
            assert_eq!(
                connector
                    .read_session(&session)
                    .await
                    .unwrap()
                    .summary
                    .state,
                crate::AgentSessionState::WaitingApproval
            );
            let resolve = ResolveAgentSessionRequest {
                session_id: session.clone(),
                request_id: request.request_id.clone(),
                response: AgentSessionRequestResolution::Approval { decision },
            };
            let mut forged = resolve.clone();
            forged.session_id = AgentSessionId::new(uuid::Uuid::new_v4().to_string());
            assert!(connector.resolve_request(forged).await.is_err());
            let mut wrong_kind = resolve.clone();
            wrong_kind.response = AgentSessionRequestResolution::Input {
                content: vec![orchestral_core::agent_protocol::wire::Content::text("yes")],
            };
            assert!(connector.resolve_request(wrong_kind).await.is_err());
            let native = tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let output: Value = serde_json::from_str(&line).unwrap();
                let result = &output["output"]["hookSpecificOutput"]["decision"];
                match decision {
                    ApprovalDecision::Allow => {
                        assert_eq!(result["behavior"], "allow");
                        assert!(result.get("updatedInput").is_none());
                    }
                    ApprovalDecision::Deny => assert_eq!(result["behavior"], "deny"),
                    _ => unreachable!(),
                }
                reader
                    .get_mut()
                    .write_all(format!("{}\n", json!({"received":output["request_id"]})).as_bytes())
                    .await
                    .unwrap();
            });
            connector.resolve_request(resolve.clone()).await.unwrap();
            native.await.unwrap();
            connector.resolve_request(resolve.clone()).await.unwrap();
            let mut opposite = resolve;
            opposite.response = AgentSessionRequestResolution::Approval {
                decision: if decision == ApprovalDecision::Allow {
                    ApprovalDecision::Deny
                } else {
                    ApprovalDecision::Allow
                },
            };
            assert_eq!(
                connector.resolve_request(opposite).await.unwrap_err().code,
                AgentConnectorErrorCode::LeaseConflict
            );
            assert!(connector
                .read_session(&session)
                .await
                .unwrap()
                .pending_requests
                .is_empty());
        }
    }

    #[tokio::test]
    async fn terminal_resolution_closes_pwa_cards_and_hook_installation_preserves_settings() {
        let (_root, connector, session) = fixture().await;
        connector
            .enable_native_approvals(Path::new("/fixture/path with spaces/orchestral"))
            .await
            .unwrap();
        let settings: Value = serde_json::from_slice(
            &std::fs::read(connector.config.config_dir.join("settings.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(settings["env"]["FIXTURE_ENV"], "preserved");
        assert_eq!(settings["permissions"]["deny"][0], "Bash(secret)");
        assert_eq!(
            settings["hooks"]["PermissionRequest"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            settings["hooks"]["PermissionRequest"][0]["hooks"][0]["command"],
            "existing-hook"
        );
        assert_eq!(
            settings["hooks"]["PermissionRequest"][1]["hooks"][0]["args"][0],
            "claude-permission-hook"
        );
        let stream = connect(&connector, &session).await;
        let request = pending(&connector, &session).await;
        drop(stream);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !connector.native_approvals.pending(&session).is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let error = connector
            .resolve_request(ResolveAgentSessionRequest {
                session_id: session,
                request_id: request.request_id,
                response: AgentSessionRequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                },
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, AgentConnectorErrorCode::NotFound);
    }
}

#[cfg(unix)]
pub(crate) use unix::ApprovalBridge;

/// Runs Claude's native permission hook. If the bridge is unavailable, no
/// decision is emitted and Claude retains its normal permission UI.
pub async fn run_permission_hook(socket: &Path) {
    #[cfg(unix)]
    if unix::run_hook(socket).await.is_ok() {
        return;
    }
    #[cfg(not(unix))]
    let _ = socket;
    println!("{{}}");
}

impl ClaudeCodeConnector {
    /// Starts the private native approval bridge and installs the official
    /// PermissionRequest hook, preserving existing Claude settings and hooks.
    #[cfg(unix)]
    pub async fn enable_native_approvals(
        self: &Arc<Self>,
        executable: &Path,
    ) -> Result<(), AgentConnectorError> {
        let bridge = self
            .approval_bridge
            .get_or_try_init(|| ApprovalBridge::bind(self))
            .await?;
        self.install_permission_hook(executable, &bridge.path).await
    }

    #[cfg(not(unix))]
    pub async fn enable_native_approvals(
        self: &Arc<Self>,
        _executable: &Path,
    ) -> Result<(), AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "native approval bridge requires a Unix socket",
        ))
    }

    #[cfg(unix)]
    async fn install_permission_hook(
        &self,
        executable: &Path,
        socket: &Path,
    ) -> Result<(), AgentConnectorError> {
        let path = self.config.config_dir.join("settings.json");
        let original = match tokio::fs::read(&path).await {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(crate::io_error(error)),
        };
        let mut settings: Value = original
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(|error| {
                AgentConnectorError::invalid(format!("invalid Claude settings: {error}"))
            })?
            .unwrap_or_else(|| json!({}));
        let object = settings
            .as_object_mut()
            .ok_or_else(|| AgentConnectorError::invalid("Claude settings must be an object"))?;
        let hooks = object
            .entry("hooks")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| AgentConnectorError::invalid("Claude hooks must be an object"))?;
        let permissions = hooks
            .entry("PermissionRequest")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .ok_or_else(|| {
                AgentConnectorError::invalid("Claude PermissionRequest hooks must be an array")
            })?;
        let args = json!(["claude-permission-hook", "--socket", socket]);
        let handler = json!({"type":"command","command":executable,"args":args,"timeout":HOOK_TIMEOUT,"statusMessage":"等待 Orchestral PWA 审批"});
        let mut found = false;
        for matcher in permissions.iter_mut() {
            if let Some(handlers) = matcher.get_mut("hooks").and_then(Value::as_array_mut) {
                for existing in handlers {
                    if existing.get("args") == Some(&args) {
                        *existing = handler.clone();
                        found = true;
                    }
                }
            }
        }
        if !found {
            permissions.push(json!({"matcher":"*","hooks":[handler]}))
        }
        let bytes = serde_json::to_vec_pretty(&settings)
            .map_err(|error| AgentConnectorError::protocol(error.to_string()))?;
        if original.as_deref() == Some(bytes.as_slice()) {
            return Ok(());
        }
        tokio::fs::create_dir_all(&self.config.config_dir)
            .await
            .map_err(crate::io_error)?;
        let root = self
            .config
            .session_store_dir
            .parent()
            .ok_or_else(|| AgentConnectorError::invalid("missing Claude state root"))?
            .to_owned();
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let write_private = |path: &Path, bytes: &[u8]| -> Result<(), AgentConnectorError> {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .map_err(crate::io_error)?;
                file.write_all(bytes).map_err(crate::io_error)?;
                file.sync_all().map_err(crate::io_error)
            };
            if let Some(old) = &original {
                let backups = root.join("hook-backups");
                std::fs::create_dir_all(&backups).map_err(crate::io_error)?;
                write_private(
                    &backups.join(format!("{}.settings.json", uuid::Uuid::new_v4())),
                    old,
                )?;
            }
            if std::fs::read(&path).ok() != original {
                return Err(AgentConnectorError::new(
                    AgentConnectorErrorCode::Busy,
                    "Claude settings changed during hook installation",
                    true,
                ));
            }
            let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            write_private(&temporary, &bytes)?;
            std::fs::rename(temporary, &path).map_err(crate::io_error)?;
            std::fs::File::open(path.parent().unwrap())
                .and_then(|directory| directory.sync_all())
                .map_err(crate::io_error)
        })
        .await
        .map_err(|error| AgentConnectorError::protocol(error.to_string()))?
    }
}
