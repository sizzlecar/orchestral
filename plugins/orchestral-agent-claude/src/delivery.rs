//! Native evidence that a peer message entered the main conversation. Initial
//! acceptance does not emit a peer receipt; idle and busy owners journal it in
//! different record types. Socket writes alone do not establish delivery.
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use orchestral_core::agent_connector::AgentConnectorError;
use serde_json::Value;

const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

pub(crate) struct JournalConfirmation {
    config: PathBuf,
    session: String,
    offsets: BTreeMap<PathBuf, u64>,
    delivered: BTreeSet<String>,
}

impl JournalConfirmation {
    pub(crate) fn new(config: &Path, session: &str) -> Result<Self, AgentConnectorError> {
        crate::validate_session_id(session)?;
        Ok(Self {
            config: config.to_owned(),
            session: session.to_owned(),
            offsets: BTreeMap::new(),
            delivered: BTreeSet::new(),
        })
    }

    /// Read each committed record once while waiting, retrying an incomplete
    /// tail after the native writer finishes it. Also discover new transcripts.
    pub(crate) fn poll(&mut self) -> Result<(), AgentConnectorError> {
        let filename = format!("{}.jsonl", self.session);
        for project in crate::entries(&self.config.join("projects"))? {
            let path = project.join(&filename);
            if project.is_dir() && path.is_file() {
                self.offsets.entry(path).or_default();
            }
        }
        for (path, offset) in &mut self.offsets {
            let file = match std::fs::File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(crate::io_error(error)),
            };
            if file.metadata().map_err(crate::io_error)?.len() < *offset {
                *offset = 0;
            }
            let mut reader = BufReader::new(file);
            reader
                .seek(SeekFrom::Start(*offset))
                .map_err(crate::io_error)?;
            loop {
                let mut line = Vec::new();
                reader
                    .by_ref()
                    .take((MAX_RECORD_BYTES + 1) as u64)
                    .read_until(b'\n', &mut line)
                    .map_err(crate::io_error)?;
                if line.len() > MAX_RECORD_BYTES {
                    return Err(AgentConnectorError::protocol(
                        "Claude delivery record is too large",
                    ));
                }
                if line.last() != Some(&b'\n') {
                    break;
                }
                let record: Value = serde_json::from_slice(&line).map_err(|error| {
                    AgentConnectorError::protocol(format!(
                        "invalid Claude delivery record: {error}"
                    ))
                })?;
                *offset += line.len() as u64;
                if let Some(id) = accepted_id(&record, &self.session) {
                    self.delivered.insert(id.to_owned());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn contains(&self, submission_id: &str) -> bool {
        self.delivered.contains(submission_id)
    }
}

fn accepted_id<'a>(record: &'a Value, session: &str) -> Option<&'a str> {
    if record.get("sessionId").and_then(Value::as_str) != Some(session)
        || record.get("isSidechain").and_then(Value::as_bool) == Some(true)
    {
        return None;
    }
    let (id, origin) = match record.get("type").and_then(Value::as_str)? {
        "user" if record.pointer("/message/role").and_then(Value::as_str) == Some("user") => (
            record.get("uuid"),
            record.get("origin").or_else(|| record.get("inputOrigin")),
        ),
        "attachment"
            if record.pointer("/attachment/type").and_then(Value::as_str)
                == Some("queued_command") =>
        {
            (
                record.pointer("/attachment/source_uuid"),
                record.pointer("/attachment/origin"),
            )
        }
        _ => return None,
    };
    if origin?.get("kind").and_then(Value::as_str) != Some("peer") {
        return None;
    }
    id?.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn confirmation_requires_committed_session_bound_peer_input_and_tracks_appends() {
        let root = tempfile::tempdir().unwrap();
        let session = uuid::Uuid::new_v4().to_string();
        let project = root.path().join("projects/project");
        std::fs::create_dir_all(&project).unwrap();
        let path = project.join(format!("{session}.jsonl"));
        let user = json!({"type":"user","sessionId":session,"uuid":"idle-message","origin":{"kind":"peer"},"message":{"role":"user"}});
        let queued = json!({"type":"attachment","sessionId":session,"uuid":"attachment-id","attachment":{"type":"queued_command","source_uuid":"busy-message","origin":{"kind":"peer"}}});
        let mut records = Vec::new();
        for (field, value) in [
            ("sessionId", json!("another-session")),
            ("isSidechain", json!(true)),
            ("origin", json!({"kind":"user"})),
            ("type", json!("assistant")),
            ("message", json!({"role":"assistant"})),
        ] {
            let mut invalid = user.clone();
            invalid[field] = value;
            invalid["uuid"] = json!("unrelated");
            records.push(invalid.to_string());
        }
        records.push(json!({"type":"queue-operation","sessionId":session,"operation":"enqueue","commandUuid":"unrelated"}).to_string());
        records.push(user.to_string());
        std::fs::write(&path, format!("{}\n{}", records.join("\n"), queued)).unwrap();
        let mut confirmation = JournalConfirmation::new(root.path(), &session).unwrap();
        confirmation.poll().unwrap();
        assert!(confirmation.contains("idle-message"));
        assert!(!confirmation.contains("busy-message"));
        assert!(!confirmation.contains("unrelated"));
        assert!(!confirmation.contains("attachment-id"));
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"\n")
            .unwrap();
        confirmation.poll().unwrap();
        assert!(confirmation.contains("busy-message"));
        confirmation.poll().unwrap();
        assert_eq!(confirmation.delivered.len(), 2);
    }
}
