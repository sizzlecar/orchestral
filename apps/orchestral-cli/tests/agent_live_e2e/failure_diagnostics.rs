//! Bounded metadata-only evidence for a failed isolated approval fixture.
use std::fs;
use std::io::Read;
use std::path::Path;

use serde_json::{json, Map, Value};

const FILE_BYTES: u64 = 1024 * 1024;
const FILES: usize = 64;
const RECORD_TAIL: usize = 16;

pub(super) fn read(workspace: &Path) -> Value {
    if !fs::symlink_metadata(workspace.join(".orchestral")).is_ok_and(|metadata| metadata.is_dir())
    {
        return json!({"omitted":"state_directory_missing_or_not_regular"});
    }
    let directory = workspace.join(".orchestral/agent-journal");
    if !fs::symlink_metadata(&directory).is_ok_and(|metadata| metadata.is_dir()) {
        return json!({"omitted":"journal_directory_missing_or_not_regular"});
    }
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            return json!({"omitted":"directory_read_error", "kind":format!("{:?}", error.kind())})
        }
    };
    let mut files = Vec::new();
    let mut entry_errors = 0;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                entry_errors += 1;
                continue;
            }
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.ends_with(".json")
            && ["run-", "session-", "effect-"]
                .iter()
                .any(|prefix| name.starts_with(prefix))
        {
            files.push(entry.path());
            if files.len() > FILES {
                break;
            }
        }
    }
    files.sort();
    json!({
        "files":files.iter().take(FILES).map(|file| json!({
            "file":file.file_name().unwrap_or_default().to_string_lossy(),
            "metadata":read_file(file),
        })).collect::<Vec<_>>(),
        "more_files_omitted":files.len() > FILES,
        "entry_errors":entry_errors,
    })
}

fn read_file(path: &Path) -> Value {
    if !fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        return json!({"omitted":"not_regular_file"});
    }
    let bytes = (|| -> std::io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        fs::File::open(path)?
            .take(FILE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        Ok(bytes)
    })();
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(error) => return json!({"omitted":"read_error", "kind":format!("{:?}", error.kind())}),
    };
    if bytes.len() as u64 > FILE_BYTES {
        return json!({"omitted":"file_byte_limit"});
    }
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => return json!({"omitted":"invalid_json"}),
    };
    let records = value
        .pointer("/run/records")
        .or_else(|| value.get("records"))
        .unwrap_or(&value);
    let Some(records) = records.as_array() else {
        return json!({"omitted":"invalid_record_shape"});
    };
    let omitted = records.len().saturating_sub(RECORD_TAIL);
    json!({"earlier_records_omitted":omitted, "records":records[omitted..].iter().map(project).collect::<Vec<_>>()})
}

fn fields(value: &Value, paths: &[(&str, &str)]) -> Map<String, Value> {
    let mut result = Map::new();
    for &(name, path) in paths {
        match value.pointer(path) {
            Some(Value::String(text)) if text.len() > 256 => {
                result.insert(name.into(), json!({"omitted":"scalar_byte_limit"}));
            }
            Some(scalar @ (Value::String(_) | Value::Bool(_) | Value::Number(_))) => {
                result.insert(name.into(), scalar.clone());
            }
            _ => {}
        }
    }
    result
}

fn project(record: &Value) -> Value {
    let envelope = record.get("event").unwrap_or(record);
    let mut result = fields(
        envelope,
        &[
            ("event_id", "/event_id"),
            ("run_id", "/run_id"),
            ("session_id", "/session_id"),
            ("run_seq", "/run_seq"),
            ("session_seq", "/session_seq"),
            ("effect_seq", "/effect_seq"),
        ],
    );
    result.extend(fields(
        &record["key"],
        &[("run_id", "/run_id"), ("call_id", "/call_id")],
    ));
    let payload = &envelope["payload"];
    result.extend(fields(
        payload,
        &[
            ("type", "/type"),
            ("request_id", "/request_id"),
            ("opened_request_id", "/request/request_id"),
            ("request_type", "/request/payload/type"),
            ("resolution_type", "/resolution/type"),
            ("decision", "/resolution/decision"),
            ("command_id", "/command_id"),
            ("received_command_id", "/command/command_id"),
            ("command_type", "/command/payload/type"),
            ("command_request_id", "/command/request_id"),
            ("command_outcome", "/outcome/outcome"),
            ("tool_id", "/effect/invocation/tool_id"),
            ("attempt_id", "/attempt_id"),
            ("authorization_kind", "/authorization/kind"),
            ("outcome_status", "/outcome/status"),
        ],
    ));
    if let Some(content) = payload.pointer("/tool/content").and_then(Value::as_array) {
        let tools = content
            .iter()
            .filter(|item| item["type"] == "tool_result")
            .collect::<Vec<_>>();
        result.insert(
            "tool_results_omitted".into(),
            json!(tools.len().saturating_sub(4)),
        );
        result.insert(
            "tool_results".into(),
            Value::Array(
                tools
                    .iter()
                    .take(4)
                    .map(|item| {
                        let mut tool =
                            fields(item, &[("call_id", "/call_id"), ("is_error", "/is_error")]);
                        tool.extend(fields(
                            &item["result"],
                            &[
                                ("alive", "/alive"),
                                ("session_id", "/session_id"),
                                ("exit_code", "/exit_code"),
                            ],
                        ));
                        for channel in ["stdout", "stderr", "output"] {
                            if let Some(text) = item["result"][channel].as_str() {
                                tool.insert(format!("{channel}_bytes"), json!(text.len()));
                            }
                        }
                        Value::Object(tool)
                    })
                    .collect(),
            ),
        );
    }
    Value::Object(result)
}

#[test]
fn projection_keeps_committed_phases_and_ids_without_content_or_authorization_secrets() {
    let secret = "PRIVATE-content-args-config-grant";
    let run = project(
        &json!({"event":{"run_id":"run-a","event_id":"event-a","run_seq":3,"payload":{
            "type":"request_resolved","request_id":"approval-a","resolution":{"type":"approval","decision":"allow","grant_ref":secret},"content":secret
        }}}),
    );
    assert_eq!(run["decision"], "allow");
    assert_eq!(run["request_id"], "approval-a");
    let effect = project(
        &json!({"key":{"run_id":"run-a","call_id":"call-a"},"effect_seq":2,"payload":{
            "type":"invoked","attempt_id":"attempt-a","authorization":{"kind":"approval","nonce":secret},"args":secret
        }}),
    );
    assert_eq!(effect["type"], "invoked");
    assert_eq!(effect["call_id"], "call-a");
    let session = project(
        &json!({"session_id":"session-a","run_id":"run-a","session_seq":2,"payload":{
            "type":"tool_exchange_committed","request_id":"model-a","assistant":{"content":secret},"tool":{"content":[{
                "type":"tool_result","call_id":"call-a","is_error":false,"result":{"alive":true,"session_id":1,"stdout":secret,"args":secret}
            }]}
        }}),
    );
    assert_eq!(session["tool_results"][0]["alive"], true);
    assert_eq!(session["tool_results"][0]["stdout_bytes"], secret.len());
    assert!(!json!([run, effect, session]).to_string().contains(secret));
}

#[test]
fn corrupt_or_oversized_journals_are_omitted_without_masking_the_original_failure() {
    let workspace = super::TestWorkspace::new("failure-diagnostic");
    let directory = workspace.path(".orchestral/agent-journal");
    fs::create_dir_all(&directory).unwrap();
    let corrupt = directory.join("run-corrupt.json");
    fs::write(&corrupt, b"PRIVATE-invalid-json").unwrap();
    assert_eq!(read_file(&corrupt)["omitted"], "invalid_json");
    let large = directory.join("effect-large.json");
    fs::File::create(&large)
        .unwrap()
        .set_len(FILE_BYTES + 1)
        .unwrap();
    assert_eq!(read_file(&large)["omitted"], "file_byte_limit");
    let session = directory.join("session-tail.json");
    fs::write(
        &session,
        serde_json::to_vec(&vec![
            json!({"payload":{"type":"run_input_committed","message":"PRIVATE-input"}});
            RECORD_TAIL + 1
        ])
        .unwrap(),
    )
    .unwrap();
    assert_eq!(read_file(&session)["earlier_records_omitted"], 1);
    fs::write(
        directory.join("generic-checkpoint-private.json"),
        b"PRIVATE-checkpoint",
    )
    .unwrap();
    let report = read(&workspace.root).to_string();
    assert!(!report.contains("PRIVATE"));
    assert!(!report.contains("generic-checkpoint"));
}
