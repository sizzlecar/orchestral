use std::env;
use std::fs;
use std::path::Path;

use serde_json::json;

fn main() {
    match run() {
        Ok((report, code)) | Err((report, code)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).unwrap_or_else(|_| {
                    "{\"ok\":false,\"error\":\"failed to serialize verification report\"}"
                        .to_string()
                })
            );
            if code != 0 {
                std::process::exit(code);
            }
        }
    }
}

fn run() -> Result<(serde_json::Value, i32), (serde_json::Value, i32)> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        return Err((
            json!({
                "ok": false,
                "error": "usage: verify_structured_patch <path>",
            }),
            2,
        ));
    }

    let path = Path::new(&args[1]);
    let raw = fs::read_to_string(path).map_err(|err| {
        (
            json!({
                "ok": false,
                "path": path.display().to_string(),
                "error": format!("read failed: {}", err),
            }),
            1,
        )
    })?;
    let data: serde_json::Value = serde_json::from_str(&raw).map_err(|err| {
        (
            json!({
                "ok": false,
                "path": path.display().to_string(),
                "error": format!("parse json failed: {}", err),
            }),
            1,
        )
    })?;

    let mut failures = Vec::new();
    if data.pointer("/service/enabled") != Some(&json!(true)) {
        failures.push("service.enabled was not set to true".to_string());
    }
    if data.pointer("/service/port") != Some(&json!(9090)) {
        failures.push("service.port was not set to 9090".to_string());
    }
    if data.pointer("/owner") != Some(&json!("platform-team")) {
        failures.push("owner was not updated to platform-team".to_string());
    }
    if data.pointer("/features/audit") != Some(&json!(true)) {
        failures.push("features.audit was not set to true".to_string());
    }
    if data.pointer("/obsolete").is_some() {
        failures.push("obsolete key still exists".to_string());
    }

    let report = json!({
        "ok": failures.is_empty(),
        "path": path.display().to_string(),
        "service": data.get("service").cloned().unwrap_or(serde_json::Value::Null),
        "owner": data.get("owner").cloned().unwrap_or(serde_json::Value::Null),
        "features": data.get("features").cloned().unwrap_or(serde_json::Value::Null),
        "failures": failures,
    });
    if report
        .get("ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        Ok((report, 0))
    } else {
        Err((report, 1))
    }
}
