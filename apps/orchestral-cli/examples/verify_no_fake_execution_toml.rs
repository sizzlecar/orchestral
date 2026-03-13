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
                "error": "usage: verify_no_fake_execution_toml <path>",
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

    let data: toml::Value = toml::from_str(&raw).map_err(|err| {
        (
            json!({
                "ok": false,
                "path": path.display().to_string(),
                "error": format!("parse toml failed: {}", err),
            }),
            1,
        )
    })?;

    let mut failures = Vec::new();
    let port = data
        .get("server")
        .and_then(|value| value.get("port"))
        .and_then(toml::Value::as_integer);
    if port != Some(9090) {
        failures.push(format!("server.port expected 9090, got {:?}", port));
    }

    let report = json!({
        "ok": failures.is_empty(),
        "path": path.display().to_string(),
        "server_port": port,
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
