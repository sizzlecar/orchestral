use std::env;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

use serde_json::{json, Value};
use tokio::sync::watch;

use orchestral_runtime::{ExecutionResult, OrchestratorResult, RuntimeApp};
use orchestral_stores::Event;

#[derive(Debug)]
enum CliCommand {
    Run {
        config: PathBuf,
        thread_id: Option<String>,
        input: String,
    },
    Repl {
        config: PathBuf,
        thread_id: Option<String>,
    },
    EventEmit {
        config: PathBuf,
        thread_id: String,
        kind: String,
        payload: Value,
    },
    Help,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = match parse_command(env::args().skip(1).collect()) {
        Ok(cmd) => cmd,
        Err(err) => {
            eprintln!("Error: {}", err);
            print_usage();
            std::process::exit(2);
        }
    };

    match command {
        CliCommand::Run {
            config,
            thread_id,
            input,
        } => {
            let app = build_app(&config, thread_id.as_deref()).await?;
            let thread_id = app.orchestrator.thread_runtime.thread_id().await;
            println!("thread_id={}", thread_id);
            let event = Event::user_input(&thread_id, "cli", json!({ "message": input }));
            let result = handle_event_with_live_progress(&app, event).await?;
            print_result(&result);
        }
        CliCommand::Repl { config, thread_id } => {
            run_repl(&config, thread_id.as_deref()).await?;
        }
        CliCommand::EventEmit {
            config,
            thread_id,
            kind,
            payload,
        } => {
            let app = build_app(&config, Some(&thread_id)).await?;
            let event = Event::external(&thread_id, kind, payload);
            let result = handle_event_with_live_progress(&app, event).await?;
            print_result(&result);
        }
        CliCommand::Help => print_usage(),
    }

    Ok(())
}

async fn build_app(
    config: &PathBuf,
    thread_id: Option<&str>,
) -> Result<RuntimeApp, Box<dyn std::error::Error>> {
    let app = RuntimeApp::from_config_path(config).await?;
    if let Some(thread_id) = thread_id {
        let mut thread = app.orchestrator.thread_runtime.thread.write().await;
        thread.id = thread_id.to_string();
    }
    Ok(app)
}

async fn run_repl(
    config: &PathBuf,
    thread_id: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = build_app(config, thread_id).await?;
    let thread_id = app.orchestrator.thread_runtime.thread_id().await;
    println!("orchestral repl started (thread_id={})", thread_id);
    println!(
        "Type messages and press enter. Commands: /exit, /event <kind> <payload_json_or_text>"
    );

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    write!(stdout, "> ")?;
    stdout.flush()?;
    for line in stdin.lock().lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "/exit" || trimmed == "/quit" {
            break;
        }

        let event = if trimmed.starts_with("/event ") {
            match parse_repl_event_command(trimmed, &thread_id) {
                Ok(event) => event,
                Err(err) => {
                    eprintln!("Error: {}", err);
                    continue;
                }
            }
        } else {
            Event::user_input(&thread_id, "cli", json!({ "message": trimmed }))
        };

        let result = handle_event_with_live_progress(&app, event).await?;
        print_result(&result);
        write!(stdout, "> ")?;
        stdout.flush()?;
    }

    Ok(())
}

fn parse_repl_event_command(line: &str, thread_id: &str) -> Result<Event, String> {
    let mut parts = line.splitn(3, ' ');
    let _cmd = parts.next();
    let kind = parts
        .next()
        .ok_or_else(|| "Usage: /event <kind> <payload_json_or_text>".to_string())?;
    let raw_payload = parts
        .next()
        .ok_or_else(|| "Usage: /event <kind> <payload_json_or_text>".to_string())?;
    let payload = parse_payload(raw_payload);
    Ok(Event::external(thread_id, kind, payload))
}

fn parse_command(args: Vec<String>) -> Result<CliCommand, String> {
    if args.is_empty() {
        return Ok(CliCommand::Help);
    }

    match args[0].as_str() {
        "help" | "--help" | "-h" => Ok(CliCommand::Help),
        "run" => parse_run_command(&args[1..]),
        "repl" => parse_repl_command(&args[1..]),
        "event" => parse_event_command(&args[1..]),
        other => Err(format!("unknown command '{}'", other)),
    }
}

fn parse_run_command(args: &[String]) -> Result<CliCommand, String> {
    let mut config = PathBuf::from("config/orchestral.yaml");
    let mut thread_id: Option<String> = None;
    let mut input: Option<String> = None;
    let mut positional_input: Vec<String> = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--config requires a path".to_string())?;
                config = PathBuf::from(value);
            }
            "--thread-id" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--thread-id requires a value".to_string())?;
                thread_id = Some(value.clone());
            }
            "--input" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--input requires a value".to_string())?;
                input = Some(value.clone());
            }
            unknown if unknown.starts_with("--") => {
                return Err(format!("unknown option '{}'", unknown));
            }
            value => {
                positional_input.push(value.to_string());
            }
        }
        i += 1;
    }

    if input.is_none() && !positional_input.is_empty() {
        input = Some(positional_input.join(" "));
    }

    let input =
        input.ok_or_else(|| "run requires input text (positional or --input)".to_string())?;
    Ok(CliCommand::Run {
        config,
        thread_id,
        input,
    })
}

fn parse_repl_command(args: &[String]) -> Result<CliCommand, String> {
    let mut config = PathBuf::from("config/orchestral.yaml");
    let mut thread_id: Option<String> = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--config requires a path".to_string())?;
                config = PathBuf::from(value);
            }
            "--thread-id" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--thread-id requires a value".to_string())?;
                thread_id = Some(value.clone());
            }
            unknown => return Err(format!("unknown option '{}'", unknown)),
        }
        i += 1;
    }

    Ok(CliCommand::Repl { config, thread_id })
}

fn parse_event_command(args: &[String]) -> Result<CliCommand, String> {
    if args.first().map(String::as_str) != Some("emit") {
        return Err("usage: orchestral event emit --thread-id <id> --kind <kind> --payload <json_or_text> [--config <path>]".to_string());
    }

    let mut config = PathBuf::from("config/orchestral.yaml");
    let mut thread_id: Option<String> = None;
    let mut kind: Option<String> = None;
    let mut payload: Option<Value> = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--config requires a path".to_string())?;
                config = PathBuf::from(value);
            }
            "--thread-id" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--thread-id requires a value".to_string())?;
                thread_id = Some(value.clone());
            }
            "--kind" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--kind requires a value".to_string())?;
                kind = Some(value.clone());
            }
            "--payload" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--payload requires a value".to_string())?;
                payload = Some(parse_payload(value));
            }
            unknown => return Err(format!("unknown option '{}'", unknown)),
        }
        i += 1;
    }

    Ok(CliCommand::EventEmit {
        config,
        thread_id: thread_id.ok_or_else(|| "--thread-id is required".to_string())?,
        kind: kind.ok_or_else(|| "--kind is required".to_string())?,
        payload: payload.unwrap_or(Value::Null),
    })
}

fn parse_payload(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

async fn handle_event_with_live_progress(
    app: &RuntimeApp,
    event: Event,
) -> Result<OrchestratorResult, Box<dyn std::error::Error>> {
    let thread_id = app.orchestrator.thread_runtime.thread_id().await;
    let mut rx = app.orchestrator.thread_runtime.subscribe_events();
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let progress_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                event = rx.recv() => {
                    match event {
                        Ok(event) => {
                            if event.thread_id() != thread_id {
                                continue;
                            }
                            if let Some(line) = format_progress_event(&event) {
                                println!("{}", line);
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    });

    let result = app.orchestrator.handle_event(event).await;
    let _ = stop_tx.send(true);
    let _ = progress_task.await;

    result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
}

fn format_progress_event(event: &Event) -> Option<String> {
    let Event::SystemTrace { payload, .. } = event else {
        return None;
    };

    if payload.get("category").and_then(|v| v.as_str()) != Some("execution_progress") {
        return None;
    }

    let phase = payload
        .get("phase")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown_phase");
    let task_id = payload
        .get("task_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown_task");
    let step = payload
        .get("step_id")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let action = payload
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let message = payload.get("message").and_then(|v| v.as_str());

    let mut line = format!(
        "progress task_id={} step_id={} action={} phase={}",
        task_id, step, action, phase
    );
    if let Some(message) = message {
        line.push_str(&format!(" message={}", message));
    }
    Some(line)
}

fn print_result(result: &OrchestratorResult) {
    match result {
        OrchestratorResult::Started {
            interaction_id,
            task_id,
            result,
        } => {
            println!(
                "started interaction_id={} task_id={}",
                interaction_id, task_id
            );
            println!("{}", format_execution_result(result));
        }
        OrchestratorResult::Merged {
            interaction_id,
            task_id,
            result,
        } => {
            println!(
                "merged interaction_id={} task_id={}",
                interaction_id, task_id
            );
            println!("{}", format_execution_result(result));
        }
        OrchestratorResult::Rejected { reason } => {
            println!("rejected: {}", reason);
        }
        OrchestratorResult::Queued => {
            println!("queued");
        }
    }
}

fn format_execution_result(result: &ExecutionResult) -> String {
    match result {
        ExecutionResult::Completed => "execution=completed".to_string(),
        ExecutionResult::Failed { step_id, error } => {
            format!("execution=failed step_id={} error={}", step_id, error)
        }
        ExecutionResult::WaitingUser { step_id, prompt } => {
            format!(
                "execution=waiting_user step_id={} prompt={}",
                step_id, prompt
            )
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => {
            format!(
                "execution=waiting_event step_id={} event_type={}",
                step_id, event_type
            )
        }
    }
}

fn print_usage() {
    println!("Orchestral CLI");
    println!();
    println!("Usage:");
    println!("  orchestral run [--config <path>] [--thread-id <id>] [--input <text>|<text>]");
    println!("  orchestral repl [--config <path>] [--thread-id <id>]");
    println!("  orchestral event emit --thread-id <id> --kind <kind> --payload <json_or_text> [--config <path>]");
}
