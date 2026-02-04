pub mod app;
pub mod bottom_pane;
pub mod event_loop;
pub mod protocol;
pub mod ui;
pub mod update;
pub mod widgets;

use std::io::IsTerminal;
use std::path::PathBuf;

use anyhow::Context;
use tokio::sync::mpsc;

use crate::runtime::{RuntimeClient, RuntimeMsg, TransientSlot};

use app::App;

pub async fn run_session(
    config: PathBuf,
    thread_id: Option<String>,
    initial_input: Option<String>,
    once: bool,
    verbose: bool,
) -> anyhow::Result<()> {
    let runtime_client = RuntimeClient::from_config(config, thread_id)
        .await
        .context("initialize runtime client")?;

    if !std::io::stdout().is_terminal() {
        return run_plain(runtime_client, initial_input).await;
    }

    let mut app = App::new(0, 0, once);
    if verbose {
        app.history
            .push(format!("thread_id={}", runtime_client.thread_id()));
    }

    event_loop::run_tui(app, runtime_client, initial_input).await
}

async fn run_plain(
    runtime_client: RuntimeClient,
    initial_input: Option<String>,
) -> anyhow::Result<()> {
    let input = initial_input.ok_or_else(|| {
        anyhow::anyhow!("interactive terminal unavailable; provide input via `run <text>`")
    })?;
    let (tx, mut rx) = mpsc::channel::<RuntimeMsg>(256);
    let client = runtime_client.clone();
    let submit = tokio::spawn(async move { client.submit_input(input, tx).await });

    while let Some(msg) = rx.recv().await {
        match msg {
            RuntimeMsg::PlanningStart => println!("Planning..."),
            RuntimeMsg::PlanningEnd => println!("Executing..."),
            RuntimeMsg::ExecutionStart { total } => println!("Execution started (total={})", total),
            RuntimeMsg::ExecutionProgress { step } => println!("Progress step={}", step),
            RuntimeMsg::ExecutionEnd => println!("Execution finished"),
            RuntimeMsg::ActivityStart { .. }
            | RuntimeMsg::ActivityItem { .. }
            | RuntimeMsg::ActivityEnd { .. } => {}
            RuntimeMsg::OutputPersist(line) => println!("{}", line),
            RuntimeMsg::OutputTransient { slot, text } => {
                if matches!(slot, TransientSlot::Status) {
                    println!("{}", text);
                }
            }
            RuntimeMsg::Error(err) => eprintln!("Error: {}", err),
        }
    }

    submit.await.context("submit task join failed")??;
    Ok(())
}
