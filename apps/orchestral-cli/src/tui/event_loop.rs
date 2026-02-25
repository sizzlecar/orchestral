use std::io;

use anyhow::Context;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures_util::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

use crate::runtime::{RuntimeClient, RuntimeMsg};

use super::app::App;
use super::protocol::UiMsg;
use super::ui::draw;
use super::update::update;

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TerminalGuard {
    fn enter() -> anyhow::Result<Self> {
        enable_raw_mode().context("enable raw mode")?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture).context("enter alt screen")?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).context("create terminal")?;
        terminal.clear().context("clear terminal")?;
        Ok(Self { terminal })
    }

    fn terminal_mut(&mut self) -> &mut Terminal<CrosstermBackend<io::Stdout>> {
        &mut self.terminal
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableMouseCapture,
            LeaveAlternateScreen
        );
        let _ = self.terminal.show_cursor();
    }
}

pub async fn run_tui(
    mut app: App,
    runtime_client: RuntimeClient,
    initial_input: Option<String>,
) -> anyhow::Result<()> {
    let mut term = TerminalGuard::enter()?;

    let (ui_tx, mut ui_rx) = mpsc::channel::<UiMsg>(1024);
    let (runtime_tx, mut runtime_rx) = mpsc::channel::<RuntimeMsg>(1024);

    // Terminal key/resize events.
    {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            let mut events = EventStream::new();
            while let Some(Ok(ev)) = events.next().await {
                match ev {
                    Event::Key(key) => {
                        if matches!(key.code, KeyCode::Enter) {
                            tracing::debug!(
                                modifiers = ?key.modifiers,
                                "submit_chain: event_loop received Enter key"
                            );
                        }
                        if ui_tx.send(UiMsg::Key(key)).await.is_err() {
                            break;
                        }
                    }
                    Event::Paste(text) => {
                        tracing::debug!(
                            paste_len = text.len(),
                            "submit_chain: event_loop received paste"
                        );
                        if ui_tx.send(UiMsg::Paste(text)).await.is_err() {
                            break;
                        }
                    }
                    Event::Resize(w, h) => {
                        if ui_tx.send(UiMsg::Resize(w, h)).await.is_err() {
                            break;
                        }
                    }
                    Event::Mouse(mouse) => match mouse.kind {
                        MouseEventKind::ScrollUp => {
                            if ui_tx.send(UiMsg::ScrollUp).await.is_err() {
                                break;
                            }
                        }
                        MouseEventKind::ScrollDown => {
                            if ui_tx.send(UiMsg::ScrollDown).await.is_err() {
                                break;
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        });
    }

    // UI tick.
    {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(16));
            loop {
                ticker.tick().await;
                if ui_tx.send(UiMsg::UiTick).await.is_err() {
                    break;
                }
            }
        });
    }

    // Animation tick.
    {
        let ui_tx = ui_tx.clone();
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(80));
            loop {
                ticker.tick().await;
                if ui_tx.send(UiMsg::AnimTick).await.is_err() {
                    break;
                }
            }
        });
    }

    let size = term.terminal_mut().size().context("read terminal size")?;
    app.width = size.width;
    app.height = size.height;
    app.set_dirty();

    if let Some(input) = initial_input {
        tracing::debug!(
            input_len = input.len(),
            input_preview = %log_preview(&input, 80),
            "submit_chain: enqueue initial input"
        );
        let _ = ui_tx.send(UiMsg::SubmitInput(input)).await;
    }

    loop {
        let msg = tokio::select! {
            biased;
            msg = runtime_rx.recv() => {
                match msg {
                    Some(rt_msg) => {
                        tracing::debug!(msg_type = %runtime_msg_label(&rt_msg), "event_loop: runtime_rx polled");
                        UiMsg::Runtime(rt_msg)
                    },
                    None => continue,
                }
            }
            msg = ui_rx.recv() => {
                match msg {
                    Some(ui_msg) => ui_msg,
                    None => break,
                }
            }
        };

        match &msg {
            UiMsg::SubmitInput(input) => tracing::debug!(
                turn_id = app.current_turn_id.saturating_add(1),
                input_len = input.len(),
                input_preview = %log_preview(input, 80),
                "submit_chain: event_loop received UiMsg::SubmitInput"
            ),
            UiMsg::Paste(text) => tracing::debug!(
                paste_len = text.len(),
                "submit_chain: event_loop received UiMsg::Paste"
            ),
            UiMsg::Key(key) if matches!(key.code, KeyCode::Enter) => {
                tracing::debug!("submit_chain: event_loop processing UiMsg::Key(Enter)")
            }
            _ => {}
        }

        update(&mut app, msg);

        if app.take_pending_interrupt() {
            let tx = runtime_tx.clone();
            let client = runtime_client.clone();
            tokio::spawn(async move {
                client.interrupt(tx).await;
            });
        }

        if let Some(enabled) = app.take_pending_mouse_capture_toggle() {
            if enabled {
                execute!(term.terminal_mut().backend_mut(), EnableMouseCapture)
                    .context("enable mouse capture")?;
            } else {
                execute!(term.terminal_mut().backend_mut(), DisableMouseCapture)
                    .context("disable mouse capture")?;
            }
        }

        if let Some(input) = app.take_pending_submit() {
            tracing::debug!(
                turn_id = app.current_turn_id,
                mode = ?app.mode,
                input_len = input.len(),
                input_preview = %log_preview(&input, 80),
                "submit_chain: pending submit dequeued"
            );
            let trimmed = input.trim();
            if trimmed == "/exit" || trimmed == "/quit" {
                app.should_quit = true;
            } else {
                let tx = runtime_tx.clone();
                let client = runtime_client.clone();
                tokio::spawn(async move {
                    tracing::debug!("submit_chain: submit task started");
                    let result = client.submit_input(input, tx).await;
                    tracing::debug!(
                        result_ok = result.is_ok(),
                        "submit_chain: submit task finished"
                    );
                    let _ = result;
                });
            }
        }

        if app.dirty {
            term.terminal_mut().draw(|frame| draw(frame, &app))?;
            app.dirty = false;
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

fn runtime_msg_label(msg: &crate::runtime::RuntimeMsg) -> &'static str {
    use crate::runtime::RuntimeMsg;
    match msg {
        RuntimeMsg::PlanningStart => "PlanningStart",
        RuntimeMsg::PlanningEnd => "PlanningEnd",
        RuntimeMsg::ExecutionStart { .. } => "ExecutionStart",
        RuntimeMsg::ExecutionProgress { .. } => "ExecutionProgress",
        RuntimeMsg::ExecutionEnd => "ExecutionEnd",
        RuntimeMsg::ActivityStart { .. } => "ActivityStart",
        RuntimeMsg::ActivityItem { .. } => "ActivityItem",
        RuntimeMsg::ActivityEnd { .. } => "ActivityEnd",
        RuntimeMsg::OutputPersist(_) => "OutputPersist",
        RuntimeMsg::AssistantDelta { .. } => "AssistantDelta",
        RuntimeMsg::OutputTransient { .. } => "OutputTransient",
        RuntimeMsg::ApprovalRequested { .. } => "ApprovalRequested",
        RuntimeMsg::Error(_) => "Error",
    }
}

fn log_preview(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}
