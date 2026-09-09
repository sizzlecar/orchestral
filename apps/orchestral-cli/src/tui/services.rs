//! Application services invoked outside the terminal event loop.
use super::menu::{Choice, LocalAction, Menu, MenuKind};
use super::state::{UiEffect, UiPhase, UiState};
use crate::agent::{AgentHost, AgentRunOptions};
use anyhow::{bail, Context, Result};
use orchestral_core::agent_protocol::wire::AgentSessionId;
use orchestral_core::session_history::{SessionHistory, SessionHistoryQuery};
use orchestral_runtime::{AgentClient, AgentRunHandle};
use std::sync::Arc;

pub(crate) struct Tasks {
    sender: tokio::sync::mpsc::UnboundedSender<(u64, Result<Response, String>)>,
    task: Option<tokio::task::JoinHandle<()>>,
    generation: u64,
    cancellable: bool,
    pub cleanup: Vec<tokio::task::JoinHandle<()>>,
}

impl Tasks {
    pub(crate) fn new(
        sender: tokio::sync::mpsc::UnboundedSender<(u64, Result<Response, String>)>,
    ) -> Self {
        Self {
            sender,
            task: None,
            generation: 0,
            cancellable: false,
            cleanup: Vec::new(),
        }
    }

    /// Invalidates queued results too: closing a panel cannot reopen it later.
    pub(crate) fn cancel_read(&mut self, state: &mut UiState) -> bool {
        if !state.host_busy || !self.cancellable {
            return false;
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.generation = self.generation.wrapping_add(1);
        state.host_busy = false;
        state.ui_notice = None;
        true
    }

    pub(crate) fn accept(&mut self, generation: u64) -> bool {
        if generation != self.generation {
            return false;
        }
        self.task.take();
        true
    }

    pub(crate) async fn finish(&mut self) {
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        for task in self.cleanup.drain(..) {
            let _ = task.await;
        }
    }

    pub(crate) fn dispatch(
        &mut self,
        effect: &UiEffect,
        host: Arc<AgentHost>,
        options: AgentRunOptions,
        state: &mut UiState,
    ) -> bool {
        let (name, selection, action, return_to) = match effect {
            UiEffect::HostCommand { command } => (command.clone(), None, None, None),
            UiEffect::MenuChoice {
                kind: MenuKind::Commands,
                value,
            } => (value.clone(), None, None, None),
            UiEffect::MenuChoice { kind, value } => {
                (String::new(), Some((*kind, value.clone())), None, None)
            }
            UiEffect::LocalAction { action, return_to } => {
                (String::new(), None, Some(action.clone()), return_to.clone())
            }
            _ => return false,
        };
        self.cancel_read(state);
        let active = matches!(
            state.phase,
            UiPhase::Running
                | UiPhase::WaitingInput
                | UiPhase::WaitingApproval
                | UiPhase::Cancelling
        );
        if (matches!(name.as_str(), "/model" | "/new")
            || matches!(selection, Some((MenuKind::Models | MenuKind::Sessions, _))))
            && active
        {
            state.ui_notice = Some(
                "Finish or cancel the current request before switching model or session".to_owned(),
            );
            return true;
        }
        match &action {
            Some(LocalAction::SetTheme(value)) => {
                state.theme = value.clone();
                state.menu = return_to.and_then(|menu| menu.parent).map(|menu| *menu);
                return true;
            }
            Some(LocalAction::Keyboard) => {
                let mut menu = detail("Keyboard shortcuts · ↑↓ read · Esc back", HOTKEYS);
                menu.parent = return_to;
                state.menu = Some(menu);
                return true;
            }
            Some(LocalAction::Appearance) => {
                let mut menu = Menu::new(
                    MenuKind::Theme,
                    "Appearance · choose terminal colors",
                    ["terminal", "dark", "light"]
                        .into_iter()
                        .map(|name| {
                            Choice::action(
                                name,
                                LocalAction::SetTheme(name.to_owned()),
                                if state.theme == name {
                                    "Current appearance"
                                } else {
                                    "Apply immediately"
                                },
                            )
                        })
                        .collect(),
                );
                menu.parent = return_to;
                state.menu = Some(menu);
                return true;
            }
            _ => {}
        }
        match name.as_str() {
            "/help" => {
                state.menu = Some(help(state));
                return true;
            }
            "/quit" => {
                state.exit_requested = true;
                return true;
            }
            _ => {}
        }
        if state.host_busy {
            state.ui_notice = Some("A session operation is still running".to_owned());
            return true;
        }
        state.host_busy = true;
        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;
        let changes_host_state = name == "/new"
            || matches!(selection, Some((MenuKind::Models | MenuKind::Sessions, _)))
            || matches!(action, Some(LocalAction::SetSkillEnabled { .. }))
            || (name.starts_with("/skills ") && name.trim() != "/skills list");
        self.cancellable = !changes_host_state;
        state.ui_notice = Some("Loading… (input remains editable)".to_owned());
        let tx = self.sender.clone();
        let session = state.session_id.clone();
        let current_run = state.run_id.clone();
        let copy = state
            .transcript
            .iter()
            .rev()
            .find(|entry| entry.role == super::state::TranscriptRole::Assistant)
            .map(|entry| entry.text.clone());
        self.task = Some(tokio::spawn(async move {
            let response = match action {
                Some(action) => local_action(&host, &session, action)
                    .await
                    .and_then(|mut menu| {
                        menu.parent = return_to
                            .map(|mut parent| -> Result<_> {
                                if parent.kind == MenuKind::Skills {
                                    // Refresh state after a toggle while preserving the list's filter.
                                    parent.choices =
                                        super::skills::list(&host.skill_manager)?.choices;
                                }
                                Ok(parent)
                            })
                            .transpose()?;
                        Ok(Response::Menu(menu))
                    }),
                None => match selection {
                    Some((kind, value)) => choose(host, options, kind, value).await,
                    None => command(host, options, session, name, copy, current_run).await,
                },
            };
            if let Err(error) =
                tx.send((generation, response.map_err(|error| format!("{error:#}"))))
            {
                if let Ok(Response::Model { host, .. }) = error.0 .1 {
                    host.shutdown().await;
                }
            }
        }));
        true
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            // Let model construction finish so its abandoned result can shut down MCP.
            if !self.cancellable {
                return;
            }
            task.abort();
        }
    }
}

const HOTKEYS: &str = "Enter       send / steer / answer\nCtrl+J      newline (Shift+Enter where supported)\n↑ / ↓       edit lines, then session input history\nCtrl+A/E    line start / end\nAlt+B/F     previous / next word\nCtrl+W/U/K  delete word / to line start / end\nPgUp/PgDn   read history or the focused panel\nEnd         follow new output\nCtrl+O      expand / collapse tool blocks\nCtrl+P      expand / collapse long input\nTab         complete a command or file path\nF1 / Ctrl+] commands (also while answering a question)\nEsc/Ctrl+C  close focused panel; otherwise interrupt work\nCtrl+C      clear an idle draft; empty input shows exit help\nCtrl+D      exit when idle and input is empty\na / d       explicitly allow / deny an approval\n↑↓ + Enter  select and confirm an approval\n\n/ opens commands; // sends a literal leading slash.\n@ opens workspace paths; selecting adds a path reference.\nInput at a question answers that question, including / text.\nMenus preserve the underlying draft. Cancellation keeps drafts.\nSession drafts persist only while this TUI process is open.\nAfter a question, edit or move the cursor before sending its restored draft.\nFile candidates refresh in the background while the file menu is open.";

pub(crate) fn help(state: &UiState) -> Menu {
    let mut choices = super::menu::commands(state);
    choices.extend([
        Choice::action(
            "Keyboard shortcuts",
            LocalAction::Keyboard,
            "Editing, navigation and task controls",
        ),
        Choice::action(
            "Appearance",
            LocalAction::Appearance,
            "Use terminal colors or a built-in light or dark background",
        ),
    ]);
    Menu::new(
        MenuKind::Commands,
        "Help · Esc to return · type to search",
        choices,
    )
}

pub(crate) enum Response {
    Menu(Menu),
    Model {
        host: Arc<AgentHost>,
        options: AgentRunOptions,
    },
    Session {
        client: AgentClient,
        history: Option<SessionHistory>,
        run: Option<AgentRunHandle>,
    },
    Notice(String),
}

pub(crate) fn detail(title: impl Into<String>, text: impl Into<String>) -> Menu {
    let mut menu = Menu::new(MenuKind::Detail, title, Vec::new());
    menu.detail = Some(text.into());
    menu
}

async fn local_action(host: &AgentHost, session: &str, action: LocalAction) -> Result<Menu> {
    match action {
        LocalAction::SkillDetails(name) => {
            super::skills::details(&host.skill_manager, &host.workspace_root, &name)
        }
        LocalAction::SetSkillEnabled { name, enabled } => {
            anyhow::ensure!(
                host.skill_manager.globally_enabled(),
                "Skills are disabled by process configuration"
            );
            host.skill_manager.set_enabled(&name, enabled)?;
            super::skills::details(&host.skill_manager, &host.workspace_root, &name)
        }
        LocalAction::SessionDetails => {
            let history = host
                .session_history
                .read(&AgentSessionId::new(session))
                .await?;
            Ok(detail(
                "Session details · ↑↓ read · Esc back",
                format!(
                    "Session: {session}\nStorage: {}\nWorkspace: {}\nModel: {} / {}\n\n{}",
                    host.metadata.journal_location,
                    host.workspace_root.display(),
                    host.backend_name,
                    host.model,
                    super::insights::usage(history.as_ref())
                ),
            ))
        }
        _ => bail!("This action must be handled in the terminal"),
    }
}

pub(crate) async fn command(
    host: Arc<AgentHost>,
    options: AgentRunOptions,
    session: String,
    name: String,
    copy: Option<String>,
    current_run: Option<String>,
) -> Result<Response> {
    match name.as_str() {
        "/model" => {
            let choices = host
                .metadata
                .models
                .iter()
                .map(|model| {
                    Choice::new(
                        &model.name,
                        &model.name,
                        format!(
                            "{} / {}{}",
                            model.backend,
                            model.model,
                            if host.backend_name == model.backend && host.model == model.model {
                                " · current"
                            } else {
                                ""
                            }
                        ),
                    )
                })
                .collect();
            Ok(Response::Menu(Menu::new(
                MenuKind::Models,
                "Models · configured profiles",
                choices,
            )))
        }
        "/resume" => {
            let sessions = host
                .session_history
                .list(&SessionHistoryQuery {
                    workspace: Some(host.workspace_root.display().to_string()),
                    search: None,
                })
                .await?;
            let mut choices = sessions
                .into_iter()
                .map(|summary| {
                    Choice::new(
                        summary.title,
                        summary.session_id.as_str(),
                        format!(
                            "{} · {:?} · {}",
                            summary.session_id,
                            summary.status,
                            relative_time(summary.updated_at_unix_ms)
                        ),
                    )
                })
                .collect::<Vec<_>>();
            choices.push(Choice::action(
                "Current session details",
                LocalAction::SessionDetails,
                "View storage location, identity and usage for this conversation",
            ));
            Ok(Response::Menu(Menu::new(
                MenuKind::Sessions,
                "Sessions · this workspace",
                choices,
            )))
        }
        "/new" => Ok(Response::Session {
            client: host.client(AgentSessionId::new(format!(
                "cli-session-{}",
                uuid::Uuid::new_v4()
            ))),
            history: None,
            run: None,
        }),
        "/context" => {
            let history = host
                .session_history
                .read(&AgentSessionId::new(&session))
                .await?;
            Ok(Response::Menu(detail(
                "Context · request snapshot · ↑↓ to read",
                format!(
                    "{}\n\nProject instructions and limits loaded for this process:\n{}",
                    super::insights::context(history.as_ref(), current_run.as_deref()),
                    host.metadata.context
                ),
            )))
        }
        "/skills" | "/skills list" => Ok(Response::Menu(super::skills::list(&host.skill_manager)?)),
        command if command.starts_with("/skills ") => {
            let message = host
                .skill_manager
                .execute_tui(command.trim_start_matches("/skills "))?;
            Ok(Response::Menu(detail("Skill preference", message)))
        }
        "/copy" => {
            let text = copy.context("No committed answer to copy")?;
            copy_text(text).await?;
            Ok(Response::Notice("Answer copied".to_owned()))
        }
        _ => {
            let _ = options;
            bail!("Unsupported command: {name}");
        }
    }
}

pub(crate) async fn choose(
    host: Arc<AgentHost>,
    mut options: AgentRunOptions,
    kind: MenuKind,
    value: String,
) -> Result<Response> {
    match kind {
        MenuKind::Models => {
            let profile = host
                .metadata
                .models
                .iter()
                .find(|model| model.name == value)
                .context("Model profile is no longer available")?;
            options.model_overrides.backend = Some(profile.backend.clone());
            options.model_overrides.model = Some(profile.model.clone());
            options.model_overrides.model_profile = Some(profile.name.clone());
            let next = host.reconfigure(&options).await?;
            Ok(Response::Model {
                host: Arc::new(next),
                options,
            })
        }
        MenuKind::Sessions => {
            let id = AgentSessionId::new(value);
            let history = host
                .session_history
                .read(&id)
                .await?
                .context("Session no longer exists")?;
            crate::local_sessions::validate_workspace(
                &history,
                &host.workspace_root.display().to_string(),
            )?;
            let client = host.client(id);
            let run = crate::agent::resume_unfinished(&client, Some(&history)).await?;
            let history = host.session_history.read(client.session_id()).await?;
            Ok(Response::Session {
                client,
                history,
                run,
            })
        }
        _ => bail!("Unsupported selection"),
    }
}

async fn copy_text(text: String) -> Result<()> {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;
    let text = super::text::plain(&text);
    let candidates: &[(&str, &[&str])] = if cfg!(target_os = "macos") {
        &[("pbcopy", &[])]
    } else {
        &[("wl-copy", &[]), ("xclip", &["-selection", "clipboard"])]
    };
    for (program, args) in candidates {
        let child = tokio::process::Command::new(program)
            .args(*args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn();
        let mut child = match child {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let operation = async {
            let mut stdin = child.stdin.take().context("Clipboard input unavailable")?;
            stdin
                .write_all(text.as_bytes())
                .await
                .context("Clipboard unavailable; select text in the terminal to copy it")?;
            drop(stdin);
            if !child.wait().await?.success() {
                bail!("Clipboard unavailable; select text in the terminal to copy it")
            }
            Ok(())
        };
        return tokio::time::timeout(std::time::Duration::from_secs(3), operation)
            .await
            .context("Clipboard timed out; select text in the terminal to copy it")?;
    }
    bail!("No clipboard utility available; select text in the terminal to copy it")
}

fn relative_time(unix_ms: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let minutes = now.saturating_sub(unix_ms.max(0) as u128) / 60_000;
    match minutes {
        0 => "just now".to_owned(),
        1..60 => format!("{minutes} min ago"),
        60..1440 => format!("{} h ago", minutes / 60),
        _ => format!("{} d ago", minutes / 1440),
    }
}
