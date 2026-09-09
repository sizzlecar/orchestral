//! Shared command discovery and searchable choices; no Host side effects.
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MenuKind {
    Commands,
    Files,
    Models,
    Sessions,
    Skills,
    Theme,
    Detail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalAction {
    Keyboard,
    Appearance,
    SessionDetails,
    SkillDetails(String),
    SetSkillEnabled { name: String, enabled: bool },
    SetTheme(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Choice {
    pub label: String,
    pub value: String,
    pub description: String,
    pub action: Option<LocalAction>,
}

impl Choice {
    pub(crate) fn new(
        label: impl Into<String>,
        value: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
            description: description.into(),
            action: None,
        }
    }

    pub(crate) fn action(
        label: impl Into<String>,
        action: LocalAction,
        description: impl Into<String>,
    ) -> Self {
        let mut choice = Self::new(label, "", description);
        choice.action = Some(action);
        choice
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Menu {
    pub kind: MenuKind,
    pub title: String,
    pub query: String,
    pub choices: Arc<Vec<Choice>>,
    pub selected: usize,
    pub detail: Option<String>,
    pub parent: Option<Box<Menu>>,
    pub toggle: Option<LocalAction>,
}

impl Menu {
    pub(crate) fn new(kind: MenuKind, title: impl Into<String>, choices: Vec<Choice>) -> Self {
        Self {
            kind,
            title: title.into(),
            query: String::new(),
            choices: Arc::new(choices),
            selected: 0,
            detail: None,
            parent: None,
            toggle: None,
        }
    }
    pub(crate) fn filtered(&self) -> Vec<&Choice> {
        let query = self.query.to_lowercase();
        if query.is_empty() {
            return self.choices.iter().collect();
        }
        let mut choices = self
            .choices
            .iter()
            .filter_map(|choice| {
                score(&choice.label.to_lowercase(), &query).map(|score| (score, choice))
            })
            .collect::<Vec<_>>();
        choices.sort_by(|(a, x), (b, y)| a.cmp(b).then_with(|| x.label.cmp(&y.label)));
        choices.into_iter().map(|(_, choice)| choice).collect()
    }
    pub(crate) fn selected(&self) -> Option<&Choice> {
        self.filtered().get(self.selected).copied()
    }
}

fn score(value: &str, query: &str) -> Option<usize> {
    if query.is_empty() || value.starts_with(query) {
        return Some(0);
    }
    if let Some(i) = value.find(query) {
        return Some(1 + i);
    }
    let mut remaining = value.char_indices();
    let mut end = 0;
    for c in query.chars() {
        end = remaining.find(|(_, v)| *v == c)?.0;
    }
    Some(value.len() + end)
}

pub(crate) const COMMANDS: &[(&str, &str, &str)] = &[
    (
        "/new",
        "New conversation",
        "Start a separate task; keep this conversation in history",
    ),
    (
        "/resume",
        "Continue a conversation",
        "Find conversations in this workspace; inspect session details",
    ),
    (
        "/model",
        "Change model",
        "Select a configured model for the next request",
    ),
    (
        "/copy",
        "Copy last answer",
        "Copy the complete last answer without terminal formatting",
    ),
    (
        "/skills",
        "Browse project skills",
        "Search discovered skills and manage enablement; browsing does not load a skill",
    ),
    (
        "/context",
        "Inspect task context",
        "Inspect project instructions and skill loads for the current or latest request",
    ),
    (
        "/help",
        "Help and settings",
        "Find actions, keyboard shortcuts and appearance settings",
    ),
    (
        "/quit",
        "Quit Orchestral",
        "Stop current work and exit; retain the conversation",
    ),
];

pub(crate) fn commands(state: &super::state::UiState) -> Vec<Choice> {
    COMMANDS
        .iter()
        .map(|(name, label, description)| {
            let unavailable = match *name {
                "/model" | "/new" | "/resume" if state.host_busy => {
                    Some("session operation running")
                }
                "/model" | "/new"
                    if matches!(
                        state.phase,
                        super::state::UiPhase::Running
                            | super::state::UiPhase::WaitingInput
                            | super::state::UiPhase::WaitingApproval
                            | super::state::UiPhase::Cancelling
                    ) =>
                {
                    Some("finish or cancel current request first")
                }
                "/copy"
                    if !state
                        .transcript
                        .iter()
                        .any(|e| e.role == super::state::TranscriptRole::Assistant) =>
                {
                    Some("no committed answer yet")
                }
                _ => None,
            };
            Choice::new(
                format!("{name}  {label}"),
                *name,
                unavailable.map_or_else(
                    || description.to_string(),
                    |reason| format!("Unavailable: {reason}"),
                ),
            )
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileReference {
    pub path: PathBuf,
    pub root: PathBuf,
    pub marker: String,
}

pub(crate) fn validate_references(references: &[FileReference], text: &str) -> anyhow::Result<()> {
    for reference in references
        .iter()
        .filter(|reference| text.contains(&reference.marker))
    {
        let path = reference.path.canonicalize().map_err(|_| {
            anyhow::anyhow!(
                "Referenced file is unavailable: {}. Edit or remove the reference.",
                reference.path.display()
            )
        })?;
        anyhow::ensure!(
            path.starts_with(&reference.root) && path == reference.path && path.is_file(),
            "Referenced path changed or is outside its workspace: {}",
            reference.path.display()
        );
    }
    Ok(())
}

pub(crate) fn file_index(
    roots: &[PathBuf],
    cancel: &tokio_util::sync::CancellationToken,
) -> Vec<Choice> {
    let mut choices = Vec::new();
    for root in roots {
        let walker = ignore::WalkBuilder::new(root)
            .hidden(false)
            .require_git(false)
            .follow_links(false)
            .sort_by_file_name(|a, b| a.cmp(b))
            .filter_entry(|entry| {
                entry.depth() == 0
                    || !matches!(
                        entry.file_name().to_str(),
                        Some(".git" | "target" | "node_modules" | "dist" | ".orchestral")
                    )
            })
            .build();
        for entry in walker {
            if cancel.is_cancelled() || choices.len() >= 50_000 {
                return choices;
            }
            let Ok(entry) = entry else {
                continue;
            };
            if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                continue;
            }
            let Ok(path) = entry.path().canonicalize() else {
                continue;
            };
            if !path.starts_with(root) {
                continue;
            }
            let label = path.strip_prefix(root).unwrap_or(&path).to_string_lossy();
            let label = if roots.len() > 1 {
                format!("{} / {label}", root.display())
            } else {
                label.into_owned()
            };
            choices.push(Choice::new(
                label,
                path.to_string_lossy(),
                root.to_string_lossy(),
            ));
        }
    }
    choices
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_and_exact_path_matches_precede_fuzzy_matches() {
        let mut menu = Menu::new(
            MenuKind::Files,
            "Files",
            vec![
                Choice::new("src/main.rs", "a", ""),
                Choice::new("main.rs", "b", ""),
                Choice::new("mock_agent_interface.rs", "c", ""),
            ],
        );
        menu.query = "main".to_owned();
        assert_eq!(
            menu.filtered()
                .iter()
                .map(|c| c.value.as_str())
                .collect::<Vec<_>>(),
            ["b", "a", "c"]
        );
    }
}
