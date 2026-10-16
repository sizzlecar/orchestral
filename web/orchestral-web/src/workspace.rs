//! Provider selection and stable two-pane session assignment.

use crate::model::{AgentConnectorView, SessionView};
use crate::state::AppState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkspaceSource {
    #[default]
    Codex,
    Claude,
    Orchestral,
}

impl WorkspaceSource {
    pub const ALL: [Self; 3] = [Self::Codex, Self::Claude, Self::Orchestral];

    pub fn key(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Orchestral => "orchestral",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Claude => "Claude Code",
            Self::Orchestral => "Orch",
        }
    }

    pub fn sessions(self, state: &AppState) -> Vec<SessionView> {
        let mut sessions = state
            .sessions
            .items
            .iter()
            .filter(|session| match session.connector_id.as_deref() {
                None => self == Self::Orchestral,
                Some(id) => state.connectors.items.iter().any(|connector| {
                    connector.connector_id == id && self.matches_connector(connector)
                }),
            })
            .cloned()
            .collect::<Vec<_>>();
        sessions.sort_by(|a, b| {
            b.updated_at_unix_ms
                .cmp(&a.updated_at_unix_ms)
                .then_with(|| a.key().cmp(&b.key()))
        });
        sessions
    }

    pub fn matches_connector(self, connector: &AgentConnectorView) -> bool {
        self != Self::Orchestral && connector.agent_product.as_deref() == Some(self.key())
    }
}

/// A source switch opens its newest pair. Background refreshes preserve each
/// reader's selection; new activity never silently replaces an occupied pane.
pub fn assign_sessions(
    sessions: &[SessionView],
    current: [Option<String>; 2],
    reset: bool,
) -> [Option<String>; 2] {
    let keys = sessions.iter().map(SessionView::key).collect::<Vec<_>>();
    let mut chosen = if reset {
        [None, None]
    } else {
        current.map(|key| key.filter(|key| keys.contains(key)))
    };
    if chosen[0].is_some() && chosen[0] == chosen[1] {
        chosen[1] = None;
    }
    for index in 0..2 {
        if chosen[index].is_none() {
            chosen[index] = keys
                .iter()
                .find(|key| !chosen.iter().flatten().any(|id| id == *key))
                .cloned();
        }
    }
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(id: &str, updated: i64, connector: Option<&str>) -> SessionView {
        serde_json::from_value(serde_json::json!({
            "id": id, "created_at_unix_ms": 0, "updated_at_unix_ms": updated,
            "connector_id": connector
        }))
        .unwrap()
    }

    #[test]
    fn source_uses_product_metadata_and_sorts_by_recent_activity() {
        let mut state = AppState::new(true);
        state.connectors.items.push(
            serde_json::from_value(serde_json::json!({
                "connector_id": "custom/coding", "agent_family": "coding-agent", "agent_product": "codex", "display_name": "Coding",
                "capabilities": {"list": true, "read": true, "create": true}
            }))
            .unwrap(),
        );
        state.sessions.items = vec![
            session("old", 1, Some("custom/coding")),
            session("native", 9, None),
            session("new", 5, Some("custom/coding")),
        ];
        let sessions = WorkspaceSource::Codex.sessions(&state);
        assert_eq!(
            sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["new", "old"]
        );
        assert_eq!(WorkspaceSource::Orchestral.sessions(&state)[0].id, "native");
        assert!(WorkspaceSource::Claude.sessions(&state).is_empty());
        assert_eq!(WorkspaceSource::default(), WorkspaceSource::Codex);
    }

    #[test]
    fn newest_pair_is_distinct_and_short_catalog_leaves_an_empty_pane() {
        let sessions = vec![
            session("new", 3, None),
            session("middle", 2, None),
            session("old", 1, None),
        ];
        assert_eq!(
            assign_sessions(&sessions, [Some("old".into()), None], true),
            [Some("new".into()), Some("middle".into())]
        );
        assert_eq!(
            assign_sessions(&sessions[..1], [None, None], true),
            [Some("new".into()), None]
        );
    }

    #[test]
    fn coding_agents_group_only_by_explicit_product_never_opaque_id_or_label() {
        let mut state = AppState::new(true);
        for (id, label, product) in [
            ("opaque-a", "Custom coding provider", Some("codex")),
            ("opaque-b", "Custom coding provider", Some("claude")),
            ("codex/local", "Codex", None),
            ("claude/local", "Claude Code", Some("another-product")),
        ] {
            state.connectors.items.push(
                serde_json::from_value(serde_json::json!({
                    "connector_id": id, "display_name": label, "agent_family": "coding-agent",
                    "agent_product": product,
                    "capabilities": {"list": true, "read": true, "create": false}
                }))
                .unwrap(),
            );
            state.sessions.items.push(session(id, 1, Some(id)));
        }
        assert_eq!(
            WorkspaceSource::Codex
                .sessions(&state)
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["opaque-a"]
        );
        assert_eq!(
            WorkspaceSource::Claude
                .sessions(&state)
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["opaque-b"]
        );
        assert!(WorkspaceSource::Orchestral.sessions(&state).is_empty());
        let legacy: AgentConnectorView = serde_json::from_value(serde_json::json!({
            "connector_id": "codex/legacy", "display_name": "Codex", "agent_family": "coding-agent",
            "capabilities": {"list": true, "read": true, "create": false}
        }))
        .unwrap();
        assert_eq!(legacy.agent_product, None);
        assert!(!WorkspaceSource::Codex.matches_connector(&legacy));
    }

    #[test]
    fn refresh_preserves_manual_choices_and_repairs_duplicate_or_removed_selection() {
        let sessions = vec![
            session("new", 3, None),
            session("middle", 2, None),
            session("old", 1, None),
        ];
        assert_eq!(
            assign_sessions(
                &sessions,
                [Some("old".into()), Some("middle".into())],
                false
            ),
            [Some("old".into()), Some("middle".into())]
        );
        assert_eq!(
            assign_sessions(&sessions, [Some("old".into()), Some("old".into())], false),
            [Some("old".into()), Some("new".into())]
        );
        assert_eq!(
            assign_sessions(
                &sessions,
                [Some("gone".into()), Some("middle".into())],
                false
            ),
            [Some("new".into()), Some("middle".into())]
        );
    }
}
