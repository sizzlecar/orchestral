//! Shared session directory reconciliation for independent conversation panes.
//!
//! Both panes receive the same baseline. Only changes since that baseline are
//! published, so a reader's unchanged copy cannot undo another reader's page,
//! metadata update, or deletion. Run projections and selections stay local.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::SessionView;
use crate::state::{
    AgentSessionListState, AppState, AuthState, ConnectorsState, DevicesState, LoadStatus,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CatalogSnapshot {
    auth: AuthState,
    connectors: ConnectorsState,
    devices: DevicesState,
    status: LoadStatus,
    sessions: BTreeMap<String, SessionView>,
    pages: BTreeMap<String, AgentSessionListState>,
    error: Option<String>,
    update_available: bool,
    install_available: bool,
}

impl CatalogSnapshot {
    pub(crate) fn capture(state: &AppState) -> Self {
        Self {
            auth: state.auth.clone(),
            connectors: state.connectors.clone(),
            devices: state.devices.clone(),
            status: state.sessions.status,
            sessions: state
                .sessions
                .items
                .iter()
                .map(|item| (item.key(), item.clone()))
                .collect(),
            pages: state.sessions.connector_pages.clone(),
            error: state.sessions.error.clone(),
            update_available: state.ui.update_available,
            install_available: state.ui.install_available,
        }
    }

    pub(crate) fn matches(&self, state: &AppState) -> bool {
        *self == Self::capture(state)
    }

    /// Apply directory fields only. Never copy another pane's selected session,
    /// pending requests, draft, history window, or live transport state.
    pub(crate) fn apply(&self, state: &mut AppState) {
        state.auth = self.auth.clone();
        state.connectors = self.connectors.clone();
        state.devices = self.devices.clone();
        state.sessions.status = self.status;
        state.sessions.items = self.sessions.values().cloned().collect();
        state.sessions.items.sort_by(|a, b| {
            b.updated_at_unix_ms
                .cmp(&a.updated_at_unix_ms)
                .then_with(|| a.key().cmp(&b.key()))
        });
        state.sessions.connector_pages = self.pages.clone();
        state.sessions.error = self.error.clone();
        state.ui.update_available = self.update_available;
        state.ui.install_available = self.install_available;
    }
}

#[derive(Default)]
pub(crate) struct CatalogCoordinator {
    baseline: Option<CatalogSnapshot>,
}

impl CatalogCoordinator {
    pub(crate) fn reset(&mut self) {
        self.baseline = None;
    }

    pub(crate) fn reconcile(
        &mut self,
        panes: [CatalogSnapshot; 2],
        selections: [Option<String>; 2],
    ) -> CatalogSnapshot {
        let Some(baseline) = self.baseline.as_ref() else {
            // Primary owns authentication/bootstrap. The untouched secondary
            // starts empty and must not delete the directory during hydration.
            self.baseline = Some(panes[0].clone());
            return panes[0].clone();
        };
        let mut shared = baseline.clone();
        let removed = panes
            .iter()
            .flat_map(|pane| {
                baseline
                    .sessions
                    .keys()
                    .filter(|key| !pane.sessions.contains_key(*key))
                    .filter(|key| {
                        // A first-page refresh is not proof that another
                        // pane's older selected session was deleted. Explicit
                        // removal clears its selection before reconciliation.
                        let selected = selections.iter().flatten().any(|id| id == *key);
                        let may_be_on_later_page = baseline.sessions[*key]
                            .connector_id
                            .as_ref()
                            .and_then(|id| pane.pages.get(id))
                            .is_some_and(|page| {
                                page.next_cursor.is_some()
                                    || page.loading_more
                                    || page.error.is_some()
                            });
                        !(selected && may_be_on_later_page)
                    })
                    .cloned()
            })
            .collect::<BTreeSet<_>>();
        for key in &removed {
            shared.sessions.remove(key);
        }
        for pane in panes {
            macro_rules! changed {
                ($($field:ident),+ $(,)?) => {$(
                    if pane.$field != baseline.$field {
                        shared.$field = pane.$field.clone();
                    }
                )+};
            }
            changed!(auth, connectors, devices, error);
            if pane.status != baseline.status {
                shared.status = pane.status;
            }
            if pane.update_available != baseline.update_available {
                shared.update_available = pane.update_available;
            }
            if pane.install_available != baseline.install_available {
                shared.install_available = pane.install_available;
            }
            merge_map_delta(&mut shared.pages, &baseline.pages, &pane.pages);
            for (key, session) in pane.sessions {
                if removed.contains(&key) {
                    continue;
                }
                match baseline.sessions.get(&key) {
                    Some(before) if before != &session => {
                        if let Some(target) = shared.sessions.get_mut(&key) {
                            merge_session_delta(target, before, &session);
                        }
                    }
                    None => {
                        shared.sessions.insert(key, session);
                    }
                    _ => {}
                }
            }
        }
        self.baseline = Some(shared.clone());
        shared
    }
}

fn merge_map_delta<T: Clone + PartialEq>(
    target: &mut BTreeMap<String, T>,
    before: &BTreeMap<String, T>,
    incoming: &BTreeMap<String, T>,
) {
    for key in before.keys().filter(|key| !incoming.contains_key(*key)) {
        target.remove(key);
    }
    for (key, value) in incoming {
        if before.get(key) != Some(value) {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn merge_session_delta(target: &mut SessionView, before: &SessionView, incoming: &SessionView) {
    macro_rules! changed {
        ($($field:ident),+ $(,)?) => {$(
            if incoming.$field != before.$field { target.$field = incoming.$field.clone(); }
        )+};
    }
    changed!(title, preview, cwd, state, input_action);
    if incoming.created_at_unix_ms != before.created_at_unix_ms {
        target.created_at_unix_ms = incoming.created_at_unix_ms;
    }
    if incoming.updated_at_unix_ms != before.updated_at_unix_ms {
        target.updated_at_unix_ms = incoming.updated_at_unix_ms;
    }
    let previous = &before.execution_profile;
    let next = &incoming.execution_profile;
    let profile = &mut target.execution_profile;
    if next.model != previous.model {
        profile.model = next.model.clone();
    }
    if next.reasoning_effort != previous.reasoning_effort {
        profile.reasoning_effort = next.reasoning_effort.clone();
    }
    if next.permissions.filesystem != previous.permissions.filesystem {
        profile.permissions.filesystem = next.permissions.filesystem;
    }
    if next.permissions.network != previous.permissions.network {
        profile.permissions.network = next.permissions.network;
    }
    if next.permissions.approvals != previous.permissions.approvals {
        profile.permissions.approvals = next.permissions.approvals;
    }
    target
        .run_ids
        .retain(|id| !before.run_ids.contains(id) || incoming.run_ids.contains(id));
    for id in &incoming.run_ids {
        if !before.run_ids.contains(id) && !target.run_ids.contains(id) {
            target.run_ids.push(id.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AgentApprovalMode, AgentFilesystemAccess};

    fn session(id: &str) -> SessionView {
        serde_json::from_value(serde_json::json!({
            "id": id, "created_at_unix_ms": 1, "updated_at_unix_ms": 2,
            "connector_id": "codex/local", "run_ids": [format!("run-{id}")]
        }))
        .unwrap()
    }

    fn reconcile(sync: &mut CatalogCoordinator, panes: &mut [AppState; 2]) -> CatalogSnapshot {
        let shared = sync.reconcile(
            [
                CatalogSnapshot::capture(&panes[0]),
                CatalogSnapshot::capture(&panes[1]),
            ],
            [
                panes[0].sessions.selected_id.clone(),
                panes[1].sessions.selected_id.clone(),
            ],
        );
        for pane in panes {
            shared.apply(pane);
        }
        shared
    }

    fn fixture() -> (CatalogCoordinator, [AppState; 2]) {
        let mut panes = [AppState::new(true), AppState::new(true)];
        panes[0].sessions.status = LoadStatus::Ready;
        panes[0].sessions.items = vec![session("first")];
        panes[0].sessions.connector_pages.insert(
            "codex/local".into(),
            AgentSessionListState {
                next_cursor: Some("page-2".into()),
                ..Default::default()
            },
        );
        let mut sync = CatalogCoordinator::default();
        reconcile(&mut sync, &mut panes);
        (sync, panes)
    }

    #[test]
    fn lower_pane_page_loading_and_result_publish_without_being_overwritten() {
        let (mut sync, mut panes) = fixture();
        panes[1]
            .sessions
            .connector_pages
            .get_mut("codex/local")
            .unwrap()
            .loading_more = true;
        reconcile(&mut sync, &mut panes);
        for pane in &panes {
            let page = &pane.sessions.connector_pages["codex/local"];
            assert!(page.loading_more);
            assert_eq!(page.next_cursor.as_deref(), Some("page-2"));
        }
        panes[1].sessions.items.push(session("older"));
        let page = panes[1]
            .sessions
            .connector_pages
            .get_mut("codex/local")
            .unwrap();
        page.loading_more = false;
        page.next_cursor = Some("page-3".into());
        let shared = reconcile(&mut sync, &mut panes);
        assert_eq!(panes[0].sessions.items.len(), 2);
        assert_eq!(
            panes[0].sessions.connector_pages["codex/local"]
                .next_cursor
                .as_deref(),
            Some("page-3")
        );
        assert_eq!(
            reconcile(&mut sync, &mut panes),
            shared,
            "synchronized copies must converge without another change"
        );
    }

    #[test]
    fn simultaneous_run_and_permission_updates_keep_both_changes() {
        let (mut sync, mut panes) = fixture();
        panes[0].sessions.items[0].run_ids.push("new-run".into());
        panes[1].sessions.items[0]
            .execution_profile
            .permissions
            .filesystem = Some(AgentFilesystemAccess::ReadOnly);
        panes[1].sessions.items[0]
            .execution_profile
            .permissions
            .approvals = Some(AgentApprovalMode::OnRequest);
        reconcile(&mut sync, &mut panes);
        for pane in panes {
            let session = &pane.sessions.items[0];
            assert!(session.run_ids.contains(&"new-run".to_owned()));
            assert_eq!(
                session.execution_profile.permissions.filesystem,
                Some(AgentFilesystemAccess::ReadOnly)
            );
            assert_eq!(
                session.execution_profile.permissions.approvals,
                Some(AgentApprovalMode::OnRequest)
            );
        }
    }

    #[test]
    fn deletion_wins_over_an_unchanged_or_concurrently_updated_copy() {
        let (mut sync, mut panes) = fixture();
        panes[1].sessions.items.clear();
        panes[0].sessions.items[0].title = Some("late metadata".into());
        reconcile(&mut sync, &mut panes);
        reconcile(&mut sync, &mut panes);
        assert!(panes.iter().all(|pane| pane.sessions.items.is_empty()));
    }

    #[test]
    fn new_lower_session_is_shared_without_copying_selection_or_run_state() {
        let (mut sync, mut panes) = fixture();
        let new = session("lower-new");
        let first_key = panes[0].sessions.items[0].key();
        panes[0].sessions.selected_id = Some(first_key.clone());
        panes[0].ui.timeline_scrolled_away = true;
        panes[1].sessions.selected_id = Some(new.key());
        panes[1].sessions.items.push(new.clone());
        panes[1].ensure_run_source("lower-only", Some(new.id.clone()), new.connector_id.clone());
        reconcile(&mut sync, &mut panes);
        assert_eq!(panes[0].sessions.items.len(), 2);
        assert_eq!(
            panes[0].sessions.selected_id.as_deref(),
            Some(first_key.as_str())
        );
        assert_eq!(
            panes[1].sessions.selected_id.as_deref(),
            Some(new.key().as_str())
        );
        assert!(panes[0].ui.timeline_scrolled_away);
        assert!(!panes[0].runs.contains_key("lower-only"));
        assert!(panes[1].runs.contains_key("lower-only"));
    }

    #[test]
    fn independent_connector_pages_do_not_erase_each_other() {
        let (mut sync, mut panes) = fixture();
        panes[0]
            .sessions
            .connector_pages
            .get_mut("codex/local")
            .unwrap()
            .loading_more = true;
        panes[1].sessions.connector_pages.insert(
            "claude/local".into(),
            AgentSessionListState {
                error: Some("retryable list error".into()),
                ..Default::default()
            },
        );
        reconcile(&mut sync, &mut panes);
        assert!(panes[0].sessions.connector_pages["codex/local"].loading_more);
        assert!(panes[0].sessions.connector_pages["claude/local"]
            .error
            .is_some());
    }

    #[test]
    fn resetting_authentication_discards_the_previous_directory_baseline() {
        let (mut sync, mut panes) = fixture();
        sync.reset();
        panes[0] = AppState::new(true);
        let shared = reconcile(&mut sync, &mut panes);
        assert!(panes[1].sessions.items.is_empty());
        assert!(shared.matches(&panes[0]));
        assert!(shared.matches(&panes[1]));
    }

    #[test]
    fn first_page_refresh_retains_the_other_panes_older_selected_session() {
        let (mut sync, mut panes) = fixture();
        let older = session("older-selected");
        panes[1].sessions.selected_id = Some(older.key());
        panes[1].sessions.items.push(older.clone());
        reconcile(&mut sync, &mut panes);
        panes[0]
            .sessions
            .items
            .retain(|item| item.key() != older.key());
        reconcile(&mut sync, &mut panes);
        assert!(panes.iter().all(|pane| pane
            .sessions
            .items
            .iter()
            .any(|item| item.key() == older.key())));
        assert_eq!(panes[1].sessions.selected_id, Some(older.key()));

        // An explicit 404 clears the owner's selection and removes the item;
        // the remaining unchanged peer copy must not bring it back.
        panes[1].sessions.selected_id = None;
        panes[1]
            .sessions
            .items
            .retain(|item| item.key() != older.key());
        reconcile(&mut sync, &mut panes);
        assert!(panes.iter().all(|pane| pane
            .sessions
            .items
            .iter()
            .all(|item| item.key() != older.key())));
    }
}
