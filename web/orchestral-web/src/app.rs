use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use dioxus::prelude::*;

use crate::browser::api::ApiCredential;
use crate::browser::controller::{AppController, LiveTransportControls};
use crate::browser::{platform, storage};
use crate::catalog::{CatalogCoordinator, CatalogSnapshot};
use crate::components::{AuthScreen, Workspace};
use crate::state::{AgentSessionReconcileCoordinator, AppState, AuthStatus, LoadStatus};
use crate::tasks::AppTaskScope;
use crate::workspace::{assign_sessions, WorkspaceSource};

const CSS: Asset = asset!("/assets/styles.css");

#[derive(Clone, Copy)]
pub struct SplitWorkspace {
    pub primary: AppController,
    pub secondary: AppController,
    pub source: Signal<WorkspaceSource>,
    pub update_blockers: Signal<BTreeSet<&'static str>>,
    pub drafts: Signal<storage::ComposerDrafts>,
    pub draft_save_error: Signal<Option<String>>,
}

impl SplitWorkspace {
    /// The shared in-memory draft is authoritative. Persistence failure must
    /// never discard edits or leave another pane with a stale attachment list.
    pub(crate) fn update_draft(
        mut self,
        session_key: &str,
        update: impl FnOnce(&mut (String, Vec<crate::model::UploadedArtifact>)),
    ) {
        let result = {
            let mut drafts = self.drafts.write();
            let draft = drafts.entry(session_key.to_owned()).or_default();
            update(draft);
            if draft.0.is_empty() && draft.1.is_empty() {
                drafts.remove(session_key);
            }
            storage::save_drafts(&drafts)
        };
        self.draft_save_error.set(result.err());
    }
}

#[component]
pub fn App() -> Element {
    use_hook(|| {
        std::panic::set_hook(Box::new(|info| {
            web_sys::console::error_1(&format!("Orchestral browser error: {info}").into());
        }));
    });
    let state = use_signal(|| AppState::new(platform::is_online()));
    let token = use_signal(|| None::<ApiCredential>);
    let pairing_secret = use_signal(platform::take_pairing_secret);
    let preferences = use_signal(storage::load_preferences);
    let stream_abort = use_signal(|| None::<web_sys::AbortController>);
    let stream_generation = use_signal(|| 0_u64);
    let agent_session_stream_abort = use_signal(|| None::<web_sys::AbortController>);
    let agent_session_stream_generation = use_signal(|| 0_u64);
    let agent_session_reconcile = use_signal(AgentSessionReconcileCoordinator::default);
    let install_event = use_signal(|| None::<wasm_bindgen::JsValue>);
    let task_scope = use_hook(AppTaskScope::current);
    let controller = use_context_provider(|| {
        AppController::new(
            task_scope,
            state,
            token,
            pairing_secret,
            preferences,
            LiveTransportControls {
                run_abort: stream_abort,
                run_generation: stream_generation,
                agent_session_abort: agent_session_stream_abort,
                agent_session_generation: agent_session_stream_generation,
                agent_session_reconcile,
            },
            install_event,
        )
    });
    let secondary_state = use_signal(|| AppState::new(platform::is_online()));
    let secondary_stream_abort = use_signal(|| None::<web_sys::AbortController>);
    let secondary_stream_generation = use_signal(|| 0_u64);
    let secondary_session_abort = use_signal(|| None::<web_sys::AbortController>);
    let secondary_session_generation = use_signal(|| 0_u64);
    let secondary_reconcile = use_signal(AgentSessionReconcileCoordinator::default);
    let secondary = use_hook(|| {
        controller.fork(
            secondary_state,
            LiveTransportControls {
                run_abort: secondary_stream_abort,
                run_generation: secondary_stream_generation,
                agent_session_abort: secondary_session_abort,
                agent_session_generation: secondary_session_generation,
                agent_session_reconcile: secondary_reconcile,
            },
            "secondary",
        )
    });
    let source = use_signal(WorkspaceSource::default);
    let update_blockers = use_signal(BTreeSet::new);
    let drafts = use_signal(storage::load_drafts);
    let draft_save_error = use_signal(|| None::<String>);
    use_context_provider(|| SplitWorkspace {
        primary: controller,
        secondary,
        source,
        update_blockers,
        drafts,
        draft_save_error,
    });
    let mut assigned_source = use_signal(|| None::<WorkspaceSource>);
    let catalog_sync = use_hook(|| Rc::new(RefCell::new(CatalogCoordinator::default())));
    use_effect(move || {
        let selected_source = source();
        let token_present = token.read().is_some();
        let primary = state.read();
        if !token_present || primary.auth.status != AuthStatus::Authenticated {
            catalog_sync.borrow_mut().reset();
            let was_authenticated = primary.auth.status == AuthStatus::Authenticated;
            drop(primary);
            controller.clear_selection();
            secondary.clear_selection();
            if was_authenticated {
                let mut state = state;
                state.write().auth.status = AuthStatus::Unpaired;
            }
            if assigned_source.peek().is_some() {
                assigned_source.set(None);
            }
            return;
        }
        let primary_selection = primary.sessions.selected_id.clone();
        let primary_catalog = CatalogSnapshot::capture(&primary);
        let online = primary.connection.online;
        drop(primary);
        let lower = secondary_state.read();
        let secondary_selection = lower.sessions.selected_id.clone();
        let secondary_catalog = CatalogSnapshot::capture(&lower);
        drop(lower);
        let shared = catalog_sync.borrow_mut().reconcile(
            [primary_catalog, secondary_catalog],
            [primary_selection.clone(), secondary_selection.clone()],
        );
        controller.sync_catalog(&shared);
        secondary.sync_catalog(&shared);
        let mut catalog = AppState::new(online);
        shared.apply(&mut catalog);
        if catalog.sessions.status != LoadStatus::Ready {
            return;
        }
        let reset = *assigned_source.peek() != Some(selected_source);
        let current = [primary_selection, secondary_selection];
        let desired = assign_sessions(&selected_source.sessions(&catalog), current.clone(), reset);
        if reset {
            assigned_source.set(Some(selected_source));
        }
        for (index, mut pane) in [controller, secondary].into_iter().enumerate() {
            if desired[index] == current[index] {
                continue;
            }
            match desired[index].clone() {
                Some(key) => {
                    // Publish the intent before spawning so another render
                    // cannot schedule the same selection a second time.
                    pane.state.write().sessions.selected_id = Some(key.clone());
                    pane.spawn(async move {
                        if pane.state.read().sessions.selected_id.as_deref() == Some(key.as_str()) {
                            pane.load_session(key).await;
                        }
                    });
                }
                None => pane.clear_selection(),
            }
        }
    });
    let _listeners = use_hook(move || Rc::new(controller.window_listeners()));
    let _secondary_listeners = use_hook(move || Rc::new(secondary.window_listeners()));
    use_drop(move || {
        controller.clear_selection();
        secondary.clear_selection();
    });
    use_future(move || async move {
        let _ = platform::register_service_worker(move || {
            let mut state = state;
            state.write().ui.update_available = true;
        })
        .await;
    });
    use_future(move || async move { controller.bootstrap().await });

    let status = state.read().auth.status.clone();
    rsx! {
        document::Stylesheet { href: CSS }
        match status {
            AuthStatus::Authenticated => rsx! { Workspace {} },
            _ => rsx! { AuthScreen {} },
        }
    }
}
