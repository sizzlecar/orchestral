use std::rc::Rc;

use dioxus::prelude::*;

use crate::browser::api::ApiCredential;
use crate::browser::controller::{AppController, LiveTransportControls};
use crate::browser::{platform, storage};
use crate::components::{AuthScreen, Workspace};
use crate::state::{AgentSessionReconcileCoordinator, AppState, AuthStatus};

const CSS: Asset = asset!("/assets/styles.css");

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
    let controller = use_context_provider(|| {
        AppController::new(
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
    let _listeners = use_hook(move || Rc::new(controller.window_listeners()));
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
