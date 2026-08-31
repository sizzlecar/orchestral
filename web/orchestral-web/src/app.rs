use std::rc::Rc;

use dioxus::prelude::*;

use crate::browser::controller::AppController;
use crate::browser::{platform, storage};
use crate::components::{AuthScreen, Workspace};
use crate::state::{AppState, AuthStatus};

const CSS: Asset = asset!("/assets/styles.css");

#[component]
pub fn App() -> Element {
    let state = use_signal(|| AppState::new(platform::is_online()));
    let token = use_signal(|| None::<String>);
    let pairing_secret = use_signal(platform::take_pairing_secret);
    let preferences = use_signal(storage::load_preferences);
    let stream_abort = use_signal(|| None::<web_sys::AbortController>);
    let stream_generation = use_signal(|| 0_u64);
    let install_event = use_signal(|| None::<wasm_bindgen::JsValue>);
    let controller = use_context_provider(|| {
        AppController::new(
            state,
            token,
            pairing_secret,
            preferences,
            stream_abort,
            stream_generation,
            install_event,
        )
    });
    let _listeners = use_hook(move || Rc::new(controller.window_listeners()));
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
