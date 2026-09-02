use std::cell::Cell;
use std::rc::Rc;

use js_sys::{Array, Function, Reflect, Uint8Array};
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

const TIMELINE_FOLLOW_EDGE_PX: i32 = 8;

pub fn window() -> Result<web_sys::Window, String> {
    web_sys::window().ok_or_else(|| "Browser window is unavailable".to_owned())
}

pub fn now() -> f64 {
    js_sys::Date::now()
}

pub fn is_online() -> bool {
    web_sys::window()
        .map(|window| window.navigator().on_line())
        .unwrap_or(false)
}

pub fn is_document_visible() -> bool {
    web_sys::window()
        .and_then(|window| window.document())
        .is_none_or(|document| document.visibility_state() == web_sys::VisibilityState::Visible)
}

pub fn new_uuid() -> Result<String, crate::browser::api::ApiError> {
    let error = |message: &str| crate::browser::api::ApiError {
        message: message.to_owned(),
        status: 0,
        code: "web_crypto_unavailable".to_owned(),
        details: None,
    };
    let crypto = window()
        .map_err(|_| error("Web Crypto is required for stable command identity"))?
        .crypto()
        .map_err(|_| error("Web Crypto is required for stable command identity"))?;
    let mut bytes = [0_u8; 16];
    crypto
        .get_random_values_with_u8_array(&mut bytes)
        .map_err(|_| error("Web Crypto could not create a command identity"))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    ))
}

pub fn take_pairing_secret() -> Option<String> {
    let window = web_sys::window()?;
    let location = window.location();
    let hash = location.hash().ok()?;
    let values = web_sys::UrlSearchParams::new_with_str(hash.strip_prefix('#')?).ok()?;
    let secret = values.get("pair")?;
    values.delete("pair");
    let remainder = values.to_string().as_string().unwrap_or_default();
    let pathname = location.pathname().unwrap_or_else(|_| "/".to_owned());
    let search = location.search().unwrap_or_default();
    let clean = if remainder.is_empty() {
        format!("{pathname}{search}")
    } else {
        format!("{pathname}{search}#{remainder}")
    };
    let _ = window
        .history()
        .and_then(|history| history.replace_state_with_url(&JsValue::NULL, "", Some(&clean)));
    Some(secret)
}

pub fn default_device_name(saved: &str) -> String {
    if !saved.is_empty() {
        return saved.to_owned();
    }
    let navigator = web_sys::window().map(|window| window.navigator());
    let user_agent = navigator
        .as_ref()
        .and_then(|navigator| navigator.user_agent().ok())
        .unwrap_or_default();
    let family = if user_agent.contains("iPad") {
        "iPad"
    } else if user_agent.contains("iPhone") {
        "iPhone"
    } else if user_agent.contains("Android") {
        "Android device"
    } else {
        "Browser"
    };
    format!("{family} · Orchestral")
}

pub fn apply_theme(theme: &str) {
    if let Some(root) = web_sys::window()
        .and_then(|window| window.document())
        .and_then(|document| document.document_element())
    {
        let _ = root.set_attribute("data-theme", theme);
    }
}

/// Pins the application root to the portion of the page that is actually
/// visible. iOS browsers keep a separate layout viewport while their address
/// bar or keyboard moves the visual viewport; a plain `position: fixed` shell
/// otherwise remains underneath browser chrome.
pub fn install_visual_viewport_sync() -> Result<Closure<dyn FnMut(web_sys::Event)>, String> {
    let viewport = window()?
        .visual_viewport()
        .ok_or_else(|| "Visual Viewport API is unavailable".to_owned())?;
    sync_visual_viewport(&viewport)?;

    let observed_viewport = viewport.clone();
    let listener = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        let _ = sync_visual_viewport(&observed_viewport);
    }) as Box<dyn FnMut(web_sys::Event)>);
    viewport
        .add_event_listener_with_callback("resize", listener.as_ref().unchecked_ref())
        .map_err(js_error)?;
    viewport
        .add_event_listener_with_callback("scroll", listener.as_ref().unchecked_ref())
        .map_err(js_error)?;
    Ok(listener)
}

fn sync_visual_viewport(viewport: &web_sys::VisualViewport) -> Result<(), String> {
    let root = window()?
        .document()
        .and_then(|document| document.document_element())
        .and_then(|element| element.dyn_into::<web_sys::HtmlElement>().ok())
        .ok_or_else(|| "Document root is unavailable".to_owned())?;
    let device_pixel_ratio = window()?.device_pixel_ratio().max(1.0);
    let css_px = |value: f64| {
        let value = (value * device_pixel_ratio).round() / device_pixel_ratio;
        format!("{value:.3}px")
    };
    let style = root.style();
    style
        .set_property("--visual-viewport-top", &css_px(viewport.offset_top()))
        .map_err(js_error)?;
    style
        .set_property("--visual-viewport-left", &css_px(viewport.offset_left()))
        .map_err(js_error)?;
    style
        .set_property("--visual-viewport-width", &css_px(viewport.width()))
        .map_err(js_error)?;
    style
        .set_property("--visual-viewport-height", &css_px(viewport.height()))
        .map_err(js_error)?;
    Ok(())
}

pub fn scroll_timeline_to_end() {
    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        return;
    };
    let Ok(Some(element)) = document.query_selector(".message-list") else {
        return;
    };
    let Ok(element) = element.dyn_into::<web_sys::HtmlElement>() else {
        return;
    };
    element.set_scroll_top(element.scroll_height());
}

/// Whether live updates should keep following the transcript. Once the user
/// scrolls up to read history, updates must not yank the viewport away.
pub fn timeline_is_near_end() -> bool {
    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        return true;
    };
    let Ok(Some(element)) = document.query_selector(".message-list") else {
        return true;
    };
    let Ok(element) = element.dyn_into::<web_sys::HtmlElement>() else {
        return true;
    };
    let remaining = element
        .scroll_height()
        .saturating_sub(element.client_height())
        .saturating_sub(element.scroll_top());
    remaining <= TIMELINE_FOLLOW_EDGE_PX
}

pub fn timeline_scroll_anchor() -> Option<(i32, i32)> {
    let document = web_sys::window()?.document()?;
    let element = document
        .query_selector(".message-list")
        .ok()??
        .dyn_into::<web_sys::HtmlElement>()
        .ok()?;
    Some((element.scroll_top(), element.scroll_height()))
}

/// Keeps the same content under the viewport after older transcript entries
/// are inserted above it.
pub fn restore_timeline_scroll_anchor((scroll_top, scroll_height): (i32, i32)) {
    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        return;
    };
    let Ok(Some(element)) = document.query_selector(".message-list") else {
        return;
    };
    let Ok(element) = element.dyn_into::<web_sys::HtmlElement>() else {
        return;
    };
    let added_height = element.scroll_height().saturating_sub(scroll_height);
    element.set_scroll_top(scroll_top.saturating_add(added_height));
}

pub async fn copy_text(text: &str) -> Result<(), String> {
    let navigator = window()?.navigator();
    let Some(clipboard) =
        optional_browser_capability::<web_sys::Clipboard>(navigator.as_ref(), "clipboard")?
    else {
        return Err("当前地址不支持系统剪贴板，请使用浏览器文本选择复制".to_owned());
    };
    JsFuture::from(clipboard.write_text(text))
        .await
        .map(|_| ())
        .map_err(js_error)
}

pub async fn register_service_worker() -> Result<(), String> {
    let navigator = window()?.navigator();
    let Some(service_workers) = optional_browser_capability::<web_sys::ServiceWorkerContainer>(
        navigator.as_ref(),
        "serviceWorker",
    )?
    else {
        // Service workers are intentionally absent on non-secure origins such
        // as a LAN HTTP address. The remote control surface must still work;
        // only offline/install support is unavailable there.
        return Ok(());
    };
    let update_pending = Rc::new(Cell::new(false));
    let pending_from_worker = update_pending.clone();
    let on_worker_message =
        Closure::<dyn FnMut(web_sys::Event)>::new(move |event: web_sys::Event| {
            let data = Reflect::get(event.as_ref(), &JsValue::from_str("data")).ok();
            let kind = data.as_ref().and_then(|data| {
                Reflect::get(data, &JsValue::from_str("type"))
                    .ok()
                    .and_then(|value| value.as_string())
            });
            if kind.as_deref() == Some("ORCHESTRAL_UPDATE_READY") {
                pending_from_worker.set(true);
                if !is_document_visible() {
                    let _ =
                        window().and_then(|window| window.location().reload().map_err(js_error));
                }
            }
        });
    service_workers
        .add_event_listener_with_callback("message", on_worker_message.as_ref().unchecked_ref())
        .map_err(js_error)?;
    on_worker_message.forget();

    if let Some(document) = web_sys::window().and_then(|window| window.document()) {
        let pending_on_visibility = update_pending;
        let on_visibility = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            if pending_on_visibility.get() && !is_document_visible() {
                let _ = window().and_then(|window| window.location().reload().map_err(js_error));
            }
        });
        document
            .add_event_listener_with_callback(
                "visibilitychange",
                on_visibility.as_ref().unchecked_ref(),
            )
            .map_err(js_error)?;
        on_visibility.forget();
    }

    JsFuture::from(service_workers.register("./sw.js"))
        .await
        .map(|_| ())
        .map_err(js_error)
}

fn optional_browser_capability<T>(owner: &JsValue, name: &str) -> Result<Option<T>, String>
where
    T: JsCast,
{
    let value = Reflect::get(owner, &JsValue::from_str(name)).map_err(js_error)?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    value
        .dyn_into::<T>()
        .map(Some)
        .map_err(|_| format!("Browser capability '{name}' has an unexpected type"))
}

pub fn install_event_prompt(event: &JsValue) {
    if let Ok(prompt) = Reflect::get(event, &JsValue::from_str("prompt"))
        .and_then(|value| value.dyn_into::<Function>())
    {
        let _ = prompt.call0(event);
    }
}

pub fn add_window_listener(
    name: &str,
    callback: impl FnMut(web_sys::Event) + 'static,
) -> Result<Closure<dyn FnMut(web_sys::Event)>, String> {
    let closure = Closure::wrap(Box::new(callback) as Box<dyn FnMut(web_sys::Event)>);
    window()?
        .add_event_listener_with_callback(name, closure.as_ref().unchecked_ref())
        .map_err(js_error)?;
    Ok(closure)
}

pub fn js_error(value: JsValue) -> String {
    value
        .as_string()
        .or_else(|| {
            Reflect::get(&value, &JsValue::from_str("message"))
                .ok()
                .and_then(|message| message.as_string())
        })
        .unwrap_or_else(|| "Browser operation failed".to_owned())
}

#[allow(dead_code)]
fn _keep_imports_typed(_: (Array, Uint8Array)) {}
