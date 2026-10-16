//! Shared mobile composer geometry and the visible area above a software keyboard.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use wasm_bindgen::{closure::Closure, JsCast};

use super::{js_error, window};

pub struct ViewportSync {
    inner: Rc<ViewportState>,
    targets: Vec<(web_sys::EventTarget, &'static str)>,
    listener: Closure<dyn FnMut(web_sys::Event)>,
    mutations: web_sys::MutationObserver,
    _mutation_callback: Closure<dyn FnMut()>,
    _resize_callback: Closure<dyn FnMut()>,
}

struct ViewportState {
    viewport: web_sys::VisualViewport,
    baseline: Cell<(f64, f64)>,
    frame: Cell<Option<i32>>,
    callback: RefCell<Option<Closure<dyn FnMut()>>>,
    resize: RefCell<Option<web_sys::ResizeObserver>>,
    composers: RefCell<Vec<web_sys::Element>>,
}

impl ViewportState {
    fn schedule(&self) {
        if self.frame.get().is_some() {
            return;
        }
        if let (Ok(window), Some(callback)) = (window(), self.callback.borrow().as_ref()) {
            if let Ok(frame) = window.request_animation_frame(callback.as_ref().unchecked_ref()) {
                self.frame.set(Some(frame));
            }
        }
    }

    fn sync(&self) -> Result<(), String> {
        let window = window()?;
        let document = window.document().ok_or("Document is unavailable")?;
        let root = document
            .document_element()
            .and_then(|root| root.dyn_into::<web_sys::HtmlElement>().ok())
            .ok_or("Document root is unavailable")?;
        let viewport = &self.viewport;
        let scale = window.device_pixel_ratio().max(1.0);
        let px = |value: f64| format!("{:.3}px", (value * scale).round() / scale);
        for (name, value) in [
            ("top", viewport.offset_top()),
            ("left", viewport.offset_left()),
            ("width", viewport.width()),
            ("height", viewport.height()),
        ] {
            root.style()
                .set_property(&format!("--visual-viewport-{name}"), &px(value))
                .map_err(js_error)?;
        }

        let width = window
            .inner_width()
            .map_err(js_error)?
            .as_f64()
            .unwrap_or(viewport.width());
        let height = window
            .inner_height()
            .map_err(js_error)?
            .as_f64()
            .unwrap_or(viewport.height());
        let (previous_width, previous_height) = self.baseline.get();
        let baseline = if (width - previous_width).abs() > 80.0 {
            height.max(viewport.height())
        } else {
            previous_height.max(height).max(viewport.height())
        };
        self.baseline.set((width, baseline));
        let focused_pane = document.active_element().and_then(|element| {
            if !element
                .matches("textarea, input:not([type='file']), [contenteditable='true']")
                .ok()?
            {
                return None;
            }
            element
                .closest(".session-pane")
                .ok()??
                .get_attribute("data-pane")
        });
        let keyboard_open = width < 860.0
            && (window.navigator().max_touch_points() > 0
                || window
                    .match_media("(pointer: coarse)")
                    .ok()
                    .flatten()
                    .is_some_and(|media| media.matches()))
            && viewport.scale() < 1.1
            && baseline - viewport.height() >= (baseline * 0.2).max(120.0);
        // Tapping Send or a toolbar button can move focus before the keyboard
        // has retracted. Keep the editing layout until the viewport recovers.
        let editing_pane = if keyboard_open {
            focused_pane.or_else(|| root.get_attribute("data-keyboard-pane"))
        } else {
            None
        };
        match editing_pane {
            Some(pane) => {
                root.set_attribute("data-keyboard-pane", &pane)
                    .map_err(js_error)?;
            }
            None => {
                root.remove_attribute("data-keyboard-pane")
                    .map_err(js_error)?;
            }
        }

        self.composers.borrow_mut().retain(|element| {
            if element.is_connected() {
                return true;
            }
            if let Some(resize) = self.resize.borrow().as_ref() {
                resize.unobserve(element);
            }
            false
        });
        let mut composer_height = 0.0_f64;
        for pane in ["primary", "secondary"] {
            let selector = format!(".session-pane[data-pane='{pane}'] .composer-dock");
            let Some(composer) = document.query_selector(&selector).map_err(js_error)? else {
                continue;
            };
            if !self.composers.borrow().contains(&composer) {
                if let Some(resize) = self.resize.borrow().as_ref() {
                    resize.observe(&composer);
                }
                self.composers.borrow_mut().push(composer.clone());
            }
            composer_height = composer_height.max(composer.get_bounding_client_rect().height());
        }
        root.style()
            .set_property("--workspace-composer-height", &px(composer_height))
            .map_err(js_error)?;
        Ok(())
    }
}

/// Own the listeners once at App scope. Both panes share the browser viewport.
pub fn install_visual_viewport_sync() -> Result<ViewportSync, String> {
    let window = window()?;
    let document = window.document().ok_or("Document is unavailable")?;
    let viewport = window
        .visual_viewport()
        .ok_or("Visual Viewport API is unavailable")?;
    let inner = Rc::new(ViewportState {
        viewport: viewport.clone(),
        baseline: Cell::new((viewport.width(), viewport.height())),
        frame: Cell::new(None),
        callback: RefCell::new(None),
        resize: RefCell::new(None),
        composers: RefCell::new(Vec::new()),
    });
    let weak = Rc::downgrade(&inner);
    *inner.callback.borrow_mut() = Some(Closure::wrap(Box::new(move || {
        if let Some(inner) = weak.upgrade() {
            inner.frame.set(None);
            let _ = inner.sync();
        }
    }) as Box<dyn FnMut()>));
    let weak = Rc::downgrade(&inner);
    let listener = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        if let Some(inner) = weak.upgrade() {
            inner.schedule();
        }
    }) as Box<dyn FnMut(web_sys::Event)>);
    let targets: Vec<(web_sys::EventTarget, &'static str)> = vec![
        (viewport.clone().unchecked_into(), "resize"),
        (viewport.unchecked_into(), "scroll"),
        (window.unchecked_into(), "resize"),
        (document.clone().unchecked_into(), "focusin"),
        (document.clone().unchecked_into(), "focusout"),
    ];
    for (target, event) in &targets {
        target
            .add_event_listener_with_callback(event, listener.as_ref().unchecked_ref())
            .map_err(js_error)?;
    }
    let weak = Rc::downgrade(&inner);
    let mutation_callback = Closure::wrap(Box::new(move || {
        if let Some(inner) = weak.upgrade() {
            inner.schedule();
        }
    }) as Box<dyn FnMut()>);
    let mutations = web_sys::MutationObserver::new(mutation_callback.as_ref().unchecked_ref())
        .map_err(js_error)?;
    let options = web_sys::MutationObserverInit::new();
    options.set_child_list(true);
    options.set_subtree(true);
    options.set_attributes(true);
    options.set_attribute_filter(&js_sys::Array::of1(&"data-active-pane".into()));
    let root = document
        .document_element()
        .ok_or("Document root is unavailable")?;
    mutations
        .observe_with_options(root.as_ref(), &options)
        .map_err(js_error)?;
    let weak = Rc::downgrade(&inner);
    let resize_callback = Closure::wrap(Box::new(move || {
        if let Some(inner) = weak.upgrade() {
            inner.schedule();
        }
    }) as Box<dyn FnMut()>);
    *inner.resize.borrow_mut() = Some(
        web_sys::ResizeObserver::new(resize_callback.as_ref().unchecked_ref()).map_err(js_error)?,
    );
    inner.sync()?;
    Ok(ViewportSync {
        inner,
        targets,
        listener,
        mutations,
        _mutation_callback: mutation_callback,
        _resize_callback: resize_callback,
    })
}

pub fn workspace_keyboard_open() -> bool {
    window()
        .ok()
        .and_then(|window| window.document())
        .and_then(|document| document.document_element())
        .is_some_and(|root| root.has_attribute("data-keyboard-pane"))
}

/// Keep focus transfer inside the user gesture, as required by iOS keyboards.
pub fn focus_composer(pane: &str) {
    if !matches!(pane, "primary" | "secondary") {
        return;
    }
    let Some(document) = window().ok().and_then(|window| window.document()) else {
        return;
    };
    if let Ok(Some(workspace)) = document.query_selector(".split-workspace") {
        let _ = workspace.set_attribute("data-active-pane", pane);
    }
    // Make the destination visible before moving focus, without waiting for a
    // framework render to finish after the user gesture has expired.
    if workspace_keyboard_open() {
        if let Some(root) = document.document_element() {
            let _ = root.set_attribute("data-keyboard-pane", pane);
        }
    }
    let selector = format!(".session-pane[data-pane='{pane}'] .message-input");
    if let Ok(Some(element)) = document.query_selector(&selector) {
        if let Ok(input) = element.dyn_into::<web_sys::HtmlElement>() {
            let _ = input.focus();
        }
    }
}

impl Drop for ViewportSync {
    fn drop(&mut self) {
        for (target, event) in &self.targets {
            let _ = target
                .remove_event_listener_with_callback(event, self.listener.as_ref().unchecked_ref());
        }
        self.mutations.disconnect();
        if let Some(resize) = self.inner.resize.borrow().as_ref() {
            resize.disconnect();
        }
        if let (Ok(window), Some(frame)) = (window(), self.inner.frame.take()) {
            let _ = window.cancel_animation_frame(frame);
        }
    }
}
