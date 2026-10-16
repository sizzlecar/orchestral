//! Session-scoped viewport following. Layout observers, rather than a timer,
//! cover delayed Dioxus commits, images, fonts, and viewport resizing.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;

use wasm_bindgen::{closure::Closure, JsCast};
use web_sys::{Element, Event, HtmlElement, MutationObserver, ResizeObserver};

const FOLLOW_EDGE_PX: i32 = 8;

thread_local! {
    static VIEWPORTS: RefCell<BTreeMap<String, Rc<TimelineViewport>>> = const { RefCell::new(BTreeMap::new()) };
    static GENERATION: Cell<u64> = const { Cell::new(0) };
}

pub struct TimelineScrollAnchor {
    generation: u64,
    element: Option<Element>,
    offset: f64,
    scroll_top: i32,
    scroll_height: i32,
}

struct TimelineViewport {
    scope: String,
    session_key: String,
    generation: u64,
    element: RefCell<Option<HtmlElement>>,
    content: RefCell<Option<Element>>,
    following: Cell<bool>,
    user_input: Cell<bool>,
    last_top: Cell<i32>,
    anchor_pending: Cell<bool>,
    anchor: RefCell<Option<TimelineScrollAnchor>>,
    frame: Cell<Option<i32>>,
    frame_callback: Closure<dyn FnMut(f64)>,
    mutation: Option<MutationObserver>,
    _mutation_callback: Closure<dyn FnMut(js_sys::Array)>,
    resize: Option<ResizeObserver>,
    _resize_callback: Closure<dyn FnMut()>,
    input_callback: Closure<dyn FnMut(Event)>,
    scroll_callback: Closure<dyn FnMut(Event)>,
}

const INPUT_EVENTS: [&str; 4] = ["wheel", "touchstart", "pointerdown", "keydown"];

impl TimelineViewport {
    fn create(scope: &str, session_key: &str, generation: u64) -> Option<Rc<Self>> {
        let document = web_sys::window()?.document()?;
        let root = document.document_element()?;
        let viewport = Rc::new_cyclic(|weak: &std::rc::Weak<Self>| {
            let frame_weak = weak.clone();
            let frame_callback = Closure::new(move |_time| {
                if let Some(viewport) = frame_weak.upgrade() {
                    viewport.frame.set(None);
                    viewport.apply_layout();
                }
            });
            let mutation_weak = weak.clone();
            let mutation_callback = Closure::new(move |records: js_sys::Array| {
                if let Some(viewport) = mutation_weak.upgrade() {
                    let relevant = viewport.current_element().is_none_or(|element| {
                        records.iter().any(|record| {
                            record
                                .unchecked_into::<web_sys::MutationRecord>()
                                .target()
                                .is_some_and(|target| {
                                    element.contains(Some(&target))
                                        || target.contains(Some(&element))
                                })
                        })
                    });
                    if relevant {
                        viewport.schedule();
                    }
                }
            });
            let resize_weak = weak.clone();
            let resize_callback = Closure::new(move || {
                if let Some(viewport) = resize_weak.upgrade() {
                    viewport.schedule();
                }
            });
            let input_weak = weak.clone();
            let input_callback = Closure::new(move |event: Event| {
                if let Some(viewport) = input_weak.upgrade() {
                    viewport.user_input.set(true);
                    viewport.anchor_pending.set(false);
                    viewport.anchor.borrow_mut().take();
                    // Stop before the next animation frame can override an
                    // upward wheel gesture while new content is arriving.
                    let upward_wheel = event
                        .dyn_ref::<web_sys::WheelEvent>()
                        .is_some_and(|event| event.delta_y() < 0.0);
                    let upward_key =
                        event
                            .dyn_ref::<web_sys::KeyboardEvent>()
                            .is_some_and(|event| {
                                matches!(event.key().as_str(), "ArrowUp" | "PageUp" | "Home")
                                    || event.key() == " " && event.shift_key()
                            });
                    if upward_wheel || upward_key {
                        viewport.following.set(false);
                    }
                    viewport.schedule();
                }
            });
            let scroll_weak = weak.clone();
            let scroll_callback = Closure::new(move |_event: Event| {
                if let Some(viewport) = scroll_weak.upgrade() {
                    viewport.observe_scroll();
                }
            });
            Self {
                scope: scope.to_owned(),
                session_key: session_key.to_owned(),
                generation,
                element: RefCell::new(None),
                content: RefCell::new(None),
                following: Cell::new(true),
                user_input: Cell::new(false),
                last_top: Cell::new(0),
                anchor_pending: Cell::new(false),
                anchor: RefCell::new(None),
                frame: Cell::new(None),
                frame_callback,
                mutation: MutationObserver::new(mutation_callback.as_ref().unchecked_ref()).ok(),
                _mutation_callback: mutation_callback,
                resize: ResizeObserver::new(resize_callback.as_ref().unchecked_ref()).ok(),
                _resize_callback: resize_callback,
                input_callback,
                scroll_callback,
            }
        });
        let options = web_sys::MutationObserverInit::new();
        options.set_child_list(true);
        options.set_character_data(true);
        options.set_subtree(true);
        options.set_attributes(true);
        options.set_attribute_filter(&js_sys::Array::of1(&"data-timeline-session".into()));
        // Session selection can precede mounting the transcript. Observe the
        // document so its eventual commit also attaches layout observers.
        if let Some(mutation) = &viewport.mutation {
            let _ = mutation.observe_with_options(&root, &options);
        }
        viewport.schedule();
        Some(viewport)
    }

    fn schedule(&self) {
        if self.frame.get().is_none() {
            if let Some(window) = web_sys::window() {
                self.frame.set(
                    window
                        .request_animation_frame(self.frame_callback.as_ref().unchecked_ref())
                        .ok(),
                );
            }
        }
    }

    fn detach(&self) {
        if let Some(resize) = &self.resize {
            resize.disconnect();
        }
        if let Some(element) = self.element.borrow_mut().take() {
            for event in INPUT_EVENTS {
                let _ = element.remove_event_listener_with_callback(
                    event,
                    self.input_callback.as_ref().unchecked_ref(),
                );
            }
            let _ = element.remove_event_listener_with_callback(
                "scroll",
                self.scroll_callback.as_ref().unchecked_ref(),
            );
        }
        self.content.borrow_mut().take();
    }

    fn current_element(&self) -> Option<HtmlElement> {
        let element = web_sys::window()?
            .document()?
            .query_selector(&format!(
                ".message-list[data-timeline-scope='{}']",
                self.scope
            ))
            .ok()??;
        if element.get_attribute("data-timeline-session").as_deref()
            != Some(self.session_key.as_str())
        {
            return None;
        }
        element.dyn_into().ok()
    }

    fn apply_layout(&self) {
        let Some(element) = self.current_element() else {
            return;
        };
        let content = element
            .query_selector("[data-timeline-content]")
            .ok()
            .flatten();
        if self.element.borrow().as_ref() != Some(&element) || *self.content.borrow() != content {
            self.detach();
            let options = web_sys::AddEventListenerOptions::new();
            options.set_passive(true);
            for event in INPUT_EVENTS {
                let _ = element.add_event_listener_with_callback_and_add_event_listener_options(
                    event,
                    self.input_callback.as_ref().unchecked_ref(),
                    &options,
                );
            }
            let _ = element.add_event_listener_with_callback(
                "scroll",
                self.scroll_callback.as_ref().unchecked_ref(),
            );
            if let Some(resize) = &self.resize {
                resize.observe(&element);
                if let Some(content) = &content {
                    resize.observe(content);
                }
            }
            *self.element.borrow_mut() = Some(element.clone());
            *self.content.borrow_mut() = content;
        }
        if let Some(anchor) = self.anchor.borrow().as_ref() {
            let top = match anchor
                .element
                .as_ref()
                .filter(|node| element.contains(Some(node)))
            {
                Some(node) => {
                    let offset = node.get_bounding_client_rect().top()
                        - element.get_bounding_client_rect().top();
                    element.scroll_top() + (offset - anchor.offset).round() as i32
                }
                None => anchor
                    .scroll_top
                    .saturating_add(element.scroll_height().saturating_sub(anchor.scroll_height)),
            };
            element.set_scroll_top(top);
        } else if self.following.get() && !self.anchor_pending.get() {
            element.set_scroll_top(element.scroll_height());
        }
        self.last_top.set(element.scroll_top());
        self.user_input.set(false);
    }

    fn observe_scroll(&self) {
        let Some(element) = self.current_element() else {
            return;
        };
        let top = element.scroll_top();
        let user_input = self.user_input.replace(false);
        let moved_up = top < self.last_top.get().saturating_sub(1);
        if !self.anchor_pending.get() && self.anchor.borrow().is_none() {
            // Our own scroll writes update last_top synchronously, before
            // their queued scroll events. An upward move can therefore stop
            // following even while another DOM/layout commit is pending.
            // Browser clamping after content shrinks remains near the end.
            if user_input || moved_up {
                self.following.set(near_end(&element));
            } else if !self.following.get() && near_end(&element) {
                self.following.set(true);
            }
        }
        self.last_top.set(top);
    }
}

impl Drop for TimelineViewport {
    fn drop(&mut self) {
        if let Some(mutation) = &self.mutation {
            mutation.disconnect();
        }
        self.detach();
        if let (Some(window), Some(frame)) = (web_sys::window(), self.frame.take()) {
            let _ = window.cancel_animation_frame(frame);
        }
    }
}

fn near_end(element: &HtmlElement) -> bool {
    element
        .scroll_height()
        .saturating_sub(element.client_height())
        .saturating_sub(element.scroll_top())
        <= FOLLOW_EDGE_PX
}

/// Starts a new viewport generation, invalidating old session callbacks and anchors.
pub fn begin_timeline_session(scope: &str, session_key: &str) {
    let generation = GENERATION.with(|counter| {
        let value = counter.get().wrapping_add(1);
        counter.set(value);
        value
    });
    VIEWPORTS.with(|slots| {
        slots.borrow_mut().remove(scope);
        if let Some(viewport) = TimelineViewport::create(scope, session_key, generation) {
            slots.borrow_mut().insert(scope.to_owned(), viewport);
        }
    });
}

/// Releases observers and pending layout work when the conversation unmounts.
pub fn end_timeline_session(scope: &str) {
    VIEWPORTS.with(|slots| slots.borrow_mut().remove(scope));
}

pub fn set_timeline_follow(scope: &str, follow: bool) {
    VIEWPORTS.with(|slots| {
        if let Some(viewport) = slots.borrow().get(scope) {
            viewport.following.set(follow);
            if follow {
                viewport.anchor_pending.set(false);
                viewport.anchor.borrow_mut().take();
                viewport.user_input.set(false);
                viewport.schedule();
            }
        }
    });
}

/// Explicit latest-message navigation also follows subsequent layout growth.
pub fn scroll_timeline_to_end(scope: &str) {
    set_timeline_follow(scope, true);
}

pub fn timeline_is_near_end(scope: &str) -> bool {
    VIEWPORTS.with(|slots| {
        slots
            .borrow()
            .get(scope)
            .and_then(|viewport| viewport.current_element())
            .is_none_or(|element| near_end(&element))
    })
}

/// Unlike a geometry-only check, content growth does not imply user scrolling.
pub fn timeline_should_follow(scope: &str) -> bool {
    VIEWPORTS.with(|slots| {
        slots.borrow().get(scope).is_none_or(|viewport| {
            viewport.observe_scroll();
            viewport.following.get() && !viewport.anchor_pending.get()
        })
    })
}

pub fn timeline_scroll_anchor(scope: &str) -> Option<TimelineScrollAnchor> {
    VIEWPORTS.with(|slots| {
        let slots = slots.borrow();
        let viewport = slots.get(scope)?;
        let element = viewport.current_element()?;
        let rect = element.get_bounding_client_rect();
        let content = viewport.content.borrow();
        let children = content.as_ref()?.children();
        let visible = (0..children.length())
            .filter_map(|index| children.item(index))
            .filter(|child| {
                !child
                    .matches(".history-pagination, .timeline-skeleton")
                    .unwrap_or(false)
            })
            .find(|child| child.get_bounding_client_rect().bottom() > rect.top());
        let offset = visible.as_ref().map_or(0.0, |child| {
            child.get_bounding_client_rect().top() - rect.top()
        });
        viewport.following.set(false);
        viewport.user_input.set(false);
        viewport.anchor_pending.set(true);
        viewport.anchor.borrow_mut().take();
        Some(TimelineScrollAnchor {
            generation: viewport.generation,
            element: visible,
            offset,
            scroll_top: element.scroll_top(),
            scroll_height: element.scroll_height(),
        })
    })
}

/// Restore after the actual commit, and retain the visible item across delayed
/// image/layout growth. A user gesture releases the anchor immediately.
pub fn restore_timeline_scroll_anchor(scope: &str, anchor: TimelineScrollAnchor) {
    VIEWPORTS.with(|slots| {
        if let Some(viewport) = slots
            .borrow()
            .get(scope)
            .filter(|viewport| viewport.generation == anchor.generation)
        {
            viewport.anchor_pending.set(false);
            *viewport.anchor.borrow_mut() = Some(anchor);
            viewport.schedule();
        }
    });
}
