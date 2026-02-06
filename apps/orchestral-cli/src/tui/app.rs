use std::collections::HashMap;
use std::time::Instant;

use crate::runtime::{ActivityKind, TransientSlot};

use super::bottom_pane::BottomPane;
use super::widgets::{Shimmer, Spinner};

#[derive(Debug, Clone)]
pub struct ActivityGroup {
    pub key: String,
    pub turn_id: usize,
    pub kind: ActivityKind,
    pub title: String,
    pub items: Vec<String>,
    pub failed: bool,
}

pub struct App {
    pub dirty: bool,
    pub mode: AppMode,
    pub history: Vec<String>,
    pub current_turn_id: usize,
    pub activities: Vec<ActivityGroup>,
    pub active_activity: Option<usize>,
    pub activity_index: HashMap<String, usize>,
    pub transient: HashMap<TransientSlot, String>,
    pub bottom: BottomPane,
    pub spinner: Spinner,
    pub shimmer: Shimmer,
    pub width: u16,
    pub height: u16,
    pub should_quit: bool,
    pending_submit: Option<String>,
    pending_interrupt: bool,
    pub once: bool,
    pub submitted_once: bool,
    pub turn_started_at: Option<Instant>,
    pub turn_elapsed_reported: bool,
    pub approved_command_prefixes: Vec<String>,
    pub assistant_stream_line: Option<usize>,
    pub history_scroll_back: u16,
    pub mouse_capture_enabled: bool,
    pending_mouse_capture_toggle: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppMode {
    Idle,
    Planning,
    Executing,
    WaitingInput,
}

impl App {
    pub fn new(width: u16, height: u16, once: bool) -> Self {
        Self {
            dirty: true,
            mode: AppMode::Idle,
            history: Vec::new(),
            current_turn_id: 0,
            activities: Vec::new(),
            active_activity: None,
            activity_index: HashMap::new(),
            transient: HashMap::from([(
                crate::runtime::TransientSlot::Footer,
                "Ready. Enter submit | Ctrl+K help | /exit quit".to_string(),
            )]),
            bottom: BottomPane::new(),
            spinner: Spinner::new(),
            shimmer: Shimmer::new(),
            width,
            height,
            should_quit: false,
            pending_submit: None,
            pending_interrupt: false,
            once,
            submitted_once: false,
            turn_started_at: None,
            turn_elapsed_reported: false,
            approved_command_prefixes: Vec::new(),
            assistant_stream_line: None,
            history_scroll_back: 0,
            mouse_capture_enabled: true,
            pending_mouse_capture_toggle: None,
        }
    }

    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn queue_submit(&mut self, input: String) {
        self.pending_submit = Some(input);
    }

    pub fn take_pending_submit(&mut self) -> Option<String> {
        self.pending_submit.take()
    }

    pub fn queue_interrupt(&mut self) {
        self.pending_interrupt = true;
    }

    pub fn take_pending_interrupt(&mut self) -> bool {
        if self.pending_interrupt {
            self.pending_interrupt = false;
            true
        } else {
            false
        }
    }

    pub fn remember_approved_prefix(&mut self, prefix: String) {
        if !self
            .approved_command_prefixes
            .iter()
            .any(|saved| saved == &prefix)
        {
            self.approved_command_prefixes.push(prefix);
        }
    }

    pub fn is_command_auto_approved(&self, command: &str) -> bool {
        self.approved_command_prefixes
            .iter()
            .any(|prefix| command.starts_with(prefix))
    }

    pub fn queue_set_mouse_capture(&mut self, enabled: bool) {
        self.pending_mouse_capture_toggle = Some(enabled);
    }

    pub fn take_pending_mouse_capture_toggle(&mut self) -> Option<bool> {
        self.pending_mouse_capture_toggle.take()
    }
}
