pub mod composer;
pub mod footer;
pub mod modal;

use crossterm::event::KeyEvent;

use composer::Composer;
use footer::Footer;
use modal::{HelpModal, ModalAction, ModalView};

pub struct BottomPane {
    pub composer: Composer,
    pub footer: Footer,
    pub modals: Vec<Box<dyn ModalView>>,
}

impl BottomPane {
    pub fn new() -> Self {
        Self {
            composer: Composer::new(),
            footer: Footer::new(),
            modals: Vec::new(),
        }
    }

    pub fn handle_key_modal(&mut self, key: KeyEvent) -> bool {
        if let Some(modal) = self.modals.last_mut() {
            let (handled, action) = modal.on_key(key);
            if matches!(action, ModalAction::Close) {
                self.modals.pop();
            }
            return handled;
        }
        false
    }

    pub fn handle_key_composer(&mut self, key: KeyEvent) -> bool {
        self.composer.on_key(key)
    }

    pub fn toggle_help_modal(&mut self) {
        if self
            .modals
            .last()
            .map(|m| m.title() == "Help")
            .unwrap_or(false)
        {
            self.modals.pop();
            return;
        }
        self.modals.push(Box::new(HelpModal::new()));
    }

    pub fn top_modal(&self) -> Option<&dyn ModalView> {
        self.modals.last().map(|m| m.as_ref())
    }
}
