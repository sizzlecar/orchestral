pub mod composer;
pub mod footer;
pub mod modal;

use crossterm::event::KeyEvent;

use composer::Composer;
use footer::Footer;
use modal::{ApprovalModal, HelpModal, ModalAction, ModalView};

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

    pub fn handle_key_modal(&mut self, key: KeyEvent) -> Option<(ModalAction, Option<String>)> {
        if let Some(modal) = self.modals.last_mut() {
            let (handled, action) = modal.on_key(key);
            let prefix = modal.command_prefix().map(str::to_string);
            if matches!(
                action,
                ModalAction::Close
                    | ModalAction::SubmitApprove
                    | ModalAction::SubmitDeny
                    | ModalAction::ApproveAndRemember
            ) {
                self.modals.pop();
            }
            if handled {
                return Some((action, prefix));
            }
        }
        None
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

    pub fn open_approval_modal(&mut self, reason: String, command: Option<String>) {
        self.modals
            .push(Box::new(ApprovalModal::new(reason, command)));
    }
}
