use crossterm::event::{KeyCode, KeyEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalAction {
    None,
    Close,
}

pub trait ModalView: Send {
    fn title(&self) -> &str;
    fn lines(&self) -> Vec<String>;
    fn on_key(&mut self, key: KeyEvent) -> (bool, ModalAction) {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => (true, ModalAction::Close),
            _ => (false, ModalAction::None),
        }
    }
}

pub struct HelpModal;

impl HelpModal {
    pub fn new() -> Self {
        Self
    }
}

impl ModalView for HelpModal {
    fn title(&self) -> &str {
        "Help"
    }

    fn lines(&self) -> Vec<String> {
        vec![
            "Enter        Submit input".to_string(),
            "Ctrl+C       Interrupt current task".to_string(),
            "Esc          Close modal / quit app".to_string(),
            "F1 or Ctrl+K Toggle this help".to_string(),
            "/exit        Quit session".to_string(),
        ]
    }
}
