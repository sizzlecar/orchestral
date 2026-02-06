use crossterm::event::{KeyCode, KeyEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalAction {
    None,
    Close,
    SubmitApprove,
    SubmitDeny,
    ApproveAndRemember,
}

pub trait ModalView: Send {
    fn title(&self) -> &str;
    fn lines(&self) -> Vec<String>;
    fn command_prefix(&self) -> Option<&str> {
        None
    }
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

pub struct ApprovalModal {
    reason: String,
    command: Option<String>,
    command_prefix: Option<String>,
    selected: usize,
}

impl ApprovalModal {
    pub fn new(reason: String, command: Option<String>) -> Self {
        let command_prefix = command.as_deref().and_then(suggest_prefix);
        Self {
            reason,
            command,
            command_prefix,
            selected: 0,
        }
    }
}

impl ModalView for ApprovalModal {
    fn title(&self) -> &str {
        "Approval Required"
    }

    fn lines(&self) -> Vec<String> {
        let mut out = vec!["Would you like to run the following command?".to_string()];
        out.push(String::new());
        out.push(format!("Reason: {}", self.reason));
        if let Some(command) = &self.command {
            out.push(String::new());
            out.push(format!("$ {}", command));
        }
        out.push(String::new());
        out.push(self.option_line(0, "Yes, proceed"));
        if let Some(prefix) = &self.command_prefix {
            out.push(self.option_line(
                1,
                &format!("Yes, and don't ask again for `{}` in this session", prefix),
            ));
        } else {
            out.push(self.option_line(1, "Yes, and don't ask again in this session"));
        }
        out.push(self.option_line(2, "No, deny"));
        out.push(String::new());
        out.push("Use Up/Down to choose, Enter to confirm.".to_string());
        out
    }

    fn command_prefix(&self) -> Option<&str> {
        self.command_prefix.as_deref()
    }

    fn on_key(&mut self, key: KeyEvent) -> (bool, ModalAction) {
        match key.code {
            KeyCode::Up => {
                self.selected = self.selected.saturating_sub(1);
                (true, ModalAction::None)
            }
            KeyCode::Down => {
                self.selected = (self.selected + 1).min(2);
                (true, ModalAction::None)
            }
            KeyCode::Enter => {
                let action = match self.selected {
                    0 => ModalAction::SubmitApprove,
                    1 => ModalAction::ApproveAndRemember,
                    _ => ModalAction::SubmitDeny,
                };
                (true, action)
            }
            KeyCode::Char('1') | KeyCode::Char('y') | KeyCode::Char('Y') => {
                (true, ModalAction::SubmitApprove)
            }
            KeyCode::Char('p') | KeyCode::Char('P') | KeyCode::Char('2') => {
                (true, ModalAction::ApproveAndRemember)
            }
            KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Char('3') => {
                (true, ModalAction::SubmitDeny)
            }
            _ => (false, ModalAction::None),
        }
    }
}

impl ApprovalModal {
    fn option_line(&self, idx: usize, label: &str) -> String {
        let marker = if self.selected == idx { ">" } else { " " };
        format!("{} {}. {}", marker, idx + 1, label)
    }
}

fn suggest_prefix(command: &str) -> Option<String> {
    let tokens: Vec<&str> = command.split_whitespace().take(3).collect();
    if tokens.is_empty() {
        None
    } else {
        Some(tokens.join(" "))
    }
}
