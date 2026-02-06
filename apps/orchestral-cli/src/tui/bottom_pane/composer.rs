use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Debug, Default, Clone)]
pub struct Composer {
    pub buffer: String,
    pub cursor: usize,
}

impl Composer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                if self.cursor <= self.buffer.len() {
                    self.buffer.insert(self.cursor, c);
                    self.cursor += c.len_utf8();
                    true
                } else {
                    false
                }
            }
            KeyCode::Backspace => {
                if self.cursor == 0 {
                    return false;
                }
                let prev_idx = prev_grapheme_boundary(&self.buffer, self.cursor);
                self.buffer.drain(prev_idx..self.cursor);
                self.cursor = prev_idx;
                true
            }
            KeyCode::Delete => {
                if self.cursor >= self.buffer.len() {
                    return false;
                }
                let next_idx = next_grapheme_boundary(&self.buffer, self.cursor);
                self.buffer.drain(self.cursor..next_idx);
                true
            }
            KeyCode::Left => {
                if self.cursor == 0 {
                    return false;
                }
                self.cursor = prev_grapheme_boundary(&self.buffer, self.cursor);
                true
            }
            KeyCode::Right => {
                if self.cursor >= self.buffer.len() {
                    return false;
                }
                self.cursor = next_grapheme_boundary(&self.buffer, self.cursor);
                true
            }
            KeyCode::Home => {
                if self.cursor == 0 {
                    return false;
                }
                self.cursor = 0;
                true
            }
            KeyCode::End => {
                if self.cursor == self.buffer.len() {
                    return false;
                }
                self.cursor = self.buffer.len();
                true
            }
            _ => false,
        }
    }

    pub fn take_buffer(&mut self) -> String {
        let out = self.buffer.trim().to_string();
        self.buffer.clear();
        self.cursor = 0;
        out
    }
}

fn prev_grapheme_boundary(text: &str, idx: usize) -> usize {
    let mut i = idx.saturating_sub(1);
    while !text.is_char_boundary(i) {
        i = i.saturating_sub(1);
    }
    i
}

fn next_grapheme_boundary(text: &str, idx: usize) -> usize {
    let mut i = idx.saturating_add(1).min(text.len());
    while i < text.len() && !text.is_char_boundary(i) {
        i += 1;
    }
    i
}
