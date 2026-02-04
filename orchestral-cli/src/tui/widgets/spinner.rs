#[derive(Debug, Clone)]
pub struct Spinner {
    frames: &'static [&'static str],
    idx: usize,
    pub enabled: bool,
}

impl Spinner {
    pub fn new() -> Self {
        Self {
            frames: &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
            idx: 0,
            enabled: false,
        }
    }

    pub fn tick(&mut self) -> bool {
        if !self.enabled {
            return false;
        }
        let old = self.idx;
        self.idx = (self.idx + 1) % self.frames.len();
        old != self.idx
    }

    pub fn current(&self) -> &'static str {
        self.frames[self.idx]
    }
}
