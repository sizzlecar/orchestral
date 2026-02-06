#[derive(Clone, Debug)]
pub struct ProgressState {
    pub step: usize,
    pub total: usize,
}

impl ProgressState {
    pub fn ratio(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.step as f64 / self.total as f64).clamp(0.0, 1.0)
    }
}
