#[derive(Debug, Clone)]
pub struct Shimmer {
    pub enabled: bool,
    phase: usize,
    table: [u8; 9],
}

impl Shimmer {
    pub fn new() -> Self {
        Self {
            enabled: false,
            phase: 0,
            table: [0, 1, 2, 3, 4, 3, 2, 1, 0],
        }
    }

    pub fn tick(&mut self) -> bool {
        if !self.enabled {
            return false;
        }
        let old = self.phase;
        self.phase = (self.phase + 1) % self.table.len();
        old != self.phase
    }

    pub fn intensity_at(&self, i: usize, len: usize) -> u8 {
        if len == 0 {
            return 0;
        }
        let center = (self.phase * len) / self.table.len();
        let dist = i.abs_diff(center);
        let band = 4;
        if dist >= band {
            return 0;
        }
        let idx = (dist * (self.table.len() - 1)) / band.max(1);
        self.table[idx]
    }
}
