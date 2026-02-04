#[derive(Debug, Clone)]
pub struct Footer {
    pub text: String,
}

impl Footer {
    pub fn new() -> Self {
        Self {
            text: "Enter submit | Ctrl+C interrupt | /exit quit".to_string(),
        }
    }
}
