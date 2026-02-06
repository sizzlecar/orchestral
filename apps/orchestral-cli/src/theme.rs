use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Theme {
    pub border: Style,
    pub title: Style,
    pub muted: Style,
    pub info: Style,
    pub success: Style,
    pub warn: Style,
    pub error: Style,
    pub input: Style,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            border: Style::default().fg(Color::DarkGray),
            title: Style::default()
                .fg(Color::Gray)
                .add_modifier(Modifier::BOLD),
            muted: Style::default().fg(Color::DarkGray),
            info: Style::default().fg(Color::Gray),
            success: Style::default().fg(Color::White),
            warn: Style::default().fg(Color::White),
            error: Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            input: Style::default().fg(Color::White),
        }
    }
}
