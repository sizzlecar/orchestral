//! The idle, empty-session welcome view. It is not part of model history.
use super::*;

const MARK: [&str; 5] = [
    "        ██      ",
    "          ██    ",
    "            ██  ",
    "          ██    ",
    "██████  ██    ██",
];

pub(super) fn render(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    if area.is_empty() {
        return;
    }
    let width = area.width.saturating_sub(CONTENT_PADDING * 2).min(76);
    let inner = Rect::new(
        area.x + (area.width - width) / 2,
        area.y,
        width,
        area.height,
    );
    let mut lines = Vec::new();
    if inner.height >= 14 && inner.width >= 48 {
        lines.push(Line::raw(""));
        for row in MARK {
            lines.push(Line::styled(row, ACCENT));
        }
        lines.push(Line::raw(""));
    }
    lines.push(Line::from(vec![
        Span::styled("_>. ", ACCENT.add_modifier(Modifier::BOLD)),
        Span::styled("ORCHESTRAL", ASSISTANT.add_modifier(Modifier::BOLD)),
        Span::styled(format!("  v{}", env!("CARGO_PKG_VERSION")), MUTED),
    ]));
    if inner.height >= 8 {
        lines.push(Line::styled(
            "A runtime for reliable, interactive AI agents.",
            MUTED,
        ));
        lines.push(Line::raw(""));
    }
    if inner.height >= 5 {
        lines.push(Line::from(vec![
            Span::styled("Model  ", MUTED),
            Span::raw(super::super::text::plain(&state.model).into_owned()),
        ]));
        let workspace = if state.workspace_path.is_empty() {
            &state.project
        } else {
            &state.workspace_path
        };
        lines.push(Line::from(vec![
            Span::styled("Folder ", MUTED),
            Span::raw(super::super::text::plain(workspace).into_owned()),
        ]));
    }
    if inner.height >= 8 {
        lines.push(Line::raw(""));
        lines.push(Line::styled("Type a task below. / opens commands.", MUTED));
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}
