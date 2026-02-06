use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;
use unicode_width::UnicodeWidthStr;

use crate::runtime::{ActivityKind, TransientSlot};
use crate::theme::Theme;

use super::app::App;

pub fn draw(frame: &mut Frame, app: &App) {
    let theme = Theme::default();
    let lines = build_activity_lines(app, &theme);

    let area = frame.area();
    let input_height = 4u16.min(area.height.saturating_sub(1));
    let max_activity_height = area.height.saturating_sub(input_height);
    let desired_activity = (lines.len() as u16).saturating_add(2);
    let activity_height = desired_activity.clamp(3, max_activity_height.max(3));

    let chunks = Layout::vertical([
        Constraint::Length(activity_height),
        Constraint::Length(input_height),
    ])
    .split(area);

    render_history(frame, chunks[0], app, &theme, lines);
    render_input(frame, chunks[1], app, &theme);
    render_modal(frame, app, &theme);
}

fn render_history(
    frame: &mut Frame,
    area: Rect,
    app: &App,
    _theme: &Theme,
    lines: Vec<Line<'static>>,
) {
    let inner_height = area.height as usize;
    let max_scroll = lines.len().saturating_sub(inner_height) as u16;
    let back = app.history_scroll_back.min(max_scroll);
    let scroll = max_scroll.saturating_sub(back);
    let widget = Paragraph::new(lines)
        .block(Block::default().borders(Borders::NONE))
        .scroll((scroll, 0))
        .wrap(Wrap { trim: true });
    frame.render_widget(widget, area);
}

fn build_activity_lines(app: &App, theme: &Theme) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let recent_history: Vec<&String> = app.history.iter().rev().take(160).rev().collect();

    let mut turn_id = 0usize;
    for line in recent_history {
        if line.starts_with('>') {
            turn_id = turn_id.saturating_add(1);
            let prompt = line.trim_start_matches('>').trim_start();
            lines.push(Line::from(Span::styled(
                format!("› {}", prompt),
                theme.input.add_modifier(Modifier::BOLD),
            )));
            append_turn_lines(&mut lines, app, theme, turn_id);
            continue;
        }

        if line.starts_with("Error:") {
            lines.push(Line::from(Span::styled(line.clone(), theme.error)));
        } else {
            lines.push(Line::from(Span::styled(line.clone(), theme.muted)));
        }
    }

    lines
}

fn append_turn_lines(lines: &mut Vec<Line<'static>>, app: &App, theme: &Theme, turn_id: usize) {
    if app.current_turn_id == turn_id && app.spinner.enabled {
        let status = app
            .transient
            .get(&TransientSlot::Status)
            .map(String::as_str)
            .unwrap_or("Working...");
        lines.push(Line::from(shimmering_spans(
            &format!("• {} {}", app.spinner.current(), status),
            app,
            true,
            theme.muted.add_modifier(Modifier::ITALIC),
        )));
    }

    let mut order: Vec<ActivityKind> = Vec::new();
    for group in app.activities.iter().filter(|g| g.turn_id == turn_id) {
        let kind = group.kind;
        if !order.contains(&kind) {
            order.push(kind);
        }
    }

    let active_kind = app
        .active_activity
        .and_then(|idx| app.activities.get(idx))
        .filter(|g| g.turn_id == turn_id)
        .map(|g| g.kind);

    for kind in order {
        let header_style = if active_kind == Some(kind) {
            theme.input.add_modifier(Modifier::BOLD)
        } else {
            theme.muted.add_modifier(Modifier::BOLD)
        };
        lines.push(Line::from(shimmering_spans(
            &format!("• {}", kind_label(kind)),
            app,
            active_kind == Some(kind),
            header_style,
        )));

        for group in app
            .activities
            .iter()
            .filter(|g| g.turn_id == turn_id && g.kind == kind)
        {
            let step_style = if group.failed {
                theme.error
            } else {
                theme.input
            };
            lines.push(Line::from(vec![
                Span::styled("  └ ", theme.muted),
                Span::styled(group.title.clone(), step_style),
            ]));
            for item in &group.items {
                lines.push(Line::from(vec![
                    Span::styled("    · ", theme.muted),
                    Span::styled(item.clone(), theme.muted),
                ]));
            }
        }
    }

    lines.push(Line::from(""));
}

fn kind_label(kind: ActivityKind) -> &'static str {
    match kind {
        ActivityKind::Ran => "Ran",
        ActivityKind::Edited => "Edited",
        ActivityKind::Explored => "Explored",
    }
}

fn shimmering_spans(text: &str, app: &App, enabled: bool, base: Style) -> Vec<Span<'static>> {
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    chars
        .into_iter()
        .enumerate()
        .map(|(idx, ch)| {
            let intensity = if enabled {
                app.shimmer.intensity_at(idx, len)
            } else {
                0
            };
            let mut style = base;
            if intensity >= 2 {
                style = style.add_modifier(Modifier::BOLD);
            }
            Span::styled(ch.to_string(), style)
        })
        .collect()
}

fn render_input(frame: &mut Frame, area: Rect, app: &App, theme: &Theme) {
    let input_line = Line::from(vec![
        Span::styled("› ", theme.muted),
        Span::styled(app.bottom.composer.buffer.clone(), theme.input),
    ]);
    let widget = Paragraph::new(input_line)
        .style(theme.input)
        .block(Block::default().borders(Borders::NONE))
        .wrap(Wrap { trim: false });
    frame.render_widget(widget, area);

    let footer_area = Rect {
        x: area.x,
        y: area.y + area.height.saturating_sub(1),
        width: area.width,
        height: 1,
    };
    let footer_text = app
        .transient
        .get(&TransientSlot::Footer)
        .cloned()
        .unwrap_or_else(|| app.bottom.footer.text.clone());
    let footer = Paragraph::new(footer_text).style(theme.muted);
    frame.render_widget(footer, footer_area);

    if area.height >= 2 {
        let cursor_x = area.x
            + 2
            + UnicodeWidthStr::width(&app.bottom.composer.buffer[..app.bottom.composer.cursor])
                as u16;
        let cursor_y = area.y;
        frame.set_cursor_position((
            cursor_x.min(area.x + area.width.saturating_sub(1)),
            cursor_y,
        ));
    }
}

fn render_modal(frame: &mut Frame, app: &App, theme: &Theme) {
    let Some(modal) = app.bottom.top_modal() else {
        return;
    };
    let lines: Vec<Line> = modal
        .lines()
        .into_iter()
        .map(|line| Line::from(Span::styled(line, theme.input)))
        .collect();
    let area = top_anchored_rect(frame.area(), lines.len() as u16 + 2);
    frame.render_widget(Clear, area);
    let widget = Paragraph::new(lines).wrap(Wrap { trim: true }).block(
        Block::default()
            .title(Span::styled(modal.title(), theme.title))
            .borders(Borders::ALL)
            .border_style(theme.border),
    );
    frame.render_widget(widget, area);
}

fn top_anchored_rect(area: Rect, desired_height: u16) -> Rect {
    let max_width = area.width.saturating_sub(4).min(110);
    let width = max_width.max(48);
    let max_height = area.height.saturating_sub(6);
    let height = desired_height.clamp(8, max_height.max(8));
    Rect {
        x: area.x.saturating_add(2),
        y: area.y.saturating_add(2),
        width,
        height,
    }
}
