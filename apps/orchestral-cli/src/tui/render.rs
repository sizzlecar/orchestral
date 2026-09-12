use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, BorderType, Borders, Padding, Paragraph, Wrap};
use ratatui::Frame;
use unicode_width::UnicodeWidthStr;

use super::activity::{ActivityDetail, ActivityDetailStyle, ActivityStatus};
use super::state::{
    ApprovalChoice, PendingOverlay, TranscriptEntry, TranscriptRole, UiPhase, UiState,
};

use super::viewport::{self, Anchor, Row};

const MUTED: Style = Style::new().fg(Color::DarkGray);
const ACCENT: Style = Style::new().fg(Color::Cyan);
const USER: Style = Style::new().fg(Color::LightCyan);
const ASSISTANT: Style = Style::new();
const ERROR: Style = Style::new().fg(Color::LightRed);
const SUCCESS: Style = Style::new().fg(Color::Green);
const BORDER: Style = Style::new().fg(Color::Gray);
const DARK_BACKGROUND: Color = Color::Rgb(29, 32, 39);
const DARK_PANEL: Color = Color::Rgb(39, 43, 53);
const DARK_TEXT: Color = Color::Rgb(245, 240, 207);
const DARK_MUTED: Color = Color::Rgb(163, 168, 181);
const DARK_ACCENT: Color = Color::Rgb(131, 255, 107);
const DARK_CYAN: Color = Color::Rgb(99, 219, 234);
const DARK_BORDER: Color = Color::Rgb(85, 91, 107);
const CONTENT_PADDING: u16 = 2;
const WORKING_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

#[derive(Default)]
pub(crate) struct RenderCache {
    key: Option<(String, u16, bool)>,
    revision: u64,
    offsets: Vec<usize>,
    committed_rows: usize,
    rows: Vec<Row>,
}

#[cfg(test)]
pub(crate) fn render(frame: &mut Frame<'_>, state: &UiState) -> Option<Anchor> {
    render_cached(frame, state, &mut RenderCache::default())
}

pub(crate) fn render_cached(
    frame: &mut Frame<'_>,
    state: &UiState,
    cache: &mut RenderCache,
) -> Option<Anchor> {
    let area = frame.area();
    if area.width < 20 || area.height < 6 {
        frame.render_widget(
            Paragraph::new("_>. Orchestral\nResize to at least 20×6")
                .style(ERROR)
                .wrap(Wrap { trim: false }),
            area,
        );
        apply_theme(frame, state);
        return state.viewport.anchor.clone();
    }

    let completion = super::interaction::completion(state);
    let menu = state.menu.as_ref().or(completion.as_ref());
    // The header still shows the run phase when a tiny screen needs these rows
    // for a selector's title, filter and first actionable choice.
    let status_height =
        u16::from(shows_working_status(state.phase) && (menu.is_none() || area.height > 2 + 4));
    let body_height = area.height.saturating_sub(2 + status_height);
    // A question must retain an editable row. Approval and selectors instead
    // reserve their actions first; transcript history can temporarily be hidden.
    let input_minimum =
        u16::from(menu.is_none() && matches!(state.pending, Some(PendingOverlay::Input { .. })))
            * 2;
    let pending_height = if menu.is_some() {
        (area.height / 2).clamp(4, 10)
    } else {
        pending_height(state, area.width)
    }
    .min(body_height.saturating_sub(input_minimum));
    let composer_height = composer_height(state, area.width)
        .min((area.height / if state.input_expanded { 2 } else { 3 }).max(2))
        .min(body_height.saturating_sub(pending_height));
    let rows = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(status_height),
        Constraint::Length(pending_height),
        Constraint::Length(composer_height),
        Constraint::Length(1),
    ])
    .split(area);

    render_header(frame, rows[0], state);
    let anchor = render_transcript(frame, rows[1], state, cache);
    render_working_status(frame, rows[2], state);
    if let Some(menu) = menu {
        render_menu(frame, rows[3], menu);
    } else if let Some(pending) = &state.pending {
        render_pending(frame, rows[3], pending, state.approval_choice);
    }
    render_composer(frame, rows[4], state);
    render_footer(frame, rows[5], state);
    apply_theme(frame, state);
    anchor
}

fn apply_theme(frame: &mut Frame<'_>, state: &UiState) {
    for cell in &mut frame.buffer_mut().content {
        let safe = super::text::plain(cell.symbol());
        if let std::borrow::Cow::Owned(safe) = safe {
            cell.set_symbol(&safe);
        }
        if !state.color_enabled {
            cell.set_style(Style::reset());
        } else if state.theme == "light" {
            cell.bg = match cell.bg {
                Color::Green => Color::Rgb(36, 124, 72),
                Color::DarkGray => Color::Rgb(233, 237, 231),
                _ => Color::White,
            };
            if cell.bg == Color::Rgb(36, 124, 72) {
                cell.fg = Color::White;
            }
            if cell.fg == Color::DarkGray {
                cell.fg = Color::Rgb(75, 85, 99);
            }
            if cell.fg == Color::Gray {
                cell.fg = Color::Rgb(145, 151, 146);
            }
            if cell.fg == Color::Reset {
                cell.fg = Color::Black;
            }
            if cell.fg == Color::LightCyan {
                cell.fg = Color::Blue;
            }
            if cell.fg == Color::Cyan {
                cell.fg = Color::Rgb(0, 95, 115);
            }
            if cell.fg == Color::LightRed {
                cell.fg = Color::Red;
            }
        } else if state.theme == "dark" {
            cell.bg = match cell.bg {
                Color::Green => DARK_ACCENT,
                Color::DarkGray => DARK_PANEL,
                Color::Reset | Color::Black => DARK_BACKGROUND,
                other => other,
            };
            cell.fg = match cell.fg {
                Color::Reset | Color::White => DARK_TEXT,
                Color::DarkGray => DARK_MUTED,
                Color::Gray => DARK_BORDER,
                Color::Cyan | Color::Green => DARK_ACCENT,
                Color::LightCyan => DARK_CYAN,
                Color::LightRed => Color::Rgb(255, 120, 120),
                Color::Yellow => Color::Rgb(255, 230, 128),
                Color::Black => Color::Rgb(21, 23, 29),
                other => other,
            };
        } else {
            // Terminal mode must not leave our dark panel behind the user's
            // foreground. Keep the explicit contrast pair only on lime badges.
            if cell.bg == Color::DarkGray {
                cell.bg = Color::Reset;
            }
            if matches!(cell.fg, Color::DarkGray | Color::Gray) {
                cell.fg = Color::Reset;
            }
        }
    }
}

fn render_menu(frame: &mut Frame<'_>, area: Rect, menu: &super::menu::Menu) {
    frame.render_widget(
        Block::default().style(Style::new().bg(Color::DarkGray)),
        area,
    );
    let block = Block::default()
        .borders(Borders::TOP)
        .border_type(BorderType::Thick)
        .border_style(BORDER)
        .padding(Padding::horizontal(CONTENT_PADDING));
    let inner = block.inner(area);
    let mut lines = vec![Line::styled(
        compact_label(&menu.title, inner.width as usize),
        ACCENT.add_modifier(Modifier::BOLD),
    )];
    if let Some(detail) = &menu.detail {
        let rows = viewport::wrap(
            "detail",
            super::text::plain(detail)
                .lines()
                .map(|line| Line::raw(line.to_owned()))
                .collect(),
            inner.width.max(1) as usize,
        );
        let height = inner.height.saturating_sub(1) as usize;
        let top = menu.selected.min(rows.len().saturating_sub(height));
        lines.extend(rows.into_iter().skip(top).take(height).map(|row| row.text));
    } else {
        lines.push(Line::styled(
            compact_label(&format!("Filter: {}", menu.query), inner.width as usize),
            MUTED,
        ));
        let choices = menu.filtered();
        let show_descriptions = inner.height >= 4;
        let choice_height = if show_descriptions { 2 } else { 1 };
        let count = (inner.height.saturating_sub(2) as usize / choice_height).max(1);
        let start = menu.selected.saturating_sub(count - 1);
        if choices.is_empty() {
            lines.push(Line::styled("No matches", MUTED));
        }
        for (index, choice) in choices.iter().enumerate().skip(start).take(count) {
            let selected = index == menu.selected;
            lines.push(Line::styled(
                format!(
                    "{} {}",
                    if selected { "›" } else { " " },
                    compact_label(&choice.label, inner.width.saturating_sub(2) as usize)
                ),
                if selected {
                    Style::new()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    ASSISTANT
                },
            ));
            if show_descriptions {
                lines.push(Line::styled(
                    format!(
                        "  {}",
                        compact_label(&choice.description, inner.width.saturating_sub(2) as usize)
                    ),
                    MUTED,
                ));
            }
        }
    }
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    frame.render_widget(
        Block::default().style(Style::new().bg(Color::DarkGray)),
        area,
    );
    let brand = Style::new()
        .fg(Color::Black)
        .bg(Color::Green)
        .add_modifier(Modifier::BOLD);
    let (phase_icon, phase_label) = phase_badge(state.phase);
    let phase = format!("{phase_icon} {phase_label}  ");
    let columns = Layout::horizontal([
        Constraint::Min(0),
        Constraint::Length(u16::try_from(phase.width()).unwrap_or(14)),
    ])
    .split(area);
    let mark = if area.width < 36 {
        " _>. "
    } else {
        " _>. ORCHESTRAL "
    };
    let mut spans = vec![Span::raw(" "), Span::styled(mark, brand)];
    if area.width >= 54 {
        let label_width = usize::from(columns[0].width).saturating_sub(mark.width() + 8);
        let project = compact_label(&state.project, label_width / 3);
        let title = compact_label(
            &state.session_title,
            label_width.saturating_sub(project.width()),
        );
        spans.push(Span::styled(format!(" // {project} / {title}"), MUTED));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), columns[0]);
    frame.render_widget(
        Paragraph::new(phase)
            .style(phase_style(state.phase))
            .alignment(Alignment::Right),
        columns[1],
    );
}

fn compact_label(text: &str, width: usize) -> String {
    use unicode_segmentation::UnicodeSegmentation;
    let text = super::text::plain(text);
    if text.width() <= width {
        return text.into_owned();
    }
    if width == 0 {
        return String::new();
    }
    let mut result = String::new();
    for grapheme in text.graphemes(true) {
        if result.width() + grapheme.width() >= width {
            break;
        }
        result.push_str(grapheme);
    }
    result.push('…');
    result
}

fn render_transcript(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &UiState,
    cache: &mut RenderCache,
) -> Option<Anchor> {
    let block = Block::default().padding(Padding::horizontal(CONTENT_PADDING));
    let inner = block.inner(area);
    let width = inner.width.max(1);
    let key = (state.session_id.clone(), width, state.tools_expanded);
    if cache.key.as_ref() != Some(&key) {
        *cache = RenderCache {
            key: Some(key),
            revision: state.transcript_revision.wrapping_sub(1),
            ..Default::default()
        };
    }
    if cache.revision != state.transcript_revision {
        cache.rows.truncate(cache.committed_rows);
        let from = state
            .transcript_dirty_from
            .min(cache.offsets.len())
            .min(state.transcript.len());
        let row = cache.offsets.get(from).copied().unwrap_or(cache.rows.len());
        cache.rows.truncate(row);
        cache.offsets.truncate(from);
        for (index, entry) in state.transcript.iter().enumerate().skip(from) {
            cache.offsets.push(cache.rows.len());
            let mut lines = Vec::new();
            if index > 0 && should_separate(state.transcript[index - 1].role, entry.role) {
                lines.push(Line::default());
            }
            push_entry_lines(&mut lines, entry, width, state.tools_expanded);
            cache.rows.extend(viewport::wrap(
                entry.id.clone().unwrap_or_else(|| format!("entry-{index}")),
                lines,
                width as usize,
            ));
        }
        cache.committed_rows = cache.rows.len();
        let stream = state.streamed_text();
        if !stream.is_empty() {
            let mut lines = Vec::new();
            if !cache.rows.is_empty() {
                lines.push(Line::default());
            }
            push_markdown(&mut lines, "• ", &stream, ASSISTANT, true, width);
            cache
                .rows
                .extend(viewport::wrap(state.stream_key(), lines, width as usize));
        } else if cache.rows.is_empty() {
            cache.rows.extend(viewport::wrap(
                "welcome",
                vec![
                    Line::styled("A runtime for reliable, interactive AI agents.", ASSISTANT),
                    Line::default(),
                    Line::styled(
                        "Describe a task, paste context, or use @ to attach a file.",
                        MUTED,
                    ),
                    Line::styled("/ commands · /skills project skills · F1 help", MUTED),
                ],
                width as usize,
            ));
        }
        cache.revision = state.transcript_revision;
    }
    let rows = &cache.rows;
    let (top, anchor) = viewport::window(rows, inner.height as usize, &state.viewport);
    frame.render_widget(
        Paragraph::new(
            rows.iter()
                .skip(top)
                .take(inner.height as usize)
                .map(|row| row.text.clone())
                .collect::<Vec<_>>(),
        )
        .block(block),
        area,
    );
    anchor
}

fn should_separate(previous: TranscriptRole, current: TranscriptRole) -> bool {
    !matches!(
        (previous, current),
        (TranscriptRole::Tool, TranscriptRole::Tool)
    )
}

fn push_entry_lines(
    lines: &mut Vec<Line<'static>>,
    entry: &TranscriptEntry,
    width: u16,
    expanded: bool,
) {
    match entry.role {
        TranscriptRole::User => push_plain(
            lines,
            if entry.continuation { "  ↳ " } else { "› " },
            &entry.text,
            USER,
        ),
        TranscriptRole::Assistant => {
            push_markdown(lines, "• ", &entry.text, ASSISTANT, false, width)
        }
        TranscriptRole::System => push_plain(lines, "○ ", &entry.text, MUTED),
        TranscriptRole::Error => push_plain(lines, "■ ", &entry.text, ERROR),
        TranscriptRole::Tool => {
            let (symbol, style) = match entry.tool_status {
                Some(ActivityStatus::Running) => ("  • ", ACCENT),
                Some(ActivityStatus::Succeeded) => ("  ✓ ", MUTED),
                Some(ActivityStatus::Failed) => ("  × ", ERROR),
                Some(ActivityStatus::Cancelled) => ("  ■ ", Style::new().fg(Color::Yellow)),
                None => ("  · ", MUTED),
            };
            push_status_text(lines, symbol, &entry.text, style);
            let limit = if expanded {
                usize::MAX
            } else if entry.tool_status == Some(ActivityStatus::Failed) {
                8
            } else {
                3
            };
            let visible = entry.tool_details.len().min(limit);
            push_activity_details(lines, &entry.tool_details[..visible]);
            if visible < entry.tool_details.len() {
                lines.push(Line::styled(
                    format!(
                        "      … {} more detail lines · ctrl+o expand",
                        entry.tool_details.len() - visible
                    ),
                    MUTED,
                ));
            }
        }
    }
}

fn push_plain(lines: &mut Vec<Line<'static>>, prefix: &str, text: &str, style: Style) {
    let indent = " ".repeat(UnicodeWidthStr::width(prefix));
    let text = super::text::plain(text);
    for (index, part) in text.split('\n').enumerate() {
        let current_prefix = if index == 0 { prefix } else { &indent };
        lines.push(Line::from(vec![
            Span::styled(
                current_prefix.to_owned(),
                style.add_modifier(Modifier::BOLD),
            ),
            Span::styled(part.to_owned(), style),
        ]));
    }
}

fn push_status_text(lines: &mut Vec<Line<'static>>, prefix: &str, text: &str, style: Style) {
    let indent = " ".repeat(UnicodeWidthStr::width(prefix));
    let text = super::text::plain(text);
    for (index, part) in text.split('\n').enumerate() {
        lines.push(Line::from(vec![
            Span::styled(
                if index == 0 {
                    prefix.to_owned()
                } else {
                    indent.clone()
                },
                style.add_modifier(Modifier::BOLD),
            ),
            Span::styled(part.to_owned(), if index == 0 { ASSISTANT } else { MUTED }),
        ]));
    }
}

fn push_activity_details(lines: &mut Vec<Line<'static>>, details: &[ActivityDetail]) {
    for detail in details {
        let prefix = if detail.depth == 0 {
            "    └ "
        } else {
            "        "
        };
        let style = match detail.style {
            ActivityDetailStyle::Primary => ASSISTANT.add_modifier(Modifier::BOLD),
            ActivityDetailStyle::Context => MUTED,
            ActivityDetailStyle::Addition => SUCCESS,
            ActivityDetailStyle::Deletion | ActivityDetailStyle::Error => ERROR,
            ActivityDetailStyle::Muted => MUTED,
        };
        lines.push(Line::from(vec![
            Span::styled(prefix.to_owned(), MUTED),
            Span::styled(super::text::plain(&detail.text).into_owned(), style),
        ]));
    }
}

fn push_markdown(
    lines: &mut Vec<Line<'static>>,
    prefix: &str,
    text: &str,
    style: Style,
    streaming: bool,
    width: u16,
) {
    let indent = " ".repeat(UnicodeWidthStr::width(prefix));
    let mut first_content = true;
    let mut in_code_block = false;
    let start_len = lines.len();

    let text = super::text::plain(text);
    let source_lines = text.split('\n').collect::<Vec<_>>();
    let mut index = 0;
    while index < source_lines.len() {
        let source = source_lines[index];
        let trimmed = source.trim_start();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            index += 1;
            continue;
        }
        if source.is_empty() {
            lines.push(Line::default());
            index += 1;
            continue;
        }

        if !in_code_block {
            if let Some((table, consumed)) = MarkdownTable::parse(&source_lines[index..]) {
                push_markdown_table(
                    lines,
                    &table,
                    prefix,
                    &indent,
                    &mut first_content,
                    style,
                    width,
                );
                index += consumed;
                continue;
            }
        }

        let current_prefix = if first_content { prefix } else { &indent };
        first_content = false;
        let mut spans = vec![Span::styled(
            current_prefix.to_owned(),
            style.add_modifier(Modifier::BOLD),
        )];

        if in_code_block {
            spans.push(Span::styled("│ ", MUTED));
            spans.push(Span::styled(source.to_owned(), ACCENT));
        } else {
            let (marker, content, line_style) = markdown_line(source, style);
            if !marker.is_empty() {
                spans.push(Span::styled(marker, MUTED));
            }
            spans.extend(inline_markdown_spans(content, line_style));
        }
        lines.push(Line::from(spans));
        index += 1;
    }

    if lines.len() == start_len {
        lines.push(Line::from(Span::styled(prefix.to_owned(), style)));
    }
    if streaming {
        if let Some(last) = lines.iter_mut().rev().find(|line| !line.spans.is_empty()) {
            last.spans.push(Span::styled("▌", ACCENT));
        } else {
            lines.push(Line::from(Span::styled("• ▌", ACCENT)));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MarkdownTable {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl MarkdownTable {
    fn parse(lines: &[&str]) -> Option<(Self, usize)> {
        let headers = parse_table_row(lines.first()?)?;
        let separators = parse_table_row(lines.get(1)?)?;
        if headers.len() < 2
            || separators.len() != headers.len()
            || !separators.iter().all(|cell| is_table_separator(cell))
        {
            return None;
        }

        let mut rows = Vec::new();
        let mut consumed = 2;
        while let Some(line) = lines.get(consumed) {
            let Some(row) = parse_table_row(line) else {
                break;
            };
            if row.len() != headers.len() {
                break;
            }
            rows.push(row);
            consumed += 1;
        }
        Some((Self { headers, rows }, consumed))
    }
}

fn parse_table_row(source: &str) -> Option<Vec<String>> {
    let mut source = source.trim();
    if !source.contains('|') {
        return None;
    }
    source = source.strip_prefix('|').unwrap_or(source);
    source = source.strip_suffix('|').unwrap_or(source);
    let cells = source
        .split('|')
        .map(|cell| plain_table_cell(cell.trim()))
        .collect::<Vec<_>>();
    (cells.len() >= 2).then_some(cells)
}

fn plain_table_cell(cell: &str) -> String {
    cell.replace("**", "").replace('`', "")
}

fn is_table_separator(cell: &str) -> bool {
    let rule = cell.trim().trim_start_matches(':').trim_end_matches(':');
    rule.len() >= 3 && rule.chars().all(|character| character == '-')
}

fn push_markdown_table(
    lines: &mut Vec<Line<'static>>,
    table: &MarkdownTable,
    prefix: &str,
    indent: &str,
    first_content: &mut bool,
    style: Style,
    width: u16,
) {
    let prefix_width = UnicodeWidthStr::width(prefix);
    let available = usize::from(width).saturating_sub(prefix_width).max(1);
    let columns = table.headers.len();
    let separator_width = columns.saturating_sub(1).saturating_mul(3);
    let content_width = available.saturating_sub(separator_width);

    if content_width < columns.saturating_mul(4) {
        push_vertical_table(lines, table, prefix, indent, first_content, style);
        return;
    }

    let base_width = content_width / columns;
    let remainder = content_width % columns;
    let widths = (0..columns)
        .map(|index| base_width + usize::from(index < remainder))
        .collect::<Vec<_>>();

    push_table_row(
        lines,
        &table.headers,
        &widths,
        prefix,
        indent,
        first_content,
        style.add_modifier(Modifier::BOLD),
    );
    let separator = widths
        .iter()
        .map(|width| "─".repeat(*width))
        .collect::<Vec<_>>()
        .join("─┼─");
    lines.push(Line::from(vec![
        Span::styled(indent.to_owned(), MUTED),
        Span::styled(separator, MUTED),
    ]));
    for row in &table.rows {
        push_table_row(lines, row, &widths, prefix, indent, first_content, style);
    }
}

fn push_table_row(
    lines: &mut Vec<Line<'static>>,
    cells: &[String],
    widths: &[usize],
    prefix: &str,
    indent: &str,
    first_content: &mut bool,
    style: Style,
) {
    let wrapped = cells
        .iter()
        .zip(widths)
        .map(|(cell, width)| wrap_display_width(cell, *width))
        .collect::<Vec<_>>();
    let row_height = wrapped.iter().map(Vec::len).max().unwrap_or(1);
    for line_index in 0..row_height {
        let mut spans = vec![Span::styled(
            take_content_prefix(prefix, indent, first_content),
            MUTED,
        )];
        for (column, width) in widths.iter().enumerate() {
            if column > 0 {
                spans.push(Span::styled(" │ ", MUTED));
            }
            let value = wrapped[column]
                .get(line_index)
                .map(String::as_str)
                .unwrap_or("");
            let padding = width.saturating_sub(UnicodeWidthStr::width(value));
            spans.push(Span::styled(
                format!("{value}{}", " ".repeat(padding)),
                style,
            ));
        }
        lines.push(Line::from(spans));
    }
}

fn push_vertical_table(
    lines: &mut Vec<Line<'static>>,
    table: &MarkdownTable,
    prefix: &str,
    indent: &str,
    first_content: &mut bool,
    style: Style,
) {
    for (row_index, row) in table.rows.iter().enumerate() {
        if row_index > 0 {
            lines.push(Line::default());
        }
        for (header, value) in table.headers.iter().zip(row) {
            lines.push(Line::from(vec![
                Span::styled(take_content_prefix(prefix, indent, first_content), MUTED),
                Span::styled(format!("{header}: "), style.add_modifier(Modifier::BOLD)),
                Span::styled(value.clone(), style),
            ]));
        }
    }
    if table.rows.is_empty() {
        lines.push(Line::from(vec![
            Span::styled(take_content_prefix(prefix, indent, first_content), MUTED),
            Span::styled(
                table.headers.join(" · "),
                style.add_modifier(Modifier::BOLD),
            ),
        ]));
    }
}

fn take_content_prefix(prefix: &str, indent: &str, first_content: &mut bool) -> String {
    let current = if *first_content { prefix } else { indent };
    *first_content = false;
    current.to_owned()
}

fn wrap_display_width(value: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut result = Vec::new();
    let mut line = String::new();
    let mut line_width = 0_usize;
    for character in value.chars() {
        let character_width = unicode_width::UnicodeWidthChar::width(character).unwrap_or(0);
        if line_width > 0 && line_width.saturating_add(character_width) > width {
            result.push(std::mem::take(&mut line));
            line_width = 0;
        }
        line.push(character);
        line_width = line_width.saturating_add(character_width);
    }
    if !line.is_empty() || result.is_empty() {
        result.push(line);
    }
    result
}

fn markdown_line(source: &str, style: Style) -> (String, &str, Style) {
    let trimmed = source.trim_start();
    let leading = &source[..source.len().saturating_sub(trimmed.len())];
    if let Some(content) = trimmed
        .strip_prefix("### ")
        .or_else(|| trimmed.strip_prefix("## "))
        .or_else(|| trimmed.strip_prefix("# "))
    {
        return (
            leading.to_owned(),
            content,
            style.add_modifier(Modifier::BOLD),
        );
    }
    if let Some(content) = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .or_else(|| trimmed.strip_prefix("+ "))
    {
        return (format!("{leading}– "), content, style);
    }
    if let Some(content) = trimmed.strip_prefix("> ") {
        return (format!("{leading}│ "), content, style);
    }
    (String::new(), source, style)
}

fn inline_markdown_spans(mut text: &str, style: Style) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    while !text.is_empty() {
        let bold = closed_delimiter(text, "**");
        let code = closed_delimiter(text, "`");
        let selected = match (bold, code) {
            (Some(bold), Some(code)) if bold.0 <= code.0 => Some((bold, "**", true)),
            (Some(_), Some(code)) => Some((code, "`", false)),
            (Some(bold), None) => Some((bold, "**", true)),
            (None, Some(code)) => Some((code, "`", false)),
            (None, None) => None,
        };
        let Some(((start, end), delimiter, is_bold)) = selected else {
            spans.push(Span::styled(text.to_owned(), style));
            break;
        };
        if start > 0 {
            spans.push(Span::styled(text[..start].to_owned(), style));
        }
        let content_start = start + delimiter.len();
        let content_end = end;
        let token_style = if is_bold {
            style.add_modifier(Modifier::BOLD)
        } else {
            ACCENT
        };
        spans.push(Span::styled(
            text[content_start..content_end].to_owned(),
            token_style,
        ));
        text = &text[end + delimiter.len()..];
    }
    spans
}

fn closed_delimiter(text: &str, delimiter: &str) -> Option<(usize, usize)> {
    let start = text.find(delimiter)?;
    let content_start = start + delimiter.len();
    let end = text[content_start..].find(delimiter)? + content_start;
    (end > content_start).then_some((start, end))
}

fn shows_working_status(phase: UiPhase) -> bool {
    matches!(phase, UiPhase::Running | UiPhase::Cancelling)
}

fn render_working_status(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    if area.height == 0 {
        return;
    }
    let (label, style) = match state.phase {
        UiPhase::Running => ("Working", ACCENT),
        UiPhase::Cancelling => ("Stopping", Style::new().fg(Color::Yellow)),
        _ => return,
    };
    let frame_index = usize::try_from(state.animation_frame).unwrap_or(0) % WORKING_FRAMES.len();
    let timer = if area.width < 54 {
        format!(" {}", fmt_elapsed_compact(state.working_elapsed.as_secs()))
    } else if state.phase == UiPhase::Running {
        format!(
            " ({} · ctrl+c to interrupt)",
            fmt_elapsed_compact(state.working_elapsed.as_secs())
        )
    } else {
        format!(
            " ({})",
            fmt_elapsed_compact(state.working_elapsed.as_secs())
        )
    };
    let mut spans = vec![
        Span::styled(WORKING_FRAMES[frame_index], style),
        Span::raw(" "),
        Span::styled(label, style.add_modifier(Modifier::BOLD)),
        Span::styled(timer, MUTED),
    ];
    let process_count = state.active_process_count();
    if process_count > 0 {
        spans.push(Span::styled(
            format!(
                " · {process_count} background terminal{} running",
                if process_count == 1 { "" } else { "s" }
            ),
            MUTED,
        ));
    } else if let Some(detail) = state.working_detail.as_deref() {
        spans.push(Span::styled(format!(" · {detail}"), MUTED));
    }
    frame.render_widget(
        Paragraph::new(Line::from(spans))
            .block(Block::default().padding(Padding::horizontal(CONTENT_PADDING))),
        area,
    );
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    if area.height == 0 {
        return;
    }
    frame.render_widget(
        Block::default().style(Style::new().bg(Color::DarkGray)),
        area,
    );
    let line_count = state.composer.lines().count();
    let title = if line_count > 20 {
        format!(
            " {line_count} lines · ctrl+p {} ",
            if state.input_expanded {
                "collapse"
            } else {
                "expand"
            }
        )
    } else {
        String::new()
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::TOP)
        .border_type(BorderType::Thick)
        .border_style(if state.phase == UiPhase::WaitingInput {
            USER
        } else {
            BORDER
        })
        .padding(Padding::new(CONTENT_PADDING, CONTENT_PADDING, 0, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let prompt_width = 2_u16.min(inner.width);
    let prompt_area = Rect {
        width: prompt_width,
        ..inner
    };
    let content_area = Rect {
        x: inner.x.saturating_add(prompt_width),
        width: inner.width.saturating_sub(prompt_width),
        ..inner
    };
    frame.render_widget(
        Paragraph::new("› ").style(
            if !matches!(state.phase, UiPhase::WaitingApproval | UiPhase::Cancelling) {
                ACCENT.add_modifier(Modifier::BOLD)
            } else {
                MUTED
            },
        ),
        prompt_area,
    );
    let mut cursor = None;
    if state.composer.is_empty() {
        frame.render_widget(
            Paragraph::new(Text::from(Line::from(Span::styled(
                if state.request_submission_pending() {
                    "Response submitted; waiting for confirmation…"
                } else {
                    composer_placeholder(state.phase)
                },
                MUTED,
            )))),
            content_area,
        );
    } else {
        let layout = composer_layout(
            &state.composer,
            state.composer_cursor,
            usize::from(content_area.width.max(1)),
        );
        let scroll = layout.scroll_for_height(usize::from(content_area.height.max(1)));
        cursor = Some((
            layout.cursor_column,
            layout.cursor_row.saturating_sub(scroll),
        ));
        frame.render_widget(
            Paragraph::new(Text::from(
                layout
                    .lines
                    .into_iter()
                    .map(|line| Line::styled(line, ASSISTANT))
                    .collect::<Vec<_>>(),
            ))
            .scroll((u16::try_from(scroll).unwrap_or(u16::MAX), 0)),
            content_area,
        );
    }

    if state.menu.is_none()
        && !state.request_submission_pending()
        && !matches!(state.phase, UiPhase::WaitingApproval | UiPhase::Cancelling)
        && content_area.width > 0
        && content_area.height > 0
    {
        let (column, row) = cursor.unwrap_or_default();
        frame.set_cursor_position((
            content_area.x
                + u16::try_from(column)
                    .unwrap_or(u16::MAX)
                    .min(content_area.width - 1),
            content_area.y
                + u16::try_from(row)
                    .unwrap_or(u16::MAX)
                    .min(content_area.height - 1),
        ));
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ComposerLayout {
    lines: Vec<String>,
    cursor_row: usize,
    cursor_column: usize,
}

impl ComposerLayout {
    fn scroll_for_height(&self, height: usize) -> usize {
        self.cursor_row.saturating_sub(height.saturating_sub(1))
    }
}

fn composer_layout(value: &str, cursor: usize, width: usize) -> ComposerLayout {
    let width = width.max(1);
    let cursor = floor_char_boundary(value, cursor.min(value.len()));
    let mut lines = Vec::new();
    let mut cursor_position = None;
    let mut source_offset = 0_usize;
    let logical_lines = value.split('\n').collect::<Vec<_>>();

    for (logical_index, logical_line) in logical_lines.iter().enumerate() {
        let mut rendered_line = String::new();
        let mut rendered_width = 0_usize;
        let mut grapheme_offset = 0_usize;
        let span = Span::raw(*logical_line);

        for grapheme in span.styled_graphemes(Style::default()) {
            let relative_start = logical_line[grapheme_offset..]
                .find(grapheme.symbol)
                .map_or(grapheme_offset, |found| grapheme_offset + found);
            let relative_end = relative_start.saturating_add(grapheme.symbol.len());
            let grapheme_width = UnicodeWidthStr::width(grapheme.symbol);

            if rendered_width > 0 && rendered_width.saturating_add(grapheme_width) > width {
                lines.push(std::mem::take(&mut rendered_line));
                rendered_width = 0;
            }

            let grapheme_start = source_offset.saturating_add(relative_start);
            let grapheme_end = source_offset.saturating_add(relative_end);
            if cursor == grapheme_start {
                cursor_position = Some((lines.len(), rendered_width));
            } else if cursor > grapheme_start && cursor < grapheme_end {
                let within_grapheme = cursor.saturating_sub(grapheme_start);
                let prefix_width = UnicodeWidthStr::width(&grapheme.symbol[..within_grapheme]);
                cursor_position = Some((lines.len(), rendered_width.saturating_add(prefix_width)));
            }

            rendered_line.push_str(grapheme.symbol);
            rendered_width = rendered_width.saturating_add(grapheme_width);
            grapheme_offset = relative_end;
        }

        let logical_end = source_offset.saturating_add(logical_line.len());
        let cursor_ends_full_line = cursor == logical_end && rendered_width >= width;
        if cursor == logical_end {
            cursor_position = Some((
                lines.len() + usize::from(cursor_ends_full_line),
                if cursor_ends_full_line {
                    0
                } else {
                    rendered_width
                },
            ));
        }
        lines.push(rendered_line);

        let is_last_logical_line = logical_index + 1 == logical_lines.len();
        if is_last_logical_line && cursor_ends_full_line {
            lines.push(String::new());
        }
        source_offset = logical_end.saturating_add(usize::from(!is_last_logical_line));
    }

    let (cursor_row, cursor_column) = cursor_position.unwrap_or_default();
    ComposerLayout {
        lines,
        cursor_row,
        cursor_column,
    }
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}

fn composer_placeholder(phase: UiPhase) -> &'static str {
    match phase {
        UiPhase::Running => "Add guidance while Orchestral works…",
        UiPhase::WaitingInput => "Type your response…",
        UiPhase::WaitingApproval => "Press a to allow or d to deny",
        UiPhase::Cancelling => "Stopping the current run…",
        UiPhase::Failed => "Ask Orchestral to retry another way…",
        UiPhase::Incomplete => "Continue from the recorded progress…",
        _ => "Ask Orchestral to do anything…",
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    frame.render_widget(
        Block::default().style(Style::new().bg(Color::DarkGray)),
        area,
    );
    let (hint, compact_hint) = if state
        .menu
        .as_ref()
        .is_some_and(|menu| menu.toggle.is_some())
    {
        ("space toggle · ↑↓ read · esc back", "space · ↑↓ · esc")
    } else if state
        .menu
        .as_ref()
        .is_some_and(|menu| menu.detail.is_some())
    {
        ("↑↓ read · esc back", "↑↓ read · esc")
    } else if state.menu.is_some() {
        ("↑↓ select · enter open · esc return", "↑↓ enter · esc")
    } else if state.request_submission_pending() {
        ("response submitted · ctrl+c stop", "sent · ^C stop")
    } else if state.viewport.anchor.is_some() {
        if state.viewport.unread {
            ("new output · end to follow", "new · end follow")
        } else {
            ("history · end to follow", "end to follow")
        }
    } else {
        match state.phase {
            UiPhase::WaitingApproval => ("↑↓ select · a/d · enter confirm", "↑↓ a/d · enter"),
            UiPhase::WaitingInput => ("enter answer · ctrl+c stop", "enter · ^C stop"),
            UiPhase::Running => ("enter steer · ctrl+c stop", "enter · ^C stop"),
            UiPhase::Cancelling => ("stopping…", "stopping…"),
            _ => ("enter send · / commands · f1 help", "enter · / · F1"),
        }
    };
    let block = Block::default().padding(Padding::horizontal(CONTENT_PADDING));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if let Some(notice) = &state.ui_notice {
        frame.render_widget(
            Paragraph::new(compact_label(notice, inner.width as usize)).style(MUTED),
            inner,
        );
        return;
    }
    let hint = if hint.width() <= usize::from(inner.width) {
        hint
    } else {
        compact_hint
    };
    if area.width < 64 {
        frame.render_widget(Paragraph::new(hint).style(MUTED), inner);
        return;
    }
    let columns = Layout::horizontal([
        Constraint::Min(0),
        Constraint::Length(u16::try_from(hint.width()).unwrap_or(inner.width)),
    ])
    .split(inner);
    let context = state
        .context_budget
        .map_or_else(|| "—".to_owned(), |budget| format!("—/{budget}"));
    let metadata = compact_label(
        &format!("{} · context: {context}", state.model),
        columns[0].width.saturating_sub(2) as usize,
    );
    frame.render_widget(Paragraph::new(metadata).style(MUTED), columns[0]);
    frame.render_widget(Paragraph::new(hint).style(MUTED), columns[1]);
}

fn render_pending(
    frame: &mut Frame<'_>,
    area: Rect,
    pending: &PendingOverlay,
    approval_choice: ApprovalChoice,
) {
    if area.is_empty() {
        return;
    }
    frame.render_widget(
        Block::default().style(Style::new().bg(Color::DarkGray)),
        area,
    );
    match pending {
        PendingOverlay::Input { prompt, .. } => {
            let block = Block::default().padding(Padding::horizontal(CONTENT_PADDING));
            let inner = block.inner(area);
            let rows = Layout::vertical([
                Constraint::Length(1),
                Constraint::Min(0),
                Constraint::Length(u16::from(inner.height >= 3)),
            ])
            .split(inner);
            frame.render_widget(block, area);
            frame.render_widget(
                Paragraph::new(compact_label("? Input requested", inner.width as usize))
                    .style(USER.add_modifier(Modifier::BOLD)),
                rows[0],
            );
            frame.render_widget(
                Paragraph::new(prompt.as_str()).wrap(Wrap { trim: false }),
                rows[1],
            );
            frame.render_widget(
                Paragraph::new("Reply below, then press Enter").style(MUTED),
                rows[2],
            );
        }
        PendingOverlay::Approval {
            summary,
            session_approval_available,
            ..
        } => {
            let block = Block::default().padding(Padding::horizontal(CONTENT_PADDING));
            let inner = block.inner(area);
            let compact = inner.width < 22;
            let mut actions = vec![approval_option_line(
                'a',
                "Allow once",
                ApprovalChoice::Allow,
                approval_choice,
            )];
            if *session_approval_available {
                actions.push(approval_option_line(
                    's',
                    if compact {
                        "Session"
                    } else {
                        "Allow for session"
                    },
                    ApprovalChoice::AllowSession,
                    approval_choice,
                ));
            }
            actions.push(approval_option_line(
                'd',
                "Deny",
                ApprovalChoice::Deny,
                approval_choice,
            ));
            // On a minimum-height screen the header already names the phase.
            // Preserve the operation summary before the redundant panel title.
            let title_height = u16::from(inner.height as usize > actions.len() + 1);
            let rows = Layout::vertical([
                Constraint::Length(title_height),
                Constraint::Min(0),
                Constraint::Length(u16::try_from(actions.len()).unwrap_or(3)),
            ])
            .split(inner);
            frame.render_widget(block, area);
            frame.render_widget(
                Paragraph::new(Line::from(Span::styled(
                    "! Approval required",
                    Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                ))),
                rows[0],
            );
            frame.render_widget(
                Paragraph::new(summary.as_str()).wrap(Wrap { trim: false }),
                rows[1],
            );
            frame.render_widget(Paragraph::new(actions), rows[2]);
        }
    }
}

fn approval_option_line(
    key: char,
    label: &'static str,
    choice: ApprovalChoice,
    selected: ApprovalChoice,
) -> Line<'static> {
    let is_selected = choice == selected;
    let marker = if is_selected { "› " } else { "  " };
    let key_style = if is_selected {
        ACCENT.add_modifier(Modifier::BOLD)
    } else {
        MUTED.add_modifier(Modifier::BOLD)
    };
    let label_style = if is_selected { ASSISTANT } else { MUTED };
    Line::from(vec![
        Span::styled(format!("{marker}{key}"), key_style),
        Span::styled(format!("  {label}"), label_style),
    ])
}

fn pending_height(state: &UiState, width: u16) -> u16 {
    let inner_width = width.saturating_sub(2 * CONTENT_PADDING).max(1) as usize;
    match state.pending.as_ref() {
        Some(PendingOverlay::Input { prompt, .. }) => 2_u16.saturating_add(
            u16::try_from(wrapped_rows(prompt, inner_width).clamp(1, 3)).unwrap_or(3),
        ),
        Some(PendingOverlay::Approval {
            summary,
            session_approval_available,
            ..
        }) => {
            let actions = if *session_approval_available { 3 } else { 2 };
            1_u16.saturating_add(actions).saturating_add(
                u16::try_from(wrapped_rows(summary, inner_width).clamp(1, 3)).unwrap_or(3),
            )
        }
        None => 0,
    }
}

fn wrapped_rows(text: &str, width: usize) -> usize {
    text.lines()
        .map(|line| UnicodeWidthStr::width(line).max(1).div_ceil(width))
        .sum()
}

fn fmt_elapsed_compact(elapsed_seconds: u64) -> String {
    if elapsed_seconds < 60 {
        return format!("{elapsed_seconds}s");
    }
    if elapsed_seconds < 3_600 {
        return format!("{}m {:02}s", elapsed_seconds / 60, elapsed_seconds % 60);
    }
    format!(
        "{}h {:02}m {:02}s",
        elapsed_seconds / 3_600,
        (elapsed_seconds % 3_600) / 60,
        elapsed_seconds % 60
    )
}

fn composer_height(state: &UiState, width: u16) -> u16 {
    if matches!(state.phase, UiPhase::WaitingApproval | UiPhase::Cancelling) {
        return 0;
    }
    let inner_width = width.saturating_sub(2 * CONTENT_PADDING + 2).max(1) as usize;
    let rows = composer_layout(&state.composer, state.composer_cursor, inner_width)
        .lines
        .len()
        .clamp(1, 5);
    u16::try_from(rows).unwrap_or(5).saturating_add(2)
}

fn phase_style(phase: UiPhase) -> Style {
    match phase {
        UiPhase::Idle | UiPhase::Completed => Style::new().fg(Color::Green),
        UiPhase::Running => ACCENT,
        UiPhase::WaitingInput => USER,
        UiPhase::WaitingApproval => Style::new().fg(Color::Yellow),
        UiPhase::Cancelling | UiPhase::Cancelled | UiPhase::Incomplete => {
            Style::new().fg(Color::Yellow)
        }
        UiPhase::Failed => ERROR,
    }
}

fn phase_badge(phase: UiPhase) -> (&'static str, &'static str) {
    match phase {
        UiPhase::Idle => ("○", "ready"),
        UiPhase::Running => ("●", "running"),
        UiPhase::WaitingInput => ("?", "input"),
        UiPhase::WaitingApproval => ("!", "approval"),
        UiPhase::Cancelling => ("◌", "stopping"),
        UiPhase::Completed => ("○", "replied"),
        UiPhase::Incomplete => ("○", "incomplete"),
        UiPhase::Failed => ("×", "failed"),
        UiPhase::Cancelled => ("■", "cancelled"),
    }
}

#[cfg(test)]
#[path = "render/brand_tests.rs"]
mod brand_tests;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use insta::assert_snapshot;
    use orchestral_core::agent_protocol::wire::{
        ToolActivityEvidence, ToolActivityState, ToolDiffLine, ToolDiffLineKind,
        ToolFileActivityKind,
    };
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use unicode_width::UnicodeWidthStr;

    use super::{composer_layout, render};
    use crate::tui::{update, TranscriptEntry, UiMsg, UiPhase, UiState};

    fn command_evidence(command: &str) -> Vec<ToolActivityEvidence> {
        vec![ToolActivityEvidence::Command {
            command: command.to_owned(),
        }]
    }

    fn file_evidence(path: &str) -> Vec<ToolActivityEvidence> {
        vec![ToolActivityEvidence::File {
            operation: ToolFileActivityKind::Read,
            path: path.to_owned(),
            diff: Vec::new(),
            diff_omitted: 0,
        }]
    }

    fn note_evidence(text: &str) -> Vec<ToolActivityEvidence> {
        vec![ToolActivityEvidence::Note {
            text: text.to_owned(),
        }]
    }

    fn edit_evidence(path: &str) -> Vec<ToolActivityEvidence> {
        vec![ToolActivityEvidence::File {
            operation: ToolFileActivityKind::Update,
            path: path.to_owned(),
            diff: vec![
                ToolDiffLine {
                    kind: ToolDiffLineKind::Deletion,
                    text: "let visible = false;".to_owned(),
                },
                ToolDiffLine {
                    kind: ToolDiffLineKind::Addition,
                    text: "let visible = true;".to_owned(),
                },
            ],
            diff_omitted: 0,
        }]
    }

    #[test]
    fn snapshot_40x12_cjk_emoji_tool_and_approval() {
        let mut state = UiState::new("会话-甲", "gemini-3.1-pro");
        state.phase = UiPhase::Running;
        state.run_id = Some("run-small".to_owned());
        state
            .transcript
            .push(TranscriptEntry::user("修复支付重试 🧪，不要重复扣款"));
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "shell-test".to_owned(),
                tool_name: "exec_command".to_owned(),
                state: ToolActivityState::Running,
                evidence: command_evidence("cargo test -p orchestral-runtime"),
            },
        );
        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-small".to_owned(),
                request_id: "approval-small".to_owned(),
                summary: "Run workspace tests with cargo".to_owned(),
                session_approval_available: false,
            },
        );
        assert_snapshot!("tui_40x12_approval", render_to_string(&state, 40, 12));
    }

    #[test]
    fn long_approval_summary_cannot_push_actions_out_of_view() {
        let mut state = UiState::new("session-approval", "model-approval");
        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-approval".to_owned(),
                request_id: "approval-long".to_owned(),
                summary: "A long approval explanation with filesystem, process, environment, and network effects. "
                    .repeat(12),
                session_approval_available: true,
            },
        );

        for (width, height) in [(20, 6), (50, 6), (50, 10)] {
            let rendered = render_to_string(&state, width, height);
            assert!(rendered.contains("› a  Allow once"), "{rendered}");
            assert!(
                rendered.contains(if width < 26 {
                    "s  Session"
                } else {
                    "s  Allow for"
                }),
                "{rendered}"
            );
            assert!(rendered.contains("d  Deny"), "{rendered}");
            assert!(rendered.contains("A long approval"), "{rendered}");
            if width >= 50 {
                assert!(rendered.contains("enter confirm"), "{rendered}");
            }
        }
    }

    #[test]
    fn cjk_composer_wrap_uses_display_cells_instead_of_byte_or_char_counts() {
        let layout = composer_layout("中文输入光标", "中文输入光标".len(), 5);

        assert_eq!(layout.lines, ["中文", "输入", "光标"]);
        assert_eq!((layout.cursor_row, layout.cursor_column), (2, 4));
    }

    #[test]
    fn long_cjk_composer_scrolls_to_the_end_cursor() {
        let mut state = UiState::new("session-composer", "model-composer");
        let input = format!("{}末", "中".repeat(64));
        update(&mut state, UiMsg::InsertText(input.clone()));
        update(&mut state, UiMsg::MoveCursorStart);
        update(&mut state, UiMsg::MoveCursorEnd);
        assert_eq!(state.composer_cursor, input.len());

        let backend = TestBackend::new(30, 12);
        let mut terminal = Terminal::new(backend).expect("create TestBackend terminal");
        terminal
            .draw(|frame| {
                render(frame, &state);
            })
            .expect("render long CJK composer");

        let cursor = terminal.backend().cursor_position();
        assert_eq!(cursor.y, 10, "cursor should stay above the footer");
        assert_eq!(
            terminal.backend().buffer()[(cursor.x.saturating_sub(2), cursor.y)].symbol(),
            "末",
            "last input character should remain visible immediately before the cursor"
        );
    }

    #[test]
    fn snapshot_80x24_long_stream_and_input_request() {
        let mut state = UiState::new("session-stream", "gpt-5.6");
        state.phase = UiPhase::Running;
        state.run_id = Some("run-stream".to_owned());
        state.transcript.push(TranscriptEntry::user(
            "Review the workspace and explain the longest risk without losing 中文 or emoji 🚀.",
        ));
        state.transcript.push(TranscriptEntry::assistant(
            "output-old",
            "I inspected the runtime boundary. The important invariant is that durable output replaces lossy streaming text instead of being appended a second time.",
        ));
        update(
            &mut state,
            UiMsg::StreamDelta {
                delta_id: "delta-2".to_owned(),
                output_id: "output-new".to_owned(),
                order: 2,
                text: "界。🚀".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::StreamDelta {
                delta_id: "delta-1".to_owned(),
                output_id: "output-new".to_owned(),
                order: 1,
                text: "Agent 边".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "inspect-runtime".to_owned(),
                tool_name: "file_read".to_owned(),
                state: ToolActivityState::Succeeded,
                evidence: file_evidence("core/orchestral-runtime/src/generic_agent/model_step.rs"),
            },
        );
        update(
            &mut state,
            UiMsg::WaitingInput {
                run_id: "run-stream".to_owned(),
                request_id: "input-stream".to_owned(),
                prompt: "Which package should receive the compatibility fix?".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::InsertText("orchestral-runtime\n保留协议兼容性".to_owned()),
        );
        assert_snapshot!("tui_80x24_stream_input", render_to_string(&state, 80, 24));
    }

    #[test]
    fn snapshot_100x24_running_with_compact_tool_activity() {
        let mut state = UiState::new("session-running", "gemini-2.5-flash");
        state.phase = UiPhase::Running;
        state.run_id = Some("run-running".to_owned());
        state.working_elapsed = Duration::from_secs(72);
        state.animation_frame = 3;
        state.transcript.push(TranscriptEntry::user(
            "阅读核心代码，说明执行链路并给出证据。",
        ));
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "read-core".to_owned(),
                tool_name: "file_read".to_owned(),
                state: ToolActivityState::Succeeded,
                evidence: file_evidence("core/orchestral-core/src/agent_protocol/types.rs"),
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "search-flow".to_owned(),
                tool_name: "exec_command".to_owned(),
                state: ToolActivityState::Running,
                evidence: command_evidence("rg -n \"ToolActivity\" core apps"),
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "edit-flow".to_owned(),
                tool_name: "apply_patch".to_owned(),
                state: ToolActivityState::Succeeded,
                evidence: edit_evidence("apps/orchestral-cli/src/tui/activity.rs"),
            },
        );
        update(
            &mut state,
            UiMsg::StreamDelta {
                delta_id: "delta-running".to_owned(),
                output_id: "output-running".to_owned(),
                order: 0,
                text: "我正在核对模型循环与工具执行边界。".to_owned(),
            },
        );
        update(
            &mut state,
            UiMsg::ProcessActivity {
                run_id: "run-running".to_owned(),
                session_id: 7,
                running: true,
            },
        );

        assert_snapshot!("tui_100x24_running", render_to_string(&state, 100, 24));
    }

    #[test]
    fn snapshot_120x40_completed_tool_recovery() {
        let mut state = UiState::new("会话-恢复", "gemini-3.1-pro");
        state.phase = UiPhase::Running;
        state.run_id = Some("run-recovery".to_owned());
        state.transcript.push(TranscriptEntry::user(
            "按照发布检查 Skill 验证 checkout 服务；如果远程查询失败，就用本地测试恢复。",
        ));
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "skill-read".to_owned(),
                tool_name: "skill_read".to_owned(),
                state: ToolActivityState::Succeeded,
                evidence: note_evidence("code-fix"),
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "mcp-inventory".to_owned(),
                tool_name: "mcp__inventory__deployment_color".to_owned(),
                state: ToolActivityState::Failed,
                evidence: note_evidence("mcp__inventory__deployment_color"),
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "exec-start".to_owned(),
                tool_name: "exec_command".to_owned(),
                state: ToolActivityState::Succeeded,
                evidence: command_evidence("cargo test -p orchestral-cli"),
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "exec-poll".to_owned(),
                tool_name: "write_stdin".to_owned(),
                state: ToolActivityState::Succeeded,
                evidence: Vec::new(),
            },
        );
        update(
            &mut state,
            UiMsg::Completed {
                final_text: Some(
                    "已恢复完成：MCP 查询超时，但本地发布检查的 18 项测试全部通过；没有发现需要修改的文件。"
                        .to_owned(),
                ),
            },
        );
        assert_snapshot!(
            "tui_120x40_completed_recovery",
            render_to_string(&state, 120, 40)
        );
    }

    #[test]
    fn auto_scroll_keeps_newest_running_input_and_completed_answer_visible() {
        let mut state = UiState::new("session-scroll", "model-scroll");
        for index in 0..12 {
            state.transcript.push(TranscriptEntry::assistant(
                format!("history-{index}"),
                "abcdefghijklmnopqrst abcdefghijklmnopqrst abcdefghijklmnopqrst abcdefghijklmnopqrst",
            ));
        }

        update(
            &mut state,
            UiMsg::InsertText("最新用户消息必须立即可见".to_owned()),
        );
        update(&mut state, UiMsg::Submit);
        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run-scroll".to_owned(),
            },
        );
        let running = render_to_string(&state, 50, 16);
        assert!(
            running.contains("最新用户消息必须立即可见"),
            "running viewport did not reach the newest input:\n{running}"
        );

        update(
            &mut state,
            UiMsg::Completed {
                final_text: Some("最终回答第一行\n最终回答末行必须可见".to_owned()),
            },
        );
        let completed = render_to_string(&state, 50, 16);
        assert!(
            completed.contains("最终回答末行必须可见"),
            "completed viewport clipped the final answer:\n{completed}"
        );
    }

    #[test]
    fn completed_assistant_markdown_is_presented_without_raw_control_markers() {
        let mut state = UiState::new("session-markdown", "model-markdown");
        state.phase = UiPhase::Completed;
        state.transcript.push(TranscriptEntry::assistant(
            "answer-markdown",
            "## 结果\n\n**修复完成**，运行 `cargo test`。\n\n```text\n24 tests passed\n```",
        ));

        let rendered = render_to_string(&state, 70, 16);
        assert!(rendered.contains("结果"), "{rendered}");
        assert!(
            rendered.contains("修复完成，运行 cargo test。"),
            "{rendered}"
        );
        assert!(rendered.contains("│ 24 tests passed"), "{rendered}");
        assert!(!rendered.contains("**"), "{rendered}");
        assert!(!rendered.contains("```"), "{rendered}");
    }

    #[test]
    fn completed_assistant_markdown_table_is_width_aware() {
        let mut state = UiState::new("session-table", "model-table");
        state.phase = UiPhase::Completed;
        state.transcript.push(TranscriptEntry::assistant(
            "answer-table",
            "| 维度 | 当前状态 | 演进建议 |\n| --- | --- | --- |\n| 任务规划 | 单循环 | 动态 Plan |\n| 记忆机制 | 简单压缩 | 长期缓存 |",
        ));

        let rendered = render_to_string(&state, 72, 16);
        assert!(rendered.contains("维度"), "{rendered}");
        assert!(rendered.contains("任务规划"), "{rendered}");
        assert!(rendered.contains("动态 Plan"), "{rendered}");
        assert!(rendered.contains('│'), "{rendered}");
        assert!(rendered.contains('┼'), "{rendered}");
        assert!(!rendered.contains("| ---"), "{rendered}");
    }

    #[test]
    fn elapsed_time_uses_compact_seconds_minutes_and_hours() {
        assert_eq!(super::fmt_elapsed_compact(0), "0s");
        assert_eq!(super::fmt_elapsed_compact(61), "1m 01s");
        assert_eq!(super::fmt_elapsed_compact(3_661), "1h 01m 01s");
    }

    pub(super) fn render_to_string(state: &UiState, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("create TestBackend terminal");
        terminal
            .draw(|frame| {
                render(frame, state);
            })
            .expect("render TUI snapshot");
        let buffer = terminal.backend().buffer();
        let mut output = String::new();
        for y in 0..height {
            let mut line = String::new();
            let mut x = 0;
            while x < width {
                let symbol = buffer[(x, y)].symbol();
                line.push_str(symbol);
                x = x.saturating_add(
                    u16::try_from(UnicodeWidthStr::width(symbol).max(1)).unwrap_or(1),
                );
            }
            output.push_str(line.trim_end());
            output.push('\n');
        }
        output
    }
}
