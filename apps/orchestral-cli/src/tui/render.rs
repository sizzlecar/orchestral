use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Clear, Padding, Paragraph, Wrap};
use ratatui::Frame;
use unicode_width::UnicodeWidthStr;

use super::state::{
    PendingOverlay, ToolActivityStatus, TranscriptEntry, TranscriptRole, UiPhase, UiState,
};

const MUTED: Style = Style::new().fg(Color::DarkGray);
const ACCENT: Style = Style::new().fg(Color::Cyan);
const USER: Style = Style::new().fg(Color::LightCyan);
const ASSISTANT: Style = Style::new().fg(Color::White);
const ERROR: Style = Style::new().fg(Color::LightRed);
const SUCCESS: Style = Style::new().fg(Color::Green);
const CONTENT_PADDING: u16 = 2;

pub(crate) fn render(frame: &mut Frame<'_>, state: &UiState) {
    let area = frame.area();
    if area.width < 20 || area.height < 6 {
        frame.render_widget(
            Paragraph::new("Orchestral needs at least 20×6").style(ERROR),
            area,
        );
        return;
    }

    let status_height = u16::from(shows_working_status(state.phase));
    let composer_height = composer_height(state, area.width).min(area.height.saturating_sub(4));
    let rows = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(status_height),
        Constraint::Length(composer_height),
        Constraint::Length(1),
    ])
    .split(area);

    render_header(frame, rows[0], state);
    render_transcript(frame, rows[1], state);
    render_working_status(frame, rows[2], state);
    render_composer(frame, rows[3], state);
    render_footer(frame, rows[4], state);
    if let Some(pending) = &state.pending {
        render_pending(frame, rows[1], pending);
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let (phase_icon, phase_label) = phase_badge(state.phase);
    if area.width < 54 {
        let columns = Layout::horizontal([Constraint::Percentage(52), Constraint::Percentage(48)])
            .split(area);
        frame.render_widget(
            Paragraph::new("  Orchestral").style(ACCENT.add_modifier(Modifier::BOLD)),
            columns[0],
        );
        frame.render_widget(
            Paragraph::new(format!("{phase_icon} {phase_label}  "))
                .style(phase_style(state.phase))
                .alignment(Alignment::Right),
            columns[1],
        );
        return;
    }
    let columns =
        Layout::horizontal([Constraint::Percentage(72), Constraint::Percentage(28)]).split(area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("  Orchestral", ACCENT.add_modifier(Modifier::BOLD)),
            Span::styled(format!("  ·  {}", state.model), MUTED),
        ])),
        columns[0],
    );
    frame.render_widget(
        Paragraph::new(format!("{phase_icon} {phase_label}  "))
            .style(phase_style(state.phase))
            .alignment(Alignment::Right),
        columns[1],
    );
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let block = Block::default().padding(Padding::horizontal(CONTENT_PADDING));
    let inner = block.inner(area);
    let lines = transcript_lines(state);
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let wrapped = paragraph.line_count(inner.width.max(1));
    let max_scroll = wrapped.saturating_sub(inner.height as usize) as u16;
    let back = u16::try_from(state.scroll_back)
        .unwrap_or(u16::MAX)
        .min(max_scroll);
    let scroll = max_scroll.saturating_sub(back);
    frame.render_widget(paragraph.block(block).scroll((scroll, 0)), area);
}

fn transcript_lines(state: &UiState) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    if state.transcript.is_empty() && state.streamed_text().is_empty() {
        lines.push(Line::from(Span::styled(
            "Ask for an outcome. Orchestral can inspect, act, and verify.",
            MUTED,
        )));
    }
    let mut previous_role = None;
    for entry in &state.transcript {
        if previous_role.is_some_and(|previous| should_separate(previous, entry.role)) {
            lines.push(Line::default());
        }
        push_entry_lines(&mut lines, entry);
        previous_role = Some(entry.role);
    }
    let stream = state.streamed_text();
    if !stream.is_empty() {
        if !lines.is_empty() {
            lines.push(Line::default());
        }
        push_markdown(&mut lines, "• ", &stream, ASSISTANT, true);
    }
    lines
}

fn should_separate(previous: TranscriptRole, current: TranscriptRole) -> bool {
    !matches!(
        (previous, current),
        (TranscriptRole::Tool, TranscriptRole::Tool)
    )
}

fn push_entry_lines(lines: &mut Vec<Line<'static>>, entry: &TranscriptEntry) {
    match entry.role {
        TranscriptRole::User => push_plain(lines, "› ", &entry.text, USER),
        TranscriptRole::Assistant => push_markdown(lines, "• ", &entry.text, ASSISTANT, false),
        TranscriptRole::System => push_plain(lines, "○ ", &entry.text, MUTED),
        TranscriptRole::Error => push_plain(lines, "■ ", &entry.text, ERROR),
        TranscriptRole::Tool => {
            let (symbol, style) = match entry.tool_status {
                Some(ToolActivityStatus::Running) => ("• ", ACCENT),
                Some(ToolActivityStatus::Succeeded) => ("✓ ", SUCCESS),
                Some(ToolActivityStatus::Failed) => ("× ", ERROR),
                None => ("· ", MUTED),
            };
            push_status_text(lines, symbol, &entry.text, style);
        }
    }
}

fn push_plain(lines: &mut Vec<Line<'static>>, prefix: &str, text: &str, style: Style) {
    let indent = " ".repeat(UnicodeWidthStr::width(prefix));
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

fn push_markdown(
    lines: &mut Vec<Line<'static>>,
    prefix: &str,
    text: &str,
    style: Style,
    streaming: bool,
) {
    let indent = " ".repeat(UnicodeWidthStr::width(prefix));
    let mut first_content = true;
    let mut in_code_block = false;
    let start_len = lines.len();

    for source in text.split('\n') {
        let trimmed = source.trim_start();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            continue;
        }
        if source.is_empty() {
            lines.push(Line::default());
            continue;
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
    let (text, style) = match state.phase {
        UiPhase::Running => ("• Working", ACCENT),
        UiPhase::WaitingInput => ("? Waiting for your response", ACCENT),
        UiPhase::WaitingApproval => ("! Approval required", Style::new().fg(Color::Yellow)),
        UiPhase::Cancelling => ("◌ Stopping…", Style::new().fg(Color::Yellow)),
        _ => return,
    };
    frame.render_widget(
        Paragraph::new(text)
            .style(style)
            .block(Block::default().padding(Padding::horizontal(CONTENT_PADDING))),
        area,
    );
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    if area.height == 0 {
        return;
    }
    let block = Block::default().padding(Padding::new(CONTENT_PADDING, CONTENT_PADDING, 1, 1));
    let inner = block.inner(area);
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
    let content = if state.composer.is_empty() {
        Text::from(Line::from(Span::styled(
            composer_placeholder(state.phase),
            MUTED.add_modifier(Modifier::ITALIC),
        )))
    } else {
        Text::styled(state.composer.clone(), ASSISTANT)
    };
    frame.render_widget(
        Paragraph::new(content).wrap(Wrap { trim: false }),
        content_area,
    );

    if !matches!(state.phase, UiPhase::WaitingApproval | UiPhase::Cancelling)
        && content_area.width > 0
        && content_area.height > 0
    {
        let (x, y) = composer_cursor(state, content_area);
        frame.set_cursor_position((x, y));
    }
}

fn composer_placeholder(phase: UiPhase) -> &'static str {
    match phase {
        UiPhase::Running => "Add guidance while Orchestral works…",
        UiPhase::WaitingInput => "Type your response…",
        UiPhase::WaitingApproval => "Press a to allow or d to deny",
        UiPhase::Cancelling => "Stopping the current run…",
        UiPhase::Failed => "Ask Orchestral to retry another way…",
        _ => "Ask Orchestral to do anything…",
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let active = matches!(
        state.phase,
        UiPhase::Running | UiPhase::WaitingInput | UiPhase::WaitingApproval
    );
    if area.width < 64 {
        let shortcuts = match state.phase {
            UiPhase::WaitingApproval => "  a allow · d deny · esc quit",
            UiPhase::WaitingInput => "  enter reply · ctrl+c stop · esc quit",
            UiPhase::Running => "  enter steer · ctrl+c stop · esc quit",
            _ if active => "  ctrl+c stop · esc quit",
            _ => "  enter send · esc quit",
        };
        frame.render_widget(Paragraph::new(shortcuts).style(MUTED), area);
        return;
    }
    let columns =
        Layout::horizontal([Constraint::Percentage(58), Constraint::Percentage(42)]).split(area);
    let left = if active {
        "  enter send  ·  ctrl+c interrupt"
    } else {
        "  enter send  ·  shift+enter newline"
    };
    frame.render_widget(Paragraph::new(left).style(MUTED), columns[0]);
    frame.render_widget(
        Paragraph::new("pgup/pgdn scroll  ·  esc quit  ")
            .style(MUTED)
            .alignment(Alignment::Right),
        columns[1],
    );
}

fn render_pending(frame: &mut Frame<'_>, viewport: Rect, pending: &PendingOverlay) {
    frame.render_widget(Clear, viewport);
    let horizontal = if viewport.width >= 48 { 4 } else { 2 };
    let top = u16::from(viewport.height >= 10);
    let area = Rect {
        x: viewport.x.saturating_add(horizontal),
        y: viewport.y.saturating_add(top),
        width: viewport.width.saturating_sub(horizontal.saturating_mul(2)),
        height: viewport.height.saturating_sub(top),
    };
    let mut lines = Vec::new();
    match pending {
        PendingOverlay::Input { prompt, .. } => {
            lines.push(Line::from(Span::styled(
                "? Input requested",
                ACCENT.add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::default());
            lines.extend(prompt.lines().map(|line| Line::from(line.to_owned())));
            lines.push(Line::default());
            lines.push(Line::from(Span::styled(
                "Reply below, then press Enter",
                MUTED,
            )));
        }
        PendingOverlay::Approval { summary, .. } => {
            lines.push(Line::from(Span::styled(
                "! Approval required",
                Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::default());
            lines.extend(summary.lines().map(|line| Line::from(line.to_owned())));
            lines.push(Line::default());
            lines.push(Line::from(vec![
                Span::styled("› a", ACCENT.add_modifier(Modifier::BOLD)),
                Span::raw("  Allow this operation"),
            ]));
            lines.push(Line::from(vec![
                Span::styled("  d", MUTED.add_modifier(Modifier::BOLD)),
                Span::raw("  Deny"),
            ]));
        }
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}

fn composer_height(state: &UiState, width: u16) -> u16 {
    if matches!(state.phase, UiPhase::WaitingApproval | UiPhase::Cancelling) {
        return 0;
    }
    let inner_width = width.saturating_sub(2 * CONTENT_PADDING + 2).max(1) as usize;
    let rows = state
        .composer
        .split('\n')
        .map(|line| UnicodeWidthStr::width(line).max(1).div_ceil(inner_width))
        .sum::<usize>()
        .clamp(1, 5);
    u16::try_from(rows).unwrap_or(5).saturating_add(2)
}

fn composer_cursor(state: &UiState, inner: Rect) -> (u16, u16) {
    let before = &state.composer[..state.composer_cursor.min(state.composer.len())];
    let mut row = 0_usize;
    let mut column = 0_usize;
    let width = inner.width.max(1) as usize;
    for (index, line) in before.split('\n').enumerate() {
        if index > 0 {
            row += 1;
        }
        let line_width = UnicodeWidthStr::width(line);
        row += line_width / width;
        column = line_width % width;
    }
    (
        inner.x
            + u16::try_from(column)
                .unwrap_or(u16::MAX)
                .min(inner.width - 1),
        inner.y + u16::try_from(row).unwrap_or(u16::MAX).min(inner.height - 1),
    )
}

fn phase_style(phase: UiPhase) -> Style {
    match phase {
        UiPhase::Idle | UiPhase::Completed => Style::new().fg(Color::Green),
        UiPhase::Running | UiPhase::WaitingInput | UiPhase::WaitingApproval => ACCENT,
        UiPhase::Cancelling | UiPhase::Cancelled => Style::new().fg(Color::Yellow),
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
        UiPhase::Completed => ("✓", "done"),
        UiPhase::Failed => ("×", "failed"),
        UiPhase::Cancelled => ("■", "cancelled"),
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;
    use unicode_width::UnicodeWidthStr;

    use super::render;
    use crate::tui::{update, ToolActivityStatus, TranscriptEntry, UiMsg, UiPhase, UiState};

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
                summary: "cargo test -p payment".to_owned(),
                status: ToolActivityStatus::Running,
            },
        );
        update(
            &mut state,
            UiMsg::WaitingApproval {
                run_id: "run-small".to_owned(),
                request_id: "approval-small".to_owned(),
                summary: "Run workspace tests with cargo".to_owned(),
            },
        );
        assert_snapshot!("tui_40x12_approval", render_to_string(&state, 40, 12));
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
                summary: "file_read core/orchestral-runtime/src/lib.rs".to_owned(),
                status: ToolActivityStatus::Succeeded,
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
        state.transcript.push(TranscriptEntry::user(
            "阅读核心代码，说明执行链路并给出证据。",
        ));
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "read-core".to_owned(),
                summary: "file_read core/orchestral-runtime/src/generic_agent/coordinator.rs"
                    .to_owned(),
                status: ToolActivityStatus::Succeeded,
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "search-flow".to_owned(),
                summary: "exec_command rg execute_model_run core/".to_owned(),
                status: ToolActivityStatus::Running,
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
                summary: "skill_read release-evidence → instructions loaded".to_owned(),
                status: ToolActivityStatus::Succeeded,
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "mcp-inventory".to_owned(),
                summary: "mcp__inventory__deployment_color → request timed out".to_owned(),
                status: ToolActivityStatus::Failed,
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "exec-start".to_owned(),
                summary: "exec_command cargo test → session 7 started".to_owned(),
                status: ToolActivityStatus::Succeeded,
            },
        );
        update(
            &mut state,
            UiMsg::ToolActivity {
                activity_id: "exec-poll".to_owned(),
                summary: "write_stdin session 7 → exited 0; 18 tests passed".to_owned(),
                status: ToolActivityStatus::Succeeded,
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

    fn render_to_string(state: &UiState, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).expect("create TestBackend terminal");
        terminal
            .draw(|frame| render(frame, state))
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
