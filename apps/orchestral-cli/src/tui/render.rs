use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, Padding, Paragraph, Wrap};
use ratatui::Frame;
use unicode_width::UnicodeWidthStr;

use super::state::{
    PendingOverlay, ToolActivityStatus, TranscriptEntry, TranscriptRole, UiPhase, UiState,
};

const BORDER: Style = Style::new().fg(Color::DarkGray);
const MUTED: Style = Style::new().fg(Color::DarkGray);
const ACCENT: Style = Style::new().fg(Color::Cyan);
const USER: Style = Style::new().fg(Color::LightCyan);
const ASSISTANT: Style = Style::new().fg(Color::White);
const ERROR: Style = Style::new().fg(Color::LightRed);

pub(crate) fn render(frame: &mut Frame<'_>, state: &UiState) {
    let area = frame.area();
    if area.width < 20 || area.height < 6 {
        frame.render_widget(
            Paragraph::new("Orchestral needs at least 20×6").style(ERROR),
            area,
        );
        return;
    }

    let composer_height = composer_height(state, area.width).min(area.height.saturating_sub(5));
    let rows = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(composer_height),
        Constraint::Length(1),
    ])
    .split(area);

    render_header(frame, rows[0], state);
    render_transcript(frame, rows[1], state);
    render_composer(frame, rows[2], state);
    render_footer(frame, rows[3], state);
    if let Some(pending) = &state.pending {
        render_pending(frame, rows[1], pending);
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    if area.width < 60 {
        let columns = Layout::horizontal([Constraint::Percentage(52), Constraint::Percentage(48)])
            .split(area);
        frame.render_widget(
            Paragraph::new(" Orchestral").style(ACCENT.add_modifier(Modifier::BOLD)),
            columns[0],
        );
        frame.render_widget(
            Paragraph::new(format!("{} ", state.phase.label()))
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
            Span::styled(" Orchestral", ACCENT.add_modifier(Modifier::BOLD)),
            Span::styled(format!("  {} / {}", state.session_id, state.model), MUTED),
        ])),
        columns[0],
    );
    frame.render_widget(
        Paragraph::new(format!("{} ", state.phase.label()))
            .style(phase_style(state.phase))
            .alignment(Alignment::Right),
        columns[1],
    );
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let block = Block::default()
        .title(" Conversation ")
        .borders(Borders::ALL)
        .border_style(BORDER)
        .padding(Padding::horizontal(1));
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
            "Describe a task. Orchestral will inspect, act, and report.",
            MUTED,
        )));
    }
    for entry in &state.transcript {
        push_entry_lines(&mut lines, entry);
        lines.push(Line::default());
    }
    let stream = state.streamed_text();
    if !stream.is_empty() {
        push_multiline(&mut lines, "Agent › ", &format!("{stream}▌"), ASSISTANT);
    }
    lines
}

fn push_entry_lines(lines: &mut Vec<Line<'static>>, entry: &TranscriptEntry) {
    match entry.role {
        TranscriptRole::User => push_multiline(lines, "You   › ", &entry.text, USER),
        TranscriptRole::Assistant => push_multiline(lines, "Agent › ", &entry.text, ASSISTANT),
        TranscriptRole::System => push_multiline(lines, "• ", &entry.text, MUTED),
        TranscriptRole::Error => push_multiline(lines, "Error › ", &entry.text, ERROR),
        TranscriptRole::Tool => {
            let (symbol, style) = match entry.tool_status {
                Some(ToolActivityStatus::Running) => ("…", ACCENT),
                Some(ToolActivityStatus::Succeeded) => ("✓", Style::new().fg(Color::Green)),
                Some(ToolActivityStatus::Failed) => ("×", ERROR),
                None => ("·", MUTED),
            };
            push_multiline(lines, &format!("Tool {symbol} "), &entry.text, style);
        }
    }
}

fn push_multiline(lines: &mut Vec<Line<'static>>, prefix: &str, text: &str, style: Style) {
    let mut parts = text.lines();
    if let Some(first) = parts.next() {
        lines.push(Line::from(vec![
            Span::styled(prefix.to_owned(), style.add_modifier(Modifier::BOLD)),
            Span::styled(first.to_owned(), style),
        ]));
    } else {
        lines.push(Line::from(Span::styled(prefix.to_owned(), style)));
    }
    let indent = " ".repeat(UnicodeWidthStr::width(prefix));
    for part in parts {
        lines.push(Line::from(vec![
            Span::raw(indent.clone()),
            Span::styled(part.to_owned(), style),
        ]));
    }
}

fn render_composer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let block = Block::default()
        .title(format!(" {} ", state.composer_title()))
        .borders(Borders::ALL)
        .border_style(if state.phase == UiPhase::WaitingInput {
            ACCENT
        } else {
            BORDER
        })
        .padding(Padding::horizontal(1));
    let inner = block.inner(area);
    let content = if state.composer.is_empty() {
        Text::from(Line::from(Span::styled(
            if state.phase == UiPhase::WaitingApproval {
                "Choose allow or deny"
            } else {
                "Type a message…"
            },
            MUTED.add_modifier(Modifier::ITALIC),
        )))
    } else {
        Text::styled(state.composer.clone(), ASSISTANT)
    };
    frame.render_widget(
        Paragraph::new(content)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );

    if !matches!(state.phase, UiPhase::WaitingApproval | UiPhase::Cancelling)
        && inner.width > 0
        && inner.height > 0
    {
        let (x, y) = composer_cursor(state, inner);
        frame.set_cursor_position((x, y));
    }
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let shortcuts = if area.width < 60 {
        match state.phase {
            UiPhase::Running | UiPhase::WaitingInput | UiPhase::WaitingApproval => {
                "Ctrl+C cancel  Esc quit"
            }
            _ => "Enter send  Esc quit",
        }
    } else {
        match state.phase {
            UiPhase::Running | UiPhase::WaitingInput | UiPhase::WaitingApproval => {
                "Enter send  Ctrl+C cancel  PgUp/PgDn scroll  Esc quit"
            }
            _ => "Enter send  Shift+Enter newline  PgUp/PgDn scroll  Esc quit",
        }
    };
    frame.render_widget(Paragraph::new(format!(" {shortcuts}")).style(MUTED), area);
}

fn render_pending(frame: &mut Frame<'_>, viewport: Rect, pending: &PendingOverlay) {
    let area = modal_area(viewport);
    let (title, body, hint) = match pending {
        PendingOverlay::Input { prompt, .. } => (
            " Input requested ",
            prompt.as_str(),
            "Type the response below, then press Enter",
        ),
        PendingOverlay::Approval { summary, .. } => (
            " Approval required ",
            summary.as_str(),
            if area.width < 50 {
                "[a] allow   [d] deny"
            } else {
                "[a] allow this exact operation   [d] deny"
            },
        ),
    };
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(body.to_owned()),
            Line::default(),
            Line::from(Span::styled(hint.to_owned(), ACCENT)),
        ])
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(ACCENT)
                .padding(Padding::horizontal(1)),
        )
        .wrap(Wrap { trim: false }),
        area,
    );
}

fn modal_area(viewport: Rect) -> Rect {
    let height = viewport.height.clamp(1, 8);
    let compact = viewport.height <= 8;
    let width = if compact {
        viewport.width
    } else {
        viewport.width.saturating_sub(4).clamp(20, 80)
    };
    Rect {
        x: if compact {
            viewport.x
        } else {
            viewport.x + viewport.width.saturating_sub(width) / 2
        },
        y: if compact { viewport.y } else { viewport.y + 1 },
        width,
        height,
    }
}

fn composer_height(state: &UiState, width: u16) -> u16 {
    let inner_width = width.saturating_sub(4).max(1) as usize;
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
