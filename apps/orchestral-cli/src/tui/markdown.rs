//! CommonMark events projected into terminal lines, shared by live and saved output.

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::UnicodeWidthStr;

use super::render::{ACCENT, MUTED};

pub(super) fn options() -> Options {
    Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_MATH
}

pub(super) fn push_markdown(
    lines: &mut Vec<Line<'static>>,
    prefix: &str,
    text: &str,
    style: Style,
    streaming: bool,
    width: u16,
) {
    let plain = super::text::plain(text);
    let source = super::math::normalize_delimiters(&plain);
    let mut writer = Writer {
        lines: Vec::new(),
        spans: Vec::new(),
        prefix,
        indent: " ".repeat(prefix.width()),
        first_content: true,
        styles: vec![style],
        lists: Vec::new(),
        quote_depth: 0,
        code: false,
        table: None,
        table_row: Vec::new(),
        table_cell: String::new(),
        table_head: false,
        width,
    };
    for (event, range) in Parser::new_ext(&source, options()).into_offset_iter() {
        writer.event(event, &source[range]);
    }
    writer.flush();
    while writer
        .lines
        .last()
        .is_some_and(|line| line.spans.is_empty())
    {
        writer.lines.pop();
    }
    if writer.lines.is_empty() {
        writer.lines.push(Line::styled(prefix.to_owned(), style));
    }
    if streaming {
        if let Some(last) = writer
            .lines
            .iter_mut()
            .rev()
            .find(|line| !line.spans.is_empty())
        {
            last.spans.push(Span::styled("▌", ACCENT));
        }
    }
    lines.extend(writer.lines);
}

struct List {
    next: Option<u64>,
    marker: String,
    first_line: bool,
}

#[derive(Default)]
struct MarkdownTable {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

struct Writer<'a> {
    lines: Vec<Line<'static>>,
    spans: Vec<Span<'static>>,
    prefix: &'a str,
    indent: String,
    first_content: bool,
    styles: Vec<Style>,
    lists: Vec<List>,
    quote_depth: usize,
    code: bool,
    table: Option<MarkdownTable>,
    table_row: Vec<String>,
    table_cell: String,
    table_head: bool,
    width: u16,
}

impl Writer<'_> {
    fn style(&self) -> Style {
        *self.styles.last().expect("base style is retained")
    }

    fn context_prefix(&mut self) -> String {
        let mut prefix = "│ ".repeat(self.quote_depth);
        for list in &mut self.lists {
            if list.first_line {
                prefix.push_str(&list.marker);
                list.first_line = false;
            } else {
                prefix.push_str(&" ".repeat(list.marker.width()));
            }
        }
        if self.code {
            prefix.push_str("│ ");
        }
        prefix
    }

    fn flush(&mut self) {
        if self.spans.is_empty() {
            return;
        }
        let context = self.context_prefix();
        let prefix = take_content_prefix(self.prefix, &self.indent, &mut self.first_content);
        let mut spans = vec![
            Span::styled(prefix, self.style()),
            Span::styled(context, MUTED),
        ];
        spans.append(&mut self.spans);
        self.lines.push(Line::from(spans));
    }

    fn blank(&mut self) {
        self.flush();
        if self.lines.last().is_some_and(|line| !line.spans.is_empty()) {
            self.lines.push(Line::default());
        }
    }

    fn text(&mut self, value: &str, style: Style) {
        if self.table.is_some() {
            self.table_cell.push_str(value);
            return;
        }
        for part in value.split_inclusive('\n') {
            let content = part.strip_suffix('\n').unwrap_or(part);
            self.spans.push(Span::styled(content.to_owned(), style));
            if part.ends_with('\n') {
                self.flush();
            }
        }
    }

    fn math(&mut self, value: &str, raw: &str, display: bool) {
        if display {
            self.flush();
        }
        let delimiter = if display { "$$" } else { "$" };
        let source = raw.trim();
        // Display math can be emitted at EOF before its closing delimiter
        // arrives. Keep that streaming fragment in source form until complete.
        let closed = source.len() >= 2 * delimiter.len()
            && source.starts_with(delimiter)
            && source.ends_with(delimiter);
        let rendered = closed.then(|| super::math::render(value)).flatten();
        self.text(rendered.as_deref().unwrap_or(raw), self.style());
        if display {
            self.flush();
        }
    }

    fn event(&mut self, event: Event<'_>, raw: &str) {
        match event {
            Event::Start(tag) => {
                let mut style = self.style();
                match &tag {
                    Tag::Heading { .. } | Tag::Strong => style = style.add_modifier(Modifier::BOLD),
                    Tag::Emphasis => style = style.add_modifier(Modifier::ITALIC),
                    Tag::Strikethrough => style = style.add_modifier(Modifier::CROSSED_OUT),
                    Tag::Link { .. } => {
                        style = style.patch(ACCENT).add_modifier(Modifier::UNDERLINED)
                    }
                    _ => {}
                }
                match tag {
                    Tag::Paragraph | Tag::Heading { .. } => self.flush(),
                    Tag::CodeBlock(_) => {
                        self.flush();
                        self.code = true;
                    }
                    Tag::BlockQuote(_) => {
                        self.flush();
                        self.quote_depth += 1;
                    }
                    Tag::List(next) => {
                        self.flush();
                        self.lists.push(List {
                            next,
                            marker: String::new(),
                            first_line: false,
                        });
                    }
                    Tag::Item => {
                        self.flush();
                        if let Some(list) = self.lists.last_mut() {
                            list.marker = if let Some(number) = list.next {
                                list.next = Some(number.saturating_add(1));
                                format!("{number}. ")
                            } else {
                                "• ".to_owned()
                            };
                            list.first_line = true;
                        }
                    }
                    Tag::Table(_) => {
                        self.flush();
                        self.table = Some(MarkdownTable::default());
                    }
                    Tag::TableHead => self.table_head = true,
                    _ => {}
                }
                self.styles.push(style);
            }
            Event::End(tag) => {
                match tag {
                    TagEnd::Paragraph | TagEnd::Heading(_) => {
                        self.flush();
                        if self.lists.is_empty() {
                            self.blank();
                        }
                    }
                    TagEnd::Item => self.flush(),
                    TagEnd::List(_) => {
                        self.flush();
                        self.lists.pop();
                        if self.lists.is_empty() {
                            self.blank();
                        }
                    }
                    TagEnd::BlockQuote(_) => {
                        self.flush();
                        self.quote_depth -= 1;
                        self.blank();
                    }
                    TagEnd::CodeBlock => {
                        self.flush();
                        self.code = false;
                        self.blank();
                    }
                    TagEnd::TableCell => self.table_row.push(std::mem::take(&mut self.table_cell)),
                    TagEnd::TableHead | TagEnd::TableRow => {
                        if let Some(table) = &mut self.table {
                            let row = std::mem::take(&mut self.table_row);
                            if self.table_head {
                                table.headers = row;
                            } else {
                                table.rows.push(row);
                            }
                        }
                        self.table_head = false;
                    }
                    TagEnd::Table => {
                        if let Some(table) = self.table.take() {
                            let context = self.context_prefix();
                            let prefix = format!(
                                "{}{context}",
                                if self.first_content {
                                    self.prefix
                                } else {
                                    &self.indent
                                }
                            );
                            let indent = format!("{}{}", self.indent, " ".repeat(context.width()));
                            let mut first = true;
                            let style = self.style();
                            push_markdown_table(
                                &mut self.lines,
                                &table,
                                &prefix,
                                &indent,
                                &mut first,
                                style,
                                self.width,
                            );
                            self.first_content = false;
                            self.blank();
                        }
                    }
                    _ => {}
                }
                self.styles.pop();
            }
            Event::Text(value) | Event::Html(value) | Event::InlineHtml(value) => {
                self.text(&value, if self.code { ACCENT } else { self.style() });
            }
            Event::Code(value) => self.text(&value, self.style().patch(ACCENT)),
            Event::InlineMath(value) => self.math(&value, raw, false),
            Event::DisplayMath(value) => self.math(&value, raw, true),
            Event::SoftBreak | Event::HardBreak => {
                if self.table.is_some() {
                    self.table_cell.push(' ');
                } else {
                    self.flush();
                }
            }
            Event::Rule => {
                self.blank();
                let context = self.context_prefix();
                let prefix =
                    take_content_prefix(self.prefix, &self.indent, &mut self.first_content);
                let available =
                    usize::from(self.width).saturating_sub(prefix.width() + context.width());
                self.lines.push(Line::styled(
                    format!("{prefix}{context}{}", "─".repeat(available)),
                    MUTED,
                ));
                self.blank();
            }
            Event::TaskListMarker(checked) => {
                self.text(if checked { "☑ " } else { "☐ " }, self.style())
            }
            Event::FootnoteReference(value) => self.text(&format!("[{value}]"), self.style()),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(source: &str, width: u16) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        push_markdown(&mut lines, "• ", source, Style::default(), false, width);
        lines
    }

    fn text(source: &str) -> String {
        lines(source, 72)
            .iter()
            .map(Line::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn commonmark_rules_are_width_aware_and_code_remains_literal() {
        for marker in ["---", "***", "___", "- - -"] {
            for width in [8, 40, 80] {
                let rendered = lines(&format!("Before\n\n{marker}\n\nAfter"), width);
                let rule = rendered
                    .iter()
                    .find(|line| line.to_string().contains('─'))
                    .unwrap();
                assert_eq!(rule.width(), usize::from(width));
                assert!(rendered
                    .iter()
                    .any(|line| line.to_string().contains("After")));
            }
        }
        let rendered = text("```md\n---\n- literal\n$x_1$\n```\n\nInline `---`.");
        for literal in ["│ ---", "│ - literal", "│ $x_1$", "Inline ---."] {
            assert!(rendered.contains(literal), "{rendered}");
        }
        assert!(!rendered.contains('─'), "{rendered}");
        // A setext heading is a heading, rather than a horizontal rule.
        assert!(!text("Heading\n---").contains('─'));
    }

    #[test]
    fn nested_lists_and_nested_inline_styles_use_structured_events() {
        let rendered = lines(
            "3. **Top `call()`**\n   - nested $x_2$\n   - [x] done\n4. Next",
            72,
        );
        let strings = rendered.iter().map(Line::to_string).collect::<Vec<_>>();
        assert!(
            strings.iter().any(|line| line == "• 3. Top call()"),
            "{strings:?}"
        );
        assert!(
            strings.iter().any(|line| line == "     • nested x₂"),
            "{strings:?}"
        );
        assert!(
            strings.iter().any(|line| line == "     • ☑ done"),
            "{strings:?}"
        );
        assert!(
            strings.iter().any(|line| line == "  4. Next"),
            "{strings:?}"
        );
        let code = rendered
            .iter()
            .flat_map(|line| &line.spans)
            .find(|span| span.content == "call()")
            .unwrap();
        assert!(code.style.add_modifier.contains(Modifier::BOLD));
        assert_eq!(code.style.fg, ACCENT.fg);
        let next = rendered
            .iter()
            .flat_map(|line| &line.spans)
            .find(|span| span.content == "Next")
            .unwrap();
        assert!(!next.style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn inline_display_and_alternate_math_delimiters_share_the_renderer() {
        let rendered = text("指标 $t_{\\text{end}} - t_{\\text{start}}$，以及 \\(x_1^2\\)。\n\n$$\n\\frac{a+b}{n-1}\n$$\n\n\\[\\sqrt{x} \\leq \\alpha\\]\n\n完成。");
        for expected in ["t_(end) − tₛₜₐᵣₜ", "x₁²", "(a+b)/(n−1)", "√x ≤ α", "完成。"]
        {
            assert!(rendered.contains(expected), "{rendered}");
        }
        assert!(!rendered.contains('$'), "{rendered}");
        assert!(!rendered.contains("\\text"), "{rendered}");
        assert!(
            rendered.lines().any(|line| line.trim() == "(a+b)/(n−1)"),
            "{rendered}"
        );
    }

    #[test]
    fn literal_dollars_unknown_commands_and_pending_math_stay_readable() {
        let rendered = text(r"Cost $5 and $10; escaped \$20. Code `$x_1$`. Unknown $\custom{x}$.");
        for expected in ["$5 and $10", "$20", "$x_1$", r"$\custom{x}$"] {
            assert!(rendered.contains(expected), "{rendered}");
        }
        assert!(text(r"Pending $\frac{a}").contains(r"$\frac{a}"));
        assert!(text(r"Broken $\frac{a}$").contains(r"$\frac{a}$"));
        let pending_display = text("$$\n\\frac{a}{b}");
        assert!(pending_display.contains("$$"), "{pending_display}");
        assert!(
            pending_display.contains(r"\frac{a}{b}"),
            "{pending_display}"
        );
    }

    #[test]
    fn formulas_in_tables_are_projected_before_width_measurement() {
        let source =
            "| Value | Formula |\n|---|---|\n| power | $x^2$ |\n| ratio | $\\frac{a}{b}$ |";
        for width in [10, 40, 80] {
            let rendered = lines(source, width);
            let rows = super::super::viewport::wrap("table", rendered, usize::from(width));
            let combined = rows
                .iter()
                .map(|row| row.text.to_string())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(combined.contains("x²"), "{combined}");
            assert!(combined.contains("a/b"), "{combined}");
            assert!(!combined.contains('$'), "{combined}");
            assert!(rows
                .iter()
                .all(|row| row.text.width() <= usize::from(width)));
        }
    }

    #[test]
    fn every_stream_prefix_is_renderable_and_completion_matches_history() {
        let source = "中文 **bold `code`** $\\frac{x_1}{n-1}$\n\n---\n\n- list\n  - child";
        for end in source
            .char_indices()
            .map(|(index, _)| index)
            .chain([source.len()])
        {
            let mut rendered = Vec::new();
            push_markdown(
                &mut rendered,
                "• ",
                &source[..end],
                Style::default(),
                true,
                32,
            );
            assert_eq!(
                rendered
                    .iter()
                    .map(Line::to_string)
                    .collect::<String>()
                    .matches('▌')
                    .count(),
                1
            );
            let rows = super::super::viewport::wrap("stream", rendered, 32);
            assert!(rows.iter().all(|row| row.text.width() <= 32));
        }
        let mut live = Vec::new();
        push_markdown(&mut live, "• ", source, Style::default(), true, 32);
        live.last_mut().unwrap().spans.pop();
        assert_eq!(live, lines(source, 32));
    }
}
