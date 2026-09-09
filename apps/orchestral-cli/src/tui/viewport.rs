//! Content anchors keep reading stable while rows are appended, folded, or reflowed.
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Anchor {
    pub entry: String,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Viewport {
    pub anchor: Option<Anchor>,
    pub movement: isize,
    pub unread: bool,
}

impl Viewport {
    pub(crate) fn follow(&mut self) {
        *self = Self::default();
    }
    pub(crate) fn changed(&mut self) {
        self.unread |= self.anchor.is_some();
    }
}

pub(crate) struct Row {
    pub anchor: Anchor,
    pub text: Line<'static>,
}

pub(crate) fn wrap(entry: impl ToString, lines: Vec<Line<'static>>, width: usize) -> Vec<Row> {
    let entry = entry.to_string();
    let width = width.max(1);
    let mut rows = Vec::new();
    for (index, line) in lines.into_iter().enumerate() {
        let graphemes = line
            .spans
            .iter()
            .flat_map(|span| span.styled_graphemes(Style::default()))
            .map(|g| (g.symbol.to_owned(), g.style))
            .collect::<Vec<_>>();
        let indent = graphemes
            .iter()
            .take_while(|(g, _)| g == " ")
            .map(|(g, _)| g.width())
            .sum::<usize>()
            .min(width.saturating_sub(1));
        let mut start = 0;
        let mut column = 0;
        loop {
            let mut end = start;
            let mut used = if start == 0 { 0 } else { indent };
            let mut last_break = None;
            let mut has_word = false;
            while end < graphemes.len() {
                let (g, _) = &graphemes[end];
                if used + g.width() > width && end > start {
                    break;
                }
                used += g.width();
                if g.chars().all(char::is_whitespace) {
                    if has_word {
                        last_break = Some(end + 1);
                    }
                } else {
                    has_word = true;
                }
                end += 1;
            }
            if end < graphemes.len() {
                if let Some(boundary) = last_break {
                    end = boundary;
                }
            }
            let mut spans: Vec<Span<'static>> = Vec::new();
            if start > 0 && indent > 0 {
                spans.push(Span::raw(" ".repeat(indent)));
            }
            let anchor = Anchor {
                entry: entry.clone(),
                line: index,
                column,
            };
            for (g, style) in &graphemes[start..end] {
                column += g.width();
                if let Some(last) = spans.last_mut().filter(|span| span.style == *style) {
                    last.content.to_mut().push_str(g);
                } else {
                    spans.push(Span::styled(g.clone(), *style));
                }
            }
            rows.push(Row {
                anchor,
                text: Line::from(spans),
            });
            start = end;
            if start == graphemes.len() {
                break;
            }
        }
    }
    rows
}

pub(crate) fn window(rows: &[Row], height: usize, view: &Viewport) -> (usize, Option<Anchor>) {
    let bottom = rows.len().saturating_sub(height);
    let anchored = view
        .anchor
        .as_ref()
        .and_then(|anchor| {
            rows.iter()
                .enumerate()
                .filter(|(_, row)| row.anchor.entry == anchor.entry)
                .take_while(|(_, row)| {
                    (row.anchor.line, row.anchor.column) <= (anchor.line, anchor.column)
                })
                .last()
                .map(|(i, _)| i)
        })
        .unwrap_or(bottom);
    let top = anchored.saturating_add_signed(view.movement).min(bottom);
    let follow = top == bottom && (view.anchor.is_none() || view.movement > 0);
    (
        top,
        if follow {
            None
        } else {
            rows.get(top).map(|row| row.anchor.clone())
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_survives_growth_above_and_below_and_width_changes() {
        let rows = wrap(0, vec![Line::raw("old")], 8)
            .into_iter()
            .chain(wrap(1, vec![Line::raw("abcdefghijklmnop")], 8))
            .chain(wrap(2, vec![Line::raw("tail")], 8))
            .collect::<Vec<_>>();
        let (_, anchor) = window(
            &rows,
            1,
            &Viewport {
                movement: -1,
                ..Default::default()
            },
        );
        assert_eq!(anchor.as_ref().unwrap().column, 8);
        let view = Viewport {
            anchor,
            ..Default::default()
        };
        let changed = wrap(0, vec![Line::raw("much longer preceding content")], 4)
            .into_iter()
            .chain(wrap(1, vec![Line::raw("abcdefghijklmnop")], 4))
            .chain(wrap(2, vec![Line::raw("many new output rows")], 4))
            .collect::<Vec<_>>();
        let (top, _) = window(&changed, 1, &view);
        assert_eq!(changed[top].text.to_string(), "ijkl");
    }
}
