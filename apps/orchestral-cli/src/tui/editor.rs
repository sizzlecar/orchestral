//! Pure editing operations. Byte offsets always land on extended grapheme boundaries.

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Edit {
    Left,
    Right,
    Start,
    End,
    WordLeft,
    WordRight,
    Backspace,
    Delete,
    DeleteWord,
    DeleteToStart,
    DeleteToEnd,
}

/// An inserted joiner or deleted separator can combine adjacent graphemes.
pub(crate) fn snap_cursor(text: &str, cursor: &mut usize) {
    *cursor = text
        .grapheme_indices(true)
        .map(|(i, _)| i)
        .find(|i| *i >= *cursor)
        .unwrap_or(text.len());
}

pub(crate) fn edit(text: &mut String, cursor: &mut usize, action: Edit) {
    let old = *cursor;
    let previous = || {
        text[..old]
            .grapheme_indices(true)
            .next_back()
            .map_or(0, |(i, _)| i)
    };
    let next = || old + text[old..].graphemes(true).next().map_or(0, str::len);
    let start = || text[..old].rfind('\n').map_or(0, |i| i + 1);
    let end = || old + text[old..].find('\n').unwrap_or(text.len() - old);
    let word_left = || {
        let mut target = old;
        let mut in_word = false;
        for (i, g) in text[..old].grapheme_indices(true).rev() {
            let whitespace = g.chars().all(char::is_whitespace);
            if whitespace && in_word {
                break;
            }
            in_word |= !whitespace;
            target = i;
        }
        target
    };
    let target = match action {
        Edit::Left | Edit::Backspace => previous(),
        Edit::Right | Edit::Delete => next(),
        Edit::Start | Edit::DeleteToStart => start(),
        Edit::End | Edit::DeleteToEnd => end(),
        Edit::WordLeft | Edit::DeleteWord => word_left(),
        Edit::WordRight => {
            let mut target = old;
            let mut in_word = false;
            for (i, g) in text[old..].grapheme_indices(true) {
                let whitespace = g.chars().all(char::is_whitespace);
                if whitespace && in_word {
                    break;
                }
                in_word |= !whitespace;
                target = old + i + g.len();
            }
            target
        }
    };
    if matches!(
        action,
        Edit::Backspace | Edit::Delete | Edit::DeleteWord | Edit::DeleteToStart | Edit::DeleteToEnd
    ) {
        text.drain(old.min(target)..old.max(target));
        *cursor = old.min(target);
        snap_cursor(text, cursor);
    } else {
        *cursor = target;
    }
}

/// Move by logical line, retaining the nearest available display column.
/// False indicates a history boundary, rather than a cursor movement.
pub(crate) fn vertical(text: &str, cursor: &mut usize, up: bool) -> bool {
    let start = text[..*cursor].rfind('\n').map_or(0, |i| i + 1);
    let column = text[start..*cursor].width();
    let target = if up {
        if start == 0 {
            return false;
        }
        let end = start - 1;
        (text[..end].rfind('\n').map_or(0, |i| i + 1), end)
    } else {
        let Some(end) = text[*cursor..].find('\n').map(|i| *cursor + i) else {
            return false;
        };
        let next = end + 1;
        (
            next,
            text[next..].find('\n').map_or(text.len(), |i| next + i),
        )
    };
    *cursor = target.0;
    let mut width = 0;
    for g in text[target.0..target.1].graphemes(true) {
        width += g.width();
        if width > column {
            break;
        }
        *cursor += g.len();
    }
    true
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct InputHistory {
    entries: Vec<String>,
    position: Option<usize>,
    draft: Option<(String, usize)>,
}

impl InputHistory {
    pub(crate) fn push(&mut self, text: &str) {
        if !text.trim().is_empty() && self.entries.last().is_none_or(|last| last != text) {
            self.entries.push(text.to_owned());
            if self.entries.len() > 500 {
                self.entries.remove(0);
            }
        }
        self.position = None;
        self.draft = None;
    }

    pub(crate) fn navigate(&mut self, text: &mut String, cursor: &mut usize, up: bool) {
        if self.entries.is_empty() {
            return;
        }
        if up {
            let i = match self.position {
                None => {
                    self.draft = Some((text.clone(), *cursor));
                    self.entries.len() - 1
                }
                Some(i) => i.saturating_sub(1),
            };
            self.position = Some(i);
            *text = self.entries[i].clone();
            *cursor = text.len();
        } else if let Some(i) = self.position {
            if i + 1 < self.entries.len() {
                self.position = Some(i + 1);
                *text = self.entries[i + 1].clone();
                *cursor = text.len();
            } else {
                self.position = None;
                if let Some((draft, offset)) = self.draft.take() {
                    *text = draft;
                    *cursor = offset;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edits_combining_marks_and_emoji_as_complete_graphemes() {
        let mut text = "中e\u{301}👩🏽‍💻🇨🇳".to_owned();
        let mut cursor = text.len();
        edit(&mut text, &mut cursor, Edit::Backspace);
        assert_eq!(text, "中e\u{301}👩🏽‍💻");
        edit(&mut text, &mut cursor, Edit::Left);
        assert_eq!(&text[cursor..], "👩🏽‍💻");
        edit(&mut text, &mut cursor, Edit::Backspace);
        assert_eq!(text, "中👩🏽‍💻");
        edit(&mut text, &mut cursor, Edit::Delete);
        assert_eq!(text, "中");
    }

    #[test]
    fn line_and_word_edits_preserve_other_lines() {
        let mut text = "first\nhello 世界 last\nthird".to_owned();
        let mut cursor = "first\nhello 世界".len();
        edit(&mut text, &mut cursor, Edit::DeleteWord);
        assert_eq!(text, "first\nhello  last\nthird");
        edit(&mut text, &mut cursor, Edit::DeleteToStart);
        assert_eq!(text, "first\n last\nthird");
        edit(&mut text, &mut cursor, Edit::DeleteToEnd);
        assert_eq!(text, "first\n\nthird");
    }

    #[test]
    fn vertical_motion_uses_display_columns_and_history_restores_draft() {
        let mut text = "ab中文\n👩‍💻cd".to_owned();
        let mut cursor = "ab中".len();
        assert!(vertical(&text, &mut cursor, false));
        assert_eq!(cursor, text.len());
        let mut history = InputHistory::default();
        history.push("old request");
        let draft = (text.clone(), cursor);
        history.navigate(&mut text, &mut cursor, true);
        assert_eq!(text, "old request");
        history.navigate(&mut text, &mut cursor, false);
        assert_eq!((text, cursor), draft);
    }
}
