//! Plain terminal content. Escape sequences are data, never display commands.
use std::borrow::Cow;

pub(crate) fn plain(text: &str) -> Cow<'_, str> {
    if !text
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Cow::Borrowed(text);
    }
    struct Plain(String);
    impl vte::Perform for Plain {
        fn print(&mut self, c: char) {
            if !c.is_control() {
                self.0.push(c);
            }
        }
        fn execute(&mut self, byte: u8) {
            match byte {
                b'\n' | b'\t' => self.0.push(byte as char),
                _ => {}
            }
        }
    }
    let mut output = Plain(String::with_capacity(text.len()));
    vte::Parser::new().advance(&mut output, text.as_bytes());
    Cow::Owned(output.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_sequences_cannot_clear_the_screen_or_change_the_clipboard() {
        assert_eq!(
            plain("before\x1b[2J\x1b]52;c;secret\x07\x1b[31m红色\x1b[0m\n\tcode"),
            "before红色\n\tcode"
        );
    }
}
