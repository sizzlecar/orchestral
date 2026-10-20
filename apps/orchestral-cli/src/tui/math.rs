//! Bounded LaTeX-to-text projection. Unknown syntax stays visible as source.

use std::borrow::Cow;

use latex_to_unicode::{ast::MathNode, parser, symbols};
use pulldown_cmark::{Event, Parser, Tag};

pub(super) fn render(source: &str) -> Option<String> {
    // The recursive parser/renderer handles model-supplied text. Bound both
    // source size and structural tokens, including unbraced command chains.
    if source.trim().is_empty()
        || source.trim_end().ends_with(['^', '_'])
        || source.len() > 8_192
        || source
            .chars()
            .filter(|c| matches!(c, '\\' | '{' | '}' | '^' | '_'))
            .count()
            > 128
    {
        return None;
    }
    let mut depth = 0_usize;
    let mut escaped = false;
    for c in source.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '{' => depth += 1,
            '}' => depth = depth.checked_sub(1)?,
            _ => {}
        }
    }
    if depth != 0 || escaped {
        return None;
    }
    let nodes = parser::Parser::new(source).parse();
    project_nodes(&nodes).map(|value| value.text)
}

struct MathText {
    text: String,
    atomic: bool,
}

impl MathText {
    fn operand(&self) -> String {
        if self.atomic {
            self.text.clone()
        } else {
            format!("({})", self.text)
        }
    }
}

fn project_nodes(nodes: &[MathNode]) -> Option<MathText> {
    let values = nodes.iter().map(project).collect::<Option<Vec<_>>>()?;
    let meaningful = values
        .iter()
        .filter(|value| !value.text.trim().is_empty())
        .collect::<Vec<_>>();
    let text = values
        .iter()
        .zip(nodes)
        .map(|(value, node)| {
            if meaningful.len() > 1 && matches!(node, MathNode::Group(_) | MathNode::Frac { .. }) {
                value.operand()
            } else {
                value.text.clone()
            }
        })
        .collect::<String>();
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return None;
    }
    let atomic =
        (meaningful.len() == 1 && meaningful[0].atomic) || text.chars().all(|c| c.is_ascii_digit());
    Some(MathText { text, atomic })
}

fn script(source: &str, superscript: bool) -> String {
    // Unicode has complete numeric scripts, but only a partial alphabet.
    // Keep all symbolic/text labels at the normal text size rather than making
    // their appearance depend on which letters happen to have script glyphs.
    let numeric = source.chars().any(|c| c.is_ascii_digit())
        && source
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '+' | '-' | '−' | '=' | '(' | ')'));
    if numeric {
        let mapped = source
            .chars()
            .map(|c| {
                let c = if c == '−' { '-' } else { c };
                if superscript {
                    symbols::to_superscript(c)
                } else {
                    symbols::to_subscript(c)
                }
            })
            .collect::<Option<String>>();
        if let Some(mapped) = mapped {
            return mapped;
        }
    }
    let marker = if superscript { '^' } else { '_' };
    let identifier = source.starts_with(char::is_alphabetic)
        && source
            .chars()
            .all(|c| c.is_alphabetic() || c.is_ascii_digit());
    if identifier {
        format!("{marker}{source}")
    } else {
        format!("{marker}({source})")
    }
}

fn project(node: &MathNode) -> Option<MathText> {
    let (text, atomic) = match node {
        MathNode::Text(value) | MathNode::Symbol(value) => {
            if value.is_empty() || value.starts_with('\\') {
                return None;
            }
            let text = if value == "-" {
                "−".to_owned()
            } else {
                value.clone()
            };
            let atomic = text.chars().all(char::is_alphanumeric) || text == "∞";
            (text, atomic)
        }
        MathNode::Space(_) => (" ".to_owned(), false),
        MathNode::Group(nodes) => return project_nodes(nodes),
        MathNode::Superscript { base, exp } => {
            let base = match base {
                Some(base) => Some(project(base)?),
                None => None,
            };
            (
                format!(
                    "{}{}",
                    base.map(|value| value.operand()).unwrap_or_default(),
                    script(&project(exp)?.text, true)
                ),
                true,
            )
        }
        MathNode::Subscript { base, sub } => {
            let base = match base {
                Some(base) => Some(project(base)?),
                None => None,
            };
            (
                format!(
                    "{}{}",
                    base.map(|value| value.operand()).unwrap_or_default(),
                    script(&project(sub)?.text, false)
                ),
                true,
            )
        }
        MathNode::SubSup { base, sub, exp } => {
            let base = match base {
                Some(base) => Some(project(base)?),
                None => None,
            };
            (
                format!(
                    "{}{}{}",
                    base.map(|value| value.operand()).unwrap_or_default(),
                    script(&project(sub)?.text, false),
                    script(&project(exp)?.text, true)
                ),
                true,
            )
        }
        MathNode::Frac { num, den } => (
            format!("{}/{}", project(num)?.operand(), project(den)?.operand()),
            false,
        ),
        MathNode::Sqrt { index, radicand } => {
            let index = match index {
                Some(index) => Some(project(index)?),
                None => None,
            };
            (
                format!(
                    "{}√{}",
                    index
                        .map(|value| script(&value.text, true))
                        .unwrap_or_default(),
                    project(radicand)?.operand()
                ),
                true,
            )
        }
        MathNode::Binom { n, k } => (
            format!("C({}, {})", project(n)?.text, project(k)?.text),
            true,
        ),
        MathNode::Delimited { left, body, right } => {
            if left.is_empty()
                || right.is_empty()
                || ["\\", "."].contains(&left.as_str())
                || ["\\", "."].contains(&right.as_str())
            {
                return None;
            }
            (format!("{left}{}{right}", project_nodes(body)?.text), true)
        }
        MathNode::Accent { accent, base } => {
            let base = project(base)?;
            if !base.atomic {
                return None;
            }
            let mut chars = base.text.chars();
            (
                format!("{}{}{}", chars.next()?, accent, chars.collect::<String>()),
                true,
            )
        }
        // Preserve layouts that need a spatial representation rather than
        // flattening a matrix, box, or spanning bar into a different formula.
        MathNode::Matrix { .. }
        | MathNode::Boxed(_)
        | MathNode::Overline(_)
        | MathNode::Underline(_) => return None,
    };
    Some(MathText { text, atomic })
}

/// CommonMark math uses dollar delimiters. Normalize TeX's alternate delimiters
/// outside code, HTML, and already-delimited formulas before the Markdown pass.
pub(super) fn normalize_delimiters(source: &str) -> Cow<'_, str> {
    if !source.contains("\\(") && !source.contains("\\[") {
        return Cow::Borrowed(source);
    }
    let mut protected = Vec::new();
    for (event, range) in Parser::new_ext(source, super::markdown::options()).into_offset_iter() {
        if matches!(
            event,
            Event::Code(_)
                | Event::Start(Tag::CodeBlock(_))
                | Event::InlineMath(_)
                | Event::DisplayMath(_)
                | Event::Html(_)
                | Event::InlineHtml(_)
        ) {
            protected.push(range);
        }
    }
    let mut result = String::with_capacity(source.len());
    let mut start = 0;
    for range in protected {
        if range.start < start {
            continue;
        }
        normalize_region(&source[start..range.start], &mut result);
        result.push_str(&source[range.clone()]);
        start = range.end;
    }
    normalize_region(&source[start..], &mut result);
    Cow::Owned(result)
}

fn normalize_region(source: &str, result: &mut String) {
    let mut chars = source.char_indices().peekable();
    while let Some((index, c)) = chars.next() {
        if c == '\\' {
            if let Some(&(_, next)) = chars.peek() {
                if next == '\\' {
                    result.push_str("\\\\");
                    chars.next();
                    continue;
                }
                if matches!(next, '(' | '[') {
                    let close = if next == '(' { "\\)" } else { "\\]" };
                    let body_start = index + 2;
                    let mut end = None;
                    let mut escaped = false;
                    for (offset, candidate) in source[body_start..].char_indices() {
                        if escaped {
                            escaped = false;
                            continue;
                        }
                        if source[body_start + offset..].starts_with(close) {
                            end = Some(body_start + offset);
                            break;
                        }
                        escaped = candidate == '\\';
                    }
                    if let Some(end) = end {
                        let body = &source[body_start..end];
                        if !body.contains('$') && !body.trim().is_empty() {
                            let delimiter = if next == '(' { "$" } else { "$$" };
                            result.push_str(delimiter);
                            result.push_str(body);
                            result.push_str(delimiter);
                        } else {
                            result.push_str(&source[index..end + 2]);
                        }
                        while chars.peek().is_some_and(|(offset, _)| *offset < end + 2) {
                            chars.next();
                        }
                        continue;
                    } else {
                        // Keep pending source without rescanning its tail for
                        // each additional opener in a streaming fragment.
                        result.push_str(&source[index..]);
                        return;
                    }
                }
            }
        }
        result.push(c);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_structured_math_with_text_indices_symbols_and_nested_fractions() {
        assert_eq!(
            render(r"t_{\text{last}} - t_{\text{first}}"),
            Some("t_last − t_first".into())
        );
        assert_eq!(
            render(r"\frac{a + \frac{1}{b}}{\sqrt{x_2}} \leq \alpha^2"),
            Some("((a + (1/b))/√x₂) ≤ α²".into())
        );
        assert_eq!(render(r"E = mc^2"), Some("E = mc²".into()));
    }

    #[test]
    fn symbolic_scripts_use_consistent_normal_size_labels() {
        for (source, expected) in [
            (r"t_{\text{last}} - t_{\text{first}}", "t_last − t_first"),
            (r"x_{start} + x_{end}", "x_start + x_end"),
            (r"x_i + x_j + x_f", "x_i + x_j + x_f"),
            (r"t_{\text{末尾}} - t_{\text{开头}}", "t_末尾 − t_开头"),
            (r"x_{sample2}^n", "x_sample2^n"),
            (r"x_{i+1}^{n-1}", "x_(i+1)^(n−1)"),
            (r"x_{i_j}^{n^2}", "x_(i_j)^(n²)"),
            (r"e^{-x^2}", "e^(−x²)"),
            (r"x_{12}^{-2}", "x₁₂⁻²"),
        ] {
            assert_eq!(render(source).as_deref(), Some(expected), "{source}");
        }
    }

    #[test]
    fn grouping_preserves_fraction_root_and_power_precedence() {
        for (source, expected) in [
            (r"\frac{(a)+b}{c}", "((a)+b)/c"),
            (r"\frac{a}{\frac{b}{c}}", "a/(b/c)"),
            (r"{a+b}^2", "(a+b)²"),
            (r"\frac{a}{b}^2", "(a/b)²"),
            (r"\sqrt{a-b}", "√(a−b)"),
            (r"\frac{a}{b}c", "(a/b)c"),
            (r"{a+b}c", "(a+b)c"),
        ] {
            assert_eq!(render(source).as_deref(), Some(expected), "{source}");
        }
    }

    #[test]
    fn unsupported_incomplete_or_excessively_nested_math_falls_back() {
        for source in [
            r"\custom{x}",
            r"\frac{a}",
            r"x_{",
            r"x^",
            r"x_",
            r"\left(x",
            r"\begin{matrix}1 & 2\end{matrix}",
        ] {
            assert_eq!(render(source), None, "{source}");
        }
        assert_eq!(
            render(&format!("{}x{}", "{".repeat(1_000), "}".repeat(1_000))),
            None
        );
        assert_eq!(render(&"\\sqrt ".repeat(1_000)), None);
        assert_eq!(render(&"a".repeat(10_000)), None);
    }

    #[test]
    fn alternate_delimiters_respect_code_blocks_code_spans_and_existing_math() {
        let source = "Inline \\(x_1\\), display \\[x^2\\]. `\\(literal\\)`\n\n```tex\n\\[literal\\]\n```\n\n    \\(indented\\)\n\n$\\text{\\(text\\)}$";
        let normalized = normalize_delimiters(source);
        assert!(
            normalized.contains("Inline $x_1$, display $$x^2$$."),
            "{normalized}"
        );
        for literal in [
            "`\\(literal\\)`",
            "\\[literal\\]",
            "\\(indented\\)",
            "$\\text{\\(text\\)}$",
        ] {
            assert!(normalized.contains(literal), "{normalized}");
        }
        assert_eq!(
            normalize_delimiters(r"\\(literal\\) and \(pending"),
            r"\\(literal\\) and \(pending"
        );
    }
}
