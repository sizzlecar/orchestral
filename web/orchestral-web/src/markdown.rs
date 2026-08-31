use pulldown_cmark::{html, CowStr, Event, Options, Parser, Tag};

/// Render model Markdown as a safe HTML subset for the conversation UI.
///
/// Normal Markdown text is escaped by `pulldown-cmark`. Embedded HTML is
/// converted back to text and unsafe link/image destinations are neutralized
/// before the generated markup enters the DOM.
pub fn render(markdown: &str) -> String {
    let options = Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TABLES
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let events = Parser::new_ext(markdown, options).map(safe_event);
    let mut output = String::new();
    html::push_html(&mut output, events);
    output
}

fn safe_event(event: Event<'_>) -> Event<'_> {
    match event {
        Event::Html(value) | Event::InlineHtml(value) => Event::Text(value),
        Event::Start(Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Link {
            link_type,
            dest_url: safe_destination(dest_url, "#"),
            title,
            id,
        }),
        Event::Start(Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Image {
            link_type,
            dest_url: safe_destination(dest_url, ""),
            title,
            id,
        }),
        other => other,
    }
}

fn safe_destination<'a>(destination: CowStr<'a>, fallback: &'static str) -> CowStr<'a> {
    let normalized = destination
        .trim()
        .chars()
        .filter(|character| !character.is_ascii_control() && !character.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let has_scheme = normalized.find(':').is_some_and(|colon| {
        normalized[..colon]
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '+' | '-' | '.'))
    });
    let allowed = !has_scheme
        || normalized.starts_with("http:")
        || normalized.starts_with("https:")
        || normalized.starts_with("mailto:");
    if allowed {
        destination
    } else {
        CowStr::Borrowed(fallback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_common_agent_markdown() {
        let html = render("### 标题\n\n- **重点** `code`\n\n| A | B |\n|---|---|\n| 1 | 2 |");

        assert!(html.contains("<h3>标题</h3>"));
        assert!(html.contains("<strong>重点</strong> <code>code</code>"));
        assert!(html.contains("<table>"));
    }

    #[test]
    fn raw_html_and_unsafe_links_do_not_execute() {
        let html = render("<script>alert(1)</script> [bad](javascript:evil)");

        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains("href=\"javascript:"));
        assert_eq!(
            safe_destination(CowStr::Borrowed("javascript:evil"), "#"),
            CowStr::Borrowed("#")
        );
    }
}
