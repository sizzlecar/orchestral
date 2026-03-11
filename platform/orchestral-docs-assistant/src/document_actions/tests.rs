use serde_json::json;

use super::support::{
    build_outline, parse_headings, render_sections, score_chunks, summarize_text,
};

#[test]
fn test_parse_headings_and_outline() {
    let markdown = "# H1\n\ntext\n\n## H2\n\n### H3";
    let headings = parse_headings(markdown);
    assert_eq!(headings.len(), 3);
    let outline = build_outline(&headings);
    assert!(outline.contains("- H1"));
    assert!(outline.contains("  - H2"));
}

#[test]
fn test_summarize_text() {
    let markdown = "First sentence. Second sentence! Third sentence? Fourth.";
    let summary = summarize_text(markdown, 2);
    assert!(summary.contains("First sentence."));
    assert!(summary.contains("Second sentence!"));
    assert!(!summary.contains("Third sentence?"));
}

#[test]
fn test_score_chunks() {
    let question = "what is rust ownership";
    let chunks = vec![
        "Rust ownership ensures memory safety.".to_string(),
        "Cooking recipe for pasta.".to_string(),
    ];
    let evidence = score_chunks(question, &chunks, 2);
    assert_eq!(evidence.len(), 1);
    assert!(evidence[0].2.to_ascii_lowercase().contains("ownership"));
}

#[test]
fn test_render_sections_validation() {
    let sections = vec![json!({"heading": "Intro", "content": "Hello"})];
    let rendered = render_sections(&sections).expect("render");
    assert!(rendered.contains("## Intro"));
    assert!(rendered.contains("Hello"));
}
