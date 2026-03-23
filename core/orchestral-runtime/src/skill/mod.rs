pub mod discovery;

use orchestral_core::planner::SkillInstruction;
use std::collections::BTreeSet;
use std::path::PathBuf;

const MAX_SKILL_KEYWORDS: usize = 10;
const MAX_SKILL_HIGHLIGHTS: usize = 8;
const MAX_SKILL_CARD_CHARS: usize = 1_600;
const MIN_SKILL_MATCH_SCORE: usize = 12;

#[derive(Debug, Clone)]
pub struct SkillEntry {
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub source_path: PathBuf,
    pub scripts_dir: Option<PathBuf>,
    /// Skill-local virtual-env python binary, auto-detected from `<skill_dir>/.venv/bin/python3`.
    pub venv_python: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct SkillCatalog {
    entries: Vec<SkillEntry>,
    max_active: usize,
}

impl SkillCatalog {
    pub fn new(entries: Vec<SkillEntry>, max_active: usize) -> Self {
        Self {
            entries,
            max_active,
        }
    }

    pub fn entries(&self) -> &[SkillEntry] {
        &self.entries
    }

    pub fn active_entries(&self) -> Vec<&SkillEntry> {
        if self.max_active == 0 {
            return Vec::new();
        }

        self.entries.iter().take(self.max_active).collect()
    }

    pub fn build_instructions(&self, intent: &str) -> Vec<SkillInstruction> {
        self.matched_entries(intent)
            .into_iter()
            .map(|entry| SkillInstruction {
                skill_name: entry.name.clone(),
                instructions: build_skill_card(entry),
                skill_path: Some(entry.source_path.to_string_lossy().to_string()),
                scripts_dir: entry
                    .scripts_dir
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
                venv_python: entry
                    .venv_python
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string()),
            })
            .collect()
    }

    fn matched_entries(&self, intent: &str) -> Vec<&SkillEntry> {
        if self.max_active == 0 {
            return Vec::new();
        }

        let mut scored = self
            .entries
            .iter()
            .filter_map(|entry| {
                let score = score_skill(intent, entry);
                if score == 0 {
                    None
                } else {
                    Some((entry, score))
                }
            })
            .collect::<Vec<_>>();

        scored.sort_by(|(left_entry, left_score), (right_entry, right_score)| {
            right_score
                .cmp(left_score)
                .then_with(|| left_entry.name.cmp(&right_entry.name))
        });

        scored
            .into_iter()
            .take(self.max_active)
            .map(|(entry, _score)| entry)
            .collect()
    }
}

fn score_skill(intent: &str, entry: &SkillEntry) -> usize {
    let normalized_intent = normalize_text(intent);
    if normalized_intent.is_empty() {
        return 0;
    }

    let raw_intent = intent.to_ascii_lowercase();
    let intent_tokens = expand_token_aliases(&tokenize(intent));
    let name_tokens = meaningful_tokens(expand_token_aliases(&tokenize(&entry.name)));
    let description_tokens = meaningful_tokens(expand_token_aliases(&tokenize(&entry.description)));
    let keyword_tokens = expand_token_aliases(&extract_skill_keywords(entry));

    let mut score = 0usize;
    let name_overlap = overlap_count(&intent_tokens, &name_tokens);
    let description_overlap = overlap_count(&intent_tokens, &description_tokens);
    let keyword_overlap = overlap_count(&intent_tokens, &keyword_tokens);
    let normalized_name = normalize_text(&entry.name);
    if !normalized_name.is_empty() {
        let exact_name = format!(" {} ", normalized_name);
        let padded_intent = format!(" {} ", normalized_intent);
        if padded_intent.contains(&exact_name)
            || raw_intent.contains(&format!("${}", entry.name.to_ascii_lowercase()))
        {
            score += 1_000;
        }
    }

    score += name_overlap * 80;
    score += description_overlap * 18;
    if score > 0 {
        score += keyword_overlap * 10;
    }

    if score >= MIN_SKILL_MATCH_SCORE {
        score
    } else {
        0
    }
}

fn meaningful_tokens(mut tokens: BTreeSet<String>) -> BTreeSet<String> {
    tokens.retain(|token| !is_generic_skill_token(token));
    tokens
}

fn build_skill_card(entry: &SkillEntry) -> String {
    let mut lines = Vec::new();

    if !entry.description.trim().is_empty() {
        lines.push(format!("summary: {}", entry.description.trim()));
    }

    let keywords = extract_skill_keywords(entry)
        .into_iter()
        .take(MAX_SKILL_KEYWORDS)
        .collect::<Vec<_>>();
    if !keywords.is_empty() {
        lines.push(format!("keywords: {}", keywords.join(", ")));
    }

    let referenced_scripts = extract_referenced_scripts(&entry.instructions);
    if !referenced_scripts.is_empty() {
        lines.push(format!("scripts: {}", referenced_scripts.join(", ")));
    }

    let highlights = extract_skill_highlights(&entry.instructions);
    if !highlights.is_empty() {
        lines.push("highlights:".to_string());
        for highlight in highlights.into_iter().take(MAX_SKILL_HIGHLIGHTS) {
            lines.push(format!("- {}", highlight));
        }
    }

    truncate_card(lines.join("\n"), MAX_SKILL_CARD_CHARS)
}

fn extract_referenced_scripts(instructions: &str) -> Vec<String> {
    let mut scripts = BTreeSet::new();
    for raw in instructions.split_whitespace() {
        let candidate = raw
            .trim_matches(|ch: char| {
                matches!(
                    ch,
                    '`' | '"' | '\'' | '(' | ')' | '[' | ']' | ',' | ';' | ':'
                )
            })
            .trim();
        if !candidate.starts_with("scripts/") {
            continue;
        }
        if !(candidate.ends_with(".py")
            || candidate.ends_with(".sh")
            || candidate.ends_with(".js")
            || candidate.ends_with(".ts")
            || candidate.ends_with(".rb"))
        {
            continue;
        }
        scripts.insert(candidate.to_string());
    }
    scripts.into_iter().collect()
}

fn extract_skill_keywords(entry: &SkillEntry) -> BTreeSet<String> {
    let mut keywords = BTreeSet::new();
    keywords.extend(tokenize(&entry.name));
    keywords.extend(tokenize(&entry.description));

    let mut in_code_block = false;
    for line in entry.instructions.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            continue;
        }
        if in_code_block || trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('#')
            || trimmed.starts_with('-')
            || trimmed.starts_with('*')
            || trimmed
                .chars()
                .next()
                .map(|ch| ch.is_ascii_digit())
                .unwrap_or(false)
        {
            keywords.extend(tokenize(trimmed));
        }
    }

    keywords.retain(|token| !is_generic_skill_token(token));
    keywords
}

fn extract_skill_highlights(instructions: &str) -> Vec<String> {
    let mut highlights = Vec::new();
    let mut seen = BTreeSet::new();
    let mut in_code_block = false;

    for line in instructions.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            continue;
        }
        if in_code_block || trimmed.is_empty() {
            continue;
        }

        let candidate = match highlight_priority(trimmed) {
            Some(text) => normalize_highlight(text),
            None => continue,
        };
        if candidate.is_empty() || candidate.len() > 180 {
            continue;
        }
        if seen.insert(candidate.clone()) {
            highlights.push(candidate);
        }
        if highlights.len() >= MAX_SKILL_HIGHLIGHTS {
            break;
        }
    }

    highlights
}

fn highlight_priority(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    if trimmed.starts_with('#') {
        return Some(trimmed.trim_start_matches('#').trim());
    }
    if trimmed.starts_with("- ") || trimmed.starts_with("* ") {
        return Some(trimmed[2..].trim());
    }
    if trimmed
        .split_once('.')
        .map(|(head, _)| !head.is_empty() && head.chars().all(|ch| ch.is_ascii_digit()))
        .unwrap_or(false)
    {
        let (_, tail) = trimmed.split_once('.').unwrap_or_default();
        return Some(tail.trim());
    }
    if trimmed.contains("MUST")
        || trimmed.contains("Do NOT")
        || trimmed.contains("Always")
        || trimmed.contains("Required")
        || trimmed.contains("Workflow")
    {
        return Some(trimmed);
    }
    None
}

fn normalize_highlight(line: &str) -> String {
    let mut text = line
        .trim()
        .trim_matches('*')
        .trim_matches('`')
        .replace("**", "")
        .replace("__", "");
    text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    text
}

fn truncate_card(mut card: String, max_chars: usize) -> String {
    if card.len() <= max_chars {
        return card;
    }

    while card.len() > max_chars && card.ends_with('\n') {
        card.pop();
    }
    if card.len() <= max_chars {
        return card;
    }

    let mut truncated = String::new();
    for ch in card.chars() {
        if truncated.len() + ch.len_utf8() > max_chars.saturating_sub(3) {
            break;
        }
        truncated.push(ch);
    }
    truncated.push_str("...");
    truncated
}

fn overlap_count(intent_tokens: &BTreeSet<String>, skill_tokens: &BTreeSet<String>) -> usize {
    intent_tokens.intersection(skill_tokens).count()
}

fn tokenize(text: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let mut current = String::new();

    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
            continue;
        }
        push_token(&mut tokens, &mut current);
    }
    push_token(&mut tokens, &mut current);
    tokens
}

fn push_token(tokens: &mut BTreeSet<String>, current: &mut String) {
    if current.len() < 3 {
        current.clear();
        return;
    }
    let token = current.clone();
    tokens.insert(token.clone());
    if token.ends_with('s') && token.len() > 4 {
        tokens.insert(token.trim_end_matches('s').to_string());
    }
    current.clear();
}

fn expand_token_aliases(tokens: &BTreeSet<String>) -> BTreeSet<String> {
    let mut expanded = tokens.clone();
    for token in tokens {
        for alias in alias_tokens(token) {
            expanded.insert((*alias).to_string());
        }
    }
    expanded
}

fn alias_tokens(token: &str) -> &'static [&'static str] {
    match token {
        "excel" | "xlsx" | "xlsm" | "spreadsheet" | "spreadsheets" | "workbook" | "workbooks"
        | "worksheet" | "worksheets" | "sheet" | "sheets" | "table" | "tables" | "csv" | "tsv"
        | "tabular" => &[
            "excel",
            "xlsx",
            "spreadsheet",
            "workbook",
            "worksheet",
            "tabular",
            "csv",
            "tsv",
        ],
        "presentation" | "presentations" | "deck" | "decks" | "slide" | "slides" | "ppt"
        | "pptx" => &["presentation", "deck", "slides", "pptx"],
        "install" | "installer" | "installation" | "setup" | "configure" => {
            &["install", "installer", "setup"]
        }
        "create" | "creator" | "author" | "build" | "write" => {
            &["create", "creator", "build", "write"]
        }
        "skill" | "skills" => &["skill"],
        _ => &[],
    }
}

fn normalize_text(text: &str) -> String {
    text.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_generic_skill_token(token: &str) -> bool {
    matches!(
        token,
        "skill"
            | "skills"
            | "doc"
            | "docs"
            | "this"
            | "that"
            | "with"
            | "when"
            | "where"
            | "from"
            | "into"
            | "your"
            | "their"
            | "there"
            | "have"
            | "must"
            | "should"
            | "will"
            | "using"
            | "user"
            | "users"
            | "workflow"
            | "common"
            | "important"
            | "overview"
            | "quick"
            | "start"
            | "reference"
            | "map"
            | "output"
            | "outputs"
            | "input"
            | "inputs"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_skill(name: &str, description: &str) -> SkillEntry {
        SkillEntry {
            name: name.to_string(),
            description: description.to_string(),
            instructions: format!("instructions for {name}"),
            source_path: PathBuf::from(format!("/tmp/{name}/SKILL.md")),
            scripts_dir: None,
            venv_python: None,
        }
    }

    #[test]
    fn test_skill_catalog_active_entries_returns_all_up_to_limit() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("xlsx", "xlsx creation and formula recalc"),
                make_skill("git", "git commit and branch operations"),
            ],
            3,
        );

        let active = catalog.active_entries();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].name, "xlsx");
        assert_eq!(active[1].name, "git");
    }

    #[test]
    fn test_skill_catalog_max_active() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("a", "demo skill one"),
                make_skill("b", "demo skill two"),
                make_skill("c", "demo skill three"),
            ],
            2,
        );

        let active = catalog.active_entries();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].name, "a");
        assert_eq!(active[1].name, "b");
    }

    #[test]
    fn test_skill_catalog_zero_max_active_disables_loading() {
        let catalog = SkillCatalog::new(vec![make_skill("xlsx", "spreadsheet skill")], 3);
        let disabled = SkillCatalog::new(vec![make_skill("xlsx", "spreadsheet skill")], 0);

        assert_eq!(catalog.active_entries().len(), 1);
        assert!(disabled.active_entries().is_empty());
    }

    #[test]
    fn test_build_instructions_uses_ranked_skill_card() {
        let catalog = SkillCatalog::new(
            vec![SkillEntry {
                name: "xlsx".to_string(),
                description: "spreadsheet skill for formulas and formatting".to_string(),
                instructions:
                    "# Workflow\n- Use openpyxl for formulas\n- Recalculate before handoff\n"
                        .to_string(),
                source_path: PathBuf::from("/tmp/xlsx/SKILL.md"),
                scripts_dir: Some(PathBuf::from("/tmp/xlsx/scripts")),
                venv_python: Some(PathBuf::from("/tmp/xlsx/.venv/bin/python3")),
            }],
            3,
        );

        let instructions = catalog.build_instructions("please help with spreadsheet data");
        assert_eq!(instructions.len(), 1);
        let first = &instructions[0];
        assert_eq!(first.skill_name, "xlsx");
        assert!(first
            .instructions
            .contains("summary: spreadsheet skill for formulas and formatting"));
        assert!(first.instructions.contains("highlights:"));
        assert!(first.instructions.contains("Use openpyxl for formulas"));
        assert_eq!(first.skill_path.as_deref(), Some("/tmp/xlsx/SKILL.md"));
        assert_eq!(first.scripts_dir.as_deref(), Some("/tmp/xlsx/scripts"));
        assert_eq!(
            first.venv_python.as_deref(),
            Some("/tmp/xlsx/.venv/bin/python3")
        );
    }

    #[test]
    fn test_build_instructions_prefers_relevant_skill_matches() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("slides", "presentation deck editing and export"),
                make_skill("xlsx", "spreadsheet workbook editing and formula repair"),
                make_skill("skill-installer", "install skills into the environment"),
            ],
            2,
        );

        let instructions = catalog.build_instructions("docs 下面有个 excel，把需要填的都填了");
        assert_eq!(instructions.len(), 1);
        assert_eq!(instructions[0].skill_name, "xlsx");
    }

    #[test]
    fn test_build_instructions_does_not_match_generic_docs_skill_for_excel_intent() {
        let catalog = SkillCatalog::new(
            vec![SkillEntry {
                name: "openai-docs".to_string(),
                description: "Use when the user asks how to build with OpenAI products or APIs"
                    .to_string(),
                instructions: "# OpenAI Docs\n- Search official docs\n".to_string(),
                source_path: PathBuf::from("/tmp/openai-docs/SKILL.md"),
                scripts_dir: None,
                venv_python: None,
            }],
            3,
        );

        assert!(catalog
            .build_instructions("docs 目录下有一个excel，帮我填一下")
            .is_empty());
    }

    #[test]
    fn test_build_instructions_requires_name_or_description_relevance_for_keyword_matches() {
        let catalog = SkillCatalog::new(
            vec![
                SkillEntry {
                    name: "skill-creator".to_string(),
                    description: "Guide for creating effective skills".to_string(),
                    instructions: "# Workflow\n- Tool integrations\n- Domain expertise\n"
                        .to_string(),
                    source_path: PathBuf::from("/tmp/skill-creator/SKILL.md"),
                    scripts_dir: None,
                    venv_python: None,
                },
                make_skill("xlsx", "spreadsheet workbook editing and formula repair"),
            ],
            3,
        );

        let instructions = catalog.build_instructions("请把 excel 表格里的空白都填上");
        assert_eq!(instructions.len(), 1);
        assert_eq!(instructions[0].skill_name, "xlsx");
    }

    #[test]
    fn test_build_instructions_respects_explicit_skill_name() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("slides", "presentation deck editing and export"),
                make_skill("xlsx", "spreadsheet workbook editing and formula repair"),
            ],
            2,
        );

        let instructions = catalog.build_instructions("请用 $slides skill 处理这个 deck");
        assert_eq!(instructions.len(), 1);
        assert_eq!(instructions[0].skill_name, "slides");
    }

    #[test]
    fn test_build_instructions_skips_irrelevant_skills() {
        let catalog = SkillCatalog::new(
            vec![
                make_skill("slides", "presentation deck editing and export"),
                make_skill("xlsx", "spreadsheet workbook editing and formula repair"),
            ],
            2,
        );

        assert!(catalog
            .build_instructions("帮我写一段 rust 单元测试")
            .is_empty());
    }
}
