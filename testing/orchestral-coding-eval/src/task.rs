use std::collections::BTreeSet;
use std::path::{Component, Path};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

pub const CATALOG: &str = include_str!("../tasks.json");
pub const TEST_BOUNDARY: &str = "\n#[cfg(test)]\nmod tests {";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Suite {
    pub version: u32,
    pub base_commit: String,
    pub description: String,
    pub tasks: Vec<Task>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    pub id: String,
    pub category: String,
    pub prompt: String,
    #[serde(default)]
    pub inspect_first: bool,
    pub mutations: Vec<Mutation>,
    pub checks: Vec<Check>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Mutation {
    pub path: String,
    pub before: String,
    pub after: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    pub package: String,
    pub target: String,
    pub filter: String,
    pub expected_tests: Vec<String>,
}

impl Suite {
    pub fn load() -> Result<Self> {
        let suite: Self = serde_json::from_str(CATALOG)?;
        ensure!(suite.version == 1, "unsupported task catalog version");
        ensure!(
            suite.base_commit.len() == 40
                && suite.base_commit.bytes().all(|c| c.is_ascii_hexdigit()),
            "catalog must pin a full Git commit"
        );
        let mut ids = BTreeSet::new();
        for task in &suite.tasks {
            ensure!(
                !task.id.is_empty()
                    && task
                        .id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-')
                    && ids.insert(&task.id),
                "invalid or duplicate task ID"
            );
            ensure!(
                !task.prompt.is_empty() && !task.mutations.is_empty(),
                "empty task"
            );
            ensure!(!task.checks.is_empty(), "task {} has no verifier", task.id);
            for mutation in &task.mutations {
                safe_relative(&mutation.path)?;
                ensure!(
                    !mutation.before.is_empty() && mutation.before != mutation.after,
                    "empty or ineffective mutation"
                );
            }
            for check in &task.checks {
                ensure!(!check.expected_tests.is_empty(), "empty expected test list");
                ensure!(
                    check.target == "lib" || check.target.starts_with("test:"),
                    "unsupported verifier target"
                );
            }
        }
        ensure!(!suite.tasks.is_empty(), "empty suite");
        Ok(suite)
    }

    pub fn select<'a>(&'a self, requested: &[String]) -> Result<Vec<&'a Task>> {
        for id in requested {
            ensure!(
                self.tasks.iter().any(|task| &task.id == id),
                "unknown task: {id}"
            );
        }
        Ok(self
            .tasks
            .iter()
            .filter(|task| requested.is_empty() || requested.contains(&task.id))
            .collect())
    }
}

impl Task {
    pub fn paths(&self) -> BTreeSet<&str> {
        self.mutations
            .iter()
            .map(|mutation| mutation.path.as_str())
            .collect()
    }

    pub fn user_prompt(&self) -> String {
        format!(
            "{}\n\nKeep existing user changes. Limit production changes to: {}. Preserve existing tests, manifests and documentation. Do not stage, commit, publish, or discard user changes. Run relevant tests and report their actual results and anything left unverified.",
            self.prompt,
            self.paths().into_iter().collect::<Vec<_>>().join(", ")
        )
    }
}

pub fn safe_relative(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && !value.contains(['\\', '\0', ':'])
            && Path::new(value)
                .components()
                .all(|c| matches!(c, Component::Normal(_))),
        "unsafe relative path: {value:?}"
    );
    Ok(())
}

pub fn mutate(source: &str, mutation: &Mutation) -> Result<String> {
    let (production, tests) = split_tests(source);
    ensure!(
        production.matches(&mutation.before).count() == 1,
        "mutation for {} must match production exactly once",
        mutation.path
    );
    Ok(format!(
        "{}{}",
        production.replacen(&mutation.before, &mutation.after, 1),
        tests
    ))
}

pub fn split_tests(source: &str) -> (&str, &str) {
    source
        .find(TEST_BOUNDARY)
        .map_or((source, ""), |offset| source.split_at(offset))
}

/// Preserve the trusted unit tests even when grading edited production files.
pub fn verified_source(original: &str, candidate: &str) -> Result<String> {
    let (_, tests) = split_tests(original);
    if tests.is_empty() {
        return Ok(candidate.to_owned());
    }
    let (production, candidate_tests) = split_tests(candidate);
    ensure!(
        candidate_tests == tests,
        "existing embedded tests changed or removed"
    );
    Ok(format!("{production}{tests}"))
}

pub fn text(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).context("source is not UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_selection_rejects_typos_and_the_catalog_contains_twenty_distinct_tasks() {
        let suite = Suite::load().unwrap();
        assert_eq!(suite.tasks.len(), 20);
        assert!(suite.select(&["misspelled".into()]).is_err());
        assert_eq!(suite.select(&[suite.tasks[0].id.clone()]).unwrap().len(), 1);
    }

    #[test]
    fn deleting_or_weakening_embedded_tests_cannot_pass_verification() {
        let original = format!(
            "fn work() {{}}{TEST_BOUNDARY}\n#[test] fn checks() {{ assert!(false); }}\n}}\n"
        );
        assert!(verified_source(&original, "fn work() {}").is_err());
        assert!(verified_source(
            &original,
            &original.replace("assert!(false)", "assert!(true)")
        )
        .is_err());
        assert!(verified_source(
            &original,
            &original.replace("fn work() {}", "fn work() { let _ = 1; }")
        )
        .is_ok());
    }

    #[test]
    fn mutations_cannot_silently_target_multiple_sites_or_only_a_test() {
        let mutation = Mutation {
            path: "src/lib.rs".into(),
            before: "old".into(),
            after: "new".into(),
        };
        assert!(mutate("old old", &mutation).is_err());
        assert!(mutate(&format!("code{TEST_BOUNDARY} old }}"), &mutation).is_err());
        assert_eq!(mutate("old", &mutation).unwrap(), "new");
        for path in ["../outside", "/absolute", "a/../../b", "C:escape", "a\\b"] {
            assert!(safe_relative(path).is_err());
        }
    }
}
