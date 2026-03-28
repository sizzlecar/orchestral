use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use orchestral_core::config::{ConfigError, OrchestralConfig};
use serde::Deserialize;

use super::SkillEntry;

pub fn discover_skills(
    config: &OrchestralConfig,
    config_path: &Path,
) -> Result<Vec<SkillEntry>, ConfigError> {
    if !config.extensions.skill.enabled || env_disable_flag("ORCHESTRAL_DISABLE_SKILLS") {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    if config.extensions.skill.auto_discover {
        for dir in candidate_skill_directories(config, config_path) {
            collect_skill_files(&dir, 3, &mut files);
        }
    }

    let mut entries_by_name = HashMap::new();
    for file in files {
        let Some(entry) = parse_skill_file(&file) else {
            continue;
        };
        entries_by_name.entry(entry.name.clone()).or_insert(entry);
    }

    let mut entries = entries_by_name.into_values().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

fn parse_skill_file(path: &Path) -> Option<SkillEntry> {
    let content = fs::read_to_string(path).ok()?;
    let meta = parse_skill_frontmatter(&content);
    let name = meta
        .name
        .or_else(|| {
            path.parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "skill".to_string());

    let instructions = extract_skill_body(&content);
    let skill_dir = path.parent();
    let scripts_dir = skill_dir.map(|p| p.join("scripts")).filter(|p| p.is_dir());
    let venv_python = skill_dir
        .map(|p| p.join(".venv/bin/python3"))
        .filter(|p| p.exists());

    Some(SkillEntry {
        name,
        description: meta.description.unwrap_or_default(),
        instructions,
        source_path: path.to_path_buf(),
        scripts_dir,
        venv_python,
        compatibility: meta.compatibility,
        license: meta.license,
        metadata: meta.metadata,
    })
}

fn extract_skill_body(content: &str) -> String {
    let Some(rest) = content.strip_prefix("---\n") else {
        return content.trim().to_string();
    };
    let Some((_frontmatter, tail)) = rest.split_once("\n---\n") else {
        return content.trim().to_string();
    };
    tail.trim().to_string()
}

fn candidate_skill_directories(config: &OrchestralConfig, config_path: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let roots = discovery_roots(config_path);
    for root in &roots {
        dirs.push(root.join(".claude").join("skills"));
        dirs.push(root.join(".codex").join("skills"));
        dirs.push(root.join("skills"));
    }

    if let Ok(home) = std::env::var("HOME") {
        let home_path = PathBuf::from(home);
        dirs.push(home_path.join(".claude").join("skills"));
        dirs.push(home_path.join(".codex").join("skills"));
    }

    let extra_dirs = extra_paths_from_config_and_env(
        &config.extensions.skill.directories,
        "ORCHESTRAL_SKILL_EXTRA_DIRS",
    );
    for custom in &extra_dirs {
        let path = PathBuf::from(custom);
        if path.is_absolute() {
            dirs.push(path);
        } else {
            for root in &roots {
                dirs.push(root.join(&path));
            }
        }
    }

    dedupe_paths(dirs)
}

fn discovery_roots(config_path: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }

    // Canonicalize config_path so discovery works regardless of CWD changes.
    let resolved = config_path
        .canonicalize()
        .unwrap_or_else(|_| config_path.to_path_buf());
    if let Some(base) = resolved.parent() {
        roots.push(base.to_path_buf());
        if let Some(parent) = base.parent() {
            roots.push(parent.to_path_buf());
        }
    }

    dedupe_paths(roots)
}

fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for path in paths {
        let key = path.to_string_lossy().to_string();
        if seen.insert(key) {
            out.push(path);
        }
    }
    out
}

fn collect_skill_files(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    if depth == 0 {
        return;
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_skill_files(&path, depth - 1, out);
            continue;
        }

        let is_skill = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.eq_ignore_ascii_case("SKILL.md"))
            .unwrap_or(false);
        if is_skill {
            out.push(path);
        }
    }
}

#[derive(Debug, Default, Clone)]
struct SkillFrontmatter {
    name: Option<String>,
    description: Option<String>,
    compatibility: Option<String>,
    license: Option<String>,
    metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
struct SkillFrontmatterRaw {
    name: Option<String>,
    description: Option<String>,
    compatibility: Option<String>,
    license: Option<String>,
    #[serde(default)]
    metadata: std::collections::HashMap<String, serde_yaml::Value>,
}

fn parse_skill_frontmatter(content: &str) -> SkillFrontmatter {
    let Some(rest) = content.strip_prefix("---\n") else {
        return SkillFrontmatter::default();
    };

    let Some((frontmatter, _tail)) = rest.split_once("\n---\n") else {
        return SkillFrontmatter::default();
    };

    match serde_yaml::from_str::<SkillFrontmatterRaw>(frontmatter) {
        Ok(raw) => SkillFrontmatter {
            name: raw.name,
            description: raw.description,
            compatibility: raw.compatibility,
            license: raw.license,
            metadata: raw
                .metadata
                .into_iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_yaml::Value::String(s) => s,
                        other => format!("{:?}", other),
                    };
                    (k, s)
                })
                .collect(),
        },
        Err(_) => SkillFrontmatter::default(),
    }
}

fn extra_paths_from_config_and_env(config_paths: &[String], env_var: &str) -> Vec<String> {
    let mut paths = config_paths.to_vec();
    if let Ok(value) = std::env::var(env_var) {
        for segment in value.split(':') {
            let trimmed = segment.trim();
            if !trimmed.is_empty() {
                paths.push(trimmed.to_string());
            }
        }
    }
    paths
}

fn env_disable_flag(var_name: &str) -> bool {
    std::env::var(var_name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|v| v.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "orchestral-{}-{}-{}",
            prefix,
            std::process::id(),
            nanos
        ));
        let _ = fs::create_dir_all(&path);
        path
    }

    #[test]
    fn test_discover_skills() {
        let dir = test_temp_dir("skill-discovery");
        let config_path = dir.join("orchestral.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");

        let skill_dir = dir.join(".claude/skills/demo");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: demo-skill\ndescription: demo description\n---\nbody content\n",
        )
        .expect("write skill");

        let mut config = OrchestralConfig::default();
        config.extensions.skill.enabled = true;
        config.extensions.skill.auto_discover = true;

        let skills = discover_skills(&config, &config_path).expect("discover should work");
        let skill = skills
            .iter()
            .find(|entry| entry.name == "demo-skill")
            .expect("demo-skill should be discovered");
        assert_eq!(skill.description, "demo description");
        assert_eq!(skill.instructions, "body content");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_discover_skills_with_scripts_dir() {
        let dir = test_temp_dir("skill-discovery-scripts");
        let config_path = dir.join("orchestral.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");

        let skill_dir = dir.join(".claude/skills/xlsx");
        fs::create_dir_all(skill_dir.join("scripts")).expect("create scripts dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: xlsx\ndescription: spreadsheets\n---\nuse scripts/recalc.py\n",
        )
        .expect("write skill");

        let mut config = OrchestralConfig::default();
        config.extensions.skill.enabled = true;
        config.extensions.skill.auto_discover = true;

        let skills = discover_skills(&config, &config_path).expect("discover should work");
        let skill = skills
            .iter()
            .find(|entry| entry.name == "xlsx")
            .expect("xlsx should be discovered");
        assert!(skill.scripts_dir.is_some());

        let _ = fs::remove_dir_all(dir);
    }
}
