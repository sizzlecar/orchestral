use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::mcp_config::user_config_root;

const SKILL_PREFERENCES_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SkillPreference {
    pub path: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SkillPreferences {
    version: u32,
    #[serde(default)]
    config: Vec<SkillPreference>,
}

impl Default for SkillPreferences {
    fn default() -> Self {
        Self {
            version: SKILL_PREFERENCES_VERSION,
            config: Vec::new(),
        }
    }
}

impl SkillPreferences {
    fn validate(&self) -> anyhow::Result<()> {
        if self.version != SKILL_PREFERENCES_VERSION {
            bail!(
                "unsupported Skill preferences version {}; expected {SKILL_PREFERENCES_VERSION}",
                self.version
            );
        }
        let mut seen = BTreeSet::new();
        for entry in &self.config {
            let path = Path::new(&entry.path);
            if !path.is_absolute() || entry.path.chars().any(char::is_control) {
                bail!("Skill preference path must be an absolute path");
            }
            if !seen.insert(entry.path.as_str()) {
                bail!("Skill preferences contain duplicate paths");
            }
        }
        Ok(())
    }

    pub(crate) fn is_enabled(&self, source: &str) -> bool {
        self.config
            .iter()
            .find(|entry| entry.path == source)
            .is_none_or(|entry| entry.enabled)
    }

    pub(crate) fn disabled_sources(&self) -> BTreeSet<String> {
        self.config
            .iter()
            .filter(|entry| !entry.enabled)
            .map(|entry| entry.path.clone())
            .collect()
    }

    pub(crate) fn set_enabled(&mut self, source: String, enabled: bool) {
        if let Some(entry) = self.config.iter_mut().find(|entry| entry.path == source) {
            entry.enabled = enabled;
        } else {
            self.config.push(SkillPreference {
                path: source,
                enabled,
            });
        }
        self.config
            .sort_by(|left, right| left.path.cmp(&right.path));
    }
}

pub(crate) fn user_skill_preferences_path() -> anyhow::Result<PathBuf> {
    Ok(user_config_root()?.join("skills.json"))
}

pub(crate) fn load_skill_preferences(path: &Path) -> anyhow::Result<SkillPreferences> {
    if !path.exists() {
        return Ok(SkillPreferences::default());
    }
    let bytes =
        fs::read(path).with_context(|| format!("read Skill preferences '{}'", path.display()))?;
    let preferences: SkillPreferences = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode Skill preferences '{}'", path.display()))?;
    preferences.validate()?;
    Ok(preferences)
}

pub(crate) fn save_skill_preferences(
    path: &Path,
    preferences: &SkillPreferences,
) -> anyhow::Result<()> {
    preferences.validate()?;
    let parent = path
        .parent()
        .context("Skill preferences path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create user config directory '{}'", parent.display()))?;
    secure_directory(parent)?;
    let temporary = parent.join(format!(
        ".skills.json.{}.{}.tmp",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let bytes = serde_json::to_vec_pretty(preferences).context("encode Skill preferences")?;
    fs::write(&temporary, bytes).with_context(|| {
        format!(
            "write temporary Skill preferences '{}'",
            temporary.display()
        )
    })?;
    secure_file(&temporary)?;
    fs::rename(&temporary, path)
        .with_context(|| format!("persist Skill preferences '{}'", path.display()))?;
    secure_file(path)
}

#[cfg(unix)]
fn secure_directory(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("secure user config directory '{}'", path.display()))
}

#[cfg(not(unix))]
fn secure_directory(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn secure_file(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("secure Skill preferences '{}'", path.display()))
}

#[cfg(not(unix))]
fn secure_file(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferences_round_trip_by_absolute_skill_path() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-skill-preferences-{}",
            uuid::Uuid::new_v4()
        ));
        let path = root.join("skills.json");
        let skill = root.join("skills/demo/SKILL.md");
        let source = skill.to_string_lossy().into_owned();
        let mut preferences = SkillPreferences::default();
        preferences.set_enabled(source.clone(), false);

        save_skill_preferences(&path, &preferences).unwrap();
        let loaded = load_skill_preferences(&path).unwrap();

        assert!(!loaded.is_enabled(&source));
        assert_eq!(loaded.disabled_sources(), BTreeSet::from([source.clone()]));
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(persisted["config"][0]["path"], source);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn enabled_is_the_default_without_an_override() {
        assert!(SkillPreferences::default().is_enabled("/tmp/demo/SKILL.md"));
    }
}
