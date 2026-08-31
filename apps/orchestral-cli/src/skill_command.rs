use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use clap::{Args, Subcommand};
use orchestral_core::agent_protocol::wire::ResourceId;
use orchestral_core::config::{load_config, OrchestralConfig};
use orchestral_core::skill_protocol::{SkillDescriptor, SkillSourceKind};
use orchestral_runtime::{SkillRoot, SkillRuntime};
use serde::Serialize;

use crate::runtime::client::prepare_runtime_config_path;
use crate::runtime::ModelOverrides;
use crate::skill_config::{
    load_skill_preferences, save_skill_preferences, user_skill_preferences_path,
};

#[derive(Debug, Args)]
pub(crate) struct SkillsCommand {
    #[command(subcommand)]
    command: SkillsSubcommand,
}

#[derive(Debug, Subcommand)]
enum SkillsSubcommand {
    /// List Skills discovered for the current workspace.
    List(OutputArgs),
    /// Enable one Skill by name or SKILL.md path.
    Enable(SelectorArgs),
    /// Disable one Skill by name or SKILL.md path.
    Disable(SelectorArgs),
}

#[derive(Debug, Args)]
struct OutputArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct SelectorArgs {
    /// Skill name or path to its SKILL.md.
    skill: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SkillStatus {
    pub name: String,
    pub description: String,
    pub path: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SkillManager {
    preferences_path: PathBuf,
    workspace: PathBuf,
    globally_enabled: bool,
    skills: Arc<Vec<SkillDescriptor>>,
}

impl SkillManager {
    fn new(
        preferences_path: PathBuf,
        workspace: PathBuf,
        globally_enabled: bool,
        discovered: Option<&SkillRuntime>,
    ) -> Self {
        Self {
            preferences_path,
            workspace,
            globally_enabled,
            skills: Arc::new(
                discovered
                    .map(|runtime| runtime.catalog().skills.clone())
                    .unwrap_or_default(),
            ),
        }
    }

    pub(crate) fn statuses(&self) -> anyhow::Result<Vec<SkillStatus>> {
        let preferences = load_skill_preferences(&self.preferences_path)?;
        Ok(self
            .skills
            .iter()
            .map(|skill| SkillStatus {
                name: skill.name.clone(),
                description: skill.description.clone(),
                path: skill.source.locator.clone(),
                enabled: self.globally_enabled && preferences.is_enabled(&skill.source.locator),
            })
            .collect())
    }

    pub(crate) fn render_list(&self) -> anyhow::Result<String> {
        let statuses = self.statuses()?;
        if statuses.is_empty() {
            return Ok("No Skills discovered for this workspace.".to_owned());
        }
        let enabled = statuses.iter().filter(|skill| skill.enabled).count();
        let mut output = format!(
            "Skills: {enabled} enabled, {} disabled\n",
            statuses.len() - enabled
        );
        for skill in statuses {
            output.push_str(&format!(
                "{} {} — {}\n  {}\n",
                if skill.enabled { "on " } else { "off" },
                skill.name,
                skill.description.replace(['\r', '\n'], " "),
                skill.path
            ));
        }
        if !self.globally_enabled {
            output.push_str(
                "All Skills are currently disabled by --no-skills or skills.enabled=false.\n",
            );
        }
        output.push_str("Use `/skills enable <name>` or `/skills disable <name>`.");
        Ok(output)
    }

    pub(crate) fn set_enabled(&self, selector: &str, enabled: bool) -> anyhow::Result<SkillStatus> {
        let descriptor = self.resolve(selector)?;
        let mut preferences = load_skill_preferences(&self.preferences_path)?;
        preferences.set_enabled(descriptor.source.locator.clone(), enabled);
        save_skill_preferences(&self.preferences_path, &preferences)?;
        Ok(SkillStatus {
            name: descriptor.name.clone(),
            description: descriptor.description.clone(),
            path: descriptor.source.locator.clone(),
            enabled: self.globally_enabled && enabled,
        })
    }

    pub(crate) fn execute_tui(&self, arguments: &str) -> anyhow::Result<String> {
        let arguments = arguments.trim();
        if arguments.is_empty() || arguments.eq_ignore_ascii_case("list") {
            return self.render_list();
        }
        let (operation, selector) = arguments
            .split_once(char::is_whitespace)
            .map(|(operation, selector)| (operation, selector.trim()))
            .context("usage: /skills [list|enable <name>|disable <name>]")?;
        if selector.is_empty() {
            bail!("usage: /skills [list|enable <name>|disable <name>]");
        }
        let enabled = match operation.to_ascii_lowercase().as_str() {
            "enable" => true,
            "disable" => false,
            _ => bail!("usage: /skills [list|enable <name>|disable <name>]"),
        };
        let status = self.set_enabled(selector, enabled)?;
        Ok(format!(
            "Skill '{}' {}. Restart Orchestral to apply the new catalog snapshot.\n{}",
            status.name,
            if enabled { "enabled" } else { "disabled" },
            status.path
        ))
    }

    fn resolve(&self, selector: &str) -> anyhow::Result<&SkillDescriptor> {
        let selector = selector.trim();
        if selector.is_empty() {
            bail!("Skill name or path must not be empty");
        }
        if let Some(skill) = self.skills.iter().find(|skill| skill.name == selector) {
            return Ok(skill);
        }
        let case_insensitive = self
            .skills
            .iter()
            .filter(|skill| skill.name.eq_ignore_ascii_case(selector))
            .collect::<Vec<_>>();
        if case_insensitive.len() == 1 {
            return Ok(case_insensitive[0]);
        }
        let requested = PathBuf::from(selector);
        let requested = if requested.is_absolute() {
            requested
        } else {
            self.workspace.join(requested)
        };
        let requested = if requested.is_dir() {
            requested.join("SKILL.md")
        } else {
            requested
        };
        let normalized = requested
            .canonicalize()
            .unwrap_or(requested)
            .to_string_lossy()
            .into_owned();
        self.skills
            .iter()
            .find(|skill| skill.source.locator == normalized)
            .with_context(|| format!("Skill '{selector}' was not discovered in this workspace"))
    }
}

impl SkillsCommand {
    pub(crate) fn run(self, config: Option<PathBuf>, cwd: Option<PathBuf>) -> anyhow::Result<()> {
        let workspace = canonical_workspace(cwd)?;
        let config_path = prepare_runtime_config_path(config, &ModelOverrides::default(), None)?;
        let config = load_config(&config_path)
            .with_context(|| format!("load Generic Agent config '{}'", config_path.display()))?;
        let (_, manager) = build_skill_setup(&config, &workspace, false)?;
        match self.command {
            SkillsSubcommand::List(args) => {
                let statuses = manager.statuses()?;
                if args.json {
                    println!("{}", serde_json::to_string_pretty(&statuses)?);
                } else {
                    println!("{}", manager.render_list()?);
                }
            }
            SkillsSubcommand::Enable(args) => {
                let status = manager.set_enabled(&args.skill, true)?;
                println!("Enabled Skill '{}' ({}).", status.name, status.path);
            }
            SkillsSubcommand::Disable(args) => {
                let status = manager.set_enabled(&args.skill, false)?;
                println!("Disabled Skill '{}' ({}).", status.name, status.path);
            }
        }
        Ok(())
    }
}

pub(crate) fn build_skill_setup(
    config: &OrchestralConfig,
    workspace: &Path,
    disabled_for_process: bool,
) -> anyhow::Result<(Option<Arc<SkillRuntime>>, SkillManager)> {
    let roots = skill_roots(config, workspace);
    let discovered = if roots.is_empty() {
        None
    } else {
        let runtime = SkillRuntime::discover(ResourceId::new("cli-skills"), &roots)
            .context("discover Skill catalog")?;
        (!runtime.catalog().skills.is_empty()).then_some(runtime)
    };
    if let Some(runtime) = &discovered {
        for conflict in runtime.conflicts() {
            tracing::warn!(
                skill = conflict.name,
                selected = conflict.selected_source,
                shadowed = conflict.shadowed_source,
                "Skill name conflict resolved by deterministic precedence"
            );
        }
    }

    let preferences_path = user_skill_preferences_path()?;
    let globally_enabled = config.skills.enabled && !disabled_for_process;
    let manager = SkillManager::new(
        preferences_path.clone(),
        workspace.to_path_buf(),
        globally_enabled,
        discovered.as_ref(),
    );
    let effective = if globally_enabled {
        let disabled = load_skill_preferences(&preferences_path)?.disabled_sources();
        discovered
            .as_ref()
            .map(|runtime| runtime.excluding_sources(&disabled))
            .transpose()?
            .filter(|runtime| !runtime.catalog().skills.is_empty())
            .map(Arc::new)
    } else {
        None
    };
    Ok((effective, manager))
}

fn skill_roots(config: &OrchestralConfig, workspace: &Path) -> Vec<SkillRoot> {
    let mut roots = Vec::new();
    for (index, configured) in config.skills.directories.iter().enumerate() {
        let path = PathBuf::from(configured);
        roots.push(SkillRoot {
            path: if path.is_absolute() {
                path
            } else {
                workspace.join(path)
            },
            source_kind: SkillSourceKind::UserConfigured,
            precedence: 10_000_u32.saturating_sub(index as u32),
            required: true,
        });
    }
    if config.skills.auto_discover {
        for (index, relative) in [".claude/skills", ".codex/skills", "skills"]
            .into_iter()
            .enumerate()
        {
            roots.push(SkillRoot {
                path: workspace.join(relative),
                source_kind: SkillSourceKind::Workspace,
                precedence: 1_000_u32.saturating_sub(index as u32),
                required: false,
            });
        }
    }
    roots
}

fn canonical_workspace(cwd: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let requested = cwd.unwrap_or(std::env::current_dir().context("resolve process directory")?);
    let workspace = requested
        .canonicalize()
        .with_context(|| format!("resolve workspace '{}'", requested.display()))?;
    if !workspace.is_dir() {
        bail!("workspace '{}' is not a directory", workspace.display());
    }
    Ok(workspace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_skill(root: &Path, name: &str) {
        let directory = root.join("skills").join(name);
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {name} description\n---\ninstructions\n"),
        )
        .unwrap();
    }

    #[test]
    fn manager_toggles_a_discovered_skill_by_name() {
        let root =
            std::env::temp_dir().join(format!("orchestral-skill-manager-{}", uuid::Uuid::new_v4()));
        write_skill(&root, "demo");
        let runtime = SkillRuntime::discover(
            ResourceId::new("skills"),
            &[SkillRoot {
                path: root.join("skills"),
                source_kind: SkillSourceKind::Workspace,
                precedence: 1,
                required: true,
            }],
        )
        .unwrap();
        let preferences_path = root.join("config/skills.json");
        let manager =
            SkillManager::new(preferences_path.clone(), root.clone(), true, Some(&runtime));

        let changed = manager.set_enabled("demo", false).unwrap();

        assert!(!changed.enabled);
        assert!(!manager.statuses().unwrap()[0].enabled);
        let preferences = load_skill_preferences(&preferences_path).unwrap();
        assert_eq!(preferences.disabled_sources().len(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tui_command_is_management_not_model_input() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-skill-tui-command-{}",
            uuid::Uuid::new_v4()
        ));
        write_skill(&root, "demo");
        let runtime = SkillRuntime::discover(
            ResourceId::new("skills"),
            &[SkillRoot {
                path: root.join("skills"),
                source_kind: SkillSourceKind::Workspace,
                precedence: 1,
                required: true,
            }],
        )
        .unwrap();
        let manager = SkillManager::new(
            root.join("config/skills.json"),
            root.clone(),
            true,
            Some(&runtime),
        );

        let output = manager.execute_tui("disable demo").unwrap();

        assert!(output.contains("Skill 'demo' disabled"));
        assert!(!manager.statuses().unwrap()[0].enabled);
        let _ = fs::remove_dir_all(root);
    }
}
