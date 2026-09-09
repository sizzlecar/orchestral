//! Skill catalog presentation. Reading details never loads model instructions.
use super::menu::{Choice, LocalAction, Menu, MenuKind};
use crate::skill_command::SkillManager;
use anyhow::{Context, Result};
use std::path::Path;

pub(crate) fn list(manager: &SkillManager) -> Result<Menu> {
    let statuses = manager.statuses()?;
    if statuses.is_empty() {
        return Ok(super::services::detail(
            "Skills · this workspace",
            "No skills discovered in this workspace.",
        ));
    }
    let title = format!("Skills · this workspace · {} discovered", statuses.len());
    let choices = statuses
        .into_iter()
        .map(|skill| {
            let enabled_now = manager.enabled_in_process(&skill.path);
            let status = if enabled_now { "enabled" } else { "disabled" };
            let pending = if enabled_now != skill.enabled {
                " · restart pending"
            } else {
                ""
            };
            Choice::action(
                format!("{} · {status}{pending}", skill.name),
                LocalAction::SkillDetails(skill.name),
                skill
                    .description
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" "),
            )
        })
        .collect();
    Ok(Menu::new(MenuKind::Skills, title, choices))
}

pub(crate) fn details(manager: &SkillManager, workspace: &Path, name: &str) -> Result<Menu> {
    let skill = manager
        .statuses()?
        .into_iter()
        .find(|skill| skill.name == name)
        .context("This skill is no longer in the discovered catalog")?;
    let enabled_now = manager.enabled_in_process(&skill.path);
    let status = if enabled_now { "enabled" } else { "disabled" };
    let source = Path::new(&skill.path)
        .strip_prefix(workspace)
        .unwrap_or(Path::new(&skill.path));
    let mut lines = vec![format!("This process: {status}")];
    if enabled_now != skill.enabled {
        lines.push(format!(
            "After restart: {} (restart pending)",
            if skill.enabled { "enabled" } else { "disabled" }
        ));
    }
    if !manager.globally_enabled() {
        lines.push("Skills are disabled by --no-skills or process configuration.".to_owned());
    } else {
        lines.push(format!(
            "Space: {} for future launches",
            if skill.enabled { "disable" } else { "enable" }
        ));
    }
    lines.push(format!("Source: {}", source.display()));
    lines.push(
        "Enabled means selectable; instructions load separately for each request.".to_owned(),
    );
    lines.push(format!(
        "\n{}\n\nFull path: {}",
        skill.description, skill.path
    ));
    let mut menu = super::services::detail(format!("Skill · {}", skill.name), lines.join("\n"));
    if manager.globally_enabled() {
        menu.toggle = Some(LocalAction::SetSkillEnabled {
            name: skill.name,
            enabled: !skill.enabled,
        });
    }
    Ok(menu)
}
