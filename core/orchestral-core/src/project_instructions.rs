//! Host-supplied project instructions, independent of filesystem discovery.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One immutable instruction document. Scope is the directory governed by the
/// document; documents in more specific directories refine ancestor guidance.
/// Instructions never grant tool permissions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectInstruction {
    /// Canonical source identity, suitable for displaying to the user.
    pub source: String,
    /// Canonical directory to which these instructions apply recursively.
    pub scope: String,
    /// Complete document text. Sources must reject oversized documents rather
    /// than silently dropping part of a project's instructions.
    pub content: String,
}

/// Discovery settings shared by application composition and source plugins.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectInstructionsConfig {
    pub enabled: bool,
    /// Total UTF-8 bytes across the discovered documents.
    pub max_bytes: u64,
    /// Additional filenames, tried after AGENTS.override.md and AGENTS.md.
    pub fallback_filenames: Vec<String>,
}

impl Default for ProjectInstructionsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bytes: 64 * 1024,
            fallback_filenames: vec!["CLAUDE.md".to_owned()],
        }
    }
}

impl ProjectInstructionsConfig {
    pub fn validate(&self) -> Result<(), ProjectInstructionError> {
        if self.max_bytes == 0 || self.max_bytes == u64::MAX {
            return Err(ProjectInstructionError::Invalid(
                "project instruction max_bytes must be positive and bounded".to_owned(),
            ));
        }
        for name in &self.fallback_filenames {
            if name.trim().is_empty()
                || matches!(name.as_str(), "." | "..")
                || name.contains(['/', '\\', '\0'])
                || name.contains(':')
            {
                return Err(ProjectInstructionError::Invalid(format!(
                    "project instruction fallback must be a filename: {name:?}"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ProjectInstructionError {
    #[error("invalid project instructions: {0}")]
    Invalid(String),
    #[error("could not load project instructions: {0}")]
    Source(String),
}

/// Extension boundary for taking a stable Host-lifetime instruction snapshot.
/// Sources return ancestor documents before descendants and deduplicate shared
/// sources when multiple workspaces overlap.
pub trait ProjectInstructionSource: Send + Sync {
    fn snapshot(
        &self,
        workspaces: &[String],
    ) -> Result<Vec<ProjectInstruction>, ProjectInstructionError>;
}
