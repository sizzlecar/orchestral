//! Compatible, bounded project instruction discovery.
//!
//! For each workspace, read the path from the nearest Git root down to that
//! workspace (only the workspace itself outside Git). Select the first nonempty
//! AGENTS.override.md, AGENTS.md, or configured fallback in each directory.
//! No global profile or unrelated descendant directory is read implicitly.

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::Path;

use cap_std::fs::Dir;
use orchestral_core::project_instructions::{
    ProjectInstruction, ProjectInstructionError, ProjectInstructionSource,
    ProjectInstructionsConfig,
};

/// Filesystem source, installed by the application rather than the runtime.
pub struct FileProjectInstructionSource {
    config: ProjectInstructionsConfig,
}

impl FileProjectInstructionSource {
    pub fn new(config: ProjectInstructionsConfig) -> Result<Self, ProjectInstructionError> {
        config.validate()?;
        Ok(Self { config })
    }
}

impl ProjectInstructionSource for FileProjectInstructionSource {
    fn snapshot(
        &self,
        workspaces: &[String],
    ) -> Result<Vec<ProjectInstruction>, ProjectInstructionError> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }
        let mut directories = BTreeSet::new();
        for workspace in workspaces {
            let workspace = fs::canonicalize(workspace).map_err(source_error)?;
            if !workspace.is_dir() {
                return Err(ProjectInstructionError::Invalid(format!(
                    "workspace is not a directory: {}",
                    workspace.display()
                )));
            }
            let root = workspace
                .ancestors()
                .find(|directory| directory.join(".git").exists())
                .unwrap_or(&workspace);
            for directory in workspace.ancestors() {
                directories.insert(directory.to_path_buf());
                if directory == root {
                    break;
                }
            }
        }
        // Path ordering puts each ancestor before its descendants, independent
        // of workspace argument order. Each directory contributes at most once.
        let mut documents = Vec::new();
        let mut remaining = self.config.max_bytes;
        for directory in directories {
            let dir = Dir::open_ambient_dir(&directory, cap_std::ambient_authority())
                .map_err(source_error)?;
            let names = ["AGENTS.override.md", "AGENTS.md"]
                .into_iter()
                .chain(self.config.fallback_filenames.iter().map(String::as_str));
            for name in names {
                let path = directory.join(name);
                match dir.symlink_metadata(name) {
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(file_error(&path, error)),
                }
                // cap-std resolves symlinks beneath this directory. Support
                // common AGENTS.md -> CLAUDE.md aliases without following a
                // link into another directory's private files.
                let metadata = dir
                    .metadata(name)
                    .map_err(|error| file_error(&path, error))?;
                if !metadata.is_file() {
                    return Err(file_error(
                        &path,
                        "instruction source must be a regular file",
                    ));
                }
                let file = dir.open(name).map_err(|error| file_error(&path, error))?;
                if !file.metadata().map_err(source_error)?.is_file() {
                    return Err(file_error(
                        &path,
                        "instruction source must be a regular file",
                    ));
                }
                let mut bytes = Vec::new();
                file.take(remaining + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|error| file_error(&path, error))?;
                if bytes.len() as u64 > remaining {
                    return Err(file_error(
                        &path,
                        format!(
                            "project instructions exceed agent.project_instructions.max_bytes ({})",
                            self.config.max_bytes
                        ),
                    ));
                }
                let content = String::from_utf8(bytes).map_err(|error| file_error(&path, error))?;
                if content.trim().is_empty() {
                    continue;
                }
                remaining -= content.len() as u64;
                documents.push(ProjectInstruction {
                    source: path_text(&path)?,
                    scope: path_text(&directory)?,
                    content,
                });
                break;
            }
        }
        Ok(documents)
    }
}

fn path_text(path: &Path) -> Result<String, ProjectInstructionError> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        ProjectInstructionError::Invalid(format!(
            "instruction path is not UTF-8: {}",
            path.display()
        ))
    })
}

fn source_error(error: impl std::fmt::Display) -> ProjectInstructionError {
    ProjectInstructionError::Source(error.to_string())
}

fn file_error(path: &Path, error: impl std::fmt::Display) -> ProjectInstructionError {
    source_error(format!("{}: {error}", path.display()))
}
