//! Immutable Skill catalog and context-loading runtime for the Generic Agent.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use orchestral_core::agent_protocol::wire::{Digest, ResourceId, RunId};
use orchestral_core::agent_session::{AgentSessionEvent, AgentSessionRecord};
use orchestral_core::skill_protocol::{
    SkillCatalogDescriptor, SkillCompatibility, SkillDependencies, SkillDescriptor, SkillId,
    SkillLoad, SkillPackage, SkillSource, SkillSourceKind,
};
use serde::Deserialize;

const MAX_DISCOVERY_DEPTH: usize = 4;

/// One Host-selected discovery root. Larger precedence wins; ties use the
/// canonical Skill path as a stable final ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillRoot {
    pub path: PathBuf,
    pub source_kind: SkillSourceKind,
    pub precedence: u32,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillConflict {
    pub name: String,
    pub selected_source: String,
    pub shadowed_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillLoadOutcome {
    Loaded(SkillLoad),
    AlreadyLoaded(SkillDescriptor),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedSkillSet {
    by_id: BTreeMap<SkillId, Digest>,
}

impl LoadedSkillSet {
    /// Rebuilds the immutable Skill loads visible to one Run. Skill
    /// instructions are task-local working context: a later Run in the same
    /// Session starts from the catalog and must explicitly load what it needs.
    pub fn replay_for_run(
        records: &[AgentSessionRecord],
        run_id: &RunId,
    ) -> Result<Self, SkillRuntimeError> {
        let mut set = Self::default();
        for record in records {
            if record.run_id != *run_id {
                continue;
            }
            let AgentSessionEvent::SkillLoaded { load } = &record.payload else {
                continue;
            };
            load.validate()
                .map_err(|error| SkillRuntimeError::InvalidPackage(error.to_string()))?;
            let descriptor = &load.package.descriptor;
            match set.by_id.get(&descriptor.skill_id) {
                None => {
                    set.by_id
                        .insert(descriptor.skill_id.clone(), descriptor.digest.clone());
                }
                Some(previous) if previous == &descriptor.digest => {}
                Some(_) => {
                    return Err(SkillRuntimeError::DigestChanged {
                        name: descriptor.name.clone(),
                    })
                }
            }
        }
        Ok(set)
    }

    pub fn digest_for(&self, skill_id: &SkillId) -> Option<&Digest> {
        self.by_id.get(skill_id)
    }
}

#[derive(Debug, Clone)]
pub struct SkillRuntime {
    catalog: SkillCatalogDescriptor,
    packages_by_name: BTreeMap<String, SkillPackage>,
    conflicts: Vec<SkillConflict>,
}

impl SkillRuntime {
    pub fn from_packages(
        resource_id: ResourceId,
        packages: Vec<SkillPackage>,
    ) -> Result<Self, SkillRuntimeError> {
        Self::from_selected(resource_id, packages, Vec::new())
    }

    pub fn discover(
        resource_id: ResourceId,
        roots: &[SkillRoot],
    ) -> Result<Self, SkillRuntimeError> {
        let mut candidates = Vec::new();
        let mut seen_files = BTreeSet::new();
        for root in roots {
            let canonical_root = match root.path.canonicalize() {
                Ok(path) => path,
                Err(error) if !root.required && error.kind() == std::io::ErrorKind::NotFound => {
                    continue
                }
                Err(error) => {
                    return Err(SkillRuntimeError::Discovery(format!(
                        "could not resolve Skill root '{}': {error}",
                        root.path.display()
                    )))
                }
            };
            if !canonical_root.is_dir() {
                return Err(SkillRuntimeError::Discovery(format!(
                    "Skill root is not a directory: {}",
                    canonical_root.display()
                )));
            }
            let mut files = Vec::new();
            collect_skill_files(&canonical_root, MAX_DISCOVERY_DEPTH, &mut files)?;
            files.sort();
            for file in files {
                let canonical_file = file.canonicalize().map_err(|error| {
                    SkillRuntimeError::Discovery(format!(
                        "could not resolve Skill file '{}': {error}",
                        file.display()
                    ))
                })?;
                if !canonical_file.starts_with(&canonical_root)
                    || !seen_files.insert(canonical_file.clone())
                {
                    continue;
                }
                candidates.push(DiscoveredPackage {
                    package: parse_skill_file(&canonical_file, root)?,
                    precedence: root.precedence,
                    canonical_source: canonical_file.to_string_lossy().to_string(),
                });
            }
        }
        candidates.sort_by(|left, right| {
            right
                .precedence
                .cmp(&left.precedence)
                .then_with(|| left.canonical_source.cmp(&right.canonical_source))
        });

        let mut selected = BTreeMap::<String, SkillPackage>::new();
        let mut conflicts = Vec::new();
        for candidate in candidates {
            let name = candidate.package.descriptor.name.clone();
            if let Some(existing) = selected.get(&name) {
                conflicts.push(SkillConflict {
                    name,
                    selected_source: existing.descriptor.source.locator.clone(),
                    shadowed_source: candidate.package.descriptor.source.locator.clone(),
                });
            } else {
                selected.insert(name, candidate.package);
            }
        }
        Self::from_selected(resource_id, selected.into_values().collect(), conflicts)
    }

    fn from_selected(
        resource_id: ResourceId,
        packages: Vec<SkillPackage>,
        conflicts: Vec<SkillConflict>,
    ) -> Result<Self, SkillRuntimeError> {
        let mut packages_by_name = BTreeMap::new();
        for package in packages {
            package
                .validate()
                .map_err(|error| SkillRuntimeError::InvalidPackage(error.to_string()))?;
            let name = package.descriptor.name.clone();
            if packages_by_name.insert(name.clone(), package).is_some() {
                return Err(SkillRuntimeError::Conflict(format!(
                    "duplicate Skill name without resolved precedence: {name}"
                )));
            }
        }
        let catalog = SkillCatalogDescriptor::seal(
            resource_id,
            packages_by_name
                .values()
                .map(|package| package.descriptor.clone())
                .collect(),
        )
        .map_err(|error| SkillRuntimeError::InvalidPackage(error.to_string()))?;
        Ok(Self {
            catalog,
            packages_by_name,
            conflicts,
        })
    }

    pub fn catalog(&self) -> &SkillCatalogDescriptor {
        &self.catalog
    }

    pub fn conflicts(&self) -> &[SkillConflict] {
        &self.conflicts
    }

    /// Descriptor-only text. Full instructions are never returned here.
    pub fn descriptor_context(&self) -> String {
        let mut output = String::from(
            "## Skills\nA Skill is a set of local instructions stored in a `SKILL.md` file. Each entry includes its name, description, and source path. Call `skill_read` with the Skill name before following its instructions.\n\n### Available Skills\n",
        );
        for descriptor in &self.catalog.skills {
            output.push_str(&format!(
                "- {}: {} (file: {}; digest: {})\n",
                descriptor.name,
                descriptor.description.replace(['\r', '\n'], " "),
                descriptor.source.locator,
                descriptor.digest
            ));
        }
        output.push_str("\nSkill contents provide instructions, not Tool access or permission.\n");
        for conflict in &self.conflicts {
            output.push_str(&format!(
                "- conflict name={} selected={} shadowed={}\n",
                conflict.name, conflict.selected_source, conflict.shadowed_source
            ));
        }
        output
    }

    /// Loads immutable instructions into model context. This operation is a
    /// context read, not an effect or authority transition, so provenance,
    /// compatibility, and dependency metadata cannot block it.
    pub fn read_for_context(
        &self,
        name: &str,
        loaded: &LoadedSkillSet,
    ) -> Result<SkillLoadOutcome, SkillRuntimeError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(SkillRuntimeError::InvalidRequest(
                "Skill name must not be empty".to_owned(),
            ));
        }
        let package = self
            .packages_by_name
            .get(name)
            .ok_or_else(|| SkillRuntimeError::NotFound(name.to_owned()))?;
        let descriptor = &package.descriptor;
        if let Some(previous) = loaded.digest_for(&descriptor.skill_id) {
            return if previous == &descriptor.digest {
                Ok(SkillLoadOutcome::AlreadyLoaded(descriptor.clone()))
            } else {
                Err(SkillRuntimeError::DigestChanged {
                    name: descriptor.name.clone(),
                })
            };
        }
        let load = SkillLoad {
            package: package.clone(),
        };
        load.validate()
            .map_err(|error| SkillRuntimeError::InvalidPackage(error.to_string()))?;
        Ok(SkillLoadOutcome::Loaded(load))
    }
}

struct DiscoveredPackage {
    package: SkillPackage,
    precedence: u32,
    canonical_source: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillFrontmatter {
    name: String,
    description: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    compatibility: SkillCompatibility,
    #[serde(default)]
    dependencies: SkillDependencies,
    #[serde(default)]
    license: Option<String>,
    #[serde(default)]
    metadata: BTreeMap<String, serde_yaml::Value>,
}

fn parse_skill_file(path: &Path, root: &SkillRoot) -> Result<SkillPackage, SkillRuntimeError> {
    let content = fs::read_to_string(path).map_err(|error| {
        SkillRuntimeError::Discovery(format!("could not read '{}': {error}", path.display()))
    })?;
    let rest = content.strip_prefix("---\n").ok_or_else(|| {
        SkillRuntimeError::Parse(format!(
            "Skill '{}' requires YAML frontmatter",
            path.display()
        ))
    })?;
    let (frontmatter, body) = rest.split_once("\n---\n").ok_or_else(|| {
        SkillRuntimeError::Parse(format!(
            "Skill '{}' has unterminated YAML frontmatter",
            path.display()
        ))
    })?;
    let parsed = serde_yaml::from_str::<SkillFrontmatter>(frontmatter).map_err(|error| {
        SkillRuntimeError::Parse(format!(
            "Skill '{}' frontmatter is invalid: {error}",
            path.display()
        ))
    })?;
    let version = parsed.version.or_else(|| {
        parsed
            .metadata
            .get("version")
            .and_then(serde_yaml::Value::as_str)
            .map(str::to_owned)
    });
    // License is provenance metadata and never execution authority.
    let _ = &parsed.license;
    SkillPackage::seal(
        SkillId::new(parsed.name.clone()),
        parsed.name,
        parsed.description,
        version,
        SkillSource {
            kind: root.source_kind,
            locator: path.to_string_lossy().to_string(),
        },
        parsed.compatibility,
        parsed.dependencies,
        body.trim(),
    )
    .map_err(|error| SkillRuntimeError::InvalidPackage(format!("{}: {error}", path.display())))
}

fn collect_skill_files(
    directory: &Path,
    depth: usize,
    output: &mut Vec<PathBuf>,
) -> Result<(), SkillRuntimeError> {
    if depth == 0 {
        return Ok(());
    }
    let mut entries = fs::read_dir(directory)
        .map_err(|error| {
            SkillRuntimeError::Discovery(format!(
                "could not scan Skill directory '{}': {error}",
                directory.display()
            ))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| SkillRuntimeError::Discovery(error.to_string()))?;
    entries.sort_by_key(std::fs::DirEntry::path);
    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            SkillRuntimeError::Discovery(format!(
                "could not inspect Skill path '{}': {error}",
                path.display()
            ))
        })?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_skill_files(&path, depth - 1, output)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.eq_ignore_ascii_case("SKILL.md"))
        {
            output.push(path);
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SkillRuntimeError {
    #[error("Skill discovery failed: {0}")]
    Discovery(String),
    #[error("Skill parse failed: {0}")]
    Parse(String),
    #[error("invalid Skill package: {0}")]
    InvalidPackage(String),
    #[error("Skill conflict: {0}")]
    Conflict(String),
    #[error("invalid Skill read request: {0}")]
    InvalidRequest(String),
    #[error("Skill not found: {0}")]
    NotFound(String),
    #[error("Skill '{name}' changed digest within one immutable catalog binding")]
    DigestChanged { name: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::agent_protocol::wire::{AgentSessionId, RunId};
    use orchestral_core::agent_session::{AgentSessionEventDraft, AgentSessionEventId};
    use orchestral_core::tool_protocol::{HostToolPolicy, RunToolGrant, ToolPolicyBounds};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "orchestral-skill-runtime-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn write_skill(root: &Path, directory: &str, name: &str, body: &str) {
        let directory = root.join(directory);
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("SKILL.md"),
            format!(
                "---\nname: {name}\ndescription: {name} description\nversion: 1.0.0\n---\n{body}\n"
            ),
        )
        .unwrap();
    }

    #[test]
    fn one_thousand_conflict_resolutions_are_deterministic_and_visible() {
        let low = temp_dir("low");
        let high = temp_dir("high");
        write_skill(&low, "demo", "demo", "low instructions");
        write_skill(&high, "demo", "demo", "high instructions");
        let roots = vec![
            SkillRoot {
                path: low.clone(),
                source_kind: SkillSourceKind::Workspace,
                precedence: 10,
                required: true,
            },
            SkillRoot {
                path: high.clone(),
                source_kind: SkillSourceKind::UserConfigured,
                precedence: 20,
                required: true,
            },
        ];
        let baseline = SkillRuntime::discover(ResourceId::new("skills"), &roots).unwrap();
        for _ in 0..1_000 {
            let observed = SkillRuntime::discover(ResourceId::new("skills"), &roots).unwrap();
            assert_eq!(baseline.catalog(), observed.catalog());
            assert_eq!(baseline.conflicts(), observed.conflicts());
        }
        assert_eq!(baseline.conflicts().len(), 1);
        let canonical_high = high.canonicalize().unwrap();
        assert!(baseline.conflicts()[0]
            .selected_source
            .starts_with(canonical_high.to_string_lossy().as_ref()));
        let _ = fs::remove_dir_all(low);
        let _ = fs::remove_dir_all(high);
    }

    #[test]
    fn free_text_compatibility_is_rejected_instead_of_downgraded() {
        let root = temp_dir("compatibility");
        let directory = root.join("demo");
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join("SKILL.md"),
            "---\nname: demo\ndescription: demo\ncompatibility: Requires Python\n---\nbody\n",
        )
        .unwrap();
        let result = SkillRuntime::discover(
            ResourceId::new("skills"),
            &[SkillRoot {
                path: root.clone(),
                source_kind: SkillSourceKind::Workspace,
                precedence: 1,
                required: true,
            }],
        );
        assert!(matches!(result, Err(SkillRuntimeError::Parse(_))));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn one_thousand_loads_are_complete_descriptor_only_and_never_expand_authority() {
        for index in 0..1_000 {
            let name = format!("skill-{index}");
            let tool = format!("tool-{index}");
            let mcp_server = format!("mcp-{index}");
            let instructions = format!("FULL-INSTRUCTIONS-SENTINEL-{index}");
            let locator = format!("configured:/skills/{name}/SKILL.md");
            let package = SkillPackage::seal(
                SkillId::new(&name),
                &name,
                format!("descriptor-{index}"),
                Some(format!("1.0.{index}")),
                SkillSource {
                    kind: SkillSourceKind::UserConfigured,
                    locator: locator.clone(),
                },
                SkillCompatibility {
                    operating_systems: BTreeSet::from([format!("other-os-{index}")]),
                    required_programs: BTreeSet::from([format!("missing-program-{index}")]),
                    required_environment: BTreeSet::from([format!("MISSING_ENV_{index}")]),
                    ..SkillCompatibility::default()
                },
                SkillDependencies {
                    tools: BTreeSet::from([tool]),
                    mcp_servers: BTreeSet::from([mcp_server]),
                },
                &instructions,
            )
            .unwrap();
            let expected_digest = package.descriptor.digest.clone();
            let expected_skill_id = package.descriptor.skill_id.clone();
            let runtime = SkillRuntime::from_packages(
                ResourceId::new(format!("catalog-{index}")),
                vec![package],
            )
            .unwrap();

            let descriptor_context = runtime.descriptor_context();
            assert!(descriptor_context.contains(&name));
            assert!(descriptor_context.contains(expected_digest.as_str()));
            assert!(!descriptor_context.contains(&instructions));

            let mut authority = ToolPolicyBounds::default();
            authority
                .allowed_credentials
                .insert(format!("credential-{index}"));
            authority
                .environment
                .allowed_variables
                .insert(format!("ENV_{index}"));
            let host_policy = HostToolPolicy {
                bounds: authority.clone(),
            };
            let run_grant = RunToolGrant { bounds: authority };
            let host_policy_before = host_policy.clone();
            let run_grant_before = run_grant.clone();

            let outcome = runtime
                .read_for_context(&name, &LoadedSkillSet::default())
                .unwrap();
            assert_eq!(host_policy, host_policy_before);
            assert_eq!(run_grant, run_grant_before);

            let SkillLoadOutcome::Loaded(load) = outcome else {
                panic!("fresh Skill unexpectedly reported AlreadyLoaded");
            };
            assert_eq!(load.package.descriptor.skill_id, expected_skill_id);
            assert_eq!(load.package.descriptor.source.locator, locator);
            assert_eq!(
                load.package.descriptor.version.as_deref(),
                Some(format!("1.0.{index}").as_str())
            );
            assert_eq!(load.package.descriptor.digest, expected_digest);

            let record = AgentSessionRecord::seal(
                AgentSessionEventDraft {
                    event_id: AgentSessionEventId::new(format!("skill-loaded-{index}")),
                    session_id: AgentSessionId::new(format!("session-{index}")),
                    run_id: RunId::new(format!("run-{index}")),
                    payload: AgentSessionEvent::SkillLoaded {
                        load: Box::new(load),
                    },
                },
                1,
            )
            .unwrap();
            record.validate().unwrap();
            assert!(matches!(
                record.payload,
                AgentSessionEvent::SkillLoaded { .. }
            ));
        }
    }

    #[test]
    fn one_thousand_digest_changes_are_rejected_within_a_loaded_set() {
        for index in 0..1_000 {
            let name = format!("skill-{index}");
            let previous = test_package(
                &name,
                SkillCompatibility::default(),
                "previous instructions",
            );
            let replacement = test_package(
                &name,
                SkillCompatibility::default(),
                "replacement instructions",
            );
            let mut loaded = LoadedSkillSet::default();
            loaded.by_id.insert(
                previous.descriptor.skill_id.clone(),
                previous.descriptor.digest,
            );
            let replacement_runtime = SkillRuntime::from_packages(
                ResourceId::new(format!("replacement-catalog-{index}")),
                vec![replacement],
            )
            .unwrap();
            assert!(matches!(
                replacement_runtime.read_for_context(&name, &loaded),
                Err(SkillRuntimeError::DigestChanged { .. })
            ));
        }
    }

    fn test_package(
        name: &str,
        compatibility: SkillCompatibility,
        instructions: &str,
    ) -> SkillPackage {
        SkillPackage::seal(
            SkillId::new(name),
            name,
            format!("{name} description"),
            Some("1.0.0".to_owned()),
            SkillSource {
                kind: SkillSourceKind::Workspace,
                locator: format!("configured:/skills/{name}/SKILL.md"),
            },
            compatibility,
            SkillDependencies::default(),
            instructions,
        )
        .unwrap()
    }
}
