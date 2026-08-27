//! Immutable Skill catalog and activation runtime for the Generic Agent.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use orchestral_core::agent_protocol::wire::{Digest, ResourceId};
use orchestral_core::agent_session::{AgentSessionEvent, AgentSessionRecord};
use orchestral_core::skill_protocol::{
    SkillActivation, SkillCatalogDescriptor, SkillCompatibility, SkillDependencies,
    SkillDescriptor, SkillId, SkillPackage, SkillSource, SkillSourceKind, SkillTrustLevel,
};
use serde::Deserialize;

const MAX_DISCOVERY_DEPTH: usize = 4;

/// One Host-selected discovery root. Larger precedence wins; ties use the
/// canonical Skill path as a stable final ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillRoot {
    pub path: PathBuf,
    pub source_kind: SkillSourceKind,
    pub trust: SkillTrustLevel,
    pub precedence: u32,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillConflict {
    pub name: String,
    pub selected_source: String,
    pub shadowed_source: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillHostProfile {
    pub operating_system: String,
    pub architecture: String,
    pub available_tools: BTreeSet<String>,
    pub available_mcp_servers: BTreeSet<String>,
    pub available_programs: BTreeSet<String>,
    pub available_environment: BTreeSet<String>,
    pub available_features: BTreeSet<String>,
}

impl SkillHostProfile {
    pub fn current() -> Self {
        Self {
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SkillActivationPolicy {
    /// An explicit Host choice. The default is fail-closed.
    pub allow_untrusted_workspace: bool,
    /// An explicit Host choice for a known structured incompatibility.
    pub allow_incompatible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillActivationRequest {
    pub name: String,
    pub expected_digest: Digest,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillActivationOutcome {
    Activated(SkillActivation),
    AlreadyActive(SkillDescriptor),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActivatedSkillSet {
    by_id: BTreeMap<SkillId, Digest>,
}

impl ActivatedSkillSet {
    pub fn replay(records: &[AgentSessionRecord]) -> Result<Self, SkillRuntimeError> {
        let mut set = Self::default();
        for record in records {
            let AgentSessionEvent::SkillActivated { activation } = &record.payload else {
                continue;
            };
            activation
                .validate()
                .map_err(|error| SkillRuntimeError::InvalidPackage(error.to_string()))?;
            let descriptor = &activation.package.descriptor;
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
    host: SkillHostProfile,
    policy: SkillActivationPolicy,
}

impl SkillRuntime {
    pub fn from_packages(
        resource_id: ResourceId,
        packages: Vec<SkillPackage>,
        host: SkillHostProfile,
        policy: SkillActivationPolicy,
    ) -> Result<Self, SkillRuntimeError> {
        Self::from_selected(resource_id, packages, Vec::new(), host, policy)
    }

    pub fn discover(
        resource_id: ResourceId,
        roots: &[SkillRoot],
        host: SkillHostProfile,
        policy: SkillActivationPolicy,
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
        Self::from_selected(
            resource_id,
            selected.into_values().collect(),
            conflicts,
            host,
            policy,
        )
    }

    fn from_selected(
        resource_id: ResourceId,
        packages: Vec<SkillPackage>,
        conflicts: Vec<SkillConflict>,
        host: SkillHostProfile,
        policy: SkillActivationPolicy,
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
            host,
            policy,
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
        output.push_str(
            "\n### Skill Rules\n- If the user names a Skill or the task clearly matches its description, read the smallest relevant set.\n- Use the listed source path when reporting provenance or resolving files referenced by the Skill.\n- A Skill supplies instructions only. Tool schemas and Host policy remain authoritative for every effect.\n",
        );
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
        active: &ActivatedSkillSet,
    ) -> Result<SkillActivationOutcome, SkillRuntimeError> {
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
        if let Some(previous) = active.digest_for(&descriptor.skill_id) {
            return if previous == &descriptor.digest {
                Ok(SkillActivationOutcome::AlreadyActive(descriptor.clone()))
            } else {
                Err(SkillRuntimeError::DigestChanged {
                    name: descriptor.name.clone(),
                })
            };
        }
        let activation = SkillActivation {
            package: package.clone(),
            reason: "selected through skill_read".to_owned(),
        };
        activation
            .validate()
            .map_err(|error| SkillRuntimeError::InvalidPackage(error.to_string()))?;
        Ok(SkillActivationOutcome::Activated(activation))
    }

    pub fn activate(
        &self,
        request: SkillActivationRequest,
        active: &ActivatedSkillSet,
    ) -> Result<SkillActivationOutcome, SkillRuntimeError> {
        if request.name.trim().is_empty() || request.reason.trim().is_empty() {
            return Err(SkillRuntimeError::InvalidRequest(
                "Skill name and activation reason must not be empty".to_owned(),
            ));
        }
        if !request.expected_digest.is_sha256() {
            return Err(SkillRuntimeError::InvalidRequest(
                "Skill activation requires a valid expected digest".to_owned(),
            ));
        }
        let package = self
            .packages_by_name
            .get(&request.name)
            .ok_or_else(|| SkillRuntimeError::NotFound(request.name.clone()))?;
        let descriptor = &package.descriptor;
        if descriptor.digest != request.expected_digest {
            return Err(SkillRuntimeError::DigestMismatch {
                name: request.name,
                expected: request.expected_digest,
                current: descriptor.digest.clone(),
            });
        }
        if let Some(previous) = active.digest_for(&descriptor.skill_id) {
            return if previous == &descriptor.digest {
                Ok(SkillActivationOutcome::AlreadyActive(descriptor.clone()))
            } else {
                Err(SkillRuntimeError::DigestChanged {
                    name: descriptor.name.clone(),
                })
            };
        }
        if !descriptor.trust.permits_activation() && !self.policy.allow_untrusted_workspace {
            return Err(SkillRuntimeError::Untrusted(descriptor.name.clone()));
        }
        if (!matches_constraint(
            &descriptor.compatibility.operating_systems,
            &self.host.operating_system,
        ) || !matches_constraint(
            &descriptor.compatibility.architectures,
            &self.host.architecture,
        )) && !self.policy.allow_incompatible
        {
            return Err(SkillRuntimeError::Incompatible(descriptor.name.clone()));
        }
        let missing_programs = descriptor
            .compatibility
            .required_programs
            .difference(&self.host.available_programs)
            .cloned()
            .collect::<Vec<_>>();
        let unsatisfied_program_groups = descriptor
            .compatibility
            .any_programs
            .iter()
            .filter(|group| group.is_disjoint(&self.host.available_programs))
            .cloned()
            .collect::<Vec<_>>();
        let missing_environment = descriptor
            .compatibility
            .required_environment
            .difference(&self.host.available_environment)
            .cloned()
            .collect::<Vec<_>>();
        let missing_features = descriptor
            .compatibility
            .required_features
            .difference(&self.host.available_features)
            .cloned()
            .collect::<Vec<_>>();
        if !missing_programs.is_empty()
            || !unsatisfied_program_groups.is_empty()
            || !missing_environment.is_empty()
            || !missing_features.is_empty()
        {
            return Err(SkillRuntimeError::MissingHostRequirements {
                name: descriptor.name.clone(),
                programs: missing_programs,
                any_programs: unsatisfied_program_groups,
                environment: missing_environment,
                features: missing_features,
            });
        }
        let missing_tools = descriptor
            .dependencies
            .tools
            .difference(&self.host.available_tools)
            .cloned()
            .collect::<Vec<_>>();
        let missing_mcp = descriptor
            .dependencies
            .mcp_servers
            .difference(&self.host.available_mcp_servers)
            .cloned()
            .collect::<Vec<_>>();
        if !missing_tools.is_empty() || !missing_mcp.is_empty() {
            return Err(SkillRuntimeError::MissingDependencies {
                name: descriptor.name.clone(),
                tools: missing_tools,
                mcp_servers: missing_mcp,
            });
        }
        let activation = SkillActivation {
            package: package.clone(),
            reason: request.reason,
        };
        activation
            .validate()
            .map_err(|error| SkillRuntimeError::InvalidPackage(error.to_string()))?;
        Ok(SkillActivationOutcome::Activated(activation))
    }
}

fn matches_constraint(allowed: &BTreeSet<String>, observed: &str) -> bool {
    allowed.is_empty()
        || allowed
            .iter()
            .any(|value| value.eq_ignore_ascii_case(observed))
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
        root.trust,
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
    #[error("invalid Skill activation request: {0}")]
    InvalidRequest(String),
    #[error("Skill not found: {0}")]
    NotFound(String),
    #[error("Skill '{name}' digest mismatch (expected {expected}, current {current})")]
    DigestMismatch {
        name: String,
        expected: Digest,
        current: Digest,
    },
    #[error(
        "Skill '{name}' changed digest and requires a new Host-confirmed replacement protocol"
    )]
    DigestChanged { name: String },
    #[error("Skill is not trusted for activation: {0}")]
    Untrusted(String),
    #[error("Skill is incompatible with this Host: {0}")]
    Incompatible(String),
    #[error(
        "Skill '{name}' dependencies are unavailable (tools={tools:?}, mcp_servers={mcp_servers:?})"
    )]
    MissingDependencies {
        name: String,
        tools: Vec<String>,
        mcp_servers: Vec<String>,
    },
    #[error(
        "Skill '{name}' Host requirements are unavailable (programs={programs:?}, any_programs={any_programs:?}, environment={environment:?}, features={features:?})"
    )]
    MissingHostRequirements {
        name: String,
        programs: Vec<String>,
        any_programs: Vec<BTreeSet<String>>,
        environment: Vec<String>,
        features: Vec<String>,
    },
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
                trust: SkillTrustLevel::WorkspaceTrusted,
                precedence: 10,
                required: true,
            },
            SkillRoot {
                path: high.clone(),
                source_kind: SkillSourceKind::UserConfigured,
                trust: SkillTrustLevel::UserTrusted,
                precedence: 20,
                required: true,
            },
        ];
        let baseline = SkillRuntime::discover(
            ResourceId::new("skills"),
            &roots,
            SkillHostProfile::current(),
            SkillActivationPolicy::default(),
        )
        .unwrap();
        for _ in 0..1_000 {
            let observed = SkillRuntime::discover(
                ResourceId::new("skills"),
                &roots,
                SkillHostProfile::current(),
                SkillActivationPolicy::default(),
            )
            .unwrap();
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
                trust: SkillTrustLevel::WorkspaceTrusted,
                precedence: 1,
                required: true,
            }],
            SkillHostProfile::current(),
            SkillActivationPolicy::default(),
        );
        assert!(matches!(result, Err(SkillRuntimeError::Parse(_))));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn untrusted_workspace_skill_cannot_activate_by_default() {
        let package = SkillPackage::seal(
            SkillId::new("demo"),
            "demo",
            "demo description",
            None,
            SkillSource {
                kind: SkillSourceKind::Workspace,
                locator: "/workspace/demo/SKILL.md".to_owned(),
            },
            SkillTrustLevel::WorkspaceUntrusted,
            SkillCompatibility::default(),
            SkillDependencies::default(),
            "do the thing",
        )
        .unwrap();
        let digest = package.descriptor.digest.clone();
        let runtime = SkillRuntime::from_packages(
            ResourceId::new("skills"),
            vec![package],
            SkillHostProfile::current(),
            SkillActivationPolicy::default(),
        )
        .unwrap();
        let result = runtime.activate(
            SkillActivationRequest {
                name: "demo".to_owned(),
                expected_digest: digest,
                reason: "task matches".to_owned(),
            },
            &ActivatedSkillSet::default(),
        );
        assert!(matches!(result, Err(SkillRuntimeError::Untrusted(_))));
    }

    #[test]
    fn one_thousand_activations_are_complete_descriptor_only_and_never_expand_authority() {
        for index in 0..1_000 {
            let name = format!("skill-{index}");
            let tool = format!("tool-{index}");
            let mcp_server = format!("mcp-{index}");
            let instructions = format!("FULL-INSTRUCTIONS-SENTINEL-{index}");
            let reason = format!("activation-reason-{index}");
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
                SkillTrustLevel::UserTrusted,
                SkillCompatibility::default(),
                SkillDependencies {
                    tools: BTreeSet::from([tool.clone()]),
                    mcp_servers: BTreeSet::from([mcp_server.clone()]),
                },
                &instructions,
            )
            .unwrap();
            let expected_digest = package.descriptor.digest.clone();
            let expected_skill_id = package.descriptor.skill_id.clone();
            let mut host_profile = SkillHostProfile::current();
            host_profile.available_tools.insert(tool);
            host_profile.available_mcp_servers.insert(mcp_server);
            let runtime = SkillRuntime::from_packages(
                ResourceId::new(format!("catalog-{index}")),
                vec![package],
                host_profile,
                SkillActivationPolicy::default(),
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
                .activate(
                    SkillActivationRequest {
                        name: name.clone(),
                        expected_digest: expected_digest.clone(),
                        reason: reason.clone(),
                    },
                    &ActivatedSkillSet::default(),
                )
                .unwrap();
            assert_eq!(host_policy, host_policy_before);
            assert_eq!(run_grant, run_grant_before);

            let SkillActivationOutcome::Activated(activation) = outcome else {
                panic!("fresh Skill unexpectedly reported AlreadyActive");
            };
            assert_eq!(activation.package.descriptor.skill_id, expected_skill_id);
            assert_eq!(activation.package.descriptor.source.locator, locator);
            assert_eq!(
                activation.package.descriptor.trust,
                SkillTrustLevel::UserTrusted
            );
            assert_eq!(
                activation.package.descriptor.version.as_deref(),
                Some(format!("1.0.{index}").as_str())
            );
            assert_eq!(activation.package.descriptor.digest, expected_digest);
            assert_eq!(activation.reason, reason);

            let record = AgentSessionRecord::seal(
                AgentSessionEventDraft {
                    event_id: AgentSessionEventId::new(format!("skill-activated-{index}")),
                    session_id: AgentSessionId::new(format!("session-{index}")),
                    run_id: RunId::new(format!("run-{index}")),
                    payload: AgentSessionEvent::SkillActivated {
                        activation: Box::new(activation),
                    },
                },
                1,
            )
            .unwrap();
            record.validate().unwrap();
            assert!(matches!(
                record.payload,
                AgentSessionEvent::SkillActivated { .. }
            ));
        }
    }

    #[test]
    fn one_thousand_variants_of_each_forbidden_activation_class_are_rejected() {
        for index in 0..1_000 {
            let name = format!("skill-{index}");
            let untrusted = test_package(
                &name,
                SkillTrustLevel::WorkspaceUntrusted,
                SkillCompatibility::default(),
                "untrusted instructions",
            );
            let untrusted_digest = untrusted.descriptor.digest.clone();
            let untrusted_runtime = SkillRuntime::from_packages(
                ResourceId::new(format!("untrusted-catalog-{index}")),
                vec![untrusted],
                SkillHostProfile::current(),
                SkillActivationPolicy::default(),
            )
            .unwrap();
            assert!(matches!(
                untrusted_runtime.activate(
                    SkillActivationRequest {
                        name: name.clone(),
                        expected_digest: untrusted_digest,
                        reason: "must remain denied".to_owned(),
                    },
                    &ActivatedSkillSet::default(),
                ),
                Err(SkillRuntimeError::Untrusted(_))
            ));

            let incompatible = test_package(
                &name,
                SkillTrustLevel::UserTrusted,
                SkillCompatibility {
                    operating_systems: BTreeSet::from([format!("unsupported-os-{index}")]),
                    ..SkillCompatibility::default()
                },
                "incompatible instructions",
            );
            let incompatible_digest = incompatible.descriptor.digest.clone();
            let incompatible_runtime = SkillRuntime::from_packages(
                ResourceId::new(format!("incompatible-catalog-{index}")),
                vec![incompatible],
                SkillHostProfile::current(),
                SkillActivationPolicy::default(),
            )
            .unwrap();
            assert!(matches!(
                incompatible_runtime.activate(
                    SkillActivationRequest {
                        name: name.clone(),
                        expected_digest: incompatible_digest,
                        reason: "must remain denied".to_owned(),
                    },
                    &ActivatedSkillSet::default(),
                ),
                Err(SkillRuntimeError::Incompatible(_))
            ));

            let previous = test_package(
                &name,
                SkillTrustLevel::UserTrusted,
                SkillCompatibility::default(),
                "previous instructions",
            );
            let replacement = test_package(
                &name,
                SkillTrustLevel::UserTrusted,
                SkillCompatibility::default(),
                "replacement instructions",
            );
            let mut active = ActivatedSkillSet::default();
            active.by_id.insert(
                previous.descriptor.skill_id.clone(),
                previous.descriptor.digest,
            );
            let replacement_digest = replacement.descriptor.digest.clone();
            let replacement_runtime = SkillRuntime::from_packages(
                ResourceId::new(format!("replacement-catalog-{index}")),
                vec![replacement],
                SkillHostProfile::current(),
                SkillActivationPolicy::default(),
            )
            .unwrap();
            assert!(matches!(
                replacement_runtime.activate(
                    SkillActivationRequest {
                        name,
                        expected_digest: replacement_digest,
                        reason: "replacement was not confirmed".to_owned(),
                    },
                    &active,
                ),
                Err(SkillRuntimeError::DigestChanged { .. })
            ));
        }
    }

    fn test_package(
        name: &str,
        trust: SkillTrustLevel,
        compatibility: SkillCompatibility,
        instructions: &str,
    ) -> SkillPackage {
        SkillPackage::seal(
            SkillId::new(name),
            name,
            format!("{name} description"),
            Some("1.0.0".to_owned()),
            SkillSource {
                kind: if trust == SkillTrustLevel::WorkspaceUntrusted {
                    SkillSourceKind::Workspace
                } else {
                    SkillSourceKind::UserConfigured
                },
                locator: format!("configured:/skills/{name}/SKILL.md"),
            },
            trust,
            compatibility,
            SkillDependencies::default(),
            instructions,
        )
        .unwrap()
    }
}
