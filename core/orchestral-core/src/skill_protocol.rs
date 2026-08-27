//! Provider-neutral Skill protocol.
//!
//! A Skill is immutable instruction/context data. It may declare dependencies
//! on Tools or MCP servers, but it never carries effect authority and is never
//! executable through the Tool protocol.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::agent_protocol::wire::{Digest, ResourceId};

pub const SKILL_PROTOCOL_V1: &str = "orchestral.skill/v1";
pub const SKILL_CATALOG_RESOURCE_KIND_V1: &str = "skill-catalog/v1";

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.trim().is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(SkillId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SkillSourceKind {
    BuiltIn,
    UserConfigured,
    Workspace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillSource {
    pub kind: SkillSourceKind,
    /// Host-resolved provenance. This is diagnostic data, not an executable path grant.
    pub locator: String,
}

/// Compatibility is deliberately structured and closed in v1. Free-text
/// compatibility cannot be verified and must be rejected by loaders.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillCompatibility {
    #[serde(default)]
    pub operating_systems: BTreeSet<String>,
    #[serde(default)]
    pub architectures: BTreeSet<String>,
    #[serde(default)]
    pub required_programs: BTreeSet<String>,
    /// Every set is an any-of group; at least one program in each group must
    /// be present in the Host snapshot.
    #[serde(default)]
    pub any_programs: Vec<BTreeSet<String>>,
    #[serde(default)]
    pub required_environment: BTreeSet<String>,
    #[serde(default)]
    pub required_features: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillDependencies {
    #[serde(default)]
    pub tools: BTreeSet<String>,
    #[serde(default)]
    pub mcp_servers: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillDescriptor {
    pub protocol: String,
    pub skill_id: SkillId,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub version: Option<String>,
    pub digest: Digest,
    pub source: SkillSource,
    #[serde(default)]
    pub compatibility: SkillCompatibility,
    #[serde(default)]
    pub dependencies: SkillDependencies,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillPackage {
    pub descriptor: SkillDescriptor,
    pub instructions: String,
}

#[derive(Serialize)]
struct SkillDigestView<'a> {
    protocol: &'a str,
    skill_id: &'a SkillId,
    name: &'a str,
    description: &'a str,
    version: &'a Option<String>,
    source: &'a SkillSource,
    compatibility: &'a SkillCompatibility,
    dependencies: &'a SkillDependencies,
    instructions: &'a str,
}

impl SkillPackage {
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        skill_id: SkillId,
        name: impl Into<String>,
        description: impl Into<String>,
        version: Option<String>,
        source: SkillSource,
        compatibility: SkillCompatibility,
        dependencies: SkillDependencies,
        instructions: impl Into<String>,
    ) -> Result<Self, SkillProtocolError> {
        let mut package = Self {
            descriptor: SkillDescriptor {
                protocol: SKILL_PROTOCOL_V1.to_owned(),
                skill_id,
                name: name.into(),
                description: description.into(),
                version,
                digest: Digest::sha256([]),
                source,
                compatibility,
                dependencies,
            },
            instructions: instructions.into(),
        };
        package.descriptor.digest = package.computed_digest()?;
        package.validate()?;
        Ok(package)
    }

    pub fn computed_digest(&self) -> Result<Digest, SkillProtocolError> {
        let view = SkillDigestView {
            protocol: &self.descriptor.protocol,
            skill_id: &self.descriptor.skill_id,
            name: &self.descriptor.name,
            description: &self.descriptor.description,
            version: &self.descriptor.version,
            source: &self.descriptor.source,
            compatibility: &self.descriptor.compatibility,
            dependencies: &self.descriptor.dependencies,
            instructions: &self.instructions,
        };
        canonical_digest(&view)
    }

    pub fn validate(&self) -> Result<(), SkillProtocolError> {
        self.descriptor.validate()?;
        if self.instructions.trim().is_empty() {
            return Err(SkillProtocolError::Invalid(
                "Skill instructions must not be empty".to_owned(),
            ));
        }
        if self.computed_digest()? != self.descriptor.digest {
            return Err(SkillProtocolError::Invalid(
                "Skill digest does not match its immutable package".to_owned(),
            ));
        }
        Ok(())
    }
}

impl SkillDescriptor {
    pub fn validate(&self) -> Result<(), SkillProtocolError> {
        if self.protocol != SKILL_PROTOCOL_V1
            || self.skill_id.is_empty()
            || self.name.trim().is_empty()
            || self.description.trim().is_empty()
            || !self.digest.is_sha256()
            || self.source.locator.trim().is_empty()
            || self
                .version
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || self
                .compatibility
                .operating_systems
                .iter()
                .chain(self.compatibility.architectures.iter())
                .chain(self.compatibility.required_programs.iter())
                .chain(self.compatibility.required_environment.iter())
                .chain(self.compatibility.required_features.iter())
                .any(|value| value.trim().is_empty())
            || self
                .compatibility
                .any_programs
                .iter()
                .any(|group| group.is_empty() || group.iter().any(|value| value.trim().is_empty()))
            || self
                .dependencies
                .tools
                .iter()
                .chain(self.dependencies.mcp_servers.iter())
                .any(|value| value.trim().is_empty())
        {
            return Err(SkillProtocolError::Invalid(
                "Skill descriptor contains an invalid identity, version, source, or constraint"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillCatalogDescriptor {
    pub resource_id: ResourceId,
    pub revision: Digest,
    pub skills: Vec<SkillDescriptor>,
}

impl SkillCatalogDescriptor {
    pub fn seal(
        resource_id: ResourceId,
        mut skills: Vec<SkillDescriptor>,
    ) -> Result<Self, SkillProtocolError> {
        skills.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.skill_id.cmp(&right.skill_id))
        });
        let revision = canonical_digest(&skills)?;
        let descriptor = Self {
            resource_id,
            revision,
            skills,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn validate(&self) -> Result<(), SkillProtocolError> {
        if self.resource_id.is_empty() || !self.revision.is_sha256() {
            return Err(SkillProtocolError::Invalid(
                "Skill catalog identity and revision must be valid".to_owned(),
            ));
        }
        let mut names = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for descriptor in &self.skills {
            descriptor.validate()?;
            if !names.insert(descriptor.name.clone()) || !ids.insert(descriptor.skill_id.clone()) {
                return Err(SkillProtocolError::Invalid(
                    "Skill catalog names and ids must be unique".to_owned(),
                ));
            }
        }
        if canonical_digest(&self.skills)? != self.revision {
            return Err(SkillProtocolError::Invalid(
                "Skill catalog revision does not match its descriptors".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Durable fact recording the immutable Skill instructions loaded into model
/// context. It carries no effect authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillLoad {
    pub package: SkillPackage,
}

impl SkillLoad {
    pub fn validate(&self) -> Result<(), SkillProtocolError> {
        self.package.validate()
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SkillProtocolError {
    #[error("invalid Skill protocol value: {0}")]
    Invalid(String),
    #[error("could not serialize Skill protocol value: {0}")]
    Serialization(String),
}

fn canonical_digest<T: Serialize + ?Sized>(value: &T) -> Result<Digest, SkillProtocolError> {
    let bytes = serde_jcs::to_vec(value)
        .map_err(|error| SkillProtocolError::Serialization(error.to_string()))?;
    Ok(Digest::sha256(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(instructions: &str) -> SkillPackage {
        SkillPackage::seal(
            SkillId::new("xlsx"),
            "xlsx",
            "Spreadsheet workflow",
            Some("1.0.0".to_owned()),
            SkillSource {
                kind: SkillSourceKind::UserConfigured,
                locator: "/skills/xlsx/SKILL.md".to_owned(),
            },
            SkillCompatibility::default(),
            SkillDependencies::default(),
            instructions,
        )
        .unwrap()
    }

    #[test]
    fn package_digest_covers_instructions() {
        assert_ne!(
            package("one").descriptor.digest,
            package("two").descriptor.digest
        );
    }

    #[test]
    fn catalog_order_and_revision_are_deterministic() {
        let left = package("left");
        let right = SkillPackage::seal(
            SkillId::new("pdf"),
            "pdf",
            "PDF workflow",
            None,
            SkillSource {
                kind: SkillSourceKind::BuiltIn,
                locator: "builtin:pdf".to_owned(),
            },
            SkillCompatibility::default(),
            SkillDependencies::default(),
            "pdf instructions",
        )
        .unwrap();
        let first = SkillCatalogDescriptor::seal(
            ResourceId::new("skills"),
            vec![left.descriptor.clone(), right.descriptor.clone()],
        )
        .unwrap();
        let second = SkillCatalogDescriptor::seal(
            ResourceId::new("skills"),
            vec![right.descriptor, left.descriptor],
        )
        .unwrap();
        assert_eq!(first, second);
    }
}
