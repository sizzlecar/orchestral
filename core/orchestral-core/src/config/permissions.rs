//! Interactive permission presets pairing sandbox boundaries with approval policy.

use serde::{Deserialize, Serialize};

use crate::agent_connector::{
    AgentApprovalMode, AgentFilesystemAccess, AgentNetworkAccess, AgentSessionPermissions,
};

/// Built-in permission preset pairing filesystem/network boundaries with approval policy.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub enum PermissionPreset {
    /// Read workspace files and run read-only sandboxed commands; mutations
    /// and network or escalated execution require approval.
    ReadOnly,
    /// Read and edit workspace files and run workspace-sandboxed commands;
    /// host escalation, ungranted network, and destructive actions require approval.
    #[default]
    Auto,
    /// Unrestricted host filesystem and network access without approval prompts.
    FullAccess,
}

impl PermissionPreset {
    pub const ALL: [Self; 3] = [Self::ReadOnly, Self::Auto, Self::FullAccess];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::Auto => "auto",
            Self::FullAccess => "full-access",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::ReadOnly => "Read Only",
            Self::Auto => "Default",
            Self::FullAccess => "Full Access",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::ReadOnly => {
                "Read files in the current workspace and run read-only commands. Approval is required to edit files, run mutating commands, or access the internet."
            }
            Self::Auto => {
                "Read and edit files in the current workspace, and run commands. Approval is required to access the internet or edit other files."
            }
            Self::FullAccess => {
                "Edit files outside this workspace, run commands, and access the internet without asking for approval. Exercise caution when using."
            }
        }
    }

    pub const fn to_session_permissions(
        self,
        has_restricted_network_targets: bool,
    ) -> AgentSessionPermissions {
        match self {
            Self::ReadOnly => AgentSessionPermissions {
                filesystem: Some(AgentFilesystemAccess::ReadOnly),
                network: Some(if has_restricted_network_targets {
                    AgentNetworkAccess::Restricted
                } else {
                    AgentNetworkAccess::Disabled
                }),
                approvals: Some(AgentApprovalMode::OnRequest),
            },
            Self::Auto => AgentSessionPermissions {
                filesystem: Some(AgentFilesystemAccess::WorkspaceWrite),
                network: Some(if has_restricted_network_targets {
                    AgentNetworkAccess::Restricted
                } else {
                    AgentNetworkAccess::Disabled
                }),
                approvals: Some(AgentApprovalMode::OnRequest),
            },
            Self::FullAccess => AgentSessionPermissions {
                filesystem: Some(AgentFilesystemAccess::FullAccess),
                network: Some(AgentNetworkAccess::Enabled),
                approvals: Some(AgentApprovalMode::Never),
            },
        }
    }
}

impl std::fmt::Display for PermissionPreset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for PermissionPreset {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "read-only" | "readonly" | "read_only" => Ok(Self::ReadOnly),
            "auto" | "default" | "workspace-write" | "workspace_write" => Ok(Self::Auto),
            "full-access" | "full_access" | "full" | "danger-full-access"
            | "danger_full_access" => Ok(Self::FullAccess),
            other => Err(format!(
                "invalid permission preset '{other}'; expected read-only, auto, or full-access"
            )),
        }
    }
}

impl TryFrom<String> for PermissionPreset {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<PermissionPreset> for String {
    fn from(value: PermissionPreset) -> Self {
        value.as_str().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_presets_parse_aliases_and_round_trip() {
        for (input, expected) in [
            ("read-only", PermissionPreset::ReadOnly),
            ("readonly", PermissionPreset::ReadOnly),
            ("read_only", PermissionPreset::ReadOnly),
            ("auto", PermissionPreset::Auto),
            ("default", PermissionPreset::Auto),
            ("workspace-write", PermissionPreset::Auto),
            ("workspace_write", PermissionPreset::Auto),
            ("full-access", PermissionPreset::FullAccess),
            ("full_access", PermissionPreset::FullAccess),
            ("full", PermissionPreset::FullAccess),
            ("danger-full-access", PermissionPreset::FullAccess),
            ("danger_full_access", PermissionPreset::FullAccess),
        ] {
            let parsed: PermissionPreset = input.parse().unwrap();
            assert_eq!(parsed, expected);
            let serialized = serde_json::to_value(parsed).unwrap();
            assert_eq!(
                serde_json::from_value::<PermissionPreset>(serialized).unwrap(),
                expected
            );
        }
        assert!("unknown".parse::<PermissionPreset>().is_err());
    }
}
