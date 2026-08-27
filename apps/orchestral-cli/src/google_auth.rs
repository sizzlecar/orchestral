use std::env;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use orchestral_core::config::BackendSpec;
use serde_json::Value;

const GOOGLE_APPLICATION_CREDENTIALS: &str = "GOOGLE_APPLICATION_CREDENTIALS";
const GOOGLE_CLOUD_PROJECT: &str = "GOOGLE_CLOUD_PROJECT";
const GCLOUD_PROJECT: &str = "GCLOUD_PROJECT";
const LOCAL_COMPATIBILITY_CREDENTIAL: &str = "credential.json";
const DEFAULT_VERTEX_LOCATION: &str = "global";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GoogleCredentialSource {
    /// An Orchestral convenience override. This is intentionally limited to a
    /// service-account key; all standard ADC credential types use ADC itself.
    ServiceAccountFile(PathBuf),
    /// Google's standard ADC chain: environment, well-known user file, then
    /// the attached service account exposed by the metadata server.
    ApplicationDefault,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoogleVertexAuthPlan {
    pub source: GoogleCredentialSource,
    pub project_id: String,
    pub location: String,
}

impl GoogleVertexAuthPlan {
    pub fn endpoint(&self) -> String {
        format!(
            "https://aiplatform.googleapis.com/v1/projects/{}/locations/{}/publishers/google",
            self.project_id, self.location
        )
    }
}

/// Resolve a Vertex credential plan without reading or exposing secret fields.
///
/// Explicit Orchestral file overrides are checked first. Standard ADC order is
/// then preserved exactly. `./credential.json` is a final compatibility bridge
/// for the repository's current development setup, not a Google convention.
pub(crate) fn resolve_google_vertex_auth(
    explicit_credential_file: Option<&Path>,
    backend: &BackendSpec,
) -> anyhow::Result<Option<GoogleVertexAuthPlan>> {
    let configured_file = backend
        .get_config::<PathBuf>("credential_file")
        .map(expand_user_path);
    if let Some(path) = explicit_credential_file
        .map(Path::to_path_buf)
        .map(expand_user_path)
        .or(configured_file)
    {
        return service_account_plan(path, backend).map(Some);
    }

    if let Some(path) = non_empty_env_path(GOOGLE_APPLICATION_CREDENTIALS) {
        return adc_plan(Some(path), backend).map(Some);
    }

    if let Some(path) = well_known_adc_path().filter(|path| path.is_file()) {
        return adc_plan(Some(path), backend).map(Some);
    }

    // A configured project makes ADC-on-Google-Cloud a valid plan even when no
    // local credential file exists; the auth SDK will use the metadata server.
    if configured_project_id(backend).is_some() {
        return adc_plan(None, backend).map(Some);
    }

    let compatibility_file = PathBuf::from(LOCAL_COMPATIBILITY_CREDENTIAL);
    if compatibility_file.is_file() {
        return service_account_plan(compatibility_file, backend).map(Some);
    }

    Ok(None)
}

pub(crate) fn has_google_credentials(explicit_credential_file: Option<&Path>) -> bool {
    explicit_credential_file.is_some_and(Path::is_file)
        || non_empty_env_path(GOOGLE_APPLICATION_CREDENTIALS).is_some()
        || well_known_adc_path().is_some_and(|path| path.is_file())
        || configured_project_env().is_some()
        || Path::new(LOCAL_COMPATIBILITY_CREDENTIAL).is_file()
}

pub(crate) fn google_adc_is_explicitly_requested(
    explicit_credential_file: Option<&Path>,
    backend: &BackendSpec,
) -> bool {
    explicit_credential_file.is_some()
        || backend
            .get_config::<String>("credential_file")
            .is_some_and(|value| !value.trim().is_empty())
        || non_empty_env_path(GOOGLE_APPLICATION_CREDENTIALS).is_some()
        || backend
            .endpoint
            .as_deref()
            .is_some_and(|endpoint| endpoint.contains("aiplatform.googleapis.com"))
}

fn service_account_plan(
    path: PathBuf,
    backend: &BackendSpec,
) -> anyhow::Result<GoogleVertexAuthPlan> {
    if !path.is_file() {
        bail!("Google credential file not found: {}", path.display());
    }
    let metadata = read_credential_metadata(&path)?;
    build_plan(
        GoogleCredentialSource::ServiceAccountFile(path),
        backend,
        Some(&metadata),
    )
}

fn adc_plan(
    metadata_path: Option<PathBuf>,
    backend: &BackendSpec,
) -> anyhow::Result<GoogleVertexAuthPlan> {
    let metadata = metadata_path
        .as_deref()
        .map(read_credential_metadata)
        .transpose()?;
    build_plan(
        GoogleCredentialSource::ApplicationDefault,
        backend,
        metadata.as_ref(),
    )
}

fn build_plan(
    source: GoogleCredentialSource,
    backend: &BackendSpec,
    metadata: Option<&Value>,
) -> anyhow::Result<GoogleVertexAuthPlan> {
    let project_id = configured_project_id(backend)
        .or_else(|| metadata.and_then(project_id_from_metadata))
        .context(
            "Google credentials were found, but the Vertex project ID is unknown; set \
             GOOGLE_CLOUD_PROJECT or providers.backends[].config.project_id",
        )?;
    validate_vertex_segment("project ID", &project_id)?;

    let location = backend
        .get_config::<String>("location")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_VERTEX_LOCATION.to_owned());
    validate_vertex_segment("location", &location)?;

    Ok(GoogleVertexAuthPlan {
        source,
        project_id,
        location,
    })
}

fn configured_project_id(backend: &BackendSpec) -> Option<String> {
    backend
        .get_config::<String>("project_id")
        .filter(|value| !value.trim().is_empty())
        .or_else(configured_project_env)
}

fn configured_project_env() -> Option<String> {
    [GOOGLE_CLOUD_PROJECT, GCLOUD_PROJECT]
        .into_iter()
        .find_map(non_empty_env)
}

fn project_id_from_metadata(value: &Value) -> Option<String> {
    ["project_id", "quota_project_id"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str))
        .map(str::to_owned)
        .filter(|value| !value.trim().is_empty())
}

fn read_credential_metadata(path: &Path) -> anyhow::Result<Value> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("read Google credential metadata '{}'", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse Google credential metadata '{}'", path.display()))
}

fn validate_vertex_segment(label: &str, value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '.'))
    {
        bail!("invalid Google Vertex {label}: '{value}'");
    }
    Ok(())
}

fn non_empty_env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn expand_user_path(path: PathBuf) -> PathBuf {
    let Some(path_text) = path.to_str() else {
        return path;
    };
    let Some(relative) = path_text.strip_prefix("~/") else {
        return path;
    };
    user_home_dir()
        .map(|home| home.join(relative))
        .unwrap_or(path)
}

fn well_known_adc_path() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        return env::var_os("APPDATA")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .map(|root| root.join("gcloud/application_default_credentials.json"));
    }
    #[cfg(not(target_os = "windows"))]
    {
        user_home_dir().map(|root| root.join(".config/gcloud/application_default_credentials.json"))
    }
}

fn user_home_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    let name = "USERPROFILE";
    #[cfg(not(target_os = "windows"))]
    let name = "HOME";

    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(config: Value) -> BackendSpec {
        BackendSpec {
            name: "google".to_owned(),
            kind: "gemini".to_owned(),
            endpoint: None,
            api_key_env: Some("GOOGLE_API_KEY".to_owned()),
            config,
        }
    }

    #[test]
    fn vertex_endpoint_uses_project_and_global_location() {
        let plan = build_plan(
            GoogleCredentialSource::ApplicationDefault,
            &backend(serde_json::json!({"project_id": "example-project"})),
            None,
        )
        .unwrap();
        assert_eq!(
            plan.endpoint(),
            "https://aiplatform.googleapis.com/v1/projects/example-project/locations/global/publishers/google"
        );
    }

    #[test]
    fn credential_metadata_supplies_project_id_without_exposing_other_fields() {
        let metadata = serde_json::json!({
            "project_id": "metadata-project",
            "private_key": "must-never-be-returned"
        });
        let plan = build_plan(
            GoogleCredentialSource::ApplicationDefault,
            &backend(Value::Object(Default::default())),
            Some(&metadata),
        )
        .unwrap();
        assert_eq!(plan.project_id, "metadata-project");
        assert!(!format!("{plan:?}").contains("must-never-be-returned"));
    }

    #[test]
    fn invalid_vertex_path_segments_are_rejected() {
        let error = build_plan(
            GoogleCredentialSource::ApplicationDefault,
            &backend(serde_json::json!({"project_id": "project/escape"})),
            None,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid Google Vertex project ID"));
    }
}
