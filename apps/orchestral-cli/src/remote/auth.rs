use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use async_trait::async_trait;
use axum::http::HeaderName;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::Value;
use tokio::sync::RwLock;

const DEFAULT_JWKS_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayPrincipal {
    pub subject: Option<String>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayAuthError {
    #[error("gateway JWT header is missing")]
    Missing,
    #[error("gateway JWT is invalid: {0}")]
    Invalid(String),
    #[error("gateway signing keys are unavailable: {0}")]
    KeysUnavailable(String),
}

#[async_trait]
pub trait GatewayAuthenticator: Send + Sync {
    fn header_name(&self) -> &HeaderName;

    async fn authenticate(&self, token: &str) -> Result<GatewayPrincipal, GatewayAuthError>;
}

#[derive(Debug, Clone)]
pub struct JwtGatewayConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    pub header_name: HeaderName,
    pub required_claims: BTreeMap<String, String>,
}

impl JwtGatewayConfig {
    pub fn new(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        jwks_url: impl Into<String>,
        header_name: impl AsRef<str>,
        required_claims: BTreeMap<String, String>,
    ) -> anyhow::Result<Self> {
        let issuer = issuer.into().trim_end_matches('/').to_owned();
        let audience = audience.into();
        let jwks_url = jwks_url.into();
        if issuer.is_empty() || audience.trim().is_empty() || jwks_url.trim().is_empty() {
            bail!("gateway JWT issuer, audience, and JWKS URL must not be empty");
        }
        validate_endpoint("issuer", &issuer)?;
        validate_endpoint("JWKS URL", &jwks_url)?;
        let header_name = HeaderName::from_bytes(header_name.as_ref().as_bytes())
            .context("gateway JWT header name is invalid")?;
        if required_claims.is_empty() {
            bail!("at least one --access-jwt-required-claim is required");
        }
        if required_claims
            .iter()
            .any(|(name, value)| name.trim().is_empty() || value.trim().is_empty())
        {
            bail!("gateway JWT required claims must use non-empty NAME=VALUE pairs");
        }
        Ok(Self {
            issuer,
            audience,
            jwks_url,
            header_name,
            required_claims,
        })
    }
}

fn validate_endpoint(label: &str, value: &str) -> anyhow::Result<()> {
    let url =
        reqwest::Url::parse(value).with_context(|| format!("gateway JWT {label} is invalid"))?;
    let secure = url.scheme() == "https";
    let local = url.scheme() == "http"
        && url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
    if !secure && !local {
        bail!("gateway JWT {label} must use HTTPS (HTTP is allowed only on loopback)");
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct CachedKeys {
    keys: JwkSet,
    fetched_at: Instant,
}

#[derive(Debug)]
pub struct JwtGatewayAuthenticator {
    config: JwtGatewayConfig,
    client: reqwest::Client,
    cache: Arc<RwLock<Option<CachedKeys>>>,
    cache_ttl: Duration,
}

impl JwtGatewayAuthenticator {
    pub fn new(config: JwtGatewayConfig) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build gateway JWT JWKS client")?;
        Ok(Self {
            config,
            client,
            cache: Arc::new(RwLock::new(None)),
            cache_ttl: DEFAULT_JWKS_TTL,
        })
    }

    async fn keys(&self, force_refresh: bool) -> Result<JwkSet, GatewayAuthError> {
        if !force_refresh {
            let cache = self.cache.read().await;
            if let Some(cached) = cache
                .as_ref()
                .filter(|cached| cached.fetched_at.elapsed() < self.cache_ttl)
            {
                return Ok(cached.keys.clone());
            }
        }
        let keys = self
            .client
            .get(&self.config.jwks_url)
            .send()
            .await
            .map_err(|error| GatewayAuthError::KeysUnavailable(error.to_string()))?
            .error_for_status()
            .map_err(|error| GatewayAuthError::KeysUnavailable(error.to_string()))?
            .json::<JwkSet>()
            .await
            .map_err(|error| GatewayAuthError::KeysUnavailable(error.to_string()))?;
        if keys.keys.is_empty() {
            return Err(GatewayAuthError::KeysUnavailable(
                "JWKS endpoint returned no signing keys".to_owned(),
            ));
        }
        *self.cache.write().await = Some(CachedKeys {
            keys: keys.clone(),
            fetched_at: Instant::now(),
        });
        Ok(keys)
    }

    async fn decoding_key(&self, kid: &str) -> Result<DecodingKey, GatewayAuthError> {
        let cached = self.keys(false).await?;
        if let Some(jwk) = cached.find(kid) {
            return DecodingKey::from_jwk(jwk)
                .map_err(|error| GatewayAuthError::Invalid(error.to_string()));
        }
        let refreshed = self.keys(true).await?;
        let jwk = refreshed.find(kid).ok_or_else(|| {
            GatewayAuthError::Invalid("JWT signing key id was not found in JWKS".to_owned())
        })?;
        DecodingKey::from_jwk(jwk).map_err(|error| GatewayAuthError::Invalid(error.to_string()))
    }
}

#[async_trait]
impl GatewayAuthenticator for JwtGatewayAuthenticator {
    fn header_name(&self) -> &HeaderName {
        &self.config.header_name
    }

    async fn authenticate(&self, token: &str) -> Result<GatewayPrincipal, GatewayAuthError> {
        let header =
            decode_header(token).map_err(|error| GatewayAuthError::Invalid(error.to_string()))?;
        if header.alg != Algorithm::RS256 {
            return Err(GatewayAuthError::Invalid(
                "only RS256 gateway assertions are accepted".to_owned(),
            ));
        }
        let kid = header.kid.as_deref().ok_or_else(|| {
            GatewayAuthError::Invalid("JWT header does not contain a key id".to_owned())
        })?;
        let key = self.decoding_key(kid).await?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[&self.config.audience]);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_required_spec_claims(&["exp", "aud", "iss"]);
        validation.validate_nbf = true;
        let claims = decode::<Value>(token, &key, &validation)
            .map_err(|error| GatewayAuthError::Invalid(error.to_string()))?
            .claims;

        let mut attributes = BTreeMap::new();
        for (name, expected) in &self.config.required_claims {
            let value = claim_at_path(&claims, name).ok_or_else(|| {
                GatewayAuthError::Invalid(format!("required claim '{name}' is missing"))
            })?;
            if !claim_contains(value, expected) {
                return Err(GatewayAuthError::Invalid(format!(
                    "required claim '{name}' does not match"
                )));
            }
            attributes.insert(name.clone(), expected.clone());
        }
        Ok(GatewayPrincipal {
            subject: claims.get("sub").and_then(Value::as_str).map(str::to_owned),
            attributes,
        })
    }
}

fn claim_at_path<'a>(claims: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(claims, |value, segment| value.get(segment))
}

fn claim_contains(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(actual) => actual == expected,
        Value::Array(values) => values.iter().any(|value| claim_contains(value, expected)),
        Value::Bool(actual) => expected.parse::<bool>() == Ok(*actual),
        Value::Number(actual) => actual.to_string() == expected,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn required_claims_support_nested_values_and_group_membership() {
        let claims = json!({"identity": {"email": "person@example.com"}, "groups": ["dev", "ops"]});
        assert!(claim_contains(
            claim_at_path(&claims, "identity.email").unwrap(),
            "person@example.com"
        ));
        assert!(claim_contains(
            claim_at_path(&claims, "groups").unwrap(),
            "ops"
        ));
        assert!(!claim_contains(
            claim_at_path(&claims, "groups").unwrap(),
            "admin"
        ));
    }

    #[test]
    fn gateway_endpoints_require_https_except_for_loopback_tests() {
        let claims = BTreeMap::from([("email".to_owned(), "person@example.com".to_owned())]);
        assert!(JwtGatewayConfig::new(
            "https://access.example.com",
            "audience",
            "https://access.example.com/keys",
            "x-access-jwt",
            claims.clone(),
        )
        .is_ok());
        assert!(JwtGatewayConfig::new(
            "http://access.example.com",
            "audience",
            "http://access.example.com/keys",
            "x-access-jwt",
            claims,
        )
        .is_err());
    }
}
