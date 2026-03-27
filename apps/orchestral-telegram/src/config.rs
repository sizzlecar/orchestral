//! Bot configuration from environment variables.

/// Telegram bot configuration, all from env vars.
#[derive(Debug)]
pub struct BotConfig {
    pub bot_token: String,
    pub backend: String,
    pub model: String,
    pub max_iterations: usize,
    pub config_path: String,
}

impl BotConfig {
    /// Load configuration from environment variables with defaults.
    pub fn from_env() -> Result<Self, String> {
        let get = |key: &str| std::env::var(key).ok();
        let args: Vec<String> = std::env::args().collect();
        Self::from_values(get, args.get(1).map(|s| s.as_str()))
    }

    /// Parse config from a value lookup function (testable without env vars).
    fn from_values(
        get: impl Fn(&str) -> Option<String>,
        cli_arg: Option<&str>,
    ) -> Result<Self, String> {
        let bot_token = get("TELEGRAM_BOT_TOKEN")
            .ok_or_else(|| "TELEGRAM_BOT_TOKEN env var required".to_string())?;

        let backend = get("ORCHESTRAL_BACKEND").unwrap_or_else(|| "google".to_string());
        let model = get("ORCHESTRAL_MODEL").unwrap_or_else(|| "gemini-2.5-flash".to_string());
        let max_iterations: usize = get("ORCHESTRAL_MAX_ITERATIONS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(6);
        let config_path = get("ORCHESTRAL_CONFIG")
            .or_else(|| cli_arg.map(String::from))
            .unwrap_or_else(|| "configs/orchestral.cli.runtime.override.yaml".to_string());

        Ok(Self {
            bot_token,
            backend,
            model,
            max_iterations,
            config_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn test_fails_without_bot_token() {
        let result = BotConfig::from_values(make_env(&[]), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("TELEGRAM_BOT_TOKEN"));
    }

    #[test]
    fn test_uses_defaults() {
        let env = make_env(&[("TELEGRAM_BOT_TOKEN", "test-token-123")]);
        let config = BotConfig::from_values(env, None).unwrap();
        assert_eq!(config.bot_token, "test-token-123");
        assert_eq!(config.backend, "google");
        assert_eq!(config.model, "gemini-2.5-flash");
        assert_eq!(config.max_iterations, 6);
        assert!(config.config_path.contains("orchestral"));
    }

    #[test]
    fn test_reads_custom_values() {
        let env = make_env(&[
            ("TELEGRAM_BOT_TOKEN", "tok"),
            ("ORCHESTRAL_BACKEND", "openai"),
            ("ORCHESTRAL_MODEL", "gpt-4o"),
            ("ORCHESTRAL_MAX_ITERATIONS", "3"),
            ("ORCHESTRAL_CONFIG", "/custom/config.yaml"),
        ]);
        let config = BotConfig::from_values(env, None).unwrap();
        assert_eq!(config.backend, "openai");
        assert_eq!(config.model, "gpt-4o");
        assert_eq!(config.max_iterations, 3);
        assert_eq!(config.config_path, "/custom/config.yaml");
    }

    #[test]
    fn test_ignores_invalid_max_iterations() {
        let env = make_env(&[
            ("TELEGRAM_BOT_TOKEN", "tok"),
            ("ORCHESTRAL_MAX_ITERATIONS", "not_a_number"),
        ]);
        let config = BotConfig::from_values(env, None).unwrap();
        assert_eq!(config.max_iterations, 6);
    }

    #[test]
    fn test_cli_arg_fallback_for_config_path() {
        let env = make_env(&[("TELEGRAM_BOT_TOKEN", "tok")]);
        let config = BotConfig::from_values(env, Some("/from/cli.yaml")).unwrap();
        assert_eq!(config.config_path, "/from/cli.yaml");
    }

    #[test]
    fn test_env_config_overrides_cli_arg() {
        let env = make_env(&[
            ("TELEGRAM_BOT_TOKEN", "tok"),
            ("ORCHESTRAL_CONFIG", "/from/env.yaml"),
        ]);
        let config = BotConfig::from_values(env, Some("/from/cli.yaml")).unwrap();
        assert_eq!(config.config_path, "/from/env.yaml");
    }
}
