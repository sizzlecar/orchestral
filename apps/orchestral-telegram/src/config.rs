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
        let bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
            .map_err(|_| "TELEGRAM_BOT_TOKEN env var required".to_string())?;

        let backend = std::env::var("ORCHESTRAL_BACKEND").unwrap_or_else(|_| "google".to_string());
        let model =
            std::env::var("ORCHESTRAL_MODEL").unwrap_or_else(|_| "gemini-2.5-flash".to_string());
        let max_iterations: usize = std::env::var("ORCHESTRAL_MAX_ITERATIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(6);
        let config_path = std::env::var("ORCHESTRAL_CONFIG")
            .or_else(|_| std::env::args().nth(1).ok_or(()))
            .unwrap_or_else(|_| "configs/orchestral.cli.runtime.override.yaml".to_string());

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

    /// Clear all env vars used by BotConfig to avoid cross-test pollution.
    fn clear_env() {
        std::env::remove_var("TELEGRAM_BOT_TOKEN");
        std::env::remove_var("ORCHESTRAL_BACKEND");
        std::env::remove_var("ORCHESTRAL_MODEL");
        std::env::remove_var("ORCHESTRAL_MAX_ITERATIONS");
        std::env::remove_var("ORCHESTRAL_CONFIG");
    }

    #[test]
    fn test_from_env_fails_without_bot_token() {
        clear_env();
        let result = BotConfig::from_env();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("TELEGRAM_BOT_TOKEN"));
    }

    #[test]
    fn test_from_env_uses_defaults() {
        clear_env();
        std::env::set_var("TELEGRAM_BOT_TOKEN", "test-token-123");

        let config = BotConfig::from_env().unwrap();
        assert_eq!(config.bot_token, "test-token-123");
        assert_eq!(config.backend, "google");
        assert_eq!(config.model, "gemini-2.5-flash");
        assert_eq!(config.max_iterations, 6);

        clear_env();
    }

    #[test]
    fn test_from_env_reads_custom_values() {
        clear_env();
        std::env::set_var("TELEGRAM_BOT_TOKEN", "tok");
        std::env::set_var("ORCHESTRAL_BACKEND", "openai");
        std::env::set_var("ORCHESTRAL_MODEL", "gpt-4o");
        std::env::set_var("ORCHESTRAL_MAX_ITERATIONS", "3");
        std::env::set_var("ORCHESTRAL_CONFIG", "/custom/config.yaml");

        let config = BotConfig::from_env().unwrap();
        assert_eq!(config.backend, "openai");
        assert_eq!(config.model, "gpt-4o");
        assert_eq!(config.max_iterations, 3);
        assert_eq!(config.config_path, "/custom/config.yaml");

        clear_env();
    }

    #[test]
    fn test_from_env_ignores_invalid_max_iterations() {
        clear_env();
        std::env::set_var("TELEGRAM_BOT_TOKEN", "tok");
        std::env::set_var("ORCHESTRAL_MAX_ITERATIONS", "not_a_number");

        let config = BotConfig::from_env().unwrap();
        assert_eq!(config.max_iterations, 6);

        clear_env();
    }
}
