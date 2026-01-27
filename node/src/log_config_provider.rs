use graph::components::log_store::{LogStore, LogStoreConfig, LogStoreFactory, NoOpLogStore};
use graph::prelude::*;
use slog::{info, warn, Logger};
use std::sync::Arc;

/// Configuration sources for log store resolution
pub struct LogStoreConfigSources {
    /// Log store config from CLI arguments (any backend)
    pub cli_config: Option<LogStoreConfig>,
}

/// Provider for resolving log store configuration from multiple sources
///
/// It handles multi-source configuration with the following priority:
/// 1. GRAPH_LOG_STORE environment variable (supports all backends)
/// 2. CLI configuration (any backend)
/// 3. NoOp/None (disabled)
pub struct LogStoreConfigProvider {
    sources: LogStoreConfigSources,
}

impl LogStoreConfigProvider {
    /// Create a new provider with given configuration sources
    pub fn new(sources: LogStoreConfigSources) -> Self {
        Self { sources }
    }

    /// Resolve and create a LogStore for querying logs
    ///
    /// Priority: GRAPH_LOG_STORE env var → CLI config → NoOp
    pub fn resolve_log_store(&self, logger: &Logger) -> Arc<dyn LogStore> {
        // Try GRAPH_LOG_STORE environment variable
        match LogStoreFactory::from_env() {
            Ok(config) => match LogStoreFactory::from_config(config) {
                Ok(store) => {
                    info!(
                        logger,
                        "Log store initialized from GRAPH_LOG_STORE environment variable"
                    );
                    return store;
                }
                Err(e) => {
                    warn!(
                        logger,
                        "Failed to initialize log store from GRAPH_LOG_STORE: {}, falling back to CLI config",
                        e
                    );
                    // Fall through to CLI fallback
                }
            },
            Err(_) => {
                // No GRAPH_LOG_STORE env var, fall through to CLI config
            }
        }

        // Try CLI config
        if let Some(cli_store) = self.resolve_cli_store(logger) {
            return cli_store;
        }

        // Default to NoOp
        info!(
            logger,
            "No log store configured, queries will return empty results"
        );
        Arc::new(NoOpLogStore)
    }

    /// Resolve LogStoreConfig for drain selection (write side)
    ///
    /// Priority: GRAPH_LOG_STORE env var → CLI config → None
    pub fn resolve_log_store_config(&self, _logger: &Logger) -> Option<LogStoreConfig> {
        // Try GRAPH_LOG_STORE environment variable
        // Note: from_env() returns Ok(Disabled) when GRAPH_LOG_STORE is not set,
        // so we need to check if it's actually configured
        if let Ok(config) = LogStoreFactory::from_env() {
            if !matches!(config, LogStoreConfig::Disabled) {
                return Some(config);
            }
        }

        // Fallback to CLI config (any backend)
        self.sources.cli_config.clone()
    }

    /// Convenience method: Resolve both log store and config at once
    ///
    /// This is the primary entry point for most callers, as it resolves both
    /// the LogStore (for querying) and LogStoreConfig (for drain selection)
    /// in a single call.
    pub fn resolve(&self, logger: &Logger) -> (Arc<dyn LogStore>, Option<LogStoreConfig>) {
        let store = self.resolve_log_store(logger);
        let config = self.resolve_log_store_config(logger);

        if let Some(ref cfg) = config {
            info!(logger, "Log drain initialized"; "backend" => format!("{:?}", cfg));
        }

        (store, config)
    }

    /// Helper: Try to create log store from CLI config (any backend)
    fn resolve_cli_store(&self, logger: &Logger) -> Option<Arc<dyn LogStore>> {
        self.sources.cli_config.as_ref().map(|config| {
            match LogStoreFactory::from_config(config.clone()) {
                Ok(store) => {
                    info!(logger, "Log store initialized from CLI configuration");
                    store
                }
                Err(e) => {
                    warn!(
                        logger,
                        "Failed to initialize log store from CLI config: {}, using NoOp", e
                    );
                    Arc::new(NoOpLogStore)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_config_returns_noop() {
        std::env::remove_var("GRAPH_LOG_STORE");

        let logger = graph::log::logger(true);
        let provider = LogStoreConfigProvider::new(LogStoreConfigSources { cli_config: None });

        let store = provider.resolve_log_store(&logger);
        assert!(!store.is_available());

        let config = provider.resolve_log_store_config(&logger);
        assert!(config.is_none());
    }

    #[test]
    fn test_elastic_from_cli() {
        std::env::remove_var("GRAPH_LOG_STORE");

        let logger = graph::log::logger(true);
        let cli_config = LogStoreConfig::Elasticsearch {
            endpoint: "http://localhost:9200".to_string(),
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            index: "test-index".to_string(),
            timeout_secs: 10,
        };

        let provider = LogStoreConfigProvider::new(LogStoreConfigSources {
            cli_config: Some(cli_config),
        });

        let config = provider.resolve_log_store_config(&logger);
        assert!(config.is_some());

        if let Some(LogStoreConfig::Elasticsearch {
            endpoint,
            username,
            password,
            index,
            ..
        }) = config
        {
            assert_eq!(endpoint, "http://localhost:9200");
            assert_eq!(username, Some("user".to_string()));
            assert_eq!(password, Some("pass".to_string()));
            assert_eq!(index, "test-index");
        } else {
            panic!("Expected Elasticsearch config");
        }
    }

    #[test]
    fn test_resolve_convenience_method() {
        std::env::remove_var("GRAPH_LOG_STORE");

        let logger = graph::log::logger(true);
        let cli_config = LogStoreConfig::Elasticsearch {
            endpoint: "http://localhost:9200".to_string(),
            username: None,
            password: None,
            index: "test-index".to_string(),
            timeout_secs: 10,
        };

        let provider = LogStoreConfigProvider::new(LogStoreConfigSources {
            cli_config: Some(cli_config),
        });

        let (_store, config) = provider.resolve(&logger);
        assert!(config.is_some());

        if let Some(LogStoreConfig::Elasticsearch { endpoint, .. }) = config {
            assert_eq!(endpoint, "http://localhost:9200");
        } else {
            panic!("Expected Elasticsearch config");
        }
    }

    #[test]
    fn test_loki_from_cli() {
        std::env::remove_var("GRAPH_LOG_STORE");

        let logger = graph::log::logger(true);
        let cli_config = LogStoreConfig::Loki {
            endpoint: "http://localhost:3100".to_string(),
            tenant_id: Some("test-tenant".to_string()),
        };

        let provider = LogStoreConfigProvider::new(LogStoreConfigSources {
            cli_config: Some(cli_config),
        });

        let config = provider.resolve_log_store_config(&logger);
        assert!(config.is_some());

        if let Some(LogStoreConfig::Loki {
            endpoint,
            tenant_id,
        }) = config
        {
            assert_eq!(endpoint, "http://localhost:3100");
            assert_eq!(tenant_id, Some("test-tenant".to_string()));
        } else {
            panic!("Expected Loki config");
        }
    }
}
