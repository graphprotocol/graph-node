pub mod config;
pub mod elasticsearch;
pub mod file;
pub mod loki;

use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

use crate::prelude::DeploymentHash;

#[derive(Error, Debug)]
pub enum LogStoreError {
    #[error("log store query failed: {0}")]
    QueryFailed(#[from] anyhow::Error),

    #[error("log store is unavailable")]
    Unavailable,

    #[error("log store initialization failed: {0}")]
    InitializationFailed(anyhow::Error),

    #[error("log store configuration error: {0}")]
    ConfigurationError(anyhow::Error),
}

/// Configuration for different log store backends
#[derive(Debug, Clone)]
pub enum LogStoreConfig {
    /// No logging - returns empty results
    Disabled,

    /// Elasticsearch backend
    Elasticsearch {
        endpoint: String,
        username: Option<String>,
        password: Option<String>,
        index: String,
    },

    /// Loki (Grafana's log aggregation system)
    Loki {
        endpoint: String,
        tenant_id: Option<String>,
    },

    /// File-based logs (JSON lines format)
    File {
        directory: PathBuf,
        max_file_size: u64, // bytes
        retention_days: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Critical,
    Error,
    Warning,
    Info,
    Debug,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Critical => "critical",
            LogLevel::Error => "error",
            LogLevel::Warning => "warning",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "critical" => Some(LogLevel::Critical),
            "error" => Some(LogLevel::Error),
            "warning" => Some(LogLevel::Warning),
            "info" => Some(LogLevel::Info),
            "debug" => Some(LogLevel::Debug),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogMeta {
    pub module: String,
    pub line: i64,
    pub column: i64,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub id: String,
    pub subgraph_id: DeploymentHash,
    pub timestamp: String,
    pub level: LogLevel,
    pub text: String,
    pub arguments: Vec<(String, String)>,
    pub meta: LogMeta,
}

#[derive(Debug, Clone)]
pub struct LogQuery {
    pub subgraph_id: DeploymentHash,
    pub level: Option<LogLevel>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub text: Option<String>,
    pub first: u32,
    pub skip: u32,
}

#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>, LogStoreError>;
    fn is_available(&self) -> bool;
}

/// Factory for creating LogStore instances from configuration
pub struct LogStoreFactory;

impl LogStoreFactory {
    /// Create a LogStore from configuration
    pub fn from_config(config: LogStoreConfig) -> Result<Arc<dyn LogStore>, LogStoreError> {
        match config {
            LogStoreConfig::Disabled => Ok(Arc::new(NoOpLogStore)),

            LogStoreConfig::Elasticsearch {
                endpoint,
                username,
                password,
                index,
            } => {
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .map_err(|e| LogStoreError::InitializationFailed(e.into()))?;

                let config = crate::log::elastic::ElasticLoggingConfig {
                    endpoint,
                    username,
                    password,
                    client,
                };

                Ok(Arc::new(elasticsearch::ElasticsearchLogStore::new(
                    config, index,
                )))
            }

            LogStoreConfig::Loki {
                endpoint,
                tenant_id,
            } => Ok(Arc::new(loki::LokiLogStore::new(endpoint, tenant_id)?)),

            LogStoreConfig::File {
                directory,
                max_file_size,
                retention_days,
            } => Ok(Arc::new(file::FileLogStore::new(
                directory,
                max_file_size,
                retention_days,
            )?)),
        }
    }

    /// Parse configuration from environment variables
    ///
    /// Supports both new (GRAPH_LOG_STORE_*) and old (deprecated) environment variable names
    /// for backward compatibility. The new keys take precedence when both are set.
    pub fn from_env() -> Result<LogStoreConfig, LogStoreError> {
        // Logger for deprecation warnings
        let logger = crate::log::logger(false);

        // Read backend selector with backward compatibility
        let backend = config::read_env_with_default(
            &logger,
            "GRAPH_LOG_STORE_BACKEND",
            "GRAPH_LOG_STORE",
            "disabled",
        );

        match backend.to_lowercase().as_str() {
            "disabled" | "none" => Ok(LogStoreConfig::Disabled),

            "elasticsearch" | "elastic" | "es" => {
                let endpoint = config::read_env_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_ELASTICSEARCH_URL",
                    "GRAPH_ELASTICSEARCH_URL",
                )
                .ok_or_else(|| {
                    LogStoreError::ConfigurationError(anyhow::anyhow!(
                        "Elasticsearch endpoint not set. Use GRAPH_LOG_STORE_ELASTICSEARCH_URL environment variable"
                    ))
                })?;

                let username = config::read_env_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_ELASTICSEARCH_USER",
                    "GRAPH_ELASTICSEARCH_USER",
                );

                let password = config::read_env_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_ELASTICSEARCH_PASSWORD",
                    "GRAPH_ELASTICSEARCH_PASSWORD",
                );

                let index = config::read_env_with_default(
                    &logger,
                    "GRAPH_LOG_STORE_ELASTICSEARCH_INDEX",
                    "GRAPH_ELASTIC_SEARCH_INDEX",
                    "subgraph",
                );

                Ok(LogStoreConfig::Elasticsearch {
                    endpoint,
                    username,
                    password,
                    index,
                })
            }

            "loki" => {
                let endpoint = config::read_env_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_LOKI_URL",
                    "GRAPH_LOG_LOKI_ENDPOINT",
                )
                .ok_or_else(|| {
                    LogStoreError::ConfigurationError(anyhow::anyhow!(
                        "Loki endpoint not set. Use GRAPH_LOG_STORE_LOKI_URL environment variable"
                    ))
                })?;

                let tenant_id = config::read_env_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_LOKI_TENANT_ID",
                    "GRAPH_LOG_LOKI_TENANT",
                );

                Ok(LogStoreConfig::Loki {
                    endpoint,
                    tenant_id,
                })
            }

            "file" | "files" => {
                let directory = config::read_env_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_FILE_DIR",
                    "GRAPH_LOG_FILE_DIR",
                )
                .ok_or_else(|| {
                    LogStoreError::ConfigurationError(anyhow::anyhow!(
                        "File log directory not set. Use GRAPH_LOG_STORE_FILE_DIR environment variable"
                    ))
                })
                .map(PathBuf::from)?;

                let max_file_size = config::read_u64_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_FILE_MAX_SIZE",
                    "GRAPH_LOG_FILE_MAX_SIZE",
                    100 * 1024 * 1024, // 100MB default
                );

                let retention_days = config::read_u32_with_fallback(
                    &logger,
                    "GRAPH_LOG_STORE_FILE_RETENTION_DAYS",
                    "GRAPH_LOG_FILE_RETENTION_DAYS",
                    30,
                );

                Ok(LogStoreConfig::File {
                    directory,
                    max_file_size,
                    retention_days,
                })
            }

            _ => Err(LogStoreError::ConfigurationError(anyhow::anyhow!(
                "Unknown log store backend: {}. Valid options: disabled, elasticsearch, loki, file",
                backend
            ))),
        }
    }
}

/// A no-op LogStore that returns empty results
/// Used when logging is disabled
pub struct NoOpLogStore;

#[async_trait]
impl LogStore for NoOpLogStore {
    async fn query_logs(&self, _query: LogQuery) -> Result<Vec<LogEntry>, LogStoreError> {
        Ok(vec![])
    }

    fn is_available(&self) -> bool {
        false
    }
}
