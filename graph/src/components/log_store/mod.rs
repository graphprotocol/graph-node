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
    pub fn from_env() -> Result<LogStoreConfig, LogStoreError> {
        let backend = std::env::var("GRAPH_LOG_STORE").unwrap_or_else(|_| "disabled".to_string());

        match backend.to_lowercase().as_str() {
            "disabled" | "none" => Ok(LogStoreConfig::Disabled),

            "elasticsearch" | "elastic" | "es" => {
                let endpoint = std::env::var("GRAPH_ELASTICSEARCH_URL").map_err(|e| {
                    LogStoreError::ConfigurationError(anyhow::anyhow!(
                        "GRAPH_ELASTICSEARCH_URL not set: {}",
                        e
                    ))
                })?;

                Ok(LogStoreConfig::Elasticsearch {
                    endpoint,
                    username: std::env::var("GRAPH_ELASTICSEARCH_USER").ok(),
                    password: std::env::var("GRAPH_ELASTICSEARCH_PASSWORD").ok(),
                    index: std::env::var("GRAPH_ELASTIC_SEARCH_INDEX")
                        .unwrap_or_else(|_| "subgraph".to_string()),
                })
            }

            "loki" => {
                let endpoint = std::env::var("GRAPH_LOG_LOKI_ENDPOINT").map_err(|e| {
                    LogStoreError::ConfigurationError(anyhow::anyhow!(
                        "GRAPH_LOG_LOKI_ENDPOINT not set: {}",
                        e
                    ))
                })?;

                Ok(LogStoreConfig::Loki {
                    endpoint,
                    tenant_id: std::env::var("GRAPH_LOG_LOKI_TENANT").ok(),
                })
            }

            "file" | "files" => {
                let directory = std::env::var("GRAPH_LOG_FILE_DIR")
                    .map(PathBuf::from)
                    .map_err(|e| {
                        LogStoreError::ConfigurationError(anyhow::anyhow!(
                            "GRAPH_LOG_FILE_DIR not set: {}",
                            e
                        ))
                    })?;

                let max_file_size = std::env::var("GRAPH_LOG_FILE_MAX_SIZE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(100 * 1024 * 1024); // 100MB default

                let retention_days = std::env::var("GRAPH_LOG_FILE_RETENTION_DAYS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(30);

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
