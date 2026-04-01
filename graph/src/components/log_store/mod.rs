pub mod elasticsearch;
pub mod file;
pub mod loki;

use async_trait::async_trait;
use slog::Level;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
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
        timeout_secs: u64,
    },

    /// Loki (Grafana's log aggregation system)
    Loki {
        endpoint: String,
        tenant_id: Option<String>,
        username: Option<String>,
        password: Option<String>,
    },

    /// File-based logs (JSON lines format)
    File {
        directory: PathBuf,
        retention_hours: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl OrderDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderDirection::Asc => "asc",
            OrderDirection::Desc => "desc",
        }
    }
}

impl FromStr for OrderDirection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "asc" | "ascending" => Ok(OrderDirection::Asc),
            "desc" | "descending" => Ok(OrderDirection::Desc),
            _ => Err(format!("Invalid order direction: {}", s)),
        }
    }
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
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
    pub level: Level,
    pub text: String,
    pub arguments: Vec<(String, String)>,
    pub meta: LogMeta,
}

#[derive(Debug, Clone)]
pub struct LogQuery {
    pub subgraph_id: DeploymentHash,
    pub level: Option<Level>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub search: Option<String>,
    pub first: u32,
    pub skip: u32,
    pub order_direction: OrderDirection,
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
                timeout_secs,
            } => {
                let timeout = std::time::Duration::from_secs(timeout_secs);
                let client = reqwest::Client::builder()
                    .timeout(timeout)
                    .build()
                    .map_err(|e| LogStoreError::InitializationFailed(e.into()))?;

                let config = crate::log::elastic::ElasticLoggingConfig {
                    endpoint,
                    username,
                    password,
                    client,
                };

                Ok(Arc::new(elasticsearch::ElasticsearchLogStore::new(
                    config, index, timeout,
                )))
            }

            LogStoreConfig::Loki {
                endpoint,
                tenant_id,
                username,
                password,
            } => Ok(Arc::new(loki::LokiLogStore::new(
                endpoint, tenant_id, username, password,
            )?)),

            LogStoreConfig::File {
                directory,
                retention_hours,
            } => Ok(Arc::new(file::FileLogStore::new(
                directory,
                retention_hours,
            )?)),
        }
    }
}

/// A no-op LogStore that returns empty results.
///
/// Used when log storage is disabled (the default). Note that subgraph logs
/// still appear in stdout/stderr - they're just not stored in a queryable format.
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
