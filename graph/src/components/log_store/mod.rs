pub mod elasticsearch;

use async_trait::async_trait;
use thiserror::Error;

use crate::prelude::DeploymentHash;

#[derive(Error, Debug)]
pub enum LogStoreError {
    #[error("log store query failed: {0}")]
    QueryFailed(#[from] anyhow::Error),

    #[error("log store is unavailable")]
    Unavailable,
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

/// A no-op LogStore that returns empty results
/// Used when Elasticsearch is not configured
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
