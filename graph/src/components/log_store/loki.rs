use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

use crate::prelude::DeploymentHash;

use super::{LogEntry, LogLevel, LogMeta, LogQuery, LogStore, LogStoreError};

pub struct LokiLogStore {
    endpoint: String,
    tenant_id: Option<String>,
    client: Client,
}

impl LokiLogStore {
    pub fn new(endpoint: String, tenant_id: Option<String>) -> Result<Self, LogStoreError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| LogStoreError::InitializationFailed(e.into()))?;

        Ok(Self {
            endpoint,
            tenant_id,
            client,
        })
    }

    fn build_logql_query(&self, query: &LogQuery) -> String {
        let mut selectors = vec![format!("subgraphId=\"{}\"", query.subgraph_id)];

        // Add log level selector if specified
        if let Some(level) = &query.level {
            selectors.push(format!("level=\"{}\"", level.as_str()));
        }

        // Base selector
        let selector = format!("{{{}}}", selectors.join(","));

        // Add line filter for text search if specified
        let query_str = if let Some(text) = &query.text {
            format!("{} |~ \"(?i){}\"", selector, regex::escape(text))
        } else {
            selector
        };

        query_str
    }

    async fn execute_query(
        &self,
        query_str: &str,
        from: &str,
        to: &str,
        limit: u32,
    ) -> Result<Vec<LogEntry>, LogStoreError> {
        let url = format!("{}/loki/api/v1/query_range", self.endpoint);

        let mut request = self
            .client
            .get(&url)
            .query(&[
                ("query", query_str),
                ("start", from),
                ("end", to),
                ("limit", &limit.to_string()),
                ("direction", "backward"), // Most recent first
            ])
            .timeout(Duration::from_secs(10));

        // Add X-Scope-OrgID header for multi-tenancy if configured
        if let Some(tenant_id) = &self.tenant_id {
            request = request.header("X-Scope-OrgID", tenant_id);
        }

        let response = request.send().await.map_err(|e| {
            LogStoreError::QueryFailed(anyhow::Error::from(e).context("Loki request failed"))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(LogStoreError::QueryFailed(anyhow::anyhow!(
                "Loki query failed with status {}",
                status
            )));
        }

        let response_body: LokiResponse = response.json().await.map_err(|e| {
            LogStoreError::QueryFailed(
                anyhow::Error::from(e)
                    .context("failed to parse Loki response: response format may have changed"),
            )
        })?;

        if response_body.status != "success" {
            return Err(LogStoreError::QueryFailed(anyhow::anyhow!(
                "Loki query failed with status: {}",
                response_body.status
            )));
        }

        // Parse results
        let entries = response_body
            .data
            .result
            .into_iter()
            .flat_map(|stream| {
                let stream_labels = stream.stream; // Take ownership
                stream
                    .values
                    .into_iter()
                    .filter_map(move |value| self.parse_log_entry(value, &stream_labels))
            })
            .collect();

        Ok(entries)
    }

    fn parse_log_entry(
        &self,
        value: LokiValue,
        _labels: &HashMap<String, String>,
    ) -> Option<LogEntry> {
        // value is [timestamp_ns, log_line]
        // We expect the log line to be JSON with our log entry structure
        let log_data: LokiLogDocument = serde_json::from_str(&value.1).ok()?;

        let level = LogLevel::from_str(&log_data.level)?;
        let subgraph_id = DeploymentHash::new(&log_data.subgraph_id).ok()?;

        Some(LogEntry {
            id: log_data.id,
            subgraph_id,
            timestamp: log_data.timestamp,
            level,
            text: log_data.text,
            arguments: log_data.arguments.into_iter().collect(),
            meta: LogMeta {
                module: log_data.meta.module,
                line: log_data.meta.line,
                column: log_data.meta.column,
            },
        })
    }
}

#[async_trait]
impl LogStore for LokiLogStore {
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>, LogStoreError> {
        let logql_query = self.build_logql_query(&query);

        // Calculate time range
        let from = query.from.as_deref().unwrap_or("now-1h");
        let to = query.to.as_deref().unwrap_or("now");

        // Execute query with limit + skip to handle pagination
        let limit = query.first + query.skip;

        let mut entries = self.execute_query(&logql_query, from, to, limit).await?;

        // Apply skip/first pagination
        if query.skip > 0 {
            entries = entries.into_iter().skip(query.skip as usize).collect();
        }
        entries.truncate(query.first as usize);

        Ok(entries)
    }

    fn is_available(&self) -> bool {
        true
    }
}

// Loki response types
#[derive(Debug, Deserialize)]
struct LokiResponse {
    status: String,
    data: LokiData,
}

#[derive(Debug, Deserialize)]
struct LokiData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<LokiStream>,
}

#[derive(Debug, Deserialize)]
struct LokiStream {
    stream: HashMap<String, String>, // Labels
    values: Vec<LokiValue>,
}

#[derive(Debug, Deserialize)]
struct LokiValue(
    String, // timestamp (nanoseconds since epoch as string)
    String, // log line
);

#[derive(Debug, Deserialize)]
struct LokiLogDocument {
    id: String,
    #[serde(rename = "subgraphId")]
    subgraph_id: String,
    timestamp: String,
    level: String,
    text: String,
    arguments: HashMap<String, String>,
    meta: LokiLogMeta,
}

#[derive(Debug, Deserialize)]
struct LokiLogMeta {
    module: String,
    line: i64,
    column: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_logql_query_basic() {
        let store = LokiLogStore::new("http://localhost:3100".to_string(), None).unwrap();
        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmTest").unwrap(),
            level: None,
            from: None,
            to: None,
            text: None,
            first: 100,
            skip: 0,
        };

        let logql = store.build_logql_query(&query);
        assert_eq!(logql, "{subgraphId=\"QmTest\"}");
    }

    #[test]
    fn test_build_logql_query_with_level() {
        let store = LokiLogStore::new("http://localhost:3100".to_string(), None).unwrap();
        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmTest").unwrap(),
            level: Some(LogLevel::Error),
            from: None,
            to: None,
            text: None,
            first: 100,
            skip: 0,
        };

        let logql = store.build_logql_query(&query);
        assert_eq!(logql, "{subgraphId=\"QmTest\",level=\"error\"}");
    }

    #[test]
    fn test_build_logql_query_with_text_filter() {
        let store = LokiLogStore::new("http://localhost:3100".to_string(), None).unwrap();
        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmTest").unwrap(),
            level: None,
            from: None,
            to: None,
            text: Some("transaction failed".to_string()),
            first: 100,
            skip: 0,
        };

        let logql = store.build_logql_query(&query);
        assert!(logql.contains("{subgraphId=\"QmTest\"}"));
        assert!(logql.contains("|~"));
        assert!(logql.contains("transaction failed"));
    }
}
