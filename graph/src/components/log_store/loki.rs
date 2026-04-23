use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use slog::{Logger, warn};
use std::collections::HashMap;
use std::time::Duration;

use crate::prelude::DeploymentHash;

use super::{LogEntry, LogMeta, LogQuery, LogStore, LogStoreError};

pub struct LokiLogStore {
    endpoint: String,
    tenant_id: Option<String>,
    username: Option<String>,
    password: Option<String>,
    client: Client,
    logger: Logger,
}

impl LokiLogStore {
    pub fn new(
        endpoint: String,
        tenant_id: Option<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self, LogStoreError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| LogStoreError::InitializationFailed(e.into()))?;

        Ok(Self {
            endpoint,
            tenant_id,
            username,
            password,
            client,
            logger: crate::log::logger(false),
        })
    }

    fn build_logql_query(&self, query: &LogQuery) -> String {
        let mut selectors = vec![format!("subgraphId=\"{}\"", query.subgraph_id)];

        // Add log level selector if specified
        if let Some(level) = &query.level {
            selectors.push(format!("level=\"{}\"", level.as_str().to_ascii_lowercase()));
        }

        // Base selector
        let selector = format!("{{{}}}", selectors.join(","));

        // Add line filter for text search if specified
        if let Some(search) = &query.search {
            format!("{} |~ \"(?i){}\"", selector, regex::escape(search))
        } else {
            selector
        }
    }

    async fn execute_query(
        &self,
        query_str: &str,
        from: &str,
        to: &str,
        limit: u32,
        order_direction: super::OrderDirection,
    ) -> Result<Vec<LogEntry>, LogStoreError> {
        let url = format!("{}/loki/api/v1/query_range", self.endpoint);

        // Map order direction to Loki's direction parameter
        let direction = match order_direction {
            super::OrderDirection::Desc => "backward", // Most recent first
            super::OrderDirection::Asc => "forward",   // Oldest first
        };

        let mut request = self
            .client
            .get(&url)
            .query(&[
                ("query", query_str),
                ("start", from),
                ("end", to),
                ("limit", &limit.to_string()),
                ("direction", direction),
            ])
            .timeout(Duration::from_secs(10));

        // Add X-Scope-OrgID header for multi-tenancy if configured
        if let Some(tenant_id) = &self.tenant_id {
            request = request.header("X-Scope-OrgID", tenant_id);
        }

        // Add basic auth if configured
        if let Some(username) = &self.username {
            request = request.basic_auth(username, self.password.as_ref());
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
        let log_data: LokiLogDocument = match serde_json::from_str(&value.1) {
            Ok(doc) => doc,
            Err(e) => {
                warn!(self.logger, "Failed to parse Loki log entry"; "error" => e.to_string());
                return None;
            }
        };

        let level = match log_data.level.parse() {
            Ok(l) => l,
            Err(_) => {
                warn!(self.logger, "Invalid log level in Loki entry"; "level" => &log_data.level);
                return None;
            }
        };

        let subgraph_id = match DeploymentHash::new(&log_data.subgraph_id) {
            Ok(id) => id,
            Err(_) => {
                warn!(self.logger, "Invalid subgraph ID in Loki entry"; "subgraph_id" => &log_data.subgraph_id);
                return None;
            }
        };

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

        let mut entries = self
            .execute_query(&logql_query, from, to, limit, query.order_direction)
            .await?;

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
    // Part of Loki API response, required for deserialization
    #[allow(dead_code)]
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
    // Timestamp in nanoseconds since epoch (part of Loki API, not currently used)
    #[allow(dead_code)] String,
    // Log line (JSON document)
    String,
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
    use slog::Level;

    #[test]
    fn test_build_logql_query_basic() {
        let store =
            LokiLogStore::new("http://localhost:3100".to_string(), None, None, None).unwrap();
        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmTest").unwrap(),
            level: None,
            from: None,
            to: None,
            search: None,
            first: 100,
            skip: 0,
            order_direction: crate::components::log_store::OrderDirection::Desc,
        };

        let logql = store.build_logql_query(&query);
        assert_eq!(logql, "{subgraphId=\"QmTest\"}");
    }

    #[test]
    fn test_build_logql_query_with_level() {
        let store =
            LokiLogStore::new("http://localhost:3100".to_string(), None, None, None).unwrap();
        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmTest").unwrap(),
            level: Some(Level::Error),
            from: None,
            to: None,
            search: None,
            first: 100,
            skip: 0,
            order_direction: crate::components::log_store::OrderDirection::Desc,
        };

        let logql = store.build_logql_query(&query);
        assert_eq!(logql, "{subgraphId=\"QmTest\",level=\"error\"}");
    }

    #[test]
    fn test_build_logql_query_with_text_filter() {
        let store =
            LokiLogStore::new("http://localhost:3100".to_string(), None, None, None).unwrap();
        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmTest").unwrap(),
            level: None,
            from: None,
            to: None,
            search: Some("transaction failed".to_string()),
            first: 100,
            skip: 0,
            order_direction: crate::components::log_store::OrderDirection::Desc,
        };

        let logql = store.build_logql_query(&query);
        assert!(logql.contains("{subgraphId=\"QmTest\"}"));
        assert!(logql.contains("|~"));
        assert!(logql.contains("transaction failed"));
    }
}
