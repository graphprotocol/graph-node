use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

use crate::log::elastic::ElasticLoggingConfig;
use crate::prelude::DeploymentHash;

use super::{LogEntry, LogLevel, LogMeta, LogQuery, LogStore, LogStoreError};

pub struct ElasticsearchLogStore {
    endpoint: String,
    username: Option<String>,
    password: Option<String>,
    client: Client,
    index: String,
    timeout: Duration,
}

impl ElasticsearchLogStore {
    pub fn new(config: ElasticLoggingConfig, index: String, timeout: Duration) -> Self {
        Self {
            endpoint: config.endpoint,
            username: config.username,
            password: config.password,
            client: config.client,
            index,
            timeout,
        }
    }

    fn build_query(&self, query: &LogQuery) -> serde_json::Value {
        let mut must_clauses = Vec::new();

        // Filter by subgraph ID
        must_clauses.push(json!({
            "term": {
                "subgraphId": query.subgraph_id.to_string()
            }
        }));

        // Filter by log level
        if let Some(level) = &query.level {
            must_clauses.push(json!({
                "term": {
                    "level": level.as_str()
                }
            }));
        }

        // Filter by time range
        if query.from.is_some() || query.to.is_some() {
            let mut range = serde_json::Map::new();
            if let Some(from) = &query.from {
                range.insert("gte".to_string(), json!(from));
            }
            if let Some(to) = &query.to {
                range.insert("lte".to_string(), json!(to));
            }
            must_clauses.push(json!({
                "range": {
                    "timestamp": range
                }
            }));
        }

        // Filter by text search
        if let Some(search) = &query.search {
            must_clauses.push(json!({
                "match": {
                    "text": search
                }
            }));
        }

        json!({
            "query": {
                "bool": {
                    "must": must_clauses
                }
            },
            "from": query.skip,
            "size": query.first,
            "sort": [
                { "timestamp": { "order": "desc" } }
            ]
        })
    }

    async fn execute_search(
        &self,
        query_body: serde_json::Value,
    ) -> Result<Vec<LogEntry>, LogStoreError> {
        let url = format!("{}/{}/_search", self.endpoint, self.index);

        let mut request = self
            .client
            .post(&url)
            .json(&query_body)
            .timeout(self.timeout);

        // Add basic auth if credentials provided
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            request = request.basic_auth(username, Some(password));
        }

        let response = request.send().await.map_err(|e| {
            LogStoreError::QueryFailed(
                anyhow::Error::from(e).context("Elasticsearch request failed"),
            )
        })?;

        if !response.status().is_success() {
            let status = response.status();
            // Include response body in error context for debugging
            // The body is part of the error chain but not the main error message to avoid
            // leaking sensitive Elasticsearch internals in logs
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read response body>".to_string());
            return Err(LogStoreError::QueryFailed(
                anyhow::anyhow!("Elasticsearch query failed with status {}", status)
                    .context(format!("Response body: {}", body_text)),
            ));
        }

        let response_body: ElasticsearchResponse = response.json().await.map_err(|e| {
            LogStoreError::QueryFailed(
                anyhow::Error::from(e).context(
                    "failed to parse Elasticsearch search response: response format may have changed or be invalid",
                ),
            )
        })?;

        let entries = response_body
            .hits
            .hits
            .into_iter()
            .filter_map(|hit| self.parse_log_entry(hit.source))
            .collect();

        Ok(entries)
    }

    fn parse_log_entry(&self, source: ElasticsearchLogDocument) -> Option<LogEntry> {
        let level = LogLevel::from_str(&source.level)?;
        let subgraph_id = DeploymentHash::new(&source.subgraph_id).ok()?;

        // Convert arguments HashMap to Vec<(String, String)>
        let arguments: Vec<(String, String)> = source.arguments.into_iter().collect();

        Some(LogEntry {
            id: source.id,
            subgraph_id,
            timestamp: source.timestamp,
            level,
            text: source.text,
            arguments,
            meta: LogMeta {
                module: source.meta.module,
                line: source.meta.line,
                column: source.meta.column,
            },
        })
    }
}

#[async_trait]
impl LogStore for ElasticsearchLogStore {
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>, LogStoreError> {
        let query_body = self.build_query(&query);
        self.execute_search(query_body).await
    }

    fn is_available(&self) -> bool {
        true
    }
}

// Elasticsearch response types
#[derive(Debug, Deserialize)]
struct ElasticsearchResponse {
    hits: ElasticsearchHits,
}

#[derive(Debug, Deserialize)]
struct ElasticsearchHits {
    hits: Vec<ElasticsearchHit>,
}

#[derive(Debug, Deserialize)]
struct ElasticsearchHit {
    #[serde(rename = "_source")]
    source: ElasticsearchLogDocument,
}

#[derive(Debug, Deserialize)]
struct ElasticsearchLogDocument {
    id: String,
    #[serde(rename = "subgraphId")]
    subgraph_id: String,
    timestamp: String,
    level: String,
    text: String,
    arguments: HashMap<String, String>,
    meta: ElasticsearchLogMeta,
}

#[derive(Debug, Deserialize)]
struct ElasticsearchLogMeta {
    module: String,
    line: i64,
    column: i64,
}
