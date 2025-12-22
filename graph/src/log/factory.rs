use std::sync::Arc;
use std::time::Duration;

use prometheus::Counter;
use slog::*;

use crate::components::log_store::LogStoreConfig;
use crate::components::metrics::MetricsRegistry;
use crate::components::store::DeploymentLocator;
use crate::log::elastic::*;
use crate::log::file::{file_logger, FileDrainConfig};
use crate::log::loki::{loki_logger, LokiDrainConfig};
use crate::log::split::*;
use crate::prelude::ENV_VARS;

/// Configuration for component-specific logging to Elasticsearch.
pub struct ElasticComponentLoggerConfig {
    pub index: String,
}

/// Configuration for component-specific logging.
pub struct ComponentLoggerConfig {
    pub elastic: Option<ElasticComponentLoggerConfig>,
}

/// Factory for creating component and subgraph loggers.
#[derive(Clone)]
pub struct LoggerFactory {
    parent: Logger,
    log_store_config: Option<LogStoreConfig>,
    metrics_registry: Arc<MetricsRegistry>,
}

impl LoggerFactory {
    /// Creates a new factory using a parent logger and optional log store configuration.
    pub fn new(
        logger: Logger,
        log_store_config: Option<LogStoreConfig>,
        metrics_registry: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            parent: logger,
            log_store_config,
            metrics_registry,
        }
    }

    /// Creates a new factory with a new parent logger.
    pub fn with_parent(&self, parent: Logger) -> Self {
        Self {
            parent,
            log_store_config: self.log_store_config.clone(),
            metrics_registry: self.metrics_registry.clone(),
        }
    }

    /// Creates a component-specific logger with optional Elasticsearch support.
    pub fn component_logger(
        &self,
        component: &str,
        config: Option<ComponentLoggerConfig>,
    ) -> Logger {
        let term_logger = self.parent.new(o!("component" => component.to_string()));

        match config {
            None => term_logger,
            Some(config) => match config.elastic {
                None => term_logger,
                Some(elastic_component_config) => {
                    // Check if we have Elasticsearch configured in log_store_config
                    match &self.log_store_config {
                        Some(LogStoreConfig::Elasticsearch {
                            endpoint,
                            username,
                            password,
                            ..
                        }) => {
                            // Build ElasticLoggingConfig on-demand
                            let elastic_config = ElasticLoggingConfig {
                                endpoint: endpoint.clone(),
                                username: username.clone(),
                                password: password.clone(),
                                client: reqwest::Client::new(),
                            };

                            split_logger(
                                term_logger.clone(),
                                elastic_logger(
                                    ElasticDrainConfig {
                                        general: elastic_config,
                                        index: elastic_component_config.index,
                                        custom_id_key: String::from("componentId"),
                                        custom_id_value: component.to_string(),
                                        flush_interval: ENV_VARS.elastic_search_flush_interval,
                                        max_retries: ENV_VARS.elastic_search_max_retries,
                                    },
                                    term_logger.clone(),
                                    self.logs_sent_counter(None),
                                ),
                            )
                        }
                        _ => {
                            // No Elasticsearch configured, just use terminal logger
                            term_logger
                        }
                    }
                }
            },
        }
    }

    /// Creates a subgraph logger with multi-backend support.
    pub fn subgraph_logger(&self, loc: &DeploymentLocator) -> Logger {
        let term_logger = self
            .parent
            .new(o!("subgraph_id" => loc.hash.to_string(), "sgd" => loc.id.to_string()));

        // Determine which drain to use based on log_store_config
        let drain = match &self.log_store_config {
            Some(LogStoreConfig::Elasticsearch {
                endpoint,
                username,
                password,
                index,
            }) => {
                // Build ElasticLoggingConfig on-demand
                let elastic_config = ElasticLoggingConfig {
                    endpoint: endpoint.clone(),
                    username: username.clone(),
                    password: password.clone(),
                    client: reqwest::Client::new(),
                };

                Some(elastic_logger(
                    ElasticDrainConfig {
                        general: elastic_config,
                        index: index.clone(),
                        custom_id_key: String::from("subgraphId"),
                        custom_id_value: loc.hash.to_string(),
                        flush_interval: ENV_VARS.elastic_search_flush_interval,
                        max_retries: ENV_VARS.elastic_search_max_retries,
                    },
                    term_logger.clone(),
                    self.logs_sent_counter(Some(loc.hash.as_str())),
                ))
            }

            None => None,

            Some(LogStoreConfig::Loki {
                endpoint,
                tenant_id,
            }) => {
                // Use Loki
                Some(loki_logger(
                    LokiDrainConfig {
                        endpoint: endpoint.clone(),
                        tenant_id: tenant_id.clone(),
                        flush_interval: Duration::from_secs(5),
                        subgraph_id: loc.hash.to_string(),
                    },
                    term_logger.clone(),
                ))
            }

            Some(LogStoreConfig::File {
                directory,
                max_file_size,
                retention_days,
            }) => {
                // Use File
                Some(file_logger(
                    FileDrainConfig {
                        directory: directory.clone(),
                        subgraph_id: loc.hash.to_string(),
                        max_file_size: *max_file_size,
                        retention_days: *retention_days,
                    },
                    term_logger.clone(),
                ))
            }

            Some(LogStoreConfig::Disabled) => None,
        };

        // Combine terminal and storage drain
        drain
            .map(|storage_drain| split_logger(term_logger.clone(), storage_drain))
            .unwrap_or(term_logger)
    }

    fn logs_sent_counter(&self, deployment: Option<&str>) -> Counter {
        self.metrics_registry
            .global_deployment_counter(
                "graph_elasticsearch_logs_sent",
                "Count of logs sent to Elasticsearch endpoint",
                deployment.unwrap_or(""),
            )
            .unwrap()
    }
}
