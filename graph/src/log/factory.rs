use std::time::Duration;

use crate::components::store::DeploymentLocator;
use crate::log::elastic::*;
use crate::log::split::*;
use slog::*;

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
    elastic_config: Option<ElasticLoggingConfig>,
}

impl LoggerFactory {
    /// Creates a new factory using a parent logger and optional Elasticsearch configuration.
    pub fn new(logger: Logger, elastic_config: Option<ElasticLoggingConfig>) -> Self {
        Self {
            parent: logger,
            elastic_config,
        }
    }

    /// Creates a new factory with a new parent logger.
    pub fn with_parent(&self, parent: Logger) -> Self {
        Self {
            parent,
            elastic_config: self.elastic_config.clone(),
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
                Some(config) => self
                    .elastic_config
                    .clone()
                    .map(|elastic_config| {
                        split_logger(
                            term_logger.clone(),
                            elastic_logger(
                                ElasticDrainConfig {
                                    general: elastic_config,
                                    index: config.index,
                                    document_type: String::from("log"),
                                    custom_id_key: String::from("componentId"),
                                    custom_id_value: component.to_string(),
                                    flush_interval: Duration::from_secs(5),
                                },
                                term_logger.clone(),
                            ),
                        )
                    })
                    .unwrap_or(term_logger),
            },
        }
    }

    /// Creates a subgraph logger with Elasticsearch support.
    pub fn subgraph_logger(&self, loc: &DeploymentLocator) -> Logger {
        let term_logger = self
            .parent
            .new(o!("subgraph_id" => loc.hash.to_string(), "sgd" => loc.id.to_string()));

        self.elastic_config
            .clone()
            .map(|elastic_config| {
                split_logger(
                    term_logger.clone(),
                    elastic_logger(
                        ElasticDrainConfig {
                            general: elastic_config,
                            index: String::from("subgraph-logs"),
                            document_type: String::from("log"),
                            custom_id_key: String::from("subgraphId"),
                            custom_id_value: loc.hash.to_string(),
                            flush_interval: Duration::from_secs(5),
                        },
                        term_logger.clone(),
                    ),
                )
            })
            .unwrap_or(term_logger)
    }
}
