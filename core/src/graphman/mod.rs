use std::sync::Arc;

use graph::{prelude::MetricsRegistry, prometheus::Registry};

pub mod chain;
pub mod config;
pub mod context;
pub mod core;
pub mod deployment;
pub mod shard;
pub mod store_builder;
pub mod utils;

pub struct MetricsContext {
    pub prometheus: Arc<Registry>,
    pub registry: Arc<MetricsRegistry>,
    pub prometheus_host: Option<String>,
    pub job_name: Option<String>,
}
