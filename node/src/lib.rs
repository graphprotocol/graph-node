use std::sync::Arc;

use graph::prometheus::Registry;
use graph_core::MetricsRegistry;

#[macro_use]
extern crate diesel;

pub mod chain;
pub mod config;
pub mod opt;
pub mod store_builder;

pub mod manager;

mod env_vars;
pub use env_vars::{EnvVars, ENV_VARS};

pub struct MetricsContext {
    pub prometheus: Arc<Registry>,
    pub registry: Arc<MetricsRegistry>,
    pub prometheus_host: Option<String>,
    pub job_name: Option<String>,
}
