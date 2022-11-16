use std::{collections::BTreeSet, sync::Arc};

use graph::{
    components::store::{SubscriptionManager, UnitStream},
    prelude::{StoreEventStreamBox, SubscriptionFilter},
    prometheus::Registry,
};
use graph_core::MetricsRegistry;

pub mod chain;
pub mod config;
pub mod opt;
pub mod store_builder;

pub struct MetricsContext {
    pub prometheus: Arc<Registry>,
    pub registry: Arc<MetricsRegistry>,
    pub prometheus_host: Option<String>,
    pub job_name: Option<String>,
}

/// A dummy subscription manager that always panics
pub struct PanicSubscriptionManager;

impl SubscriptionManager for PanicSubscriptionManager {
    fn subscribe(&self, _: BTreeSet<SubscriptionFilter>) -> StoreEventStreamBox {
        panic!("we were never meant to call `subscribe`");
    }

    fn subscribe_no_payload(&self, _: BTreeSet<SubscriptionFilter>) -> UnitStream {
        panic!("we were never meant to call `subscribe_no_payload`");
    }
}
