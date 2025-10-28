use std::sync::Arc;

use graph::{
    cheap_clone::CheapClone,
    components::{
        metrics::{stopwatch::StopwatchMetrics, MetricsRegistry},
        store::WritableStore,
    },
    data::subgraph::DeploymentHash,
};
use slog::Logger;

/// Contains deployment specific metrics.
pub(super) struct Metrics {
    pub(super) stopwatch: StopwatchMetrics,
}

impl Metrics {
    /// Creates new deployment specific metrics.
    pub(super) fn new(
        logger: &Logger,
        metrics_registry: Arc<MetricsRegistry>,
        store: Arc<dyn WritableStore>,
        deployment: DeploymentHash,
    ) -> Self {
        let stopwatch = StopwatchMetrics::new(
            logger.cheap_clone(),
            deployment,
            "nozzle-process",
            metrics_registry,
            store.shard().to_string(),
        );

        Self { stopwatch }
    }
}
