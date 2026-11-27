use std::{collections::HashMap, sync::Arc, time::Instant};

use graph::{
    cheap_clone::CheapClone as _,
    components::{
        metrics::subgraph::SubgraphCountMetric, store::DeploymentLocator,
        subgraph::SubgraphInstanceManager,
    },
    log::factory::LoggerFactory,
};
use parking_lot::RwLock;
use slog::debug;

/// Starts and stops subgraph deployments.
///
/// Forwards subgraph deployment start and stop requests to the subgraph instance manager.
pub struct SubgraphProvider {
    logger_factory: LoggerFactory,
    count_metrics: Arc<SubgraphCountMetric>,

    /// Handles the processing of started subgraph deployments.
    instance_manager: Arc<dyn SubgraphInstanceManager>,

    /// Maintains a list of started subgraphs.
    ///
    /// Used to ignore duplicate start requests and to stop active start request tasks.
    assignments: SubgraphAssignments,
}

impl SubgraphProvider {
    /// Creates a new subgraph provider.
    ///
    /// # Arguments
    /// - `logger_factory`: Creates loggers for each subgraph deployment start/stop request
    /// - `count_metrics`: Tracks the number of started subgraph deployments
    /// - `instance_manager`: Handles the processing of started subgraph deployments
    pub fn new(
        logger_factory: &LoggerFactory,
        count_metrics: Arc<SubgraphCountMetric>,
        instance_manager: Arc<dyn SubgraphInstanceManager>,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphProvider", None);
        let logger_factory = logger_factory.with_parent(logger.cheap_clone());

        debug!(logger, "Creating subgraph provider");

        Self {
            logger_factory,
            count_metrics,
            instance_manager,
            assignments: SubgraphAssignments::new(),
        }
    }
}

#[async_trait::async_trait]
impl SubgraphInstanceManager for SubgraphProvider {
    async fn start_subgraph(self: Arc<Self>, loc: DeploymentLocator, stop_block: Option<i32>) {
        let logger = self
            .logger_factory
            .subgraph_logger(&loc)
            .new(slog::o!("method" => "start_subgraph"));

        if self.assignments.is_assigned(&loc) {
            debug!(logger, "Subgraph is already started");
            return;
        }

        self.count_metrics.deployment_count.inc();

        let handle = tokio::spawn({
            let instance_manager = self.instance_manager.cheap_clone();
            let loc = loc.cheap_clone();
            let start_instant = Instant::now();

            async move {
                debug!(logger, "Starting subgraph");

                instance_manager.start_subgraph(loc, stop_block).await;

                debug!(logger, "Subgraph started";
                    "duration_ms" => start_instant.elapsed().as_millis()
                );
            }
        });

        self.assignments.add(loc, handle)
    }

    async fn stop_subgraph(&self, loc: DeploymentLocator) {
        let logger = self
            .logger_factory
            .subgraph_logger(&loc)
            .new(slog::o!("method" => "stop_subgraph"));

        debug!(logger, "Stopping subgraph");

        let Some(handle) = self.assignments.take(&loc) else {
            debug!(logger, "Subgraph is not started");
            return;
        };

        handle.abort();
        self.count_metrics.deployment_count.dec();

        self.instance_manager.stop_subgraph(loc).await;
        debug!(logger, "Subgraph stopped");
    }
}

/// Maintains a list of started subgraph deployments with the handles to their start request tasks.
struct SubgraphAssignments(RwLock<HashMap<DeploymentLocator, tokio::task::JoinHandle<()>>>);

impl SubgraphAssignments {
    /// Creates a new empty list of started subgraph deployments.
    fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Adds a new subgraph deployment to the list of started subgraph deployments.
    fn add(&self, loc: DeploymentLocator, handle: tokio::task::JoinHandle<()>) {
        self.0.write().insert(loc, handle);
    }

    /// Checks if the subgraph deployment is started.
    fn is_assigned(&self, loc: &DeploymentLocator) -> bool {
        self.0.read().contains_key(loc)
    }

    /// Removes the subgraph deployment from the list of started subgraph deployments and returns its start request task handle.
    fn take(&self, loc: &DeploymentLocator) -> Option<tokio::task::JoinHandle<()>> {
        self.0.write().remove(loc)
    }
}
