use std::sync::Mutex;
use std::{collections::HashSet, time::Instant};

use async_trait::async_trait;

use graph::{
    components::store::{DeploymentId, DeploymentLocator},
    prelude::{SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, *},
};

#[derive(Debug)]
struct DeploymentRegistry {
    subgraphs_deployed: Arc<Mutex<HashSet<DeploymentId>>>,
    subgraph_metrics: Arc<SubgraphCountMetric>,
}

impl DeploymentRegistry {
    fn new(subgraph_metrics: Arc<SubgraphCountMetric>) -> Self {
        Self {
            subgraphs_deployed: Arc::new(Mutex::new(HashSet::new())),
            subgraph_metrics,
        }
    }

    fn insert(&self, id: DeploymentId) -> bool {
        if !self.subgraphs_deployed.lock().unwrap().insert(id) {
            return false;
        }

        self.subgraph_metrics.deployment_count.inc();
        true
    }

    fn remove(&self, id: &DeploymentId) -> bool {
        if !self.subgraphs_deployed.lock().unwrap().remove(id) {
            return false;
        }

        self.subgraph_metrics.deployment_count.dec();
        true
    }
}

pub struct SubgraphAssignmentProvider<I> {
    logger_factory: LoggerFactory,
    deployment_registry: DeploymentRegistry,
    instance_manager: Arc<I>,
}

impl<I: SubgraphInstanceManager> SubgraphAssignmentProvider<I> {
    pub fn new(
        logger_factory: &LoggerFactory,
        instance_manager: I,
        subgraph_metrics: Arc<SubgraphCountMetric>,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphAssignmentProvider", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        // Create the subgraph provider
        SubgraphAssignmentProvider {
            logger_factory,
            instance_manager: Arc::new(instance_manager),
            deployment_registry: DeploymentRegistry::new(subgraph_metrics),
        }
    }
}

#[async_trait]
impl<I: SubgraphInstanceManager> SubgraphAssignmentProviderTrait for SubgraphAssignmentProvider<I> {
    async fn start(&self, loc: DeploymentLocator, stop_block: Option<BlockNumber>) {
        let logger = self.logger_factory.subgraph_logger(&loc);

        // If subgraph ID already in set
        if !self.deployment_registry.insert(loc.id) {
            info!(logger, "Subgraph deployment is already running");

            return;
        }

        let start_time = Instant::now();

        self.instance_manager
            .cheap_clone()
            .start_subgraph(loc, stop_block)
            .await;

        debug!(
            logger,
            "Subgraph started";
            "start_ms" => start_time.elapsed().as_millis()
        );
    }

    async fn stop(&self, deployment: DeploymentLocator) {
        // If subgraph ID was in set
        if self.deployment_registry.remove(&deployment.id) {
            // Shut down subgraph processing
            self.instance_manager.stop_subgraph(deployment).await;
        }
    }
}
