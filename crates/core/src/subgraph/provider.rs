use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;

use graph::{
    components::store::{DeploymentId, DeploymentLocator},
    prelude::{SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, *},
};

pub struct SubgraphAssignmentProvider<I> {
    logger_factory: LoggerFactory,
    subgraphs_running: Arc<Mutex<HashSet<DeploymentId>>>,
    link_resolver: Arc<dyn LinkResolver>,
    instance_manager: Arc<I>,
}

impl<I: SubgraphInstanceManager> SubgraphAssignmentProvider<I> {
    pub fn new(
        logger_factory: &LoggerFactory,
        link_resolver: Arc<dyn LinkResolver>,
        instance_manager: I,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphAssignmentProvider", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        // Create the subgraph provider
        SubgraphAssignmentProvider {
            logger_factory,
            subgraphs_running: Arc::new(Mutex::new(HashSet::new())),
            link_resolver: link_resolver.with_retries().into(),
            instance_manager: Arc::new(instance_manager),
        }
    }
}

#[async_trait]
impl<I: SubgraphInstanceManager> SubgraphAssignmentProviderTrait for SubgraphAssignmentProvider<I> {
    async fn start(
        &self,
        loc: DeploymentLocator,
        stop_block: Option<BlockNumber>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        let logger = self.logger_factory.subgraph_logger(&loc);

        // If subgraph ID already in set
        if !self.subgraphs_running.lock().unwrap().insert(loc.id) {
            info!(logger, "Subgraph deployment is already running");

            return Err(SubgraphAssignmentProviderError::AlreadyRunning(
                loc.hash.clone(),
            ));
        }

        let file_bytes = self
            .link_resolver
            .cat(&logger, &loc.hash.to_ipfs_link())
            .await
            .map_err(SubgraphAssignmentProviderError::ResolveError)?;

        let raw: serde_yaml::Mapping = serde_yaml::from_slice(&file_bytes)
            .map_err(|e| SubgraphAssignmentProviderError::ResolveError(e.into()))?;

        self.instance_manager
            .cheap_clone()
            .start_subgraph(loc, raw, stop_block)
            .await;

        Ok(())
    }

    async fn stop(
        &self,
        deployment: DeploymentLocator,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        // If subgraph ID was in set
        if self
            .subgraphs_running
            .lock()
            .unwrap()
            .remove(&deployment.id)
        {
            // Shut down subgraph processing
            self.instance_manager.stop_subgraph(deployment).await;
            Ok(())
        } else {
            Err(SubgraphAssignmentProviderError::NotRunning(deployment))
        }
    }
}
