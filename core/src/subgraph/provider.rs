use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;

use graph::prelude::{SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, *};

pub struct SubgraphAssignmentProvider<L, I> {
    logger_factory: LoggerFactory,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphDeploymentId>>>,
    link_resolver: Arc<L>,
    instance_manager: Arc<I>,
}

impl<L, I> SubgraphAssignmentProvider<L, I>
where
    L: LinkResolver,
    I: SubgraphInstanceManager,
{
    pub fn new(logger_factory: &LoggerFactory, link_resolver: Arc<L>, instance_manager: I) -> Self {
        let logger = logger_factory.component_logger("SubgraphAssignmentProvider", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        // Create the subgraph provider
        SubgraphAssignmentProvider {
            logger_factory,
            subgraphs_running: Arc::new(Mutex::new(HashSet::new())),
            link_resolver,
            instance_manager: Arc::new(instance_manager),
        }
    }
}

#[async_trait]
impl<L, I> SubgraphAssignmentProviderTrait for SubgraphAssignmentProvider<L, I>
where
    L: LinkResolver,
    I: SubgraphInstanceManager,
{
    async fn start(&self, id: SubgraphDeploymentId) -> Result<(), SubgraphAssignmentProviderError> {
        let logger = self.logger_factory.subgraph_logger(&id);

        // If subgraph ID already in set
        if !self.subgraphs_running.lock().unwrap().insert(id.clone()) {
            info!(logger, "Subgraph deployment is already running");

            return Err(SubgraphAssignmentProviderError::AlreadyRunning(id.clone()));
        }

        let file_bytes = self
            .link_resolver
            .cat(&logger, &id.to_ipfs_link())
            .await
            .map_err(SubgraphAssignmentProviderError::ResolveError)?;

        let raw: serde_yaml::Mapping = serde_yaml::from_slice(&file_bytes)
            .map_err(|e| SubgraphAssignmentProviderError::ResolveError(e.into()))?;

        self.instance_manager
            .cheap_clone()
            .start_subgraph(id, raw)
            .await;

        Ok(())
    }

    async fn stop(&self, id: SubgraphDeploymentId) -> Result<(), SubgraphAssignmentProviderError> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut down subgraph processing
            self.instance_manager.stop_subgraph(id);
            Ok(())
        } else {
            Err(SubgraphAssignmentProviderError::NotRunning(id))
        }
    }
}
