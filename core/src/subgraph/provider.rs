use std::collections::HashSet;
use std::ops::Deref as _;
use std::sync::Mutex;

use async_trait::async_trait;
use futures01::sync::mpsc::{channel, Receiver, Sender};

use graph::prelude::{
    DataSourceLoader as _, SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, *,
};

use crate::subgraph::registrar::IPFS_SUBGRAPH_LOADING_TIMEOUT;
use crate::DataSourceLoader;

pub struct SubgraphAssignmentProvider<L, S> {
    logger_factory: LoggerFactory,
    event_stream: Option<Receiver<SubgraphAssignmentProviderEvent>>,
    event_sink: Sender<SubgraphAssignmentProviderEvent>,
    resolver: Arc<L>,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphDeploymentId>>>,
    store: Arc<S>,
}

impl<L, S> SubgraphAssignmentProvider<L, S>
where
    L: LinkResolver + Clone,
    S: SubgraphStore,
{
    pub fn new(logger_factory: &LoggerFactory, resolver: Arc<L>, store: Arc<S>) -> Self {
        let (event_sink, event_stream) = channel(100);

        let logger = logger_factory.component_logger("SubgraphAssignmentProvider", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        // Create the subgraph provider
        SubgraphAssignmentProvider {
            logger_factory,
            event_stream: Some(event_stream),
            event_sink,
            resolver: Arc::new(
                resolver
                    .as_ref()
                    .clone()
                    .with_timeout(*IPFS_SUBGRAPH_LOADING_TIMEOUT)
                    .with_retries(),
            ),
            subgraphs_running: Arc::new(Mutex::new(HashSet::new())),
            store,
        }
    }
}

#[async_trait]
impl<L, S> SubgraphAssignmentProviderTrait for SubgraphAssignmentProvider<L, S>
where
    L: LinkResolver + Clone,
    S: SubgraphStore,
{
    async fn start(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        let loader = Arc::new(DataSourceLoader::new(self.store.clone()));

        let link = format!("/ipfs/{}", id);

        let logger = self.logger_factory.subgraph_logger(id);

        info!(logger, "Resolve subgraph files using IPFS");

        let mut subgraph = SubgraphManifest::resolve(Link { link }, self.resolver.deref(), &logger)
            .map_err(SubgraphAssignmentProviderError::ResolveError)
            .await?;

        let data_sources = loader
            .load_dynamic_data_sources(id.clone(), logger.clone(), subgraph.clone())
            .map_err(SubgraphAssignmentProviderError::DynamicDataSourcesError)
            .await?;

        info!(logger, "Successfully resolved subgraph files using IPFS");

        // Add dynamic data sources to the subgraph
        subgraph.data_sources.extend(data_sources);

        // If subgraph ID already in set
        if !self
            .subgraphs_running
            .lock()
            .unwrap()
            .insert(subgraph.id.clone())
        {
            info!(logger, "Subgraph deployment is already running");

            return Err(SubgraphAssignmentProviderError::AlreadyRunning(subgraph.id));
        }

        // Send events to trigger subgraph processing
        if let Err(e) = self
            .event_sink
            .clone()
            .send(SubgraphAssignmentProviderEvent::SubgraphStart(subgraph))
            .compat()
            .await
        {
            panic!("failed to forward subgraph: {}", e);
        }

        Ok(())
    }

    async fn stop(&self, id: SubgraphDeploymentId) -> Result<(), SubgraphAssignmentProviderError> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut down subgraph processing
            self.event_sink
                .clone()
                .send(SubgraphAssignmentProviderEvent::SubgraphStop(id))
                .map_err(|e| panic!("failed to forward subgraph shut down event: {}", e))
                .map(|_| ())
                .compat()
                .await
        } else {
            Err(SubgraphAssignmentProviderError::NotRunning(id))
        }
    }
}

impl<L, S> EventProducer<SubgraphAssignmentProviderEvent> for SubgraphAssignmentProvider<L, S> {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = SubgraphAssignmentProviderEvent, Error = ()> + Send>> {
        self.event_stream.take().map(|s| {
            Box::new(s)
                as Box<dyn Stream<Item = SubgraphAssignmentProviderEvent, Error = ()> + Send>
        })
    }
}
