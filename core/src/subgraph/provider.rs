use std::collections::HashSet;
use std::ops::Deref as _;
use std::sync::Mutex;

use async_trait::async_trait;
use futures01::sync::mpsc::{channel, Receiver, Sender};

use graph::data::subgraph::schema::SubgraphError;
use graph::prelude::{
    DataSourceLoader as _, GraphQlRunner,
    SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, *,
};

use crate::subgraph::registrar::IPFS_SUBGRAPH_LOADING_TIMEOUT;
use crate::DataSourceLoader;

pub struct SubgraphAssignmentProvider<L, Q, S> {
    logger_factory: LoggerFactory,
    event_stream: Option<Receiver<SubgraphAssignmentProviderEvent>>,
    event_sink: Sender<SubgraphAssignmentProviderEvent>,
    resolver: Arc<L>,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphDeploymentId>>>,
    store: Arc<S>,
    graphql_runner: Arc<Q>,
}

impl<L, Q, S> SubgraphAssignmentProvider<L, Q, S>
where
    L: LinkResolver + Clone,
    Q: GraphQlRunner,
    S: Store,
{
    pub fn new(
        logger_factory: &LoggerFactory,
        resolver: Arc<L>,
        store: Arc<S>,
        graphql_runner: Arc<Q>,
    ) -> Self {
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
            graphql_runner,
        }
    }

    /// Clones but forcing receivers to `None`.
    fn clone_no_receivers(&self) -> Self {
        SubgraphAssignmentProvider {
            event_stream: None,
            event_sink: self.event_sink.clone(),
            resolver: self.resolver.clone(),
            subgraphs_running: self.subgraphs_running.clone(),
            store: self.store.clone(),
            graphql_runner: self.graphql_runner.clone(),
            logger_factory: self.logger_factory.clone(),
        }
    }
}

#[async_trait]
impl<L, Q, S> SubgraphAssignmentProviderTrait for SubgraphAssignmentProvider<L, Q, S>
where
    L: LinkResolver + Clone,
    Q: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    async fn start(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        let self_clone = self.clone_no_receivers();
        let store = self.store.clone();
        let subgraph_id = id.clone();

        let loader = Arc::new(DataSourceLoader::new(
            store.clone(),
            self.resolver.clone(),
            self.graphql_runner.clone(),
        ));

        let link = format!("/ipfs/{}", id);

        let logger = self.logger_factory.subgraph_logger(id);
        let logger_for_resolve = logger.clone();
        let logger_for_err = logger.clone();

        info!(logger, "Resolve subgraph files using IPFS");

        if let Err(e) = async move {
            let mut subgraph = SubgraphManifest::resolve(
                Link { link },
                self.resolver.deref(),
                &logger_for_resolve,
            )
            .map_err(SubgraphAssignmentProviderError::ResolveError)
            .await?;

            let data_sources = loader
                .load_dynamic_data_sources(id.clone(), logger.clone())
                .map_err(SubgraphAssignmentProviderError::DynamicDataSourcesError)
                .await?;

            info!(logger, "Successfully resolved subgraph files using IPFS");

            // Add dynamic data sources to the subgraph
            subgraph.data_sources.extend(data_sources);

            // If subgraph ID already in set
            if !self_clone
                .subgraphs_running
                .lock()
                .unwrap()
                .insert(subgraph.id.clone())
            {
                info!(logger, "Subgraph deployment is already running");

                return Err(SubgraphAssignmentProviderError::AlreadyRunning(subgraph.id));
            }

            // Send events to trigger subgraph processing
            if let Err(e) = self_clone
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
        .await
        {
            error!(
                logger_for_err,
                "Failed to resolve subgraph files using IPFS";
                "error" => format!("{}", e)
            );

            let error = SubgraphError {
                subgraph_id: subgraph_id.clone(),
                message: e.to_string(),
                block_ptr: None,
                handler: None,
                deterministic: false,
            };

            let _ignore_error = store.fail_subgraph(subgraph_id, error).await;
            return Err(e);
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

impl<L, Q, S> EventProducer<SubgraphAssignmentProviderEvent>
    for SubgraphAssignmentProvider<L, Q, S>
{
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = SubgraphAssignmentProviderEvent, Error = ()> + Send>> {
        self.event_stream.take().map(|s| {
            Box::new(s)
                as Box<dyn Stream<Item = SubgraphAssignmentProviderEvent, Error = ()> + Send>
        })
    }
}
