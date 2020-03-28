use std::collections::HashSet;
use std::ops::Deref as _;
use std::sync::Mutex;

use async_trait::async_trait;
use futures01::sync::mpsc::{channel, Receiver, Sender};

use graph::data::subgraph::schema::attribute_index_definitions;
use graph::prelude::{
    DataSourceLoader as _, DeploymentController as DeploymentManagerTrait, GraphQlRunner, *,
};

use crate::subgraph::registrar::IPFS_SUBGRAPH_LOADING_TIMEOUT;
use crate::DataSourceLoader;

pub struct DeploymentController<L, Q, S> {
    logger_factory: LoggerFactory,
    event_stream: Option<Receiver<DeploymentControllerEvent>>,
    event_sink: Sender<DeploymentControllerEvent>,
    resolver: Arc<L>,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphDeploymentId>>>,
    store: Arc<S>,
    graphql_runner: Arc<Q>,
}

impl<L, Q, S> DeploymentController<L, Q, S>
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

        let logger = logger_factory.component_logger("DeploymentController", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        // Create the subgraph provider
        DeploymentController {
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
        DeploymentController {
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
impl<L, Q, S> DeploymentManagerTrait for DeploymentController<L, Q, S>
where
    L: LinkResolver + Clone,
    Q: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    async fn start(&self, id: &SubgraphDeploymentId) -> Result<(), DeploymentControllerError> {
        let self_clone = self.clone_no_receivers();
        let store = self.store.clone();
        let subgraph_id = id.clone();
        let logger = self.logger_factory.subgraph_logger(id);
        let logger_for_error = logger.clone();
        let loader = Arc::new(DataSourceLoader::new(
            store.clone(),
            self.resolver.clone(),
            self.graphql_runner.clone(),
        ));

        async move {
            let link = format!("/ipfs/{}", id);

            info!(logger, "Resolve subgraph files using IPFS");

            let mut subgraph =
                SubgraphManifest::resolve(Link { link }, self.resolver.deref(), &logger)
                    .map_err(DeploymentControllerError::ResolveError)
                    .await?;

            let data_sources = loader
                .load_dynamic_data_sources(id.clone(), logger.clone())
                .map_err(DeploymentControllerError::DynamicDataSourcesError)
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
                return Ok(());
            }

            info!(logger, "Create attribute indexes for subgraph entities");

            // Build indexes for each entity attribute in the Subgraph
            let index_definitions =
                attribute_index_definitions(subgraph.id.clone(), subgraph.schema.document.clone());
            self_clone
                .store
                .build_entity_attribute_indexes(&subgraph.id, index_definitions)
                .map(|_| {
                    info!(
                        logger,
                        "Successfully created attribute indexes for subgraph entities"
                    )
                })
                .ok();

            // Send events to trigger subgraph processing
            if let Err(e) = self_clone
                .event_sink
                .clone()
                .send(DeploymentControllerEvent::Start(subgraph))
                .compat()
                .await
            {
                panic!("failed to forward subgraph: {}", e);
            }
            Ok(())
        }
        .map_err(move |e| {
            error!(
                logger_for_error,
                "Failed to start subgraph";
                "error" => format!("{}", e)
            );

            let _ignore_error = store.apply_metadata_operations(
                SubgraphDeploymentEntity::update_failed_operations(&subgraph_id, true),
            );
            e
        })
        .await
    }

    async fn stop(&self, id: &SubgraphDeploymentId) -> Result<(), DeploymentControllerError> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut the subgraph deployment down
            self.event_sink
                .clone()
                .send(DeploymentControllerEvent::Stop(id.clone()))
                .map_err(|e| panic!("failed to forward subgraph shut down event: {}", e))
                .map(|_| ())
                .compat()
                .await
        } else {
            Err(DeploymentControllerError::NotRunning(id.clone()))
        }
    }
}

impl<L, Q, S> EventProducer<DeploymentControllerEvent> for DeploymentController<L, Q, S> {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = DeploymentControllerEvent, Error = ()> + Send>> {
        self.event_stream.take().map(|s| {
            Box::new(s) as Box<dyn Stream<Item = DeploymentControllerEvent, Error = ()> + Send>
        })
    }
}
