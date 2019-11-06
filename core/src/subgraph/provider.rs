use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashSet;
use std::sync::Mutex;

use graph::data::subgraph::schema::attribute_index_definitions;
use graph::prelude::{
    DataSourceLoader as _, GraphQlRunner,
    SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, *,
};

use crate::subgraph::registrar::IPFS_SUBGRAPH_LOADING_TIMEOUT;
use crate::DataSourceLoader;

pub struct SubgraphAssignmentProvider<L, Q, S> {
    logger: Logger,
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
            logger,
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
    fn clone(&self) -> Self {
        SubgraphAssignmentProvider {
            logger: self.logger.clone(),
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

impl<L, Q, S> SubgraphAssignmentProviderTrait for SubgraphAssignmentProvider<L, Q, S>
where
    L: LinkResolver + Clone,
    Q: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    fn start(
        &self,
        id: SubgraphDeploymentId,
    ) -> Box<dyn Future<Item = (), Error = SubgraphAssignmentProviderError> + Send + 'static> {
        let self_clone = self.clone();
        let store = self.store.clone();
        let subgraph_id = id.clone();
        let subgraph_id_for_data_sources = id.clone();

        let loader = Arc::new(DataSourceLoader::new(
            store.clone(),
            self.resolver.clone(),
            self.graphql_runner.clone(),
        ));

        let link = format!("/ipfs/{}", id);

        let logger = self.logger_factory.subgraph_logger(&id);
        let logger_for_resolve = logger.clone();
        let logger_for_err = logger.clone();
        let logger_for_data_sources = logger.clone();

        info!(logger, "Resolve subgraph files using IPFS");

        Box::new(
            SubgraphManifest::resolve(Link { link }, self.resolver.clone(), logger_for_resolve)
                .map_err(SubgraphAssignmentProviderError::ResolveError)
                .and_then(move |manifest| {
                    (
                        future::ok(manifest),
                        loader
                            .load_dynamic_data_sources(
                                &subgraph_id_for_data_sources,
                                logger_for_data_sources,
                            )
                            .map_err(SubgraphAssignmentProviderError::DynamicDataSourcesError),
                    )
                })
                .and_then(
                    move |(mut subgraph, data_sources)| -> Box<dyn Future<Item = _, Error = _> + Send> {
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

                            return Box::new(future::err(
                                SubgraphAssignmentProviderError::AlreadyRunning(subgraph.id),
                            ));
                        }

                        info!(logger, "Create attribute indexes for subgraph entities");

                        // Build indexes for each entity attribute in the Subgraph
                        let index_definitions = attribute_index_definitions(
                            subgraph.id.clone(),
                            subgraph.schema.document.clone(),
                        );
                        self_clone
                            .store
                            .clone()
                            .build_entity_attribute_indexes(&subgraph.id, index_definitions)
                            .map(|_| {
                                info!(
                                    logger,
                                    "Successfully created attribute indexes for subgraph entities"
                                )
                            })
                            .ok();

                        // Send events to trigger subgraph processing
                        Box::new(
                            self_clone
                                .event_sink
                                .clone()
                                .send(SubgraphAssignmentProviderEvent::SubgraphStart(subgraph))
                                .map_err(|e| panic!("failed to forward subgraph: {}", e))
                                .map(|_| ()),
                        )
                    },
                )
                .map_err(move |e| {
                    error!(
                        logger_for_err,
                        "Failed to resolve subgraph files using IPFS";
                        "error" => format!("{}", e)
                    );

                    let _ = store.apply_metadata_operations(
                        SubgraphDeploymentEntity::update_failed_operations(&subgraph_id, true),
                    );
                    e
                }),
        )
    }

    fn stop(
        &self,
        id: SubgraphDeploymentId,
    ) -> Box<dyn Future<Item = (), Error = SubgraphAssignmentProviderError> + Send + 'static> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut down subgraph processing
            Box::new(
                self.event_sink
                    .clone()
                    .send(SubgraphAssignmentProviderEvent::SubgraphStop(id))
                    .map_err(|e| panic!("failed to forward subgraph shut down event: {}", e))
                    .map(|_| ()),
            )
        } else {
            Box::new(future::err(SubgraphAssignmentProviderError::NotRunning(id)))
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
