use std::collections::HashMap;
use std::ops::Deref as _;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use futures03::future::AbortHandle;
use graph::data::subgraph::schema::attribute_index_definitions;
use graph::prelude::{
    error, futures03, info, DataSourceLoader as _, GraphQlRunner, Link, LinkResolver, Logger,
    LoggerFactory, NetworkRegistry, Store,
    SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, SubgraphAssignmentProviderError,
    SubgraphDeploymentEntity, SubgraphDeploymentId, SubgraphDeploymentStore, SubgraphManifest,
    TryFutureExt,
};

use crate::subgraph::registrar::IPFS_SUBGRAPH_LOADING_TIMEOUT;
use crate::DataSourceLoader;

pub struct SubgraphAssignmentProvider<L, Q, S> {
    logger: Logger,
    logger_factory: LoggerFactory,
    resolver: Arc<L>,
    store: Arc<S>,
    graphql_runner: Arc<Q>,
    network_registry: Arc<dyn NetworkRegistry>,
    subgraphs_running: Arc<Mutex<HashMap<SubgraphDeploymentId, AbortHandle>>>,
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
        network_registry: Arc<dyn NetworkRegistry>,
        store: Arc<S>,
        graphql_runner: Arc<Q>,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphAssignmentProvider", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        // Create the subgraph provider
        SubgraphAssignmentProvider {
            logger,
            logger_factory,
            resolver: Arc::new(
                resolver
                    .as_ref()
                    .clone()
                    .with_timeout(*IPFS_SUBGRAPH_LOADING_TIMEOUT)
                    .with_retries(),
            ),
            network_registry,
            store,
            graphql_runner,
            subgraphs_running: Default::default(),
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
        info!(self.logger, "Start subgraph"; "subgraph_id" => format!("{}", id));

        let store = self.store.clone();
        let subgraph_id = id.clone();

        let loader = Arc::new(DataSourceLoader::new(
            store.clone(),
            self.resolver.clone(),
            self.graphql_runner.clone(),
        ));

        let link = format!("/ipfs/{}", id);

        let logger = self.logger_factory.subgraph_logger(id);
        let logger_for_err = logger.clone();

        info!(logger, "Resolve subgraph files using IPFS");

        async move {
            // Don't do anything if the subgraph is already running
            if self.subgraphs_running.lock().unwrap().contains_key(id) {
                info!(logger, "Subgraph deployment is already running");
                return Err(SubgraphAssignmentProviderError::AlreadyRunning(id.clone()));
            }

            // Load subgraph files from IPFS
            let mut subgraph =
                SubgraphManifest::resolve(Link { link }, self.resolver.deref(), &logger)
                    .map_err(SubgraphAssignmentProviderError::ResolveError)
                    .await?;

            // Load dynamic data sources
            let data_sources = loader
                .load_dynamic_data_sources(id.clone(), logger.clone())
                .map_err(SubgraphAssignmentProviderError::DynamicDataSourcesError)
                .await?;

            // Add dynamic data sources to the subgraph
            subgraph.data_sources.extend(data_sources);

            info!(logger, "Successfully resolved subgraph files using IPFS");

            // Build indexes for each entity attribute in the Subgraph
            info!(logger, "Create attribute indexes for subgraph entities");
            let index_definitions =
                attribute_index_definitions(subgraph.id.clone(), subgraph.schema.document.clone());
            self.store
                .build_entity_attribute_indexes(&subgraph.id, index_definitions)
                .map(|_| {
                    info!(
                        logger,
                        "Successfully created attribute indexes for subgraph entities"
                    )
                })
                .ok();

            // Identify network for subgraph
            let network_instance_id = subgraph.network();
            let network_instance = self
                .network_registry
                .instance(&network_instance_id)
                .ok_or_else(|| {
                    SubgraphAssignmentProviderError::UnsupportedNetwork(network_instance_id)
                })?;

            // Ask the network instance to index this subgraph
            let abort_handle = network_instance
                .start_subgraph(subgraph)
                .await
                .map_err(SubgraphAssignmentProviderError::Unknown)?;

            let mut subgraphs_running = self.subgraphs_running.lock().unwrap();
            subgraphs_running.insert(id.clone(), abort_handle);

            Ok(())
        }
        .map_err(move |e| {
            error!(
                logger_for_err,
                "Failed to resolve subgraph files using IPFS";
                "error" => format!("{}", e)
            );

            let _ignore_error = store.apply_metadata_operations(
                SubgraphDeploymentEntity::update_failed_operations(&subgraph_id, true),
            );
            e
        })
        .await
    }

    async fn stop(&self, id: SubgraphDeploymentId) -> Result<(), SubgraphAssignmentProviderError> {
        info!(self.logger, "Stop subgraph"; "subgraph_id" => format!("{}", id));

        // When starting (or resuming) to index the subgraph, the network instance
        // returned an abort handle. By removing this abort handle from the running
        // subgraphs map, we stop indexing the subgraph.
        self.subgraphs_running
            .lock()
            .unwrap()
            .remove(&id)
            .map(|_| ())
            .ok_or_else(|| SubgraphAssignmentProviderError::NotRunning(id))
    }
}
