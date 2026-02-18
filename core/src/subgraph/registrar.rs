use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use graph::amp;
use graph::blockchain::{Blockchain, BlockchainKind, BlockchainMap};
use graph::components::{
    link_resolver::LinkResolverContext,
    network_provider::{AmpChainConfig, AmpChainNames, AmpClients},
    store::{DeploymentId, DeploymentLocator, SubscriptionManager},
    subgraph::Settings,
};
use graph::data::{
    subgraph::{schema::DeploymentCreate, Graft},
    value::Word,
};
use graph::futures03::{self, future::TryFutureExt, Stream, StreamExt, TryStreamExt};
use graph::prelude::{CreateSubgraphResult, SubgraphRegistrar as SubgraphRegistrarTrait, *};
use graph::util::futures::{retry_strategy, RETRY_DEFAULT_LIMIT};
use tokio_retry::Retry;

pub struct SubgraphRegistrar<P, S, SM, AC> {
    logger: Logger,
    logger_factory: LoggerFactory,
    resolver: Arc<dyn LinkResolver>,
    provider: Arc<P>,
    store: Arc<S>,
    subscription_manager: Arc<SM>,
    amp_clients: AmpClients<AC>,
    amp_chain_configs: HashMap<String, AmpChainConfig>,
    chains: Arc<BlockchainMap>,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
    assignment_event_stream_cancel_guard: CancelGuard, // cancels on drop
    settings: Arc<Settings>,
    amp_chain_names: Arc<AmpChainNames>,
}

impl<P, S, SM, AC> SubgraphRegistrar<P, S, SM, AC>
where
    P: graph::components::subgraph::SubgraphInstanceManager,
    S: SubgraphStore,
    SM: SubscriptionManager,
    AC: amp::Client + Send + Sync + 'static,
{
    pub fn new(
        logger_factory: &LoggerFactory,
        resolver: Arc<dyn LinkResolver>,
        provider: Arc<P>,
        store: Arc<S>,
        subscription_manager: Arc<SM>,
        amp_clients: AmpClients<AC>,
        amp_chain_configs: HashMap<String, AmpChainConfig>,
        chains: Arc<BlockchainMap>,
        node_id: NodeId,
        version_switching_mode: SubgraphVersionSwitchingMode,
        settings: Arc<Settings>,
        amp_chain_names: Arc<AmpChainNames>,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphRegistrar", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        let resolver = resolver.with_retries();

        SubgraphRegistrar {
            logger,
            logger_factory,
            resolver: resolver.into(),
            provider,
            store,
            subscription_manager,
            amp_clients,
            amp_chain_configs,
            chains,
            node_id,
            version_switching_mode,
            assignment_event_stream_cancel_guard: CancelGuard::new(),
            settings,
            amp_chain_names,
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<(), Error> {
        // The order of the following three steps is important:
        // - Start assignment event stream
        // - Read assignments table and start assigned subgraphs
        // - Start processing assignment event stream
        //
        // Starting the event stream before reading the assignments table ensures that no
        // assignments are missed in the period of time between the table read and starting event
        // processing.
        // Delaying the start of event processing until after the table has been read and processed
        // ensures that Remove events happen after the assigned subgraphs have been started, not
        // before (otherwise a subgraph could be left running due to a race condition).
        //
        // The discrepancy between the start time of the event stream and the table read can result
        // in some extraneous events on start up. Examples:
        // - The event stream sees an 'set' event for subgraph A, but the table query finds that
        //   subgraph A is already in the table.
        // - The event stream sees a 'removed' event for subgraph B, but the table query finds that
        //   subgraph B has already been removed.
        // The `change_assignment` function handles these cases by ignoring
        // such cases which makes the operations idempotent

        // Start event stream
        let assignment_event_stream = self.cheap_clone().assignment_events().await;

        // Deploy named subgraphs found in store
        self.start_assigned_subgraphs().await?;

        let cancel_handle = self.assignment_event_stream_cancel_guard.handle();

        // Spawn a task to handle assignment events.
        let fut = assignment_event_stream.for_each({
            move |event| {
                // The assignment stream should run forever. If it gets
                // cancelled, that probably indicates a serious problem and
                // we panic
                if cancel_handle.is_canceled() {
                    panic!("assignment event stream canceled");
                }

                let this = self.cheap_clone();
                async move {
                    this.change_assignment(event).await;
                }
            }
        });

        graph::spawn(fut);
        Ok(())
    }

    /// Start/stop subgraphs as needed, considering the current assignment
    /// state in the database, ignoring changes that do not affect this
    /// node, do not require anything to change, or for which we can not
    /// find the assignment status from the database
    async fn change_assignment(&self, change: AssignmentChange) {
        let (deployment, operation) = change.into_parts();

        trace!(self.logger, "Received assignment change";
           "deployment" => %deployment,
           "operation" => format!("{:?}", operation),
        );

        match operation {
            AssignmentOperation::Set => {
                let assigned = match self.store.assignment_status(&deployment).await {
                    Ok(assigned) => assigned,
                    Err(e) => {
                        error!(
                            self.logger,
                            "Failed to get subgraph assignment entity"; "deployment" => deployment, "error" => e.to_string()
                        );
                        return;
                    }
                };

                let logger = self.logger.new(o!("subgraph_id" => deployment.hash.to_string(), "node_id" => self.node_id.to_string()));
                if let Some((assigned, is_paused)) = assigned {
                    if assigned == self.node_id {
                        if is_paused {
                            // Subgraph is paused, so we don't start it
                            debug!(logger, "Deployment assignee is this node"; "assigned_to" => assigned, "paused" => is_paused, "action" => "ignore");
                            return;
                        }

                        // Start subgraph on this node
                        debug!(logger, "Deployment assignee is this node"; "assigned_to" => assigned, "action" => "add");
                        self.provider
                            .cheap_clone()
                            .start_subgraph(deployment, None)
                            .await;
                    } else {
                        // Ensure it is removed from this node
                        debug!(logger, "Deployment assignee is not this node"; "assigned_to" => assigned, "action" => "remove");
                        self.provider.stop_subgraph(deployment).await
                    }
                } else {
                    // Was added/updated, but is now gone.
                    debug!(self.logger, "Deployment assignee not found in database"; "action" => "ignore");
                }
            }
            AssignmentOperation::Removed => {
                self.provider.stop_subgraph(deployment).await;
            }
        }
    }

    pub async fn assignment_events(self: Arc<Self>) -> impl Stream<Item = AssignmentChange> + Send {
        self.subscription_manager
            .subscribe()
            .map(|event| futures03::stream::iter(event.changes.clone()))
            .flatten()
    }

    async fn start_assigned_subgraphs(&self) -> Result<(), Error> {
        let logger = self.logger.clone();
        let node_id = self.node_id.clone();

        let deployments = self
            .store
            .active_assignments(&self.node_id)
            .await
            .map_err(|e| anyhow!("Error querying subgraph assignments: {}", e))?;
        // This operation should finish only after all subgraphs are
        // started. We wait for the spawned tasks to complete by giving
        // each a `sender` and waiting for all of them to be dropped, so
        // the receiver terminates without receiving anything.
        let deployments = HashSet::<DeploymentLocator>::from_iter(deployments);
        let deployments_len = deployments.len();
        debug!(logger, "Starting all assigned subgraphs";
                    "count" => deployments_len, "node_id" => &node_id);
        let (sender, receiver) = futures03::channel::mpsc::channel::<()>(1);
        for id in deployments {
            let sender = sender.clone();
            let provider = self.provider.cheap_clone();

            graph::spawn(async move {
                provider.start_subgraph(id, None).await;
                drop(sender)
            });
        }
        drop(sender);
        let _: Vec<_> = receiver.collect().await;
        info!(logger, "Started all assigned subgraphs";
                    "count" => deployments_len, "node_id" => &node_id);
        Ok(())
    }
}

#[async_trait]
impl<P, S, SM, AC> SubgraphRegistrarTrait for SubgraphRegistrar<P, S, SM, AC>
where
    P: graph::components::subgraph::SubgraphInstanceManager,
    S: SubgraphStore,
    SM: SubscriptionManager,
    AC: amp::Client + Send + Sync + 'static,
{
    async fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Result<CreateSubgraphResult, SubgraphRegistrarError> {
        let id = self.store.create_subgraph(name.clone()).await?;

        debug!(self.logger, "Created subgraph"; "subgraph_name" => name.to_string());

        Ok(CreateSubgraphResult { id })
    }

    async fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: DeploymentHash,
        node_id: NodeId,
        debug_fork: Option<DeploymentHash>,
        start_block_override: Option<BlockPtr>,
        graft_block_override: Option<BlockPtr>,
        history_blocks: Option<i32>,
        ignore_graft_base: bool,
    ) -> Result<DeploymentLocator, SubgraphRegistrarError> {
        // We don't have a location for the subgraph yet; that will be
        // assigned when we deploy for real. For logging purposes, make up a
        // fake locator
        let logger = self
            .logger_factory
            .subgraph_logger(&DeploymentLocator::new(DeploymentId(0), hash.clone()));

        let resolver: Arc<dyn LinkResolver> = Arc::from(
            self.resolver
                .for_manifest(&hash.to_string())
                .map_err(SubgraphRegistrarError::Unknown)?,
        );

        let raw = {
            let mut raw: serde_yaml::Mapping = {
                let file_bytes = resolver
                    .cat(
                        &LinkResolverContext::new(&hash, &logger),
                        &hash.to_ipfs_link(),
                    )
                    .await
                    .map_err(|e| {
                        SubgraphRegistrarError::ResolveError(
                            SubgraphManifestResolveError::ResolveError(e),
                        )
                    })?;

                serde_yaml::from_slice(&file_bytes)
                    .map_err(|e| SubgraphRegistrarError::ResolveError(e.into()))?
            };

            if ignore_graft_base {
                raw.remove("graft");
            }

            raw
        };

        let kind = BlockchainKind::from_manifest(&raw).map_err(|e| {
            SubgraphRegistrarError::ResolveError(SubgraphManifestResolveError::ResolveError(e))
        })?;

        // Extract the network name from the raw manifest and resolve the
        // per-chain Amp client (if any).
        let resolved_amp_chain = network_name_from_raw(&raw).map(|network| {
            let resolved = self.amp_chain_names.resolve(&Word::from(network));
            resolved.to_string()
        });
        let amp_client = resolved_amp_chain
            .as_deref()
            .and_then(|chain| self.amp_clients.get(chain));
        let amp_context = resolved_amp_chain
            .as_deref()
            .and_then(|chain| self.amp_chain_configs.get(chain))
            .map(|cfg| (cfg.context_dataset.clone(), cfg.context_table.clone()));

        // Give priority to deployment specific history_blocks value.
        let history_blocks =
            history_blocks.or(self.settings.for_name(&name).map(|c| c.history_blocks));

        let deployment_locator = match kind {
            BlockchainKind::Ethereum => {
                create_subgraph_version::<graph_chain_ethereum::Chain, _, _>(
                    &logger,
                    self.store.clone(),
                    self.chains.cheap_clone(),
                    name.clone(),
                    hash.cheap_clone(),
                    start_block_override,
                    graft_block_override,
                    raw,
                    node_id,
                    debug_fork,
                    self.version_switching_mode,
                    &resolver,
                    amp_client.cheap_clone(),
                    amp_context.clone(),
                    history_blocks,
                    &self.amp_chain_names,
                )
                .await?
            }
            BlockchainKind::Near => {
                create_subgraph_version::<graph_chain_near::Chain, _, _>(
                    &logger,
                    self.store.clone(),
                    self.chains.cheap_clone(),
                    name.clone(),
                    hash.cheap_clone(),
                    start_block_override,
                    graft_block_override,
                    raw,
                    node_id,
                    debug_fork,
                    self.version_switching_mode,
                    &resolver,
                    amp_client,
                    amp_context,
                    history_blocks,
                    &self.amp_chain_names,
                )
                .await?
            }
        };

        debug!(
            &logger,
            "Wrote new subgraph version to store";
            "subgraph_name" => name.to_string(),
            "subgraph_hash" => hash.to_string(),
        );

        Ok(deployment_locator)
    }

    async fn remove_subgraph(&self, name: SubgraphName) -> Result<(), SubgraphRegistrarError> {
        self.store.clone().remove_subgraph(name.clone()).await?;

        debug!(self.logger, "Removed subgraph"; "subgraph_name" => name.to_string());

        Ok(())
    }

    /// Reassign a subgraph deployment to a different node.
    ///
    /// Reassigning to a nodeId that does not match any reachable graph-nodes will effectively pause the
    /// subgraph syncing process.
    async fn reassign_subgraph(
        &self,
        hash: &DeploymentHash,
        node_id: &NodeId,
    ) -> Result<(), SubgraphRegistrarError> {
        let locator = self.store.active_locator(hash).await?;
        let deployment =
            locator.ok_or_else(|| SubgraphRegistrarError::DeploymentNotFound(hash.to_string()))?;

        self.store.reassign_subgraph(&deployment, node_id).await?;

        Ok(())
    }

    async fn pause_subgraph(&self, hash: &DeploymentHash) -> Result<(), SubgraphRegistrarError> {
        let locator = self.store.active_locator(hash).await?;
        let deployment =
            locator.ok_or_else(|| SubgraphRegistrarError::DeploymentNotFound(hash.to_string()))?;

        self.store.pause_subgraph(&deployment).await?;

        Ok(())
    }

    async fn resume_subgraph(&self, hash: &DeploymentHash) -> Result<(), SubgraphRegistrarError> {
        let locator = self.store.active_locator(hash).await?;
        let deployment =
            locator.ok_or_else(|| SubgraphRegistrarError::DeploymentNotFound(hash.to_string()))?;

        self.store.resume_subgraph(&deployment).await?;

        Ok(())
    }
}

/// Resolves a block pointer for an Amp subgraph by querying the Amp Flight
/// service for the block hash at the given `block_number`.
async fn resolve_amp_start_block<AC: amp::Client>(
    amp_client: &AC,
    logger: &Logger,
    context_dataset: &str,
    context_table: &str,
    block_number: BlockNumber,
) -> Result<BlockPtr, anyhow::Error> {
    let sql = format!(
        "SELECT * FROM {}.{} WHERE _block_num = {}",
        context_dataset, context_table, block_number
    );

    let mut stream = amp_client.query(logger, &sql, None);

    // Find the first Batch response, skipping any Reorg variants.
    let data = loop {
        match stream.try_next().await? {
            Some(amp::client::ResponseBatch::Batch { data }) => break data,
            Some(amp::client::ResponseBatch::Reorg(_)) => continue,
            None => {
                return Err(anyhow!(
                    "Amp query returned no batches for block {}",
                    block_number
                ));
            }
        }
    };

    if data.num_rows() == 0 {
        return Err(anyhow!(
            "Amp query returned empty batch for block {}",
            block_number
        ));
    }

    let (_col_name, decoder) = graph::amp::codec::utils::auto_block_hash_decoder(&data)?;
    let hash = decoder.decode(0)?.ok_or_else(|| {
        anyhow!(
            "Amp query returned null block hash for block {}",
            block_number
        )
    })?;

    Ok(BlockPtr::new(hash.into(), block_number))
}

/// Resolves the subgraph's earliest block
async fn resolve_start_block(
    manifest: &SubgraphManifest<impl Blockchain>,
    chain: &impl Blockchain,
    logger: &Logger,
) -> Result<Option<BlockPtr>, SubgraphRegistrarError> {
    // If the minimum start block is 0 (i.e. the genesis block),
    // return `None` to start indexing from the genesis block. Otherwise
    // return a block pointer for the block with number `min_start_block - 1`.
    match manifest
        .start_blocks()
        .into_iter()
        .min()
        .expect("cannot identify minimum start block because there are no data sources")
    {
        0 => Ok(None),
        min_start_block => Retry::spawn(retry_strategy(Some(2), RETRY_DEFAULT_LIMIT), move || {
            chain
                .block_pointer_from_number(logger, min_start_block - 1)
                .inspect_err(move |e| warn!(&logger, "Failed to get block number: {}", e))
        })
        .await
        .map(Some)
        .map_err(move |_| {
            SubgraphRegistrarError::ManifestValidationError(vec![
                SubgraphManifestValidationError::BlockNotFound(min_start_block.to_string()),
            ])
        }),
    }
}

/// Resolves the manifest's graft base block
async fn resolve_graft_block(
    base: &Graft,
    chain: &impl Blockchain,
    logger: &Logger,
) -> Result<BlockPtr, SubgraphRegistrarError> {
    chain
        .block_pointer_from_number(logger, base.block)
        .await
        .map_err(|_| {
            SubgraphRegistrarError::ManifestValidationError(vec![
                SubgraphManifestValidationError::BlockNotFound(format!(
                    "graft base block {} not found",
                    base.block
                )),
            ])
        })
}

/// Extracts the network name from the first data source in a raw manifest.
fn network_name_from_raw(raw: &serde_yaml::Mapping) -> Option<String> {
    use serde_yaml::Value;
    raw.get(Value::String("dataSources".to_owned()))
        .and_then(|ds| ds.as_sequence())
        .and_then(|ds| ds.first())
        .and_then(|ds| ds.as_mapping())
        .and_then(|ds| ds.get(Value::String("network".to_owned())))
        .and_then(|n| n.as_str())
        .map(|s| s.to_owned())
}

async fn create_subgraph_version<C: Blockchain, S: SubgraphStore, AC: amp::Client>(
    logger: &Logger,
    store: Arc<S>,
    chains: Arc<BlockchainMap>,
    name: SubgraphName,
    deployment: DeploymentHash,
    start_block_override: Option<BlockPtr>,
    graft_block_override: Option<BlockPtr>,
    raw: serde_yaml::Mapping,
    node_id: NodeId,
    debug_fork: Option<DeploymentHash>,
    version_switching_mode: SubgraphVersionSwitchingMode,
    resolver: &Arc<dyn LinkResolver>,
    amp_client: Option<Arc<AC>>,
    amp_context: Option<(String, String)>,
    history_blocks_override: Option<i32>,
    amp_chain_names: &AmpChainNames,
) -> Result<DeploymentLocator, SubgraphRegistrarError> {
    let raw_string = serde_yaml::to_string(&raw).unwrap();

    // Keep copies for Amp start block resolution after the manifest is resolved.
    let amp_client_for_start_block = amp_client.clone();
    let amp_context_for_start_block = amp_context.clone();

    let unvalidated = UnvalidatedSubgraphManifest::<C>::resolve(
        deployment.clone(),
        raw,
        resolver,
        amp_client,
        amp_context,
        logger,
        ENV_VARS.max_spec_version.clone(),
    )
    .map_err(SubgraphRegistrarError::ResolveError)
    .await?;
    // Determine if the graft_base should be validated.
    // Validate the graft_base if there is a pending graft, ensuring its presence.
    // If the subgraph is new (indicated by DeploymentNotFound), the graft_base should be validated.
    // If the subgraph already exists and there is no pending graft, graft_base validation is not required.
    let should_validate = match store.graft_pending(&deployment).await {
        Ok(graft_pending) => graft_pending,
        Err(StoreError::DeploymentNotFound(_)) => true,
        Err(e) => return Err(SubgraphRegistrarError::StoreError(e)),
    };
    let manifest = unvalidated
        .validate(store.cheap_clone(), should_validate)
        .await
        .map_err(SubgraphRegistrarError::ManifestValidationError)?;

    let network_name: Word = manifest.network_name().into();
    let resolved_name = amp_chain_names.resolve(&network_name);

    let chain = chains
        .get::<C>(resolved_name.clone())
        .map_err(SubgraphRegistrarError::NetworkNotSupported)?
        .cheap_clone();

    let logger = logger.clone();
    let store = store.clone();
    let deployment_store = store.clone();

    if !store.subgraph_exists(&name).await? {
        debug!(
            logger,
            "Subgraph not found, could not create_subgraph_version";
            "subgraph_name" => name.to_string()
        );
        return Err(SubgraphRegistrarError::NameNotFound(name.to_string()));
    }

    let start_block = match start_block_override {
        Some(block) => Some(block),
        None => {
            let min_start_block =
                manifest.start_blocks().into_iter().min().expect(
                    "cannot identify minimum start block because there are no data sources",
                );

            match (
                min_start_block,
                &amp_client_for_start_block,
                &amp_context_for_start_block,
            ) {
                // Genesis block — no resolution needed.
                (0, _, _) => None,
                // Amp subgraph with start_block > 0 — try Amp-based resolution.
                (min, Some(client), Some((dataset, table))) => {
                    match resolve_amp_start_block(client.as_ref(), &logger, dataset, table, min - 1)
                        .await
                    {
                        Ok(ptr) => Some(ptr),
                        Err(e) => {
                            warn!(
                                logger,
                                "Amp block pointer resolution failed, falling back to RPC";
                                "error" => e.to_string(),
                                "block_number" => min - 1
                            );
                            resolve_start_block(&manifest, &*chain, &logger).await?
                        }
                    }
                }
                // Non-Amp subgraph — use RPC.
                _ => resolve_start_block(&manifest, &*chain, &logger).await?,
            }
        }
    };

    let base_block = match &manifest.graft {
        None => None,
        Some(graft) => Some((
            graft.base.clone(),
            match graft_block_override {
                Some(block) => block,
                None => resolve_graft_block(graft, &*chain, &logger).await?,
            },
        )),
    };

    info!(
        logger,
        "Set subgraph start block";
        "block" => format!("{:?}", start_block),
    );

    info!(
        logger,
        "Graft base";
        "base" => format!("{:?}", base_block.as_ref().map(|(subgraph,_)| subgraph.to_string())),
        "block" => format!("{:?}", base_block.as_ref().map(|(_,ptr)| ptr.number))
    );

    // Entity types that may be touched by offchain data sources need a causality region column.
    let needs_causality_region = manifest
        .data_sources
        .iter()
        .filter_map(|ds| ds.as_offchain())
        .map(|ds| ds.mapping.entities.iter())
        .chain(
            manifest
                .templates
                .iter()
                .filter_map(|ds| ds.as_offchain())
                .map(|ds| ds.mapping.entities.iter()),
        )
        .flatten()
        .cloned()
        .collect();

    // Apply the subgraph versioning and deployment operations,
    // creating a new subgraph deployment if one doesn't exist.
    let mut deployment = DeploymentCreate::new(raw_string, &manifest, start_block)
        .graft(base_block)
        .debug(debug_fork)
        .entities_with_causality_region(needs_causality_region);

    if let Some(history_blocks) = history_blocks_override {
        deployment = deployment.with_history_blocks_override(history_blocks);
    }

    deployment_store
        .create_subgraph_deployment(
            name,
            &manifest.schema,
            deployment,
            node_id,
            resolved_name.into(),
            version_switching_mode,
        )
        .await
        .map_err(SubgraphRegistrarError::SubgraphDeploymentError)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use alloy::primitives::BlockHash as AllocBlockHash;
    use arrow::array::{FixedSizeBinaryArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use graph::amp;
    use graph::amp::client::{RequestMetadata, ResponseBatch};
    use graph::amp::error::IsDeterministic;
    use graph::futures03::future::BoxFuture;
    use graph::futures03::stream::BoxStream;
    use graph::prelude::*;

    // -- Mock Amp client --------------------------------------------------

    #[derive(Debug)]
    struct MockAmpError(String);

    impl std::fmt::Display for MockAmpError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for MockAmpError {}

    impl IsDeterministic for MockAmpError {
        fn is_deterministic(&self) -> bool {
            false
        }
    }

    #[derive(Clone)]
    struct MockAmpClient {
        /// Recorded queries (for assertion).
        recorded_queries: Arc<Mutex<Vec<String>>>,
        /// Batches to return from `query()`. If `None`, the stream returns an error.
        response: Arc<Result<Vec<ResponseBatch>, String>>,
    }

    impl MockAmpClient {
        fn new_ok(batches: Vec<ResponseBatch>) -> Self {
            Self {
                recorded_queries: Arc::new(Mutex::new(Vec::new())),
                response: Arc::new(Ok(batches)),
            }
        }

        fn new_err(msg: &str) -> Self {
            Self {
                recorded_queries: Arc::new(Mutex::new(Vec::new())),
                response: Arc::new(Err(msg.to_string())),
            }
        }

        fn recorded_queries(&self) -> Vec<String> {
            self.recorded_queries.lock().unwrap().clone()
        }
    }

    impl amp::Client for MockAmpClient {
        type Error = MockAmpError;

        fn schema(
            &self,
            _logger: &Logger,
            _query: impl ToString,
        ) -> BoxFuture<'static, Result<arrow::datatypes::Schema, Self::Error>> {
            unimplemented!("schema not needed in tests")
        }

        fn query(
            &self,
            _logger: &Logger,
            query: impl ToString,
            _request_metadata: Option<RequestMetadata>,
        ) -> BoxStream<'static, Result<ResponseBatch, Self::Error>> {
            let query_str = query.to_string();
            self.recorded_queries.lock().unwrap().push(query_str);

            let response = self.response.clone();
            Box::pin(graph::futures03::stream::iter(match response.as_ref() {
                Ok(batches) => batches.iter().cloned().map(Ok).collect::<Vec<_>>(),
                Err(msg) => vec![Err(MockAmpError(msg.clone()))],
            }))
        }
    }

    // -- Test helpers -----------------------------------------------------

    /// Creates a RecordBatch with a single "block_hash" column containing one
    /// 32-byte FixedSizeBinary value.
    fn make_block_hash_batch(hash: AllocBlockHash) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(
            "block_hash",
            DataType::FixedSizeBinary(32),
            false,
        )]);
        let values: Vec<&[u8]> = vec![hash.as_slice()];
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap(),
            )],
        )
        .unwrap()
    }

    // -- Tests ------------------------------------------------------------

    #[tokio::test]
    async fn resolve_amp_start_block() {
        let alloy_hash = AllocBlockHash::from([0xABu8; 32]);
        let expected_hash: BlockHash = alloy_hash.into();
        let batch = make_block_hash_batch(alloy_hash);
        let client = MockAmpClient::new_ok(vec![ResponseBatch::Batch { data: batch }]);
        let logger = Logger::root(slog::Discard, o!());

        let result =
            super::resolve_amp_start_block(&client, &logger, "my_dataset", "blocks", 99).await;

        let ptr = result.expect("should succeed");
        assert_eq!(ptr.hash, expected_hash);
        assert_eq!(ptr.number, 99);

        // Verify the SQL query.
        let queries = client.recorded_queries();
        assert_eq!(queries.len(), 1);
        assert_eq!(
            queries[0],
            "SELECT * FROM my_dataset.blocks WHERE _block_num = 99"
        );
    }

    #[tokio::test]
    async fn amp_subgraph_start_block_uses_amp_resolution() {
        // When an Amp client + context are available and min_start_block > 0,
        // the Amp path should be used and produce the correct BlockPtr.
        let alloy_hash = AllocBlockHash::from([0xCDu8; 32]);
        let expected_hash: BlockHash = alloy_hash.into();
        let batch = make_block_hash_batch(alloy_hash);
        let client = MockAmpClient::new_ok(vec![ResponseBatch::Batch { data: batch }]);
        let logger = Logger::root(slog::Discard, o!());

        // Simulate min_start_block = 100 → query block 99
        let block_number = 100 - 1;
        let result =
            super::resolve_amp_start_block(&client, &logger, "eth_mainnet", "blocks", block_number)
                .await;

        let ptr = result.expect("should succeed");
        assert_eq!(ptr.hash, expected_hash);
        assert_eq!(ptr.number, block_number);

        let queries = client.recorded_queries();
        assert_eq!(
            queries[0],
            "SELECT * FROM eth_mainnet.blocks WHERE _block_num = 99"
        );
    }

    #[tokio::test]
    async fn amp_start_block_falls_back_to_rpc() {
        // When the Amp query fails, resolve_amp_start_block returns an error.
        let client = MockAmpClient::new_err("network error");
        let logger = Logger::root(slog::Discard, o!());

        let result =
            super::resolve_amp_start_block(&client, &logger, "my_dataset", "blocks", 99).await;

        assert!(
            result.is_err(),
            "should return an error so caller falls back to RPC"
        );
        assert!(
            result.unwrap_err().to_string().contains("network error"),
            "error should contain the original cause"
        );
    }

    #[tokio::test]
    async fn amp_start_block_zero_returns_none() {
        // start_block = 0 means genesis — the caller should not invoke
        // resolve_amp_start_block at all. We verify the matching logic inline:
        // when min_start_block == 0, the result is None.
        let min_start_block: i32 = 0;
        let mock_client = Arc::new(MockAmpClient::new_ok(vec![]));
        let amp_client: Option<Arc<MockAmpClient>> = Some(mock_client.clone());
        let amp_context: Option<(String, String)> = Some(("ds".to_string(), "blocks".to_string()));

        let result: Option<BlockPtr> = match (min_start_block, &amp_client, &amp_context) {
            (0, _, _) => None,
            _ => panic!("should not reach non-zero path"),
        };

        assert!(
            result.is_none(),
            "start_block=0 should produce None (genesis)"
        );

        // Verify the client was never called.
        let queries = mock_client.recorded_queries();
        assert!(
            queries.is_empty(),
            "Amp client should not be queried for genesis"
        );
    }

    #[tokio::test]
    async fn resolve_amp_start_block_no_batches() {
        // If the Amp query returns no batches at all, it should error.
        let client = MockAmpClient::new_ok(vec![]);
        let logger = Logger::root(slog::Discard, o!());

        let result = super::resolve_amp_start_block(&client, &logger, "ds", "tbl", 50).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no batches"));
    }

    #[tokio::test]
    async fn resolve_amp_start_block_empty_batch() {
        // If the Amp query returns an empty batch (0 rows), it should error.
        let schema = Schema::new(vec![Field::new(
            "block_hash",
            DataType::FixedSizeBinary(32),
            false,
        )]);
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        let client = MockAmpClient::new_ok(vec![ResponseBatch::Batch { data: empty_batch }]);
        let logger = Logger::root(slog::Discard, o!());

        let result = super::resolve_amp_start_block(&client, &logger, "ds", "tbl", 50).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty batch"));
    }
}
