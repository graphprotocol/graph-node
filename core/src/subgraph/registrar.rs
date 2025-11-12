use std::collections::HashSet;

use async_trait::async_trait;
use graph::blockchain::Blockchain;
use graph::blockchain::BlockchainKind;
use graph::blockchain::BlockchainMap;
use graph::components::link_resolver::LinkResolverContext;
use graph::components::store::{DeploymentId, DeploymentLocator, SubscriptionManager};
use graph::components::subgraph::Settings;
use graph::data::subgraph::schema::DeploymentCreate;
use graph::data::subgraph::Graft;
use graph::data::value::Word;
use graph::futures03;
use graph::futures03::future::TryFutureExt;
use graph::futures03::Stream;
use graph::futures03::StreamExt;
use graph::prelude::{
    CreateSubgraphResult, SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait,
    SubgraphRegistrar as SubgraphRegistrarTrait, *,
};
use graph::util::futures::retry_strategy;
use graph::util::futures::RETRY_DEFAULT_LIMIT;
use tokio_retry::Retry;

pub struct SubgraphRegistrar<P, S, SM> {
    logger: Logger,
    logger_factory: LoggerFactory,
    resolver: Arc<dyn LinkResolver>,
    provider: Arc<P>,
    store: Arc<S>,
    subscription_manager: Arc<SM>,
    chains: Arc<BlockchainMap>,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
    assignment_event_stream_cancel_guard: CancelGuard, // cancels on drop
    settings: Arc<Settings>,
}

impl<P, S, SM> SubgraphRegistrar<P, S, SM>
where
    P: SubgraphAssignmentProviderTrait,
    S: SubgraphStore,
    SM: SubscriptionManager,
{
    pub fn new(
        logger_factory: &LoggerFactory,
        resolver: Arc<dyn LinkResolver>,
        provider: Arc<P>,
        store: Arc<S>,
        subscription_manager: Arc<SM>,
        chains: Arc<BlockchainMap>,
        node_id: NodeId,
        version_switching_mode: SubgraphVersionSwitchingMode,
        settings: Arc<Settings>,
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
            chains,
            node_id,
            version_switching_mode,
            assignment_event_stream_cancel_guard: CancelGuard::new(),
            settings,
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
                    if &assigned == &self.node_id {
                        if is_paused {
                            // Subgraph is paused, so we don't start it
                            debug!(logger, "Deployment assignee is this node"; "assigned_to" => assigned, "paused" => is_paused, "action" => "ignore");
                            return;
                        }

                        // Start subgraph on this node
                        debug!(logger, "Deployment assignee is this node"; "assigned_to" => assigned, "action" => "add");
                        self.provider.start(deployment, None).await;
                    } else {
                        // Ensure it is removed from this node
                        debug!(logger, "Deployment assignee is not this node"; "assigned_to" => assigned, "action" => "remove");
                        self.provider.stop(deployment).await
                    }
                } else {
                    // Was added/updated, but is now gone.
                    debug!(self.logger, "Deployment assignee not found in database"; "action" => "ignore");
                }
            }
            AssignmentOperation::Removed => {
                // Send remove event without checking node ID.
                // If node ID does not match, then this is a no-op when handled in
                // assignment provider.
                self.provider.stop(deployment).await;
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
                provider.start(id, None).await;
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
impl<P, S, SM> SubgraphRegistrarTrait for SubgraphRegistrar<P, S, SM>
where
    P: SubgraphAssignmentProviderTrait,
    S: SubgraphStore,
    SM: SubscriptionManager,
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

        // Give priority to deployment specific history_blocks value.
        let history_blocks =
            history_blocks.or(self.settings.for_name(&name).map(|c| c.history_blocks));

        let deployment_locator = match kind {
            BlockchainKind::Ethereum => {
                create_subgraph_version::<graph_chain_ethereum::Chain, _>(
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
                    history_blocks,
                )
                .await?
            }
            BlockchainKind::Near => {
                create_subgraph_version::<graph_chain_near::Chain, _>(
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
                    history_blocks,
                )
                .await?
            }
            BlockchainKind::Substreams => {
                create_subgraph_version::<graph_chain_substreams::Chain, _>(
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
                    history_blocks,
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
                .block_pointer_from_number(&logger, min_start_block - 1)
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

async fn create_subgraph_version<C: Blockchain, S: SubgraphStore>(
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
    history_blocks_override: Option<i32>,
) -> Result<DeploymentLocator, SubgraphRegistrarError> {
    let raw_string = serde_yaml::to_string(&raw).unwrap();

    let unvalidated = UnvalidatedSubgraphManifest::<C>::resolve(
        deployment.clone(),
        raw,
        &resolver,
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

    let chain = chains
        .get::<C>(network_name.clone())
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
        None => resolve_start_block(&manifest, &*chain, &logger).await?,
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
            network_name.into(),
            version_switching_mode,
        )
        .await
        .map_err(SubgraphRegistrarError::SubgraphDeploymentError)
}
