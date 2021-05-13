use std::collections::HashSet;
use std::env;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lazy_static::lazy_static;

use graph::components::store::{DeploymentId, DeploymentLocator, SubscriptionManager};
use graph::data::subgraph::schema::SubgraphDeploymentEntity;
use graph::prelude::{
    CreateSubgraphResult, SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait,
    SubgraphRegistrar as SubgraphRegistrarTrait, *,
};
use graph::{blockchain::DataSource, components::store::BlockStore};
use graph_chain_ethereum::{EthereumAdapterTrait, EthereumNetworks};

lazy_static! {
    // The timeout for IPFS requests in seconds
    pub static ref IPFS_SUBGRAPH_LOADING_TIMEOUT: Duration = Duration::from_secs(
        env::var("GRAPH_IPFS_SUBGRAPH_LOADING_TIMEOUT")
            .unwrap_or("60".into())
            .parse::<u64>()
            .expect("invalid IPFS subgraph loading timeout")
    );
}

pub struct SubgraphRegistrar<L, P, S, BS, SM> {
    logger: Logger,
    logger_factory: LoggerFactory,
    resolver: Arc<L>,
    provider: Arc<P>,
    store: Arc<S>,
    subscription_manager: Arc<SM>,
    block_store: Arc<BS>,
    ethereum_networks: EthereumNetworks,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
    assignment_event_stream_cancel_guard: CancelGuard, // cancels on drop
}

impl<L, P, S, BS, SM> SubgraphRegistrar<L, P, S, BS, SM>
where
    L: LinkResolver + Clone,
    P: SubgraphAssignmentProviderTrait,
    S: SubgraphStore,
    BS: BlockStore,
    SM: SubscriptionManager,
{
    pub fn new(
        logger_factory: &LoggerFactory,
        resolver: Arc<L>,
        provider: Arc<P>,
        store: Arc<S>,
        subscription_manager: Arc<SM>,
        block_store: Arc<BS>,
        ethereum_networks: EthereumNetworks,
        node_id: NodeId,
        version_switching_mode: SubgraphVersionSwitchingMode,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphRegistrar", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        SubgraphRegistrar {
            logger,
            logger_factory,
            resolver: Arc::new(
                resolver
                    .as_ref()
                    .clone()
                    .with_timeout(*IPFS_SUBGRAPH_LOADING_TIMEOUT)
                    .with_retries(),
            ),
            provider,
            store,
            subscription_manager,
            block_store,
            ethereum_networks,
            node_id,
            version_switching_mode,
            assignment_event_stream_cancel_guard: CancelGuard::new(),
        }
    }

    pub fn start(&self) -> impl Future<Item = (), Error = Error> {
        let logger_clone1 = self.logger.clone();
        let logger_clone2 = self.logger.clone();
        let provider = self.provider.clone();
        let node_id = self.node_id.clone();
        let assignment_event_stream_cancel_handle =
            self.assignment_event_stream_cancel_guard.handle();

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
        // - The event stream sees an Add event for subgraph A, but the table query finds that
        //   subgraph A is already in the table.
        // - The event stream sees a Remove event for subgraph B, but the table query finds that
        //   subgraph B has already been removed.
        // The `handle_assignment_events` function handles these cases by ignoring AlreadyRunning
        // (on subgraph start) or NotRunning (on subgraph stop) error types, which makes the
        // operations idempotent.

        // Start event stream
        let assignment_event_stream = self.assignment_events();

        // Deploy named subgraphs found in store
        self.start_assigned_subgraphs().and_then(move |()| {
            // Spawn a task to handle assignment events.
            // Blocking due to store interactions. Won't be blocking after #905.
            graph::spawn_blocking(
                assignment_event_stream
                    .map_err(SubgraphAssignmentProviderError::Unknown)
                    .map_err(CancelableError::Error)
                    .cancelable(&assignment_event_stream_cancel_handle, || {
                        CancelableError::Cancel
                    })
                    .for_each(move |assignment_event| {
                        assert_eq!(assignment_event.node_id(), &node_id);
                        handle_assignment_event(
                            assignment_event,
                            provider.clone(),
                            logger_clone1.clone(),
                        )
                        .boxed()
                        .compat()
                    })
                    .map_err(move |e| match e {
                        CancelableError::Cancel => panic!("assignment event stream canceled"),
                        CancelableError::Error(e) => {
                            error!(logger_clone2, "Assignment event stream failed: {}", e);
                            panic!("assignment event stream failed: {}", e);
                        }
                    })
                    .compat(),
            );

            Ok(())
        })
    }

    pub fn assignment_events(&self) -> impl Stream<Item = AssignmentEvent, Error = Error> + Send {
        let store = self.store.clone();
        let node_id = self.node_id.clone();
        let logger = self.logger.clone();

        self.subscription_manager
            .subscribe(vec![SubscriptionFilter::Assignment])
            .map_err(|()| anyhow!("Entity change stream failed"))
            .map(|event| {
                // We're only interested in the SubgraphDeploymentAssignment change; we
                // know that there is at least one, as that is what we subscribed to
                let filter = SubscriptionFilter::Assignment;
                let assignments = event
                    .changes
                    .iter()
                    .filter(|change| filter.matches(change))
                    .map(|change| match change {
                        EntityChange::Data { .. } => unreachable!(),
                        EntityChange::Assignment {
                            deployment,
                            operation,
                        } => (deployment.clone(), operation.clone()),
                    })
                    .collect::<Vec<_>>();
                stream::iter_ok(assignments)
            })
            .flatten()
            .and_then(
                move |(deployment, operation)| -> Result<Box<dyn Stream<Item = _, Error = _> + Send>, _> {
                    trace!(logger, "Received assignment change";
                                   "deployment" => &deployment,
                                   "operation" => format!("{:?}", operation));

                    match operation {
                        EntityChangeOperation::Set => {
                            store
                                .assigned_node(&deployment)
                                .map_err(|e| {
                                    anyhow!("Failed to get subgraph assignment entity: {}", e)
                                })
                                .map(|assigned| -> Box<dyn Stream<Item = _, Error = _> + Send> {
                                    if let Some(assigned) = assigned {
                                        if assigned == node_id {
                                            // Start subgraph on this node
                                            Box::new(stream::once(Ok(AssignmentEvent::Add {
                                                deployment,
                                                node_id: node_id.clone(),
                                            })))
                                        } else {
                                            // Ensure it is removed from this node
                                            Box::new(stream::once(Ok(AssignmentEvent::Remove {
                                                deployment,
                                                node_id: node_id.clone(),
                                            })))
                                        }
                                    } else {
                                        // Was added/updated, but is now gone.
                                        // We will get a separate Removed event later.
                                        Box::new(stream::empty())
                                    }
                                })
                        }
                        EntityChangeOperation::Removed => {
                            // Send remove event without checking node ID.
                            // If node ID does not match, then this is a no-op when handled in
                            // assignment provider.
                            Ok(Box::new(stream::once(Ok(AssignmentEvent::Remove {
                                deployment,
                                node_id: node_id.clone(),
                            }))))
                        }
                    }
                },
            )
            .flatten()
    }

    fn start_assigned_subgraphs(&self) -> impl Future<Item = (), Error = Error> {
        let provider = self.provider.clone();
        let logger = self.logger.clone();

        future::result(self.store.assignments(&self.node_id))
            .map_err(|e| anyhow!("Error querying subgraph assignments: {}", e))
            .and_then(move |deployments| {
                // This operation should finish only after all subgraphs are
                // started. We wait for the spawned tasks to complete by giving
                // each a `sender` and waiting for all of them to be dropped, so
                // the receiver terminates without receiving anything.
                let deployments = HashSet::<DeploymentLocator>::from_iter(deployments);
                let (sender, receiver) = futures01::sync::mpsc::channel::<()>(1);
                for id in deployments {
                    let sender = sender.clone();
                    let logger = logger.clone();

                    graph::spawn(
                        start_subgraph(id, provider.clone(), logger).map(move |()| drop(sender)),
                    );
                }
                drop(sender);
                receiver.collect().then(move |_| {
                    info!(logger, "Started all subgraphs");
                    future::ok(())
                })
            })
    }
}

#[async_trait]
impl<L, P, S, BS, SM> SubgraphRegistrarTrait for SubgraphRegistrar<L, P, S, BS, SM>
where
    L: LinkResolver,
    P: SubgraphAssignmentProviderTrait,
    S: SubgraphStore,
    BS: BlockStore,
    SM: SubscriptionManager,
{
    async fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Result<CreateSubgraphResult, SubgraphRegistrarError> {
        let id = self.store.create_subgraph(name.clone())?;

        debug!(self.logger, "Created subgraph"; "subgraph_name" => name.to_string());

        Ok(CreateSubgraphResult { id })
    }

    async fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: DeploymentHash,
        node_id: NodeId,
    ) -> Result<(), SubgraphRegistrarError> {
        // We don't have a location for the subgraph yet; that will be
        // assigned when we deploy for real. For logging purposes, make up a
        // fake locator
        let logger = self
            .logger_factory
            .subgraph_logger(&DeploymentLocator::new(DeploymentId(0), hash.clone()));

        let unvalidated = UnvalidatedSubgraphManifest::<graph_chain_ethereum::DataSource>::resolve(
            hash,
            self.resolver.clone(),
            &logger,
        )
        .map_err(SubgraphRegistrarError::ResolveError)
        .await?;

        let (manifest, validation_warnings) = unvalidated
            .validate(self.store.clone())
            .map_err(SubgraphRegistrarError::ManifestValidationError)?;

        let network_name = manifest.network_name();

        let chain_store = self.block_store.chain_store(&network_name).ok_or(
            SubgraphRegistrarError::NetworkNotSupported(network_name.clone()),
        )?;

        let subgraph_eth_requirements = manifest.required_ethereum_capabilities();

        let ethereum_adapter = self
            .ethereum_networks
            .adapter_with_capabilities(network_name.clone(), &subgraph_eth_requirements)
            .map_err(|_| {
                SubgraphRegistrarError::SubgraphNetworkRequirementsNotSupported(
                    network_name,
                    subgraph_eth_requirements,
                )
            })?;

        let manifest_id = manifest.id.clone();
        create_subgraph_version(
            &logger,
            self.store.clone(),
            chain_store.clone(),
            ethereum_adapter.clone(),
            name.clone(),
            manifest,
            node_id,
            self.version_switching_mode,
        )
        .compat()
        .await?;

        debug!(
            &logger,
            "Wrote new subgraph version to store";
            "subgraph_name" => name.to_string(),
            "subgraph_hash" => manifest_id.to_string(),
            "validation_warnings" => format!("{:?}", validation_warnings),
        );

        Ok(())
    }

    async fn remove_subgraph(&self, name: SubgraphName) -> Result<(), SubgraphRegistrarError> {
        self.store.clone().remove_subgraph(name.clone())?;

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
        let locations = self.store.locators(hash)?;
        let deployment = match locations.len() {
            0 => return Err(SubgraphRegistrarError::DeploymentNotFound(hash.to_string())),
            1 => locations[0].clone(),
            _ => {
                return Err(SubgraphRegistrarError::StoreError(
                    anyhow!(
                        "there are {} different deployments with id {}",
                        locations.len(),
                        hash.as_str()
                    )
                    .into(),
                ))
            }
        };
        self.store.reassign_subgraph(&deployment, node_id)?;

        Ok(())
    }
}

async fn handle_assignment_event(
    event: AssignmentEvent,
    provider: Arc<impl SubgraphAssignmentProviderTrait>,
    logger: Logger,
) -> Result<(), CancelableError<SubgraphAssignmentProviderError>> {
    let logger = logger.to_owned();

    debug!(logger, "Received assignment event: {:?}", event);

    match event {
        AssignmentEvent::Add {
            deployment,
            node_id: _,
        } => Ok(start_subgraph(deployment, provider.clone(), logger).await),
        AssignmentEvent::Remove {
            deployment,
            node_id: _,
        } => match provider.stop(deployment).await {
            Ok(()) => Ok(()),
            Err(SubgraphAssignmentProviderError::NotRunning(_)) => Ok(()),
            Err(e) => Err(CancelableError::Error(e)),
        },
    }
}

async fn start_subgraph(
    deployment: DeploymentLocator,
    provider: Arc<impl SubgraphAssignmentProviderTrait>,
    logger: Logger,
) {
    let logger = logger
        .new(o!("subgraph_id" => deployment.hash.to_string(), "sgd" => deployment.id.to_string()));

    trace!(logger, "Start subgraph");

    let start_time = Instant::now();
    let result = provider.start(deployment.clone()).await;

    debug!(
        logger,
        "Subgraph started";
        "start_ms" => start_time.elapsed().as_millis()
    );

    match result {
        Ok(()) => (),
        Err(SubgraphAssignmentProviderError::AlreadyRunning(_)) => (),
        Err(e) => {
            // Errors here are likely an issue with the subgraph.
            error!(
                logger,
                "Subgraph instance failed to start";
                "error" => e.to_string()
            );
        }
    }
}

/// Resolves the subgraph's earliest block and the manifest's graft base block
fn resolve_subgraph_chain_blocks(
    manifest: &SubgraphManifest<impl DataSource>,
    chain_store: Arc<impl ChainStore>,
    ethereum_adapter: Arc<dyn EthereumAdapterTrait>,
    logger: &Logger,
) -> Box<
    dyn Future<
            Item = (Option<BlockPtr>, Option<(DeploymentHash, BlockPtr)>),
            Error = SubgraphRegistrarError,
        > + Send,
> {
    let logger1 = logger.clone();
    let chain_store1 = chain_store.clone();
    let graft = manifest.graft.clone();

    Box::new(
        // If the minimum start block is 0 (i.e. the genesis block),
        // return `None` to start indexing from the genesis block. Otherwise
        // return a block pointer for the block with number `min_start_block - 1`.
        match manifest
            .start_blocks()
            .into_iter()
            .min()
            .expect("cannot identify minimum start block because there are no data sources")
        {
            0 => Box::new(future::ok(None)) as Box<dyn Future<Item = _, Error = _> + Send>,
            min_start_block => Box::new(
                ethereum_adapter
                    .block_pointer_from_number(logger, chain_store.clone(), min_start_block - 1)
                    .map(Some)
                    .map_err(move |_| {
                        SubgraphRegistrarError::ManifestValidationError(vec![
                            SubgraphManifestValidationError::BlockNotFound(
                                min_start_block.to_string(),
                            ),
                        ])
                    }),
            ) as Box<dyn Future<Item = _, Error = _> + Send>,
        }
        .and_then(move |start_block_ptr| {
            match graft {
                None => Box::new(future::ok(None)) as Box<dyn Future<Item = _, Error = _> + Send>,
                Some(base) => {
                    let base_block = base.block;
                    Box::new(
                        ethereum_adapter
                            .block_pointer_from_number(&logger1, chain_store1.clone(), base.block)
                            .map(|ptr| Some((base.base, ptr)))
                            .map_err(move |_| {
                                SubgraphRegistrarError::ManifestValidationError(vec![
                                    SubgraphManifestValidationError::BlockNotFound(format!(
                                        "graft base block {} not found",
                                        base_block
                                    )),
                                ])
                            }),
                    ) as Box<dyn Future<Item = _, Error = _> + Send>
                }
            }
            .map(move |base_ptr| (start_block_ptr, base_ptr))
        }),
    )
}

fn create_subgraph_version(
    logger: &Logger,
    store: Arc<impl SubgraphStore>,
    chain_store: Arc<impl ChainStore>,
    ethereum_adapter: Arc<dyn EthereumAdapterTrait>,
    name: SubgraphName,
    manifest: SubgraphManifest<impl DataSource>,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
) -> Box<dyn Future<Item = (), Error = SubgraphRegistrarError> + Send> {
    let logger = logger.clone();
    let store = store.clone();
    let deployment_store = store.clone();

    match store.subgraph_exists(&name) {
        Err(e) => return Box::new(future::err(e.into())),
        Ok(false) => {
            debug!(
                logger,
                "Subgraph not found, could not create_subgraph_version";
                "subgraph_name" => name.to_string()
            );
            return Box::new(future::err(SubgraphRegistrarError::NameNotFound(
                name.to_string(),
            )));
        }
        Ok(true) => { /* everything is fine, continue */ }
    }

    Box::new(
            resolve_subgraph_chain_blocks(
                &manifest,
                chain_store.clone(),
                ethereum_adapter.clone(),
                &logger.clone(),
            )
            .and_then(move |(start_block, base_block)| {
                info!(
                    logger,
                    "Set subgraph start block";
                    "block_number" => format!("{:?}", start_block.as_ref().map(|block| block.number)),
                    "block_hash" => format!("{:?}", start_block.as_ref().map(|block| &block.hash)),
                );

                info!(
                    logger,
                    "Graft base";
                    "base" => format!("{:?}", base_block.as_ref().map(|(subgraph,_)| subgraph.to_string())),
                    "block" => format!("{:?}", base_block.as_ref().map(|(_,ptr)| ptr.number))
                );

                // Apply the subgraph versioning and deployment operations,
                // creating a new subgraph deployment if one doesn't exist.
                let network = manifest.network_name();
                let deployment = SubgraphDeploymentEntity::new(
                        &manifest,
                        false,
                        start_block,
                    ).graft(base_block);
                    deployment_store
                        .create_subgraph_deployment(name, &manifest.schema, deployment, node_id, network, version_switching_mode)
                        .map_err(|e| SubgraphRegistrarError::SubgraphDeploymentError(e)).map(|_| ())
            })
    )
}
