use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, iter};

use async_trait::async_trait;
use lazy_static::lazy_static;

use graph::components::ethereum::EthereumNetworks;
use graph::data::subgraph::schema::{
    generate_entity_id, SubgraphDeploymentAssignmentEntity, SubgraphDeploymentEntity,
    SubgraphEntity, SubgraphVersionEntity, TypedEntity,
};
use graph::prelude::{
    CreateSubgraphResult, SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait,
    SubgraphRegistrar as SubgraphRegistrarTrait, *,
};

lazy_static! {
    // The timeout for IPFS requests in seconds
    pub static ref IPFS_SUBGRAPH_LOADING_TIMEOUT: Duration = Duration::from_secs(
        env::var("GRAPH_IPFS_SUBGRAPH_LOADING_TIMEOUT")
            .unwrap_or("60".into())
            .parse::<u64>()
            .expect("invalid IPFS subgraph loading timeout")
    );
}

pub struct SubgraphRegistrar<L, P, S, CS> {
    logger: Logger,
    logger_factory: LoggerFactory,
    resolver: Arc<L>,
    provider: Arc<P>,
    store: Arc<S>,
    chain_stores: HashMap<String, Arc<CS>>,
    ethereum_networks: EthereumNetworks,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
    assignment_event_stream_cancel_guard: CancelGuard, // cancels on drop
}

impl<L, P, S, CS> SubgraphRegistrar<L, P, S, CS>
where
    L: LinkResolver + Clone,
    P: SubgraphAssignmentProviderTrait,
    S: Store + SubgraphDeploymentStore,
    CS: ChainStore,
{
    pub fn new(
        logger_factory: &LoggerFactory,
        resolver: Arc<L>,
        provider: Arc<P>,
        store: Arc<S>,
        chain_stores: HashMap<String, Arc<CS>>,
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
            chain_stores,
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

        store
            .subscribe(vec![
                SubgraphDeploymentAssignmentEntity::subgraph_entity_pair(),
            ])
            .map_err(|()| format_err!("Entity change stream failed"))
            .map(|event| {
                // We're only interested in the SubgraphDeploymentAssignment change; we
                // know that there is at least one, as that is what we subscribed to
                let assignments = event
                    .changes
                    .iter()
                    .filter(|change| change.entity_type == "SubgraphDeploymentAssignment")
                    .map(|change| change.to_owned())
                    .collect::<Vec<_>>();
                stream::iter_ok(assignments)
            })
            .flatten()
            .and_then(
                move |entity_change| -> Result<Box<dyn Stream<Item = _, Error = _> + Send>, _> {
                    trace!(logger, "Received assignment change";
                                   "entity_change" => format!("{:?}", entity_change));
                    let subgraph_hash = SubgraphDeploymentId::new(entity_change.entity_id.clone())
                        .map_err(|s| {
                            format_err!(
                                "Invalid subgraph hash `{}` in assignment entity: {:#?}",
                                s,
                                entity_change.clone(),
                            )
                        })?;

                    match entity_change.operation {
                        EntityChangeOperation::Set => {
                            store
                                .get(SubgraphDeploymentAssignmentEntity::key(
                                    subgraph_hash.clone(),
                                ))
                                .map_err(|e| {
                                    format_err!("Failed to get subgraph assignment entity: {}", e)
                                })
                                .map(
                                    |entity_opt| -> Box<dyn Stream<Item = _, Error = _> + Send> {
                                        if let Some(entity) = entity_opt {
                                            if entity.get("nodeId")
                                                == Some(&node_id.to_string().into())
                                            {
                                                // Start subgraph on this node
                                                Box::new(stream::once(Ok(AssignmentEvent::Add {
                                                    subgraph_id: subgraph_hash,
                                                    node_id: node_id.clone(),
                                                })))
                                            } else {
                                                // Ensure it is removed from this node
                                                Box::new(stream::once(Ok(
                                                    AssignmentEvent::Remove {
                                                        subgraph_id: subgraph_hash,
                                                        node_id: node_id.clone(),
                                                    },
                                                )))
                                            }
                                        } else {
                                            // Was added/updated, but is now gone.
                                            // We will get a separate Removed event later.
                                            Box::new(stream::empty())
                                        }
                                    },
                                )
                        }
                        EntityChangeOperation::Removed => {
                            // Send remove event without checking node ID.
                            // If node ID does not match, then this is a no-op when handled in
                            // assignment provider.
                            Ok(Box::new(stream::once(Ok(AssignmentEvent::Remove {
                                subgraph_id: subgraph_hash,
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

        // Create a query to find all assignments with this node ID
        let assignment_query = SubgraphDeploymentAssignmentEntity::query()
            .filter(EntityFilter::new_equal("nodeId", self.node_id.to_string()));

        future::result(self.store.find(assignment_query))
            .map_err(|e| format_err!("Error querying subgraph assignments: {}", e))
            .and_then(move |assignment_entities| {
                assignment_entities
                    .into_iter()
                    .map(|assignment_entity| {
                        // Parse as subgraph hash
                        assignment_entity.id().and_then(|id| {
                            SubgraphDeploymentId::new(id).map_err(|s| {
                                format_err!("Invalid subgraph hash `{}` in assignment entity", s)
                            })
                        })
                    })
                    .collect::<Result<HashSet<SubgraphDeploymentId>, _>>()
            })
            .and_then(move |subgraph_ids| {
                // This operation should finish only after all subgraphs are
                // started. We wait for the spawned tasks to complete by giving
                // each a `sender` and waiting for all of them to be dropped, so
                // the receiver terminates without receiving anything.
                let (sender, receiver) = futures01::sync::mpsc::channel::<()>(1);
                for id in subgraph_ids {
                    let sender = sender.clone();
                    let logger = logger.clone();

                    // Blocking due to store interactions. Won't be blocking after #905.
                    graph::spawn_blocking(
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
impl<L, P, S, CS> SubgraphRegistrarTrait for SubgraphRegistrar<L, P, S, CS>
where
    L: LinkResolver,
    P: SubgraphAssignmentProviderTrait,
    S: Store + SubgraphDeploymentStore,
    CS: ChainStore,
{
    async fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Result<CreateSubgraphResult, SubgraphRegistrarError> {
        create_subgraph(&self.logger, self.store.clone(), name)
    }

    async fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: SubgraphDeploymentId,
        node_id: NodeId,
    ) -> Result<(), SubgraphRegistrarError> {
        let logger = self.logger_factory.subgraph_logger(&hash);

        let unvalidated = UnvalidatedSubgraphManifest::resolve(
            hash.to_ipfs_link(),
            self.resolver.clone(),
            &logger,
        )
        .map_err(SubgraphRegistrarError::ResolveError)
        .await?;

        let (manifest, validation_warnings) = unvalidated
            .validate(self.store.clone())
            .map_err(SubgraphRegistrarError::ManifestValidationError)?;

        let network_name = manifest.network_name();

        let chain_store = self.chain_stores.get(&network_name).ok_or(
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
        remove_subgraph(&self.logger, self.store.clone(), name)
    }

    async fn reassign_subgraph(
        &self,
        hash: SubgraphDeploymentId,
        node_id: NodeId,
    ) -> Result<(), SubgraphRegistrarError> {
        reassign_subgraph(self.store.clone(), hash, node_id)
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
            subgraph_id,
            node_id: _,
        } => Ok(start_subgraph(subgraph_id, provider.clone(), logger).await),
        AssignmentEvent::Remove {
            subgraph_id,
            node_id: _,
        } => match provider.stop(subgraph_id).await {
            Ok(()) => Ok(()),
            Err(SubgraphAssignmentProviderError::NotRunning(_)) => Ok(()),
            Err(e) => Err(CancelableError::Error(e)),
        },
    }
}

async fn start_subgraph(
    subgraph_id: SubgraphDeploymentId,
    provider: Arc<impl SubgraphAssignmentProviderTrait>,
    logger: Logger,
) {
    trace!(
        logger,
        "Start subgraph";
        "subgraph_id" => subgraph_id.to_string()
    );

    let start_time = Instant::now();
    let result = provider.start(&subgraph_id).await;

    debug!(
        logger,
        "Subgraph started";
        "subgraph_id" => subgraph_id.to_string(),
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
                "error" => e.to_string(),
                "subgraph_id" => subgraph_id.to_string()
            );
        }
    }
}

fn create_subgraph(
    logger: &Logger,
    store: Arc<impl Store>,
    name: SubgraphName,
) -> Result<CreateSubgraphResult, SubgraphRegistrarError> {
    let mut ops = vec![];

    // Check if this subgraph already exists
    let subgraph_entity_opt = store.find_one(
        SubgraphEntity::query().filter(EntityFilter::new_equal("name", name.to_string())),
    )?;
    if subgraph_entity_opt.is_some() {
        debug!(
            logger,
            "Subgraph name already exists: {:?}",
            name.to_string()
        );
        return Err(SubgraphRegistrarError::NameExists(name.to_string()));
    }

    ops.push(SubgraphEntity::abort_unless(
        "Subgraph entity should not exist",
        EntityFilter::new_equal("name", name.to_string()),
        vec![],
    ));

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let entity = SubgraphEntity::new(name.clone(), None, None, created_at);
    let entity_id = generate_entity_id();
    ops.extend(
        entity
            .write_operations(&entity_id)
            .into_iter()
            .map(|op| op.into()),
    );

    store.apply_metadata_operations(ops)?;

    debug!(logger, "Created subgraph"; "subgraph_name" => name.to_string());

    Ok(CreateSubgraphResult { id: entity_id })
}

/// Resolves the subgraph's earliest block and the manifest's graft base block
fn resolve_subgraph_chain_blocks(
    manifest: SubgraphManifest,
    chain_store: Arc<impl ChainStore>,
    ethereum_adapter: Arc<dyn EthereumAdapter>,
    logger: &Logger,
) -> Box<
    dyn Future<
            Item = (
                Option<EthereumBlockPointer>,
                Option<(SubgraphDeploymentId, EthereumBlockPointer)>,
            ),
            Error = SubgraphRegistrarError,
        > + Send,
> {
    let logger1 = logger.clone();
    let chain_store1 = chain_store.clone();

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
            match manifest.graft {
                None => Box::new(future::ok(None)) as Box<dyn Future<Item = _, Error = _> + Send>,
                Some(base) => {
                    let base_block = base.block;
                    Box::new(
                        ethereum_adapter
                            .block_pointer_from_number(
                                &logger1,
                                chain_store1.clone(),
                                base.block as u64,
                            )
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

struct SubraphVersionUpdatingMetadata {
    subgraph_entity_id: String,
    version_entity_id: String,
    current_is_synced: bool,
    current_version_id_opt: Option<String>,
    pending_version_id_opt: Option<String>,
    version_summaries_before: Vec<SubgraphVersionSummary>,
    version_summaries_after: Vec<SubgraphVersionSummary>,
    read_summaries_ops: Vec<MetadataOperation>,
}

fn get_version_ids_and_summaries(
    logger: Logger,
    store: Arc<impl Store>,
    name: String,
    manifest_id: SubgraphDeploymentId,
    version_switching_mode: SubgraphVersionSwitchingMode,
) -> Result<SubraphVersionUpdatingMetadata, SubgraphRegistrarError> {
    let name = name.clone();
    // Look up subgraph entity by name
    let subgraph_entity_opt = store
        .find_one(SubgraphEntity::query().filter(EntityFilter::new_equal("name", name.clone())))?;
    let subgraph_entity = subgraph_entity_opt.ok_or_else(|| {
        debug!(
            logger,
            "Subgraph not found, could not create_subgraph_version";
            "subgraph_name" => name.to_string()
        );
        SubgraphRegistrarError::NameNotFound(name)
    })?;
    let subgraph_entity_id = subgraph_entity.id()?;
    let current_version_id_opt = match subgraph_entity.get("currentVersion") {
        Some(Value::String(current_version_id)) => Some(current_version_id.to_owned()),
        Some(Value::Null) => None,
        None => None,
        Some(_) => panic!("subgraph entity has invalid type in currentVersion field"),
    };
    let pending_version_id_opt = match subgraph_entity.get("pendingVersion") {
        Some(Value::String(pending_version_id)) => Some(pending_version_id.to_owned()),
        Some(Value::Null) => None,
        None => None,
        Some(_) => panic!("subgraph entity has invalid type in pendingVersion field"),
    };

    let store = store.clone();
    // Look up current version's deployment hash
    let current_version_hash_opt = match current_version_id_opt {
        Some(ref current_version_id) => Some(get_subgraph_version_deployment_id(
            store.clone(),
            current_version_id.clone(),
        )?),
        None => None,
    };

    // Look up pending version's deployment hash
    let pending_version_hash_opt = match pending_version_id_opt {
        Some(ref pending_version_id) => Some(get_subgraph_version_deployment_id(
            store.clone(),
            pending_version_id.clone(),
        )?),
        None => None,
    };

    // See if current version is fully synced
    let current_is_synced = match &current_version_hash_opt {
        Some(hash) => store.is_deployment_synced(hash.to_owned())?,
        None => false,
    };

    // Find all subgraph version entities that point to this hash or a hash
    let (version_summaries_before, read_summaries_ops) = store.read_subgraph_version_summaries(
        iter::once(manifest_id.clone())
            .chain(current_version_hash_opt)
            .chain(pending_version_hash_opt)
            .collect(),
    )?;

    let version_entity_id = generate_entity_id();

    // Simulate the creation of the new version and updating of Subgraph.pending/current
    let version_summaries_after = match version_switching_mode {
        SubgraphVersionSwitchingMode::Instant => {
            // Previously pending or current versions will no longer be pending/current
            let mut version_summaries_after = version_summaries_before
                .clone()
                .into_iter()
                .map(|mut version_summary| {
                    if Some(&version_summary.id) == pending_version_id_opt.as_ref() {
                        version_summary.pending = false;
                    }

                    if Some(&version_summary.id) == current_version_id_opt.as_ref() {
                        version_summary.current = false;
                    }

                    version_summary
                })
                .collect::<Vec<_>>();

            // Add new version, which will immediately be current
            version_summaries_after.push(SubgraphVersionSummary {
                id: version_entity_id.clone(),
                subgraph_id: subgraph_entity_id.clone(),
                deployment_id: manifest_id.clone(),
                pending: false,
                current: true,
            });

            version_summaries_after
        }
        SubgraphVersionSwitchingMode::Synced => {
            // There is a current version. Depending on whether it is synced
            // or not, make the new version the pending or the current version
            if current_version_id_opt.is_some() {
                // Previous pending version (if there was one) is no longer pending
                // Previous current version if it's not fully synced is no
                // longer the current version
                let mut version_summaries_after = version_summaries_before
                    .clone()
                    .into_iter()
                    .map(|mut version_summary| {
                        if Some(&version_summary.id) == pending_version_id_opt.as_ref() {
                            version_summary.pending = false;
                        }
                        if !current_is_synced
                            && Some(&version_summary.id) == current_version_id_opt.as_ref()
                        {
                            // We will make the new version the current version
                            version_summary.current = false;
                        }

                        version_summary
                    })
                    .collect::<Vec<_>>();

                // Determine if this new version should be the pending or
                // current version. When we add a version to a subgraph that
                // already has a current version, the new version becomes the
                // pending version if the current version is synced,
                // and replaces the current version if the current version is still syncing
                let (pending, current) = if current_is_synced {
                    (true, false)
                } else {
                    (false, true)
                };

                // Add new version, setting pending and current appropriately
                version_summaries_after.push(SubgraphVersionSummary {
                    id: version_entity_id.clone(),
                    subgraph_id: subgraph_entity_id.clone(),
                    deployment_id: manifest_id.clone(),
                    pending,
                    current,
                });

                version_summaries_after
            } else {
                // No need to process list, as there is no current version
                let mut version_summaries_after = version_summaries_before.clone();

                // Add new version, which will immediately be current
                version_summaries_after.push(SubgraphVersionSummary {
                    id: version_entity_id.clone(),
                    subgraph_id: subgraph_entity_id.clone(),
                    deployment_id: manifest_id.clone(),
                    pending: false,
                    current: true,
                });

                version_summaries_after
            }
        }
    };
    Ok(SubraphVersionUpdatingMetadata {
        subgraph_entity_id,
        version_entity_id,
        current_is_synced,
        current_version_id_opt,
        pending_version_id_opt,
        version_summaries_before,
        version_summaries_after,
        read_summaries_ops,
    })
}

fn create_subgraph_version(
    logger: &Logger,
    store: Arc<impl Store>,
    chain_store: Arc<impl ChainStore>,
    ethereum_adapter: Arc<dyn EthereumAdapter>,
    name: SubgraphName,
    manifest: SubgraphManifest,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
) -> Box<dyn Future<Item = (), Error = SubgraphRegistrarError> + Send> {
    let logger = logger.clone();
    let manifest = manifest.clone();
    let manifest_id = manifest.id.clone();
    let store = store.clone();
    let deployment_store = store.clone();

    Box::new(
        future::result(get_version_ids_and_summaries(
            logger.clone(),
            store.clone(),
            name.to_string(),
            manifest_id.clone(),
            version_switching_mode,
        ))
        .and_then(move |subgraph_version_data| {
            let mut ops = vec![];
            ops.push(SubgraphEntity::abort_unless("Subgraph entity must still exist, have same name/currentVersion/pendingVersion",
                EntityFilter::And(vec![
                    EntityFilter::new_equal("name", name.to_string()),
                    EntityFilter::new_equal(
                        "currentVersion",
                        subgraph_version_data.current_version_id_opt.clone(),
                    ),
                    EntityFilter::new_equal(
                        "pendingVersion",
                        subgraph_version_data.pending_version_id_opt.clone(),
                    ),
                ]),
                 vec![subgraph_version_data.subgraph_entity_id.clone()],
            ));

            // Create the subgraph version entity
            let created_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            ops.extend(subgraph_version_data.read_summaries_ops);
            ops.extend(
                SubgraphVersionEntity::new(
                    subgraph_version_data.subgraph_entity_id.clone(),
                    manifest_id.clone(),
                    created_at,
                )
                .write_operations(&subgraph_version_data.version_entity_id)
                .into_iter()
                .map(|op| op.into()),
            );

            // Possibly add assignment for new deployment hash, and possibly remove
            // assignments for old current/pending
            ops.extend(
                store
                    .reconcile_assignments(
                        &logger.clone(),
                        subgraph_version_data.version_summaries_before,
                        subgraph_version_data.version_summaries_after,
                        Some(node_id),
                    )
                    .into_iter()
                    .map(|op| op.into()),
            );

            // Update current/pending versions in Subgraph entity
            match version_switching_mode {
                SubgraphVersionSwitchingMode::Instant => {
                    ops.extend(SubgraphEntity::update_pending_version_operations(
                        &subgraph_version_data.subgraph_entity_id,
                        None,
                    ));
                    ops.extend(SubgraphEntity::update_current_version_operations(
                        &subgraph_version_data.subgraph_entity_id,
                        Some(subgraph_version_data.version_entity_id),
                    ));
                }
                SubgraphVersionSwitchingMode::Synced => {
                    // Set the new version as pending, unless there is no fully synced
                    // current versions
                    if subgraph_version_data.current_is_synced {
                        ops.extend(SubgraphEntity::update_pending_version_operations(
                            &subgraph_version_data.subgraph_entity_id,
                            Some(subgraph_version_data.version_entity_id),
                        ));
                    } else {
                        ops.extend(SubgraphEntity::update_pending_version_operations(
                            &subgraph_version_data.subgraph_entity_id,
                            None,
                        ));
                        ops.extend(SubgraphEntity::update_current_version_operations(
                            &subgraph_version_data.subgraph_entity_id,
                            Some(subgraph_version_data.version_entity_id),
                        ));
                    }
                }
            }

            // Check if deployment exists
            store
                .get(SubgraphDeploymentEntity::key(manifest_id.clone()))
                .map_err(|e| SubgraphRegistrarError::QueryExecutionError(e))
                .map(|a| (logger, ops, a.is_some()))
        })
        .and_then(move |(logger, mut ops, deployment_exists)| {
            ops.push(SubgraphDeploymentEntity::abort_unless("Subgraph deployment entity must continue to exist/not exist"
                    ,
                EntityFilter::new_equal("id", manifest.id.to_string()),
                if deployment_exists {
                    vec![manifest.id.to_string()]
                } else {
                    vec![]
                },
            ));

            resolve_subgraph_chain_blocks(
                manifest.clone(),
                chain_store.clone(),
                ethereum_adapter.clone(),
                &logger.clone(),
            )
            .and_then(move |(start_block, base_block)| {
                info!(
                    logger,
                    "Set subgraph start block";
                    "block_number" => format!("{:?}", start_block.map(|block| block.number)),
                    "block_hash" => format!("{:?}", start_block.map(|block| block.hash)),
                );

                info!(
                    logger,
                    "Graft base";
                    "base" => format!("{:?}", base_block.as_ref().map(|(subgraph,_)| subgraph.to_string())),
                    "block" => format!("{:?}", base_block.as_ref().map(|(_,ptr)| ptr.number))
                );

                // Apply the subgraph versioning and deployment operations,
                // creating a new subgraph deployment if one doesn't exist.
                if deployment_exists {
                    deployment_store
                        .apply_metadata_operations(ops)
                        .map_err(|e| SubgraphRegistrarError::SubgraphDeploymentError(e))
                } else {
                    let deployment = SubgraphDeploymentEntity::new(
                        &manifest,
                        false,
                        start_block,
                    ).graft(base_block);
                    ops.extend(
                        deployment
                        .create_operations(&manifest.id),
                    );
                    deployment_store
                        .create_subgraph_deployment(&manifest.schema, ops)
                        .map_err(|e| SubgraphRegistrarError::SubgraphDeploymentError(e))
                }
            })
        }),
    )
}

fn get_subgraph_version_deployment_id(
    store: Arc<impl Store>,
    version_id: String,
) -> Result<SubgraphDeploymentId, SubgraphRegistrarError> {
    let version_entity = store
        .get(SubgraphVersionEntity::key(version_id))?
        .ok_or_else(|| TransactionAbortError::Other(format!("Subgraph version entity missing")))
        .map_err(StoreError::from)?;

    Ok(SubgraphDeploymentId::new(
        version_entity
            .get("deployment")
            .unwrap()
            .to_owned()
            .as_string()
            .unwrap(),
    )
    .unwrap())
}

fn remove_subgraph(
    logger: &Logger,
    store: Arc<impl Store>,
    name: SubgraphName,
) -> Result<(), SubgraphRegistrarError> {
    let mut ops = vec![];

    // Find the subgraph entity
    let subgraph_entity_opt = store
        .find_one(SubgraphEntity::query().filter(EntityFilter::new_equal("name", name.to_string())))
        .map_err(|e| format_err!("query execution error: {}", e))?;
    let subgraph_entity = subgraph_entity_opt
        .ok_or_else(|| SubgraphRegistrarError::NameNotFound(name.to_string()))?;

    ops.push(SubgraphEntity::abort_unless(
        "Subgraph entity must still exist",
        EntityFilter::new_equal("name", name.to_string()),
        vec![subgraph_entity.id().unwrap()],
    ));

    // Find subgraph version entities
    let subgraph_version_entities = store.find(SubgraphVersionEntity::query().filter(
        EntityFilter::new_equal("subgraph", subgraph_entity.id().unwrap()),
    ))?;

    ops.push(SubgraphVersionEntity::abort_unless(
        "Subgraph must have same set of versions",
        EntityFilter::new_equal("subgraph", subgraph_entity.id().unwrap()),
        subgraph_version_entities
            .iter()
            .map(|entity| entity.id().unwrap())
            .collect(),
    ));

    // Remove subgraph version entities, and their deployment/assignment when applicable
    ops.extend(
        remove_subgraph_versions(logger, store.clone(), subgraph_version_entities)?
            .into_iter()
            .map(|op| op.into()),
    );

    // Remove the subgraph entity
    ops.push(MetadataOperation::Remove {
        entity: SubgraphEntity::TYPENAME,
        id: subgraph_entity.id()?,
    });

    store.apply_metadata_operations(ops)?;

    debug!(logger, "Removed subgraph"; "subgraph_name" => name.to_string());

    Ok(())
}

/// Remove a set of subgraph versions atomically.
///
/// It may seem like it would be easier to generate the EntityOperations for subgraph versions
/// removal one at a time, but that approach is significantly complicated by the fact that the
/// store does not reflect the EntityOperations that have been accumulated so far. Earlier subgraph
/// version creations/removals can affect later ones by affecting whether or not a subgraph deployment
/// or assignment needs to be created/removed.
fn remove_subgraph_versions(
    logger: &Logger,
    store: Arc<impl Store>,
    version_entities_to_delete: Vec<Entity>,
) -> Result<Vec<MetadataOperation>, SubgraphRegistrarError> {
    let mut ops = vec![];

    let version_entity_ids_to_delete = version_entities_to_delete
        .iter()
        .map(|version_entity| version_entity.id().unwrap())
        .collect::<HashSet<_>>();

    // Get hashes that are referenced by versions that will be deleted.
    // These are candidates for clean up.
    let referenced_subgraph_hashes = version_entities_to_delete
        .iter()
        .map(|version_entity| {
            SubgraphDeploymentId::new(
                version_entity
                    .get("deployment")
                    .unwrap()
                    .to_owned()
                    .as_string()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect::<HashSet<_>>();

    // Find all subgraph version entities that point to these subgraph deployments
    let (version_summaries, read_summaries_ops) =
        store.read_subgraph_version_summaries(referenced_subgraph_hashes.into_iter().collect())?;
    ops.extend(read_summaries_ops);

    // Simulate the planned removal of SubgraphVersion entities
    let version_summaries_after_delete = version_summaries
        .clone()
        .into_iter()
        .filter(|version_summary| !version_entity_ids_to_delete.contains(&version_summary.id))
        .collect::<Vec<_>>();

    // Create/remove assignments based on the subgraph version changes.
    // We are only deleting versions here, so no assignments will be created,
    // and we can safely pass None for the node ID.
    ops.extend(
        store
            .reconcile_assignments(
                logger,
                version_summaries,
                version_summaries_after_delete,
                None,
            )
            .into_iter()
            .map(|op| op.into()),
    );

    // Actually remove the subgraph version entities.
    // Note: we do this last because earlier AbortUnless ops depend on these entities still
    // existing.
    ops.extend(
        version_entities_to_delete
            .iter()
            .map(|version_entity| MetadataOperation::Remove {
                entity: SubgraphVersionEntity::TYPENAME,
                id: version_entity.id().unwrap(),
            }),
    );

    Ok(ops)
}

/// Reassign a subgraph deployment to a different node.
///
/// Reassigning to a nodeId that does not match any reachable graph-nodes will effectively pause the
/// subgraph syncing process.
fn reassign_subgraph(
    store: Arc<impl Store>,
    hash: SubgraphDeploymentId,
    node_id: NodeId,
) -> Result<(), SubgraphRegistrarError> {
    let mut ops = vec![];

    let current_deployment = store.find(
        SubgraphDeploymentAssignmentEntity::query()
            .filter(EntityFilter::new_equal("id", hash.clone().to_string())),
    )?;

    let current_node_id = current_deployment
        .first()
        .and_then(|d| d.get("nodeId"))
        .ok_or_else(|| SubgraphRegistrarError::DeploymentNotFound(hash.clone().to_string()))?;

    if current_node_id.to_string() == node_id.to_string() {
        return Err(SubgraphRegistrarError::DeploymentAssignmentUnchanged(
            hash.clone().to_string(),
        ));
    }

    ops.push(SubgraphDeploymentAssignmentEntity::abort_unless(
        "Deployment assignment is unchanged",
        EntityFilter::And(vec![
            EntityFilter::new_equal("nodeId", current_node_id.to_string()),
            EntityFilter::new_equal("id", hash.clone().to_string()),
        ]),
        vec![hash.clone().to_string()],
    ));

    // Create the assignment update operations.
    // Note: This will also generate a remove operation for the existing subgraph assignment.
    ops.extend(
        SubgraphDeploymentAssignmentEntity::new(node_id)
            .write_operations(&hash.clone())
            .into_iter()
            .map(|op| op.into()),
    );

    store.apply_metadata_operations(ops)?;

    Ok(())
}
