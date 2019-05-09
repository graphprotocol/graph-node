use std::collections::HashSet;
use std::iter;
use std::time::{SystemTime, UNIX_EPOCH};

use super::validation;
use graph::data::subgraph::schema::*;
use graph::prelude::{
    CreateSubgraphResult, SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait,
    SubgraphRegistrar as SubgraphRegistrarTrait, *,
};

pub struct SubgraphRegistrar<L, P, S, CS> {
    logger: Logger,
    resolver: Arc<L>,
    provider: Arc<P>,
    store: Arc<S>,
    chain_store: Arc<CS>,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
    assignment_event_stream_cancel_guard: CancelGuard, // cancels on drop
}

impl<L, P, S, CS> SubgraphRegistrar<L, P, S, CS>
where
    L: LinkResolver,
    P: SubgraphAssignmentProviderTrait,
    S: Store,
    CS: ChainStore,
{
    pub fn new(
        logger: Logger,
        resolver: Arc<L>,
        provider: Arc<P>,
        store: Arc<S>,
        chain_store: Arc<CS>,
        node_id: NodeId,
        version_switching_mode: SubgraphVersionSwitchingMode,
    ) -> Self {
        let logger = logger.new(o!("component" => "SubgraphRegistrar"));

        SubgraphRegistrar {
            logger,
            resolver,
            provider,
            store,
            chain_store,
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
            // Spawn a task to handle assignment events
            tokio::spawn(future::lazy(move || {
                assignment_event_stream
                    .map_err(SubgraphAssignmentProviderError::Unknown)
                    .map_err(CancelableError::Error)
                    .cancelable(&assignment_event_stream_cancel_handle, || {
                        CancelableError::Cancel
                    })
                    .for_each(move |assignment_event| {
                        assert_eq!(assignment_event.node_id(), &node_id);
                        handle_assignment_event(assignment_event, provider.clone(), &logger_clone1)
                    })
                    .map_err(move |e| match e {
                        CancelableError::Cancel => {}
                        CancelableError::Error(e) => {
                            error!(logger_clone2, "Assignment event stream failed: {}", e);
                            panic!("assignment event stream failed: {}", e);
                        }
                    })
            }));

            Ok(())
        })
    }

    pub fn assignment_events(&self) -> impl Stream<Item = AssignmentEvent, Error = Error> + Send {
        let store = self.store.clone();
        let node_id = self.node_id.clone();

        store
            .subscribe(vec![
                SubgraphDeploymentAssignmentEntity::subgraph_entity_pair(),
            ])
            .map_err(|()| format_err!("Entity change stream failed"))
            .map(|event| {
                // We're only interested in the SubgraphDeploymentAssignment change; we
                // know that there is at least one, as that is what we subscribed to
                stream::iter_ok(
                    event
                        .changes
                        .into_iter()
                        .filter(|change| change.entity_type == "SubgraphDeploymentAssignment"),
                )
            })
            .flatten()
            .and_then(
                move |entity_change| -> Result<Box<Stream<Item = _, Error = _> + Send>, _> {
                    let subgraph_hash = SubgraphDeploymentId::new(entity_change.entity_id.clone())
                        .map_err(|()| {
                            format_err!(
                                "Invalid subgraph hash in assignment entity: {:#?}",
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
                                .map(|entity_opt| -> Box<Stream<Item = _, Error = _> + Send> {
                                    if let Some(entity) = entity_opt {
                                        if entity.get("nodeId") == Some(&node_id.to_string().into())
                                        {
                                            // Start subgraph on this node
                                            Box::new(stream::once(Ok(AssignmentEvent::Add {
                                                subgraph_id: subgraph_hash,
                                                node_id: node_id.clone(),
                                            })))
                                        } else {
                                            // Ensure it is removed from this node
                                            Box::new(stream::once(Ok(AssignmentEvent::Remove {
                                                subgraph_id: subgraph_hash,
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
                            SubgraphDeploymentId::new(id).map_err(|()| {
                                format_err!("Invalid subgraph hash in assignment entity")
                            })
                        })
                    })
                    .collect::<Result<HashSet<SubgraphDeploymentId>, _>>()
            })
            .and_then(move |subgraph_ids| {
                let provider = provider.clone();
                stream::iter_ok(subgraph_ids).for_each(move |id| {
                    start_subgraph(id, &*provider, logger.clone()).map_err(|()| unreachable!())
                })
            })
    }
}

impl<L, P, S, CS> SubgraphRegistrarTrait for SubgraphRegistrar<L, P, S, CS>
where
    L: LinkResolver,
    P: SubgraphAssignmentProviderTrait,
    S: Store,
    CS: ChainStore,
{
    fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Box<Future<Item = CreateSubgraphResult, Error = SubgraphRegistrarError> + Send + 'static>
    {
        Box::new(future::result(create_subgraph(
            &self.logger,
            self.store.clone(),
            name,
        )))
    }

    fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: SubgraphDeploymentId,
        node_id: NodeId,
    ) -> Box<Future<Item = (), Error = SubgraphRegistrarError> + Send + 'static> {
        let logger = self.logger.clone();
        let store = self.store.clone();
        let version_switching_mode = self.version_switching_mode;
        let chain_store = self.chain_store.clone();

        Box::new(
            SubgraphManifest::resolve(hash.to_ipfs_link(), self.resolver.clone())
                .map_err(SubgraphRegistrarError::ResolveError)
                .and_then(validation::validate_manifest)
                .and_then(move |manifest| {
                    create_subgraph_version(
                        &logger,
                        store,
                        chain_store,
                        name,
                        manifest,
                        node_id,
                        version_switching_mode,
                    )
                }),
        )
    }

    fn remove_subgraph(
        &self,
        name: SubgraphName,
    ) -> Box<Future<Item = (), Error = SubgraphRegistrarError> + Send + 'static> {
        Box::new(future::result(remove_subgraph(
            &self.logger,
            self.store.clone(),
            name,
        )))
    }
}

fn handle_assignment_event<P>(
    event: AssignmentEvent,
    provider: Arc<P>,
    logger: &Logger,
) -> Box<Future<Item = (), Error = CancelableError<SubgraphAssignmentProviderError>> + Send>
where
    P: SubgraphAssignmentProviderTrait,
{
    let logger = logger.to_owned();

    debug!(logger, "Received assignment event: {:?}", event);

    match event {
        AssignmentEvent::Add {
            subgraph_id,
            node_id: _,
        } => Box::new(start_subgraph(subgraph_id, &*provider, logger).map_err(|()| unreachable!())),
        AssignmentEvent::Remove {
            subgraph_id,
            node_id: _,
        } => Box::new(
            provider
                .stop(subgraph_id)
                .then(|result| match result {
                    Ok(()) => Ok(()),
                    Err(SubgraphAssignmentProviderError::NotRunning(_)) => Ok(()),
                    Err(e) => Err(e),
                })
                .map_err(CancelableError::Error),
        ),
    }
}

// Never errors.
fn start_subgraph<P: SubgraphAssignmentProviderTrait>(
    subgraph_id: SubgraphDeploymentId,
    provider: &P,
    logger: Logger,
) -> impl Future<Item = (), Error = ()> + 'static {
    provider
        .start(subgraph_id.clone())
        .then(move |result| -> Result<(), _> {
            match result {
                Ok(()) => Ok(()),
                Err(SubgraphAssignmentProviderError::AlreadyRunning(_)) => Ok(()),
                Err(e) => {
                    // Errors here are likely an issue with the subgraph.
                    error!(
                        logger,
                        "Subgraph instance failed to start";
                        "error" => e.to_string(),
                        "subgraph_id" => subgraph_id.to_string()
                    );
                    Ok(())
                }
            }
        })
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

    ops.push(EntityOperation::AbortUnless {
        description: "Subgraph entity should not exist".to_owned(),
        query: SubgraphEntity::query().filter(EntityFilter::new_equal("name", name.to_string())),
        entity_ids: vec![],
    });

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let entity = SubgraphEntity::new(name.clone(), None, None, created_at);
    let entity_id = generate_entity_id();
    ops.extend(entity.write_operations(&entity_id));

    store.apply_entity_operations(ops, None)?;

    debug!(logger, "Created subgraph"; "subgraph_name" => name.to_string());

    Ok(CreateSubgraphResult { id: entity_id })
}

fn create_subgraph_version(
    logger: &Logger,
    store: Arc<impl Store>,
    chain_store: Arc<impl ChainStore>,
    name: SubgraphName,
    manifest: SubgraphManifest,
    node_id: NodeId,
    version_switching_mode: SubgraphVersionSwitchingMode,
) -> Result<(), SubgraphRegistrarError> {
    let mut ops = vec![];

    // Look up subgraph entity by name
    let subgraph_entity_opt = store.find_one(
        SubgraphEntity::query().filter(EntityFilter::new_equal("name", name.to_string())),
    )?;
    let subgraph_entity = subgraph_entity_opt.ok_or_else(|| {
        debug!(
            logger,
            "Subgraph not found, could not create_subgraph_version";
            "subgraph_name" => name.to_string()
        );
        SubgraphRegistrarError::NameNotFound(name.to_string())
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
    ops.push(EntityOperation::AbortUnless {
        description:
            "Subgraph entity must still exist, have same name/currentVersion/pendingVersion"
                .to_owned(),
        query: SubgraphEntity::query().filter(EntityFilter::And(vec![
            EntityFilter::new_equal("name", name.to_string()),
            EntityFilter::new_equal("currentVersion", current_version_id_opt.clone()),
            EntityFilter::new_equal("pendingVersion", pending_version_id_opt.clone()),
        ])),
        entity_ids: vec![subgraph_entity_id.clone()],
    });

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
        iter::once(manifest.id.clone())
            .chain(current_version_hash_opt)
            .chain(pending_version_hash_opt)
            .collect(),
    )?;
    ops.extend(read_summaries_ops);

    // Create the subgraph version entity
    let version_entity_id = generate_entity_id();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    ops.extend(
        SubgraphVersionEntity::new(subgraph_entity_id.clone(), manifest.id.clone(), created_at)
            .write_operations(&version_entity_id),
    );

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
                deployment_id: manifest.id.clone(),
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
                    deployment_id: manifest.id.clone(),
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
                    deployment_id: manifest.id.clone(),
                    pending: false,
                    current: true,
                });

                version_summaries_after
            }
        }
    };

    // Check if subgraph deployment already exists for this hash
    let deployment_exists = store
        .get(SubgraphDeploymentEntity::key(manifest.id.clone()))?
        .is_some();

    ops.push(EntityOperation::AbortUnless {
        description: "Subgraph deployment entity must continue to exist/not exist".to_owned(),
        query: SubgraphDeploymentEntity::query()
            .filter(EntityFilter::new_equal("id", manifest.id.to_string())),
        entity_ids: if deployment_exists {
            vec![manifest.id.to_string()]
        } else {
            vec![]
        },
    });

    // Create deployment only if it does not exist already
    if !deployment_exists {
        let chain_head_ptr_opt = chain_store.chain_head_ptr()?;
        let chain_head_block_number = match chain_head_ptr_opt {
            Some(chain_head_ptr) => chain_head_ptr.number,
            None => 0,
        };
        let genesis_block_ptr = chain_store.genesis_block_ptr()?;
        ops.extend(
            SubgraphDeploymentEntity::new(
                &manifest,
                false,
                false,
                genesis_block_ptr,
                chain_head_block_number,
            )
            .create_operations(&manifest.id),
        );
    }

    // Possibly add assignment for new deployment hash, and possibly remove assignments for old
    // current/pending
    ops.extend(store.reconcile_assignments(
        logger,
        version_summaries_before,
        version_summaries_after,
        Some(node_id),
    ));

    // Update current/pending versions in Subgraph entity
    match version_switching_mode {
        SubgraphVersionSwitchingMode::Instant => {
            ops.extend(SubgraphEntity::update_pending_version_operations(
                &subgraph_entity_id,
                None,
            ));
            ops.extend(SubgraphEntity::update_current_version_operations(
                &subgraph_entity_id,
                Some(version_entity_id),
            ));
        }
        SubgraphVersionSwitchingMode::Synced => {
            // Set the new version as pending, unless there is no fully synced
            // current versions
            if current_is_synced {
                ops.extend(SubgraphEntity::update_pending_version_operations(
                    &subgraph_entity_id,
                    Some(version_entity_id),
                ));
            } else {
                ops.extend(SubgraphEntity::update_pending_version_operations(
                    &subgraph_entity_id,
                    None,
                ));
                ops.extend(SubgraphEntity::update_current_version_operations(
                    &subgraph_entity_id,
                    Some(version_entity_id),
                ));
            }
        }
    }

    // Commit entity ops
    let manifest_id = manifest.id.to_string();
    if deployment_exists {
        store.apply_entity_operations(ops, None)?
    } else {
        store.create_subgraph_deployment(&manifest.id, ops)?;
    }

    debug!(
        logger,
        "Wrote new subgraph version to store";
        "subgraph_name" => name.to_string(),
        "subgraph_hash" => manifest_id
    );

    Ok(())
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

    ops.push(EntityOperation::AbortUnless {
        description: "Subgraph entity must still exist".to_owned(),
        query: SubgraphEntity::query().filter(EntityFilter::new_equal("name", name.to_string())),
        entity_ids: vec![subgraph_entity.id().unwrap()],
    });

    // Find subgraph version entities
    let subgraph_version_entities = store.find(SubgraphVersionEntity::query().filter(
        EntityFilter::new_equal("subgraph", subgraph_entity.id().unwrap()),
    ))?;

    ops.push(EntityOperation::AbortUnless {
        description: "Subgraph must have same set of versions".to_owned(),
        query: SubgraphVersionEntity::query().filter(EntityFilter::new_equal(
            "subgraph",
            subgraph_entity.id().unwrap(),
        )),
        entity_ids: subgraph_version_entities
            .iter()
            .map(|entity| entity.id().unwrap())
            .collect(),
    });

    // Remove subgraph version entities, and their deployment/assignment when applicable
    ops.extend(remove_subgraph_versions(
        logger,
        store.clone(),
        subgraph_version_entities,
    )?);

    // Remove the subgraph entity
    ops.push(EntityOperation::Remove {
        key: SubgraphEntity::key(subgraph_entity.id()?),
    });

    store.apply_entity_operations(ops, None)?;

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
) -> Result<Vec<EntityOperation>, SubgraphRegistrarError> {
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
    ops.extend(store.reconcile_assignments(
        logger,
        version_summaries,
        version_summaries_after_delete,
        None,
    ));

    // Actually remove the subgraph version entities.
    // Note: we do this last because earlier AbortUnless ops depend on these entities still
    // existing.
    ops.extend(
        version_entities_to_delete
            .iter()
            .map(|version_entity| EntityOperation::Remove {
                key: SubgraphVersionEntity::key(version_entity.id().unwrap()),
            }),
    );

    Ok(ops)
}
