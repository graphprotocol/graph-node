use futures::future::FutureResult;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use graph::data::subgraph::schema::*;

fn check_subgraph_exists(
    store: Arc<dyn NetworkStore>,
    subgraph_id: SubgraphDeploymentId,
) -> impl Future<Item = bool, Error = Error> {
    future::result(
        store
            .get(SubgraphDeploymentEntity::key(subgraph_id))
            .map_err(|e| e.into())
            .map(|entity| entity.map_or(false, |_| true)),
    )
}

fn create_subgraph(
    store: Arc<dyn NetworkStore>,
    subgraph_name: SubgraphName,
    subgraph_id: SubgraphDeploymentId,
    start_block: Option<EthereumBlockPointer>,
) -> FutureResult<(), Error> {
    let mut ops = vec![];

    // Ensure the subgraph itself doesn't already exist
    ops.push(SubgraphEntity::abort_unless(
        "Subgraph entity should not exist",
        EntityFilter::new_equal("name", subgraph_name.to_string()),
        vec![],
    ));

    // Create the subgraph entity (e.g. `ethereum/mainnet`)
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let subgraph_entity_id = generate_entity_id();
    ops.extend(
        SubgraphEntity::new(subgraph_name.clone(), None, None, created_at)
            .write_operations(&subgraph_entity_id)
            .into_iter()
            .map(|op| op.into()),
    );

    // Ensure the subgraph version doesn't already exist
    ops.push(SubgraphVersionEntity::abort_unless(
        "Subgraph version should not exist",
        EntityFilter::new_equal("id", subgraph_id.to_string()),
        vec![],
    ));

    // Create a subgraph version entity; we're using the same ID for
    // version and deployment to make clear they belong together
    let version_entity_id = subgraph_id.to_string();
    ops.extend(
        SubgraphVersionEntity::new(subgraph_entity_id.clone(), subgraph_id.clone(), created_at)
            .write_operations(&version_entity_id)
            .into_iter()
            .map(|op| op.into()),
    );

    // Immediately make this version the current one
    ops.extend(SubgraphEntity::update_pending_version_operations(
        &subgraph_entity_id,
        None,
    ));
    ops.extend(SubgraphEntity::update_current_version_operations(
        &subgraph_entity_id,
        Some(version_entity_id),
    ));

    // Ensure the deployment doesn't already exist
    ops.push(SubgraphDeploymentEntity::abort_unless(
        "Subgraph deployment entity must not exist",
        EntityFilter::new_equal("id", subgraph_id.to_string()),
        vec![],
    ));

    // Create a fake manifest
    let manifest = SubgraphManifest {
        id: subgraph_id.clone(),
        location: subgraph_name.to_string(),
        spec_version: String::from("0.0.1"),
        description: None,
        repository: None,
        schema: Schema::parse(include_str!("./ethereum.graphql"), subgraph_id.clone())
            .expect("valid Ethereum network subgraph schema"),
        data_sources: vec![],
        graft: None,
        templates: vec![],
    };

    ops.extend(
        SubgraphDeploymentEntity::new(&manifest, false, start_block)
            .create_operations(&manifest.id),
    );

    // Create a deployment assignment entity
    ops.extend(
        SubgraphDeploymentAssignmentEntity::new(NodeId::new("__builtin").unwrap())
            .write_operations(&subgraph_id)
            .into_iter()
            .map(|op| op.into()),
    );

    future::result(
        store
            .create_subgraph_deployment(&manifest.schema, ops)
            .map_err(|e| e.into()),
    )
}

pub fn ensure_subgraph_exists(
    subgraph_name: SubgraphName,
    subgraph_id: SubgraphDeploymentId,
    logger: Logger,
    store: Arc<dyn NetworkStore>,
    start_block: Option<EthereumBlockPointer>,
) -> impl Future<Item = (), Error = Error> {
    debug!(logger, "Ensure that the network subgraph exists");

    let logger_for_created = logger.clone();

    check_subgraph_exists(store.clone(), subgraph_id.clone())
        .from_err()
        .and_then(move |subgraph_exists| {
            if subgraph_exists {
                debug!(logger, "Network subgraph deployment already exists");
                Box::new(future::ok(())) as Box<dyn Future<Item = _, Error = _> + Send>
            } else {
                debug!(logger, "Network subgraph deployment needs to be created");
                Box::new(
                    create_subgraph(
                        store.clone(),
                        subgraph_name.clone(),
                        subgraph_id.clone(),
                        start_block,
                    )
                    .inspect(move |_| {
                        debug!(logger_for_created, "Created Ethereum network subgraph");
                    }),
                )
            }
        })
        .map_err(move |e| format_err!("Failed to ensure Ethereum network subgraph exists: {}", e))
}
