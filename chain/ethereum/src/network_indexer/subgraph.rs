use super::*;
use futures::future::FutureResult;
use std::collections::BTreeSet;

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
    // Create a fake manifest
    let manifest = SubgraphManifest {
        id: subgraph_id.clone(),
        location: subgraph_name.to_string(),
        spec_version: String::from("0.0.1"),
        features: BTreeSet::new(),
        description: None,
        repository: None,
        schema: Schema::parse(include_str!("./ethereum.graphql"), subgraph_id.clone())
            .expect("valid Ethereum network subgraph schema"),
        data_sources: vec![],
        graft: None,
        templates: vec![],
    };

    let deployment = SubgraphDeploymentEntity::new(&manifest, false, start_block);
    future::result(
        store
            .create_subgraph_deployment(
                subgraph_name,
                &manifest.schema,
                deployment,
                NodeId::new("__builtin").unwrap(),
                SubgraphVersionSwitchingMode::Instant,
            )
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
