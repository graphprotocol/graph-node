use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::chain::create_ipfs_clients;
use crate::config::Config;
use crate::manager::PanicSubscriptionManager;
use crate::network_setup::Networks;
use crate::store_builder::StoreBuilder;
use crate::MetricsContext;
use graph::anyhow::bail;
use graph::cheap_clone::CheapClone;
use graph::components::adapter::IdentValidator;
use graph::components::link_resolver::{ArweaveClient, FileSizeLimit};
use graph::components::store::DeploymentLocator;
use graph::components::subgraph::Settings;
use graph::endpoint::EndpointMetrics;
use graph::env::EnvVars;
use graph::prelude::{
    anyhow, tokio, BlockNumber, DeploymentHash, IpfsResolver, LoggerFactory, NodeId,
    SubgraphAssignmentProvider, SubgraphCountMetric, SubgraphName, SubgraphRegistrar,
    SubgraphStore, SubgraphVersionSwitchingMode, ENV_VARS,
};
use graph::slog::{debug, info, Logger};
use graph_core::polling_monitor::{arweave_service, ipfs_service};
use graph_core::{
    SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider, SubgraphInstanceManager,
    SubgraphRegistrar as IpfsSubgraphRegistrar,
};

fn locate(store: &dyn SubgraphStore, hash: &str) -> Result<DeploymentLocator, anyhow::Error> {
    let mut locators = store.locators(hash)?;
    match locators.len() {
        0 => bail!("could not find subgraph {hash} we just created"),
        1 => Ok(locators.pop().unwrap()),
        n => bail!("there are {n} subgraphs with hash {hash}"),
    }
}

pub async fn run(
    logger: Logger,
    store_builder: StoreBuilder,
    _network_name: String,
    ipfs_url: Vec<String>,
    arweave_url: String,
    config: Config,
    metrics_ctx: MetricsContext,
    node_id: NodeId,
    subgraph: String,
    stop_block: BlockNumber,
) -> Result<(), anyhow::Error> {
    println!(
        "Run command: starting subgraph => {}, stop_block = {}",
        subgraph, stop_block
    );

    let env_vars = Arc::new(EnvVars::from_env().unwrap());
    let metrics_registry = metrics_ctx.registry.clone();
    let logger_factory = LoggerFactory::new(logger.clone(), None, metrics_ctx.registry.clone());

    // FIXME: Hard-coded IPFS config, take it from config file instead?
    let ipfs_clients: Vec<_> = create_ipfs_clients(&logger, &ipfs_url);
    let ipfs_client = ipfs_clients.first().cloned().expect("Missing IPFS client");
    let ipfs_service = ipfs_service(
        ipfs_client,
        env_vars.mappings.max_ipfs_file_bytes,
        env_vars.mappings.ipfs_timeout,
        env_vars.mappings.ipfs_request_limit,
    );
    let arweave_resolver = Arc::new(ArweaveClient::new(
        logger.cheap_clone(),
        arweave_url.parse().expect("invalid arweave url"),
    ));
    let arweave_service = arweave_service(
        arweave_resolver.cheap_clone(),
        env_vars.mappings.ipfs_request_limit,
        match env_vars.mappings.max_ipfs_file_bytes {
            0 => FileSizeLimit::Unlimited,
            n => FileSizeLimit::MaxBytes(n as u64),
        },
    );

    let endpoint_metrics = Arc::new(EndpointMetrics::new(
        logger.clone(),
        &config.chains.providers(),
        metrics_registry.cheap_clone(),
    ));

    // Convert the clients into a link resolver. Since we want to get past
    // possible temporary DNS failures, make the resolver retry
    let link_resolver = Arc::new(IpfsResolver::new(ipfs_clients, env_vars.cheap_clone()));

    let chain_head_update_listener = store_builder.chain_head_update_listener();
    let network_store = store_builder.network_store(config.chain_ids());
    let block_store = network_store.block_store();
    let ident_validator: Arc<dyn IdentValidator> = network_store.block_store();
    let networks = Networks::from_config(
        logger.cheap_clone(),
        &config,
        metrics_registry.cheap_clone(),
        endpoint_metrics,
        ident_validator,
    )
    .await
    .expect("unable to parse network configuration");

    let subgraph_store = network_store.subgraph_store();

    let blockchain_map = Arc::new(
        networks
            .blockchain_map(
                &env_vars,
                &node_id,
                &logger,
                block_store,
                &logger_factory,
                metrics_registry.cheap_clone(),
                chain_head_update_listener,
            )
            .await,
    );

    let static_filters = ENV_VARS.experimental_static_filters;

    let sg_metrics = Arc::new(SubgraphCountMetric::new(metrics_registry.clone()));

    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger_factory,
        env_vars.cheap_clone(),
        subgraph_store.clone(),
        blockchain_map.clone(),
        sg_metrics.cheap_clone(),
        metrics_registry.clone(),
        link_resolver.cheap_clone(),
        ipfs_service,
        arweave_service,
        static_filters,
    );

    // Create IPFS-based subgraph provider
    let subgraph_provider = Arc::new(IpfsSubgraphAssignmentProvider::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_instance_manager,
        sg_metrics,
    ));

    let panicking_subscription_manager = Arc::new(PanicSubscriptionManager {});

    let subgraph_registrar = Arc::new(IpfsSubgraphRegistrar::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_provider.clone(),
        subgraph_store.clone(),
        panicking_subscription_manager,
        blockchain_map,
        node_id.clone(),
        SubgraphVersionSwitchingMode::Instant,
        Arc::new(Settings::default()),
    ));

    let (name, hash) = if subgraph.contains(':') {
        let mut split = subgraph.split(':');
        (split.next().unwrap(), split.next().unwrap().to_owned())
    } else {
        ("cli", subgraph)
    };

    let subgraph_name = SubgraphName::new(name)
        .expect("Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'");
    let subgraph_hash =
        DeploymentHash::new(hash.clone()).expect("Subgraph hash must be a valid IPFS hash");

    info!(&logger, "Creating subgraph {}", name);
    let create_result =
        SubgraphRegistrar::create_subgraph(subgraph_registrar.as_ref(), subgraph_name.clone())
            .await?;

    info!(
        &logger,
        "Looking up subgraph deployment {} (Deployment hash => {}, id => {})",
        name,
        subgraph_hash,
        create_result.id,
    );

    SubgraphRegistrar::create_subgraph_version(
        subgraph_registrar.as_ref(),
        subgraph_name.clone(),
        subgraph_hash.clone(),
        node_id.clone(),
        None,
        None,
        None,
        None,
    )
    .await?;

    let locator = locate(subgraph_store.as_ref(), &hash)?;

    SubgraphAssignmentProvider::start(subgraph_provider.as_ref(), locator, Some(stop_block))
        .await?;

    loop {
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let block_ptr = subgraph_store
            .least_block_ptr(&subgraph_hash)
            .await
            .unwrap()
            .unwrap();

        debug!(&logger, "subgraph block: {:?}", block_ptr);

        if block_ptr.number >= stop_block {
            info!(
                &logger,
                "subgraph now at block {}, reached stop block {}", block_ptr.number, stop_block
            );
            break;
        }
    }

    info!(&logger, "Removing subgraph {}", name);
    subgraph_store.clone().remove_subgraph(subgraph_name)?;

    if let Some(host) = metrics_ctx.prometheus_host {
        let mfs = metrics_ctx.prometheus.gather();
        let job_name = match metrics_ctx.job_name {
            Some(name) => name,
            None => "graphman run".into(),
        };

        tokio::task::spawn_blocking(move || {
            prometheus::push_metrics(&job_name, HashMap::new(), &host, mfs, None)
        })
        .await??;
    }

    Ok(())
}
