use anyhow::Result;

use git_testament::{git_testament, render_testament};
use graph::futures01::Future as _;
use graph::futures03::compat::Future01CompatExt;
use graph::futures03::future::TryFutureExt;

use crate::config::Config;
use crate::dev::helpers::{watch_subgraph_updates, DevModeContext};
use crate::network_setup::Networks;
use crate::opt::Opt;
use crate::store_builder::StoreBuilder;
use graph::blockchain::{Blockchain, BlockchainKind, BlockchainMap};
use graph::components::link_resolver::{ArweaveClient, FileSizeLimit};
use graph::components::subgraph::Settings;
use graph::data::graphql::load_manager::LoadManager;
use graph::endpoint::EndpointMetrics;
use graph::env::EnvVars;
use graph::log::logger;
use graph::prelude::*;
use graph::prometheus::Registry;
use graph::url::Url;
use graph_core::polling_monitor::{arweave_service, ipfs_service, ArweaveService, IpfsService};
use graph_core::{
    SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider, SubgraphInstanceManager,
    SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_graphql::prelude::GraphQlRunner;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_index_node::IndexNodeServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_metrics::PrometheusMetricsServer;
use graph_store_postgres::{
    register_jobs as register_store_jobs, ChainHeadUpdateListener, ConnectionPool,
    NotificationSender, Store, SubgraphStore, SubscriptionManager,
};
use graphman_server::GraphmanServer;
use graphman_server::GraphmanServerConfig;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;

git_testament!(TESTAMENT);

/// Sets up metrics and monitoring
fn setup_metrics(logger: &Logger) -> (Arc<Registry>, Arc<MetricsRegistry>) {
    // Set up Prometheus registry
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ));

    (prometheus_registry, metrics_registry)
}

/// Sets up the store and database connections
async fn setup_store(
    logger: &Logger,
    node_id: &NodeId,
    config: &Config,
    fork_base: Option<Url>,
    metrics_registry: Arc<MetricsRegistry>,
) -> (
    ConnectionPool,
    Arc<SubscriptionManager>,
    Arc<ChainHeadUpdateListener>,
    Arc<Store>,
) {
    let store_builder = StoreBuilder::new(
        logger,
        node_id,
        config,
        fork_base,
        metrics_registry.cheap_clone(),
    )
    .await;

    let primary_pool = store_builder.primary_pool();
    let subscription_manager = store_builder.subscription_manager();
    let chain_head_update_listener = store_builder.chain_head_update_listener();
    let network_store = store_builder.network_store(config.chain_ids());

    (
        primary_pool,
        subscription_manager,
        chain_head_update_listener,
        network_store,
    )
}

async fn build_blockchain_map(
    logger: &Logger,
    config: &Config,
    env_vars: &Arc<EnvVars>,
    node_id: &NodeId,
    network_store: Arc<Store>,
    metrics_registry: Arc<MetricsRegistry>,
    endpoint_metrics: Arc<EndpointMetrics>,
    chain_head_update_listener: Arc<ChainHeadUpdateListener>,
    logger_factory: &LoggerFactory,
) -> Arc<BlockchainMap> {
    use graph::components::network_provider;
    let block_store = network_store.block_store();

    let mut provider_checks: Vec<Arc<dyn network_provider::ProviderCheck>> = Vec::new();

    if env_vars.genesis_validation_enabled {
        provider_checks.push(Arc::new(network_provider::GenesisHashCheck::new(
            block_store.clone(),
        )));
    }

    provider_checks.push(Arc::new(network_provider::ExtendedBlocksCheck::new(
        env_vars
            .firehose_disable_extended_blocks_for_chains
            .iter()
            .map(|x| x.as_str().into()),
    )));

    let network_adapters = Networks::from_config(
        logger.cheap_clone(),
        &config,
        metrics_registry.cheap_clone(),
        endpoint_metrics,
        &provider_checks,
    )
    .await
    .expect("unable to parse network configuration");

    let blockchain_map = network_adapters
        .blockchain_map(
            &env_vars,
            &node_id,
            &logger,
            block_store,
            &logger_factory,
            metrics_registry.cheap_clone(),
            chain_head_update_listener,
        )
        .await;

    Arc::new(blockchain_map)
}

fn cleanup_ethereum_shallow_blocks(blockchain_map: &BlockchainMap, network_store: &Arc<Store>) {
    match blockchain_map
        .get_all_by_kind::<graph_chain_ethereum::Chain>(BlockchainKind::Ethereum)
        .ok()
        .map(|chains| {
            chains
                .iter()
                .flat_map(|c| {
                    if !c.chain_client().is_firehose() {
                        Some(c.name.to_string())
                    } else {
                        None
                    }
                })
                .collect()
        }) {
        Some(eth_network_names) => {
            network_store
                .block_store()
                .cleanup_ethereum_shallow_blocks(eth_network_names)
                .unwrap();
        }
        // This code path only happens if the downcast on the blockchain map fails, that
        // probably means we have a problem with the chain loading logic so it's probably
        // safest to just refuse to start.
        None => unreachable!(
            "If you are seeing this message just use a different version of graph-node"
        ),
    }
}

async fn spawn_block_ingestor(
    logger: &Logger,
    blockchain_map: &Arc<BlockchainMap>,
    network_store: &Arc<Store>,
    primary_pool: ConnectionPool,
    metrics_registry: &Arc<MetricsRegistry>,
) {
    let logger = logger.clone();
    let ingestors = Networks::block_ingestors(&logger, &blockchain_map)
        .await
        .expect("unable to start block ingestors");

    ingestors.into_iter().for_each(|ingestor| {
        let logger = logger.clone();
        info!(logger,"Starting block ingestor for network";"network_name" => &ingestor.network_name().as_str(), "kind" => ingestor.kind().to_string());

        graph::spawn(ingestor.run());
    });

    // Start a task runner
    let mut job_runner = graph::util::jobs::Runner::new(&logger);
    register_store_jobs(
        &mut job_runner,
        network_store.clone(),
        primary_pool,
        metrics_registry.clone(),
    );
    graph::spawn_blocking(job_runner.start());
}

fn deploy_subgraph_from_flag(
    subgraph: String,
    opt: &Opt,
    subgraph_registrar: Arc<impl SubgraphRegistrar>,
    node_id: NodeId,
) {
    let (name, hash) = if subgraph.contains(':') {
        let mut split = subgraph.split(':');
        (split.next().unwrap(), split.next().unwrap().to_owned())
    } else {
        ("cli", subgraph)
    };

    let name = SubgraphName::new(name)
        .expect("Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'");
    let subgraph_id = DeploymentHash::new(hash).expect("Subgraph hash must be a valid IPFS hash");
    let debug_fork = opt
        .debug_fork
        .clone()
        .map(DeploymentHash::new)
        .map(|h| h.expect("Debug fork hash must be a valid IPFS hash"));
    let start_block = opt
        .start_block
        .clone()
        .map(|block| {
            let mut split = block.split(':');
            (
                // BlockHash
                split.next().unwrap().to_owned(),
                // BlockNumber
                split.next().unwrap().parse::<i64>().unwrap(),
            )
        })
        .map(|(hash, number)| BlockPtr::try_from((hash.as_str(), number)))
        .map(Result::unwrap);

    graph::spawn(
        async move {
            subgraph_registrar.create_subgraph(name.clone()).await?;
            subgraph_registrar
                .create_subgraph_version(
                    name,
                    subgraph_id,
                    node_id,
                    debug_fork,
                    start_block,
                    None,
                    None,
                    false,
                )
                .await
        }
        .map_err(|e| panic!("Failed to deploy subgraph from `--subgraph` flag: {}", e)),
    );
}

fn build_subgraph_registrar(
    metrics_registry: Arc<MetricsRegistry>,
    network_store: &Arc<Store>,
    logger_factory: &LoggerFactory,
    env_vars: &Arc<EnvVars>,
    blockchain_map: Arc<BlockchainMap>,
    node_id: NodeId,
    subgraph_settings: Settings,
    link_resolver: Arc<dyn LinkResolver>,
    subscription_manager: Arc<SubscriptionManager>,
    arweave_service: ArweaveService,
    ipfs_service: IpfsService,
) -> Arc<
    IpfsSubgraphRegistrar<
        IpfsSubgraphAssignmentProvider<SubgraphInstanceManager<SubgraphStore>>,
        SubgraphStore,
        SubscriptionManager,
    >,
> {
    let static_filters = ENV_VARS.experimental_static_filters;
    let sg_count = Arc::new(SubgraphCountMetric::new(metrics_registry.cheap_clone()));

    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger_factory,
        env_vars.cheap_clone(),
        network_store.subgraph_store(),
        blockchain_map.cheap_clone(),
        sg_count.cheap_clone(),
        metrics_registry.clone(),
        link_resolver.clone(),
        ipfs_service,
        arweave_service,
        static_filters,
    );

    // Create IPFS-based subgraph provider
    let subgraph_provider = IpfsSubgraphAssignmentProvider::new(
        &logger_factory,
        link_resolver.clone(),
        subgraph_instance_manager,
        sg_count,
    );

    // Check version switching mode environment variable
    let version_switching_mode = ENV_VARS.subgraph_version_switching_mode;

    // Create named subgraph provider for resolving subgraph name->ID mappings
    let subgraph_registrar = Arc::new(IpfsSubgraphRegistrar::new(
        &logger_factory,
        link_resolver,
        Arc::new(subgraph_provider),
        network_store.subgraph_store(),
        subscription_manager,
        blockchain_map,
        node_id.clone(),
        version_switching_mode,
        Arc::new(subgraph_settings),
    ));

    subgraph_registrar
}

fn build_graphql_server(
    config: &Config,
    logger: &Logger,
    expensive_queries: Vec<Arc<q::Document>>,
    metrics_registry: Arc<MetricsRegistry>,
    network_store: &Arc<Store>,
    logger_factory: &LoggerFactory,
) -> GraphQLQueryServer<GraphQlRunner<Store>> {
    let shards: Vec<_> = config.stores.keys().cloned().collect();
    let load_manager = Arc::new(LoadManager::new(
        &logger,
        shards,
        expensive_queries,
        metrics_registry.clone(),
    ));
    let graphql_runner = Arc::new(GraphQlRunner::new(
        &logger,
        network_store.clone(),
        load_manager,
        metrics_registry,
    ));
    let graphql_server = GraphQLQueryServer::new(&logger_factory, graphql_runner.clone());

    graphql_server
}

pub async fn run(opt: Opt, env_vars: Arc<EnvVars>, dev_ctx: Option<DevModeContext>) {
    // Set up logger
    let logger = logger(opt.debug);

    // Log version information
    info!(
        logger,
        "Graph Node version: {}",
        render_testament!(TESTAMENT)
    );

    if !graph_server_index_node::PoiProtection::from_env(&ENV_VARS).is_active() {
        warn!(
            logger,
            "GRAPH_POI_ACCESS_TOKEN not set; might leak POIs to the public via GraphQL"
        );
    }

    // Get configuration
    let (config, subgraph_settings, fork_base) = setup_configuration(&opt, &logger, &env_vars);

    let node_id = NodeId::new(opt.node_id.clone())
        .expect("Node ID must be between 1 and 63 characters in length");

    // Obtain subgraph related command-line arguments
    let subgraph = opt.subgraph.clone();

    // Obtain ports to use for the GraphQL server(s)
    let http_port = opt.http_port;

    // Obtain JSON-RPC server port
    let json_rpc_port = opt.admin_port;

    // Obtain index node server port
    let index_node_port = opt.index_node_port;

    // Obtain metrics server port
    let metrics_port = opt.metrics_port;

    info!(logger, "Starting up");

    // Set up metrics
    let (prometheus_registry, metrics_registry) = setup_metrics(&logger);

    // Optionally, identify the Elasticsearch logging configuration
    let elastic_config = opt
        .elasticsearch_url
        .clone()
        .map(|endpoint| ElasticLoggingConfig {
            endpoint,
            username: opt.elasticsearch_user.clone(),
            password: opt.elasticsearch_password.clone(),
            client: reqwest::Client::new(),
        });

    // Create a component and subgraph logger factory
    let logger_factory =
        LoggerFactory::new(logger.clone(), elastic_config, metrics_registry.clone());

    let ipfs_client = graph::ipfs::new_ipfs_client(&opt.ipfs, &logger)
        .await
        .unwrap_or_else(|err| panic!("Failed to create IPFS client: {err:#}"));

    let ipfs_service = ipfs_service(
        ipfs_client.cheap_clone(),
        ENV_VARS.mappings.max_ipfs_file_bytes,
        ENV_VARS.mappings.ipfs_timeout,
        ENV_VARS.mappings.ipfs_request_limit,
    );

    let arweave_resolver = Arc::new(ArweaveClient::new(
        logger.cheap_clone(),
        opt.arweave
            .parse()
            .expect("unable to parse arweave gateway address"),
    ));

    let arweave_service = arweave_service(
        arweave_resolver.cheap_clone(),
        env_vars.mappings.ipfs_request_limit,
        match env_vars.mappings.max_ipfs_file_bytes {
            0 => FileSizeLimit::Unlimited,
            n => FileSizeLimit::MaxBytes(n as u64),
        },
    );

    // Convert the clients into a link resolver. Since we want to get past
    // possible temporary DNS failures, make the resolver retry
    let link_resolver: Arc<dyn LinkResolver> = if let Some(dev_ctx) = &dev_ctx {
        dev_ctx.file_link_resolver.clone()
    } else {
        Arc::new(IpfsResolver::new(ipfs_client, env_vars.cheap_clone()))
    };

    let metrics_server = PrometheusMetricsServer::new(&logger_factory, prometheus_registry.clone());

    let endpoint_metrics = Arc::new(EndpointMetrics::new(
        logger.clone(),
        &config.chains.providers(),
        metrics_registry.cheap_clone(),
    ));

    // TODO: make option loadable from configuration TOML and environment:
    let expensive_queries =
        read_expensive_queries(&logger, opt.expensive_queries_filename.clone()).unwrap();

    let (primary_pool, subscription_manager, chain_head_update_listener, network_store) =
        setup_store(
            &logger,
            &node_id,
            &config,
            fork_base,
            metrics_registry.cheap_clone(),
        )
        .await;

    let graphman_server_config = make_graphman_server_config(
        primary_pool.clone(),
        network_store.cheap_clone(),
        metrics_registry.cheap_clone(),
        &env_vars,
        &logger,
        &logger_factory,
    );

    start_graphman_server(opt.graphman_port, graphman_server_config).await;

    let launch_services = |logger: Logger, env_vars: Arc<EnvVars>| async move {
        let blockchain_map = build_blockchain_map(
            &logger,
            &config,
            &env_vars,
            &node_id,
            network_store.clone(),
            metrics_registry.clone(),
            endpoint_metrics,
            chain_head_update_listener,
            &logger_factory,
        )
        .await;

        // see comment on cleanup_ethereum_shallow_blocks
        if !opt.disable_block_ingestor {
            cleanup_ethereum_shallow_blocks(&blockchain_map, &network_store);
        }

        let graphql_server = build_graphql_server(
            &config,
            &logger,
            expensive_queries,
            metrics_registry.clone(),
            &network_store,
            &logger_factory,
        );

        let index_node_server = IndexNodeServer::new(
            &logger_factory,
            blockchain_map.clone(),
            network_store.clone(),
            link_resolver.clone(),
        );

        if !opt.disable_block_ingestor {
            spawn_block_ingestor(
                &logger,
                &blockchain_map,
                &network_store,
                primary_pool,
                &metrics_registry,
            )
            .await;
        }

        let subgraph_registrar = build_subgraph_registrar(
            metrics_registry.clone(),
            &network_store,
            &logger_factory,
            &env_vars,
            blockchain_map.clone(),
            node_id.clone(),
            subgraph_settings,
            link_resolver.clone(),
            subscription_manager,
            arweave_service,
            ipfs_service,
        );

        graph::spawn(
            subgraph_registrar
                .start()
                .map_err(|e| panic!("failed to initialize subgraph provider {}", e))
                .compat(),
        );

        // Start admin JSON-RPC server.
        let json_rpc_server = JsonRpcServer::serve(
            json_rpc_port,
            http_port,
            subgraph_registrar.clone(),
            node_id.clone(),
            logger.clone(),
        )
        .await
        .expect("failed to start JSON-RPC admin server");

        // Let the server run forever.
        std::mem::forget(json_rpc_server);

        // Add the CLI subgraph with a REST request to the admin server.
        if let Some(subgraph) = subgraph {
            deploy_subgraph_from_flag(subgraph, &opt, subgraph_registrar.clone(), node_id.clone());
        }

        // Serve GraphQL queries over HTTP
        graph::spawn(async move { graphql_server.start(http_port).await });

        // Run the index node server
        graph::spawn(async move { index_node_server.start(index_node_port).await });

        graph::spawn(async move {
            metrics_server
                .start(metrics_port)
                .await
                .expect("Failed to start metrics server")
        });

        // If we are in dev mode, watch for subgraph updates
        // And drop and recreate the subgraph when it changes
        if let Some(dev_ctx) = dev_ctx {
            if dev_ctx.watch {
                graph::spawn(async move {
                    watch_subgraph_updates(
                        &logger,
                        network_store.subgraph_store(),
                        subgraph_registrar.clone(),
                        node_id.clone(),
                        dev_ctx.updates_rx,
                    )
                    .await;
                });
            }
        }
    };

    graph::spawn(launch_services(logger.clone(), env_vars.cheap_clone()));

    spawn_contention_checker(logger.clone());

    graph::futures03::future::pending::<()>().await;
}

fn spawn_contention_checker(logger: Logger) {
    // Periodically check for contention in the tokio threadpool. First spawn a
    // task that simply responds to "ping" requests. Then spawn a separate
    // thread to periodically ping it and check responsiveness.
    let (ping_send, mut ping_receive) = mpsc::channel::<std::sync::mpsc::SyncSender<()>>(1);
    graph::spawn(async move {
        while let Some(pong_send) = ping_receive.recv().await {
            let _ = pong_send.clone().send(());
        }
        panic!("ping sender dropped");
    });
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        let (pong_send, pong_receive) = std::sync::mpsc::sync_channel(1);
        if graph::futures03::executor::block_on(ping_send.clone().send(pong_send)).is_err() {
            debug!(logger, "Shutting down contention checker thread");
            break;
        }
        let mut timeout = Duration::from_millis(10);
        while pong_receive.recv_timeout(timeout) == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            debug!(logger, "Possible contention in tokio threadpool";
                                     "timeout_ms" => timeout.as_millis(),
                                     "code" => LogCode::TokioContention);
            if timeout < ENV_VARS.kill_if_unresponsive_timeout {
                timeout *= 10;
            } else if ENV_VARS.kill_if_unresponsive {
                // The node is unresponsive, kill it in hopes it will be restarted.
                crit!(logger, "Node is unresponsive, killing process");
                std::process::abort()
            }
        }
    });
}

/// Sets up and loads configuration based on command line options
fn setup_configuration(
    opt: &Opt,
    logger: &Logger,
    env_vars: &Arc<EnvVars>,
) -> (Config, Settings, Option<Url>) {
    let config = match Config::load(logger, &opt.clone().into()) {
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
        Ok(config) => config,
    };

    let subgraph_settings = match env_vars.subgraph_settings {
        Some(ref path) => {
            info!(logger, "Reading subgraph configuration file `{}`", path);
            match Settings::from_file(path) {
                Ok(rules) => rules,
                Err(e) => {
                    eprintln!("configuration error in subgraph settings {}: {}", path, e);
                    std::process::exit(1);
                }
            }
        }
        None => Settings::default(),
    };

    if opt.check_config {
        match config.to_json() {
            Ok(txt) => println!("{}", txt),
            Err(e) => eprintln!("error serializing config: {}", e),
        }
        eprintln!("Successfully validated configuration");
        std::process::exit(0);
    }

    // Obtain the fork base URL
    let fork_base = match &opt.fork_base {
        Some(url) => {
            // Make sure the endpoint ends with a terminating slash.
            let url = if !url.ends_with('/') {
                let mut url = url.clone();
                url.push('/');
                Url::parse(&url)
            } else {
                Url::parse(url)
            };

            Some(url.expect("Failed to parse the fork base URL"))
        }
        None => {
            warn!(
                logger,
                "No fork base URL specified, subgraph forking is disabled"
            );
            None
        }
    };

    (config, subgraph_settings, fork_base)
}

async fn start_graphman_server(port: u16, config: Option<GraphmanServerConfig<'_>>) {
    let Some(config) = config else {
        return;
    };

    let server = GraphmanServer::new(config)
        .unwrap_or_else(|err| panic!("Invalid graphman server configuration: {err:#}"));

    server
        .start(port)
        .await
        .unwrap_or_else(|err| panic!("Failed to start graphman server: {err:#}"));
}

fn make_graphman_server_config<'a>(
    pool: ConnectionPool,
    store: Arc<Store>,
    metrics_registry: Arc<MetricsRegistry>,
    env_vars: &EnvVars,
    logger: &Logger,
    logger_factory: &'a LoggerFactory,
) -> Option<GraphmanServerConfig<'a>> {
    let Some(auth_token) = &env_vars.graphman_server_auth_token else {
        warn!(
            logger,
            "Missing graphman server auth token; graphman server will not start",
        );

        return None;
    };

    let notification_sender = Arc::new(NotificationSender::new(metrics_registry.clone()));

    Some(GraphmanServerConfig {
        pool,
        notification_sender,
        store,
        logger_factory,
        auth_token: auth_token.to_owned(),
    })
}

fn read_expensive_queries(
    logger: &Logger,
    expensive_queries_filename: String,
) -> Result<Vec<Arc<q::Document>>, std::io::Error> {
    // A file with a list of expensive queries, one query per line
    // Attempts to run these queries will return a
    // QueryExecutionError::TooExpensive to clients
    let path = Path::new(&expensive_queries_filename);
    let mut queries = Vec::new();
    if path.exists() {
        info!(
            logger,
            "Reading expensive queries file: {}", expensive_queries_filename
        );
        let file = std::fs::File::open(path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let query = q::parse_query(&line)
                .map_err(|e| {
                    let msg = format!(
                        "invalid GraphQL query in {}: {}\n{}",
                        expensive_queries_filename, e, line
                    );
                    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
                })?
                .into_static();
            queries.push(Arc::new(query));
        }
    } else {
        warn!(
            logger,
            "Expensive queries file not set to a valid file: {}", expensive_queries_filename
        );
    }
    Ok(queries)
}
