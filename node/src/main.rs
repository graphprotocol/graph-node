use clap::Parser as _;
use git_testament::{git_testament, render_testament};
use graph::components::adapter::IdentValidator;
use graph::futures01::Future as _;
use graph::futures03::compat::Future01CompatExt;
use graph::futures03::future::TryFutureExt;

use graph::blockchain::{Blockchain, BlockchainKind};
use graph::components::link_resolver::{ArweaveClient, FileSizeLimit};
use graph::components::subgraph::Settings;
use graph::data::graphql::load_manager::LoadManager;
use graph::endpoint::EndpointMetrics;
use graph::env::EnvVars;
use graph::log::logger;
use graph::prelude::*;
use graph::prometheus::Registry;
use graph::url::Url;
use graph_core::polling_monitor::{arweave_service, ipfs_service};
use graph_core::{
    SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider, SubgraphInstanceManager,
    SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_graphql::prelude::GraphQlRunner;
use graph_node::chain::create_ipfs_clients;
use graph_node::config::Config;
use graph_node::network_setup::Networks;
use graph_node::opt;
use graph_node::store_builder::StoreBuilder;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_index_node::IndexNodeServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_metrics::PrometheusMetricsServer;
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::register_jobs as register_store_jobs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;

git_testament!(TESTAMENT);

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

#[tokio::main]
async fn main() {
    env_logger::init();

    let env_vars = Arc::new(EnvVars::from_env().unwrap());
    let opt = opt::Opt::parse();

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

    let config = match Config::load(&logger, &opt.clone().into()) {
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

    let node_id = NodeId::new(opt.node_id.clone())
        .expect("Node ID must be between 1 and 63 characters in length");

    // Obtain subgraph related command-line arguments
    let subgraph = opt.subgraph.clone();

    // Obtain ports to use for the GraphQL server(s)
    let http_port = opt.http_port;
    let ws_port = opt.ws_port;

    // Obtain JSON-RPC server port
    let json_rpc_port = opt.admin_port;

    // Obtain index node server port
    let index_node_port = opt.index_node_port;

    // Obtain metrics server port
    let metrics_port = opt.metrics_port;

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

    info!(logger, "Starting up");

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

    // Set up Prometheus registry
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ));

    // Create a component and subgraph logger factory
    let logger_factory =
        LoggerFactory::new(logger.clone(), elastic_config, metrics_registry.clone());

    // Try to create IPFS clients for each URL specified in `--ipfs`
    let ipfs_clients: Vec<_> = create_ipfs_clients(&logger, &opt.ipfs);
    let ipfs_client = ipfs_clients.first().cloned().expect("Missing IPFS client");
    let ipfs_service = ipfs_service(
        ipfs_client,
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
    let link_resolver = Arc::new(IpfsResolver::new(ipfs_clients, env_vars.cheap_clone()));
    let metrics_server = PrometheusMetricsServer::new(&logger_factory, prometheus_registry.clone());

    let endpoint_metrics = Arc::new(EndpointMetrics::new(
        logger.clone(),
        &config.chains.providers(),
        metrics_registry.cheap_clone(),
    ));

    let graphql_metrics_registry = metrics_registry.clone();

    let contention_logger = logger.clone();

    // TODO: make option loadable from configuration TOML and environment:
    let expensive_queries =
        read_expensive_queries(&logger, opt.expensive_queries_filename).unwrap();

    let store_builder = StoreBuilder::new(
        &logger,
        &node_id,
        &config,
        fork_base,
        metrics_registry.cheap_clone(),
    )
    .await;

    let launch_services = |logger: Logger, env_vars: Arc<EnvVars>| async move {
        let subscription_manager = store_builder.subscription_manager();
        let chain_head_update_listener = store_builder.chain_head_update_listener();
        let primary_pool = store_builder.primary_pool();

        let network_store = store_builder.network_store(config.chain_ids());
        let block_store = network_store.block_store();
        let validator: Arc<dyn IdentValidator> = network_store.block_store();
        let network_adapters = Networks::from_config(
            logger.cheap_clone(),
            &config,
            metrics_registry.cheap_clone(),
            endpoint_metrics,
            validator,
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

        // see comment on cleanup_ethereum_shallow_blocks
        if !opt.disable_block_ingestor {
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

        let blockchain_map = Arc::new(blockchain_map);

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
            subscription_manager.clone(),
            load_manager,
            graphql_metrics_registry,
        ));
        let graphql_server = GraphQLQueryServer::new(&logger_factory, graphql_runner.clone());
        let subscription_server =
            GraphQLSubscriptionServer::new(&logger, graphql_runner.clone(), network_store.clone());

        let index_node_server = IndexNodeServer::new(
            &logger_factory,
            blockchain_map.clone(),
            network_store.clone(),
            link_resolver.clone(),
        );

        if !opt.disable_block_ingestor {
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
            ws_port,
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
            let (name, hash) = if subgraph.contains(':') {
                let mut split = subgraph.split(':');
                (split.next().unwrap(), split.next().unwrap().to_owned())
            } else {
                ("cli", subgraph)
            };

            let name = SubgraphName::new(name)
                .expect("Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'");
            let subgraph_id =
                DeploymentHash::new(hash).expect("Subgraph hash must be a valid IPFS hash");
            let debug_fork = opt
                .debug_fork
                .map(DeploymentHash::new)
                .map(|h| h.expect("Debug fork hash must be a valid IPFS hash"));
            let start_block = opt
                .start_block
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
                        )
                        .await
                }
                .map_err(|e| panic!("Failed to deploy subgraph from `--subgraph` flag: {}", e)),
            );
        }

        // Serve GraphQL queries over HTTP
        graph::spawn(async move { graphql_server.start(http_port, ws_port).await });

        // Serve GraphQL subscriptions over WebSockets
        graph::spawn(subscription_server.serve(ws_port));

        // Run the index node server
        graph::spawn(async move { index_node_server.start(index_node_port).await });

        graph::spawn(async move {
            metrics_server
                .start(metrics_port)
                .await
                .expect("Failed to start metrics server")
        });
    };

    graph::spawn(launch_services(logger.clone(), env_vars.cheap_clone()));

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
            debug!(contention_logger, "Shutting down contention checker thread");
            break;
        }
        let mut timeout = Duration::from_millis(10);
        while pong_receive.recv_timeout(timeout) == Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        {
            debug!(contention_logger, "Possible contention in tokio threadpool";
                                     "timeout_ms" => timeout.as_millis(),
                                     "code" => LogCode::TokioContention);
            if timeout < ENV_VARS.kill_if_unresponsive_timeout {
                timeout *= 10;
            } else if ENV_VARS.kill_if_unresponsive {
                // The node is unresponsive, kill it in hopes it will be restarted.
                crit!(contention_logger, "Node is unresponsive, killing process");
                std::process::abort()
            }
        }
    });

    graph::futures03::future::pending::<()>().await;
}
