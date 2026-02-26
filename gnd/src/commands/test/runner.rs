//! Test runner: orchestrates subgraph indexing with mock blockchain data.
//!
//! This is the core of `gnd test`. For each test file, it:
//!
//! 1. Creates a test database (`TestDatabase::Temporary` via pgtemp, or
//!    `TestDatabase::Persistent` via `--postgres-url`) for test isolation
//! 2. Initializes graph-node stores (entity storage, block storage, chain store)
//! 3. Constructs a mock Ethereum chain that feeds pre-defined blocks
//! 4. Deploys the subgraph and starts the indexer
//! 5. Waits for all blocks to be processed (or a fatal error)
//! 6. Runs GraphQL assertions against the indexed entity state

use super::assertion::run_assertions;
use super::block_stream::StaticStreamBuilder;
use super::mock_chain;
use super::noop::{NoopAdapterSelector, StaticBlockRefetcher};
use super::schema::{TestFile, TestResult};
use super::trigger::build_blocks_with_triggers;
use super::TestOpt;
use crate::manifest::{load_manifest, Manifest};
use anyhow::{anyhow, ensure, Context, Result};
use graph::amp::FlightClient;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::{BlockPtr, BlockchainMap, ChainIdentifier};
use graph::cheap_clone::CheapClone;
use graph::components::link_resolver::{ArweaveClient, FileLinkResolver};
use graph::components::metrics::MetricsRegistry;
use graph::components::network_provider::{
    AmpChainNames, ChainName, ProviderCheckStrategy, ProviderManager,
};
use graph::components::store::DeploymentLocator;
use graph::components::subgraph::{Settings, SubgraphInstanceManager as _};
use graph::data::graphql::load_manager::LoadManager;
use graph::data::subgraph::schema::SubgraphError;
use graph::endpoint::EndpointMetrics;
use graph::env::EnvVars;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints, SubgraphLimit};
use graph::ipfs::{IpfsMetrics, IpfsRpcClient, ServerAddress};
use graph::prelude::{
    DeploymentHash, LoggerFactory, NodeId, SubgraphCountMetric, SubgraphName, SubgraphRegistrar,
    SubgraphStore as SubgraphStoreTrait, SubgraphVersionSwitchingMode,
};
use graph::slog::{info, o, Logger};
use graph_chain_ethereum::chain::EthereumRuntimeAdapterBuilder;
use graph_chain_ethereum::network::{EthereumNetworkAdapter, EthereumNetworkAdapters};
use graph_chain_ethereum::{
    Chain, EthereumAdapter, NodeCapabilities, ProviderEthRpcMetrics, Transport,
};
use graph_core::polling_monitor::{arweave_service, ipfs_service};
use graph_graphql::prelude::GraphQlRunner;
use graph_node::config::Config;
use graph_node::manager::PanicSubscriptionManager;
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::{ChainHeadUpdateListener, ChainStore, Store, SubgraphStore};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use pgtemp::PgTempDBBuilder;

const NODE_ID: &str = "gnd-test";

/// Build a logger from the `-v` flag. `GRAPH_LOG` env var always takes precedence.
/// `verbose`: 0=off, 1=Info, 2=Debug, 3+=Trace.
fn make_test_logger(verbose: u8) -> Logger {
    if std::env::var("GRAPH_LOG").is_ok() {
        return graph::log::logger(true);
    }

    match verbose {
        0 => graph::log::discard(),
        1 => graph::log::logger_with_levels(false, None),
        2 => graph::log::logger_with_levels(true, None),
        // "trace" is parsed by slog_envlogger::LogBuilder::parse() as a global
        // level filter — equivalent to setting GRAPH_LOG=trace.
        _ => graph::log::logger_with_levels(true, Some("trace")),
    }
}

struct TestStores {
    network_name: ChainName,
    /// Listens for chain head updates — needed by the Chain constructor.
    chain_head_listener: Arc<ChainHeadUpdateListener>,
    network_store: Arc<Store>,
    chain_store: Arc<ChainStore>,
}

/// Components needed to run a test after infrastructure setup.
pub(super) struct TestContext {
    pub(super) provider: Arc<graph_core::subgraph_provider::SubgraphProvider>,
    pub(super) store: Arc<SubgraphStore>,
    pub(super) deployment: DeploymentLocator,
    pub(super) graphql_runner: Arc<GraphQlRunner<Store>>,
}

/// Pre-computed manifest data shared across all tests in a run.
///
/// Loaded once to avoid redundant parsing.
pub(super) struct ManifestInfo {
    pub build_dir: PathBuf,
    /// Canonical path to the built manifest file (e.g., `build/subgraph.yaml`).
    /// Registered as an alias for `hash` in `FileLinkResolver` so that
    /// `clone_for_manifest` can resolve the Qm hash to a real filesystem path.
    pub manifest_path: PathBuf,
    pub network_name: ChainName,
    pub min_start_block: u64,
    /// Override for on-chain block validation when startBlock > 0.
    pub start_block_override: Option<BlockPtr>,
    pub hash: DeploymentHash,
    pub subgraph_name: SubgraphName,
}

/// Compute a `DeploymentHash` from a path and seed.
///
/// Produces `"Qm" + hex(sha1(path + '\0' + seed))`. The seed makes each run
/// produce a distinct hash so sequential runs never collide in the store.
fn deployment_hash_from_path_and_seed(path: &Path, seed: u128) -> Result<DeploymentHash> {
    use sha1::{Digest, Sha1};

    let input = format!("{}\0{}", path.display(), seed);
    let digest = Sha1::digest(input.as_bytes());
    let qm = format!("Qm{:x}", digest);
    DeploymentHash::new(qm).map_err(|e| anyhow!("Failed to create deployment hash: {}", e))
}

pub(super) fn load_manifest_info(opt: &TestOpt) -> Result<ManifestInfo> {
    let manifest_dir = opt
        .manifest
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    let build_dir = manifest_dir.join("build");

    let manifest_filename = opt
        .manifest
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("subgraph.yaml");
    let built_manifest_path = build_dir.join(manifest_filename);
    let built_manifest_path = built_manifest_path
        .canonicalize()
        .context("Failed to resolve built manifest path — did you run 'gnd build'?")?;

    let manifest = load_manifest(&built_manifest_path)?;

    let network_name: ChainName = extract_network_from_manifest(&manifest)?.into();
    let min_start_block = extract_start_block_from_manifest(&manifest)?;

    let start_block_override = if min_start_block > 0 {
        use graph::prelude::alloy::primitives::keccak256;
        let hash = keccak256((min_start_block - 1).to_be_bytes());
        ensure!(
            min_start_block - 1 <= i32::MAX as u64,
            "block number {} exceeds i32::MAX",
            min_start_block - 1
        );
        Some(BlockPtr::new(hash.into(), (min_start_block - 1) as i32))
    } else {
        None
    };

    // Use Unix epoch millis as a per-run seed so each invocation gets a unique
    // deployment hash and subgraph name, avoiding conflicts with previous runs.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let hash = deployment_hash_from_path_and_seed(&built_manifest_path, seed)?;

    // Derive subgraph name from the root directory (e.g., "my-subgraph" → "test/my-subgraph-<seed>").
    // Sanitize to alphanumeric + hyphens + underscores for SubgraphName compatibility.
    let root_dir_name = manifest_dir
        .canonicalize()
        .unwrap_or(manifest_dir.clone())
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("gnd-test")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect::<String>();
    let subgraph_name = SubgraphName::new(format!("test/{}-{}", root_dir_name, seed))
        .map_err(|e| anyhow!("{}", e))?;

    Ok(ManifestInfo {
        build_dir,
        manifest_path: built_manifest_path,
        network_name,
        min_start_block,
        start_block_override,
        hash,
        subgraph_name,
    })
}

fn extract_network_from_manifest(manifest: &Manifest) -> Result<String> {
    let network = manifest
        .data_sources
        .first()
        .and_then(|ds| ds.network.clone())
        .unwrap_or_else(|| "mainnet".to_string());

    Ok(network)
}

/// Extract the minimum `startBlock` across all data sources.
///
/// Used to build `start_block_override` for bypassing on-chain validation.
fn extract_start_block_from_manifest(manifest: &Manifest) -> Result<u64> {
    Ok(manifest
        .data_sources
        .iter()
        .map(|ds| ds.start_block)
        .min()
        .unwrap_or(0))
}

pub async fn run_single_test(
    opt: &TestOpt,
    manifest_info: &ManifestInfo,
    test_file: &TestFile,
) -> Result<TestResult> {
    // Warn (and short-circuit) when there are no assertions.
    if test_file.assertions.is_empty() {
        if test_file.blocks.is_empty() {
            eprintln!(
                "  {} Test '{}' has no blocks and no assertions",
                console::style("⚠").yellow(),
                test_file.name
            );
            return Ok(TestResult {
                handler_error: None,
                assertions: vec![],
            });
        } else {
            eprintln!(
                "  {} Test '{}' has blocks but no assertions",
                console::style("⚠").yellow(),
                test_file.name
            );
        }
    }

    // Default block numbering starts at the manifest's startBlock so that
    // test blocks without explicit numbers fall in the subgraph's indexed range.
    let blocks = build_blocks_with_triggers(test_file, manifest_info.min_start_block)?;

    // Create the database for this test. For pgtemp, the `db` value must
    // stay alive for the duration of the test — dropping it destroys the database.
    let db = create_test_database(opt, &manifest_info.build_dir)?;

    let logger = make_test_logger(opt.verbose).new(o!("test" => test_file.name.clone()));

    let stores = setup_stores(&logger, &db, &manifest_info.network_name).await?;

    let chain = setup_chain(&logger, blocks.clone(), &stores).await?;

    let ctx = setup_context(&logger, &stores, &chain, manifest_info).await?;

    // Populate eth_call cache with mock responses before starting indexer.
    // This ensures handlers can successfully retrieve mocked contract call results.
    super::eth_calls::populate_eth_call_cache(
        &logger,
        stores.chain_store.cheap_clone(),
        &blocks,
        test_file,
    )
    .await?;

    // Determine the target block — the indexer will process until it reaches this.
    let stop_block = if blocks.is_empty() {
        mock_chain::genesis_ptr()
    } else {
        mock_chain::final_block_ptr(&blocks).ok_or_else(|| anyhow!("No blocks to process"))?
    };

    info!(logger, "Starting subgraph indexing"; "stop_block" => stop_block.number);

    ctx.provider
        .clone()
        .start_subgraph(ctx.deployment.clone(), Some(stop_block.number))
        .await;

    let result = match wait_for_sync(
        &logger,
        ctx.store.clone(),
        &ctx.deployment,
        stop_block.clone(),
    )
    .await
    {
        Ok(()) => run_assertions(&ctx, &test_file.assertions).await,
        Err(subgraph_error) => {
            // The subgraph handler threw a fatal error during indexing.
            // Report it as a test failure without running assertions.
            Ok(TestResult {
                handler_error: Some(subgraph_error.message),
                assertions: vec![],
            })
        }
    };

    // Always stop the subgraph to ensure cleanup, even when wait_for_sync errors
    ctx.provider
        .clone()
        .stop_subgraph(ctx.deployment.clone())
        .await;

    // For persistent databases, clean up the deployment after the test so the
    // database is left in a clean state. On failure, skip cleanup so the data
    // is preserved for inspection.
    if db.needs_cleanup() {
        let test_passed = result.as_ref().map(|r| r.is_passed()).unwrap_or(false);
        if test_passed {
            cleanup(
                &ctx.store,
                &manifest_info.subgraph_name,
                &manifest_info.hash,
            )
            .await
            .ok();
        } else {
            eprintln!(
                "  {} Test failed — database preserved for inspection: {}",
                console::style("ℹ").cyan(),
                db.url()
            );
        }
    }

    result
}

/// Create the database for this test run.
///
/// Returns `Temporary` (pgtemp, auto-dropped) or `Persistent` (--postgres-url).
/// On non-Unix systems, `--postgres-url` is required.
fn create_test_database(opt: &TestOpt, build_dir: &Path) -> Result<TestDatabase> {
    if let Some(url) = &opt.postgres_url {
        return Ok(TestDatabase::Persistent { url: url.clone() });
    }

    #[cfg(unix)]
    {
        if !build_dir.exists() {
            anyhow::bail!(
                "Build directory does not exist: {}. Run 'gnd build' first.",
                build_dir.display()
            );
        }

        // pgtemp sets `unix_socket_directories` to the data dir by default.
        // On macOS the temp dir path can exceed the 104-byte Unix socket limit
        // (e.g. /private/var/folders/.../build/pgtemp-xxx/pg_data_dir/.s.PGSQL.PORT),
        // causing postgres to silently fail to start. Override to /tmp so the
        // socket path stays short. Different port numbers prevent conflicts.
        let db = PgTempDBBuilder::new()
            .with_data_dir_prefix(build_dir)
            .persist_data(false)
            .with_initdb_arg("-E", "UTF8")
            .with_initdb_arg("--locale", "C")
            .with_config_param("unix_socket_directories", "/tmp")
            .start();

        let url = db.connection_uri().to_string();
        Ok(TestDatabase::Temporary { url, _handle: db })
    }

    #[cfg(not(unix))]
    {
        let _ = build_dir;
        Err(anyhow!(
            "On non-Unix systems, please provide --postgres-url"
        ))
    }
}

/// Database used for a single test run.
enum TestDatabase {
    #[cfg(unix)]
    Temporary {
        url: String,
        _handle: pgtemp::PgTempDB,
    },
    Persistent {
        url: String,
    },
}

impl TestDatabase {
    fn url(&self) -> &str {
        match self {
            #[cfg(unix)]
            Self::Temporary { url, .. } => url,
            Self::Persistent { url } => url,
        }
    }

    /// Persistent databases accumulate state across test runs and need
    /// explicit post-test cleanup to remove each run's deployment.
    /// Temporary databases are dropped automatically — no cleanup needed.
    fn needs_cleanup(&self) -> bool {
        match self {
            #[cfg(unix)]
            Self::Temporary { .. } => false,
            Self::Persistent { .. } => true,
        }
    }
}

async fn setup_stores(
    logger: &Logger,
    db: &TestDatabase,
    network_name: &ChainName,
) -> Result<TestStores> {
    let config_str = format!(
        r#"
[store]
[store.primary]
connection = "{}"
pool_size = 2

[deployment]
[[deployment.rule]]
store = "primary"
indexers = [ "default" ]

[chains]
ingestor = "default"
"#,
        db.url()
    );

    let config = Config::from_str(&config_str, "default")
        .map_err(|e| anyhow!("Failed to parse config: {}", e))?;

    let mock_registry = Arc::new(MetricsRegistry::mock());
    let node_id = NodeId::new(NODE_ID).unwrap();

    // StoreBuilder runs migrations and creates connection pools.
    let store_builder =
        StoreBuilder::new(logger, &node_id, &config, None, mock_registry.clone()).await;

    let chain_head_listener = store_builder.chain_head_update_listener();
    let network_identifiers: Vec<ChainName> = vec![network_name.clone()];
    let network_store = store_builder.network_store(network_identifiers).await;

    let block_store = network_store.block_store();

    // Synthetic chain identifier — net_version "1" with zero genesis hash.
    let ident = ChainIdentifier {
        net_version: "1".into(),
        genesis_block_hash: graph::prelude::alloy::primitives::B256::ZERO.into(),
    };

    let chain_store = block_store
        .create_chain_store(network_name, ident)
        .await
        .context("Failed to create chain store")?;

    Ok(TestStores {
        network_name: network_name.clone(),
        chain_head_listener,
        network_store,
        chain_store,
    })
}

/// Construct a mock Ethereum `Chain` with pre-built blocks.
///
/// Uses `StaticStreamBuilder` for blocks, noops for unused adapters,
/// and a dummy firehose endpoint (never connected to).
async fn setup_chain(
    logger: &Logger,
    blocks: Vec<BlockWithTriggers<Chain>>,
    stores: &TestStores,
) -> Result<Arc<Chain>> {
    let mock_registry = Arc::new(MetricsRegistry::mock());
    let logger_factory = LoggerFactory::new(logger.clone(), None, mock_registry.clone());

    // Dummy firehose endpoint — required by Chain constructor but never used.
    let firehose_endpoints = FirehoseEndpoints::for_testing(vec![Arc::new(FirehoseEndpoint::new(
        "",
        "http://0.0.0.0:0",
        None,
        None,
        true,
        false,
        SubgraphLimit::Unlimited,
        Arc::new(EndpointMetrics::mock()),
    ))]);

    let client =
        Arc::new(graph::blockchain::client::ChainClient::<Chain>::new_firehose(firehose_endpoints));

    let block_stream_builder: Arc<dyn graph::blockchain::block_stream::BlockStreamBuilder<Chain>> =
        Arc::new(StaticStreamBuilder { chain: blocks });

    // Create a dummy Ethereum adapter with archive capabilities.
    // The adapter itself is never used for RPC — ethereum.call results come from
    // the pre-populated call cache. But the RuntimeAdapter needs to resolve an
    // adapter with matching capabilities before it can invoke the cache lookup.
    let endpoint_metrics = Arc::new(EndpointMetrics::mock());
    let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));
    let transport = Transport::new_rpc(
        graph::url::Url::parse("http://0.0.0.0:0").unwrap(),
        graph::http::HeaderMap::new(),
        endpoint_metrics.clone(),
        "",
        false, // no_eip2718
        graph_chain_ethereum::Compression::None,
    );
    let dummy_adapter = Arc::new(
        EthereumAdapter::new(
            logger.clone(),
            String::new(),
            transport,
            provider_metrics,
            true,
            false,
        )
        .await,
    );
    let adapter = EthereumNetworkAdapter::new(
        endpoint_metrics,
        NodeCapabilities {
            archive: true,
            traces: false,
        },
        dummy_adapter,
        SubgraphLimit::Unlimited,
    );
    let provider_manager = ProviderManager::new(
        logger.clone(),
        vec![(stores.network_name.clone(), vec![adapter])],
        ProviderCheckStrategy::MarkAsValid,
    );
    let eth_adapters = Arc::new(EthereumNetworkAdapters::new(
        stores.network_name.clone(),
        provider_manager,
        vec![],
        None,
    ));

    let chain = Chain::new(
        logger_factory,
        stores.network_name.clone(),
        mock_registry,
        stores.chain_store.cheap_clone(),
        stores.chain_store.cheap_clone(),
        client,
        stores.chain_head_listener.cheap_clone(),
        block_stream_builder,
        Arc::new(StaticBlockRefetcher {
            _phantom: PhantomData,
        }),
        Arc::new(NoopAdapterSelector {
            _phantom: PhantomData,
        }),
        Arc::new(EthereumRuntimeAdapterBuilder {}),
        eth_adapters,
        graph::prelude::ENV_VARS.reorg_threshold(),
        graph::prelude::ENV_VARS.ingestor_polling_interval,
        true,
    );

    Ok(Arc::new(chain))
}

/// Wire up all graph-node components and deploy the subgraph.
async fn setup_context(
    logger: &Logger,
    stores: &TestStores,
    chain: &Arc<Chain>,
    manifest_info: &ManifestInfo,
) -> Result<TestContext> {
    let build_dir = &manifest_info.build_dir;
    let manifest_path = &manifest_info.manifest_path;
    let hash = manifest_info.hash.clone();
    let subgraph_name = manifest_info.subgraph_name.clone();
    let start_block_override = manifest_info.start_block_override.clone();

    let env_vars = Arc::new(EnvVars::from_env().unwrap_or_default());
    let mock_registry = Arc::new(MetricsRegistry::mock());
    let logger_factory = LoggerFactory::new(logger.clone(), None, mock_registry.clone());
    let node_id = NodeId::new(NODE_ID).unwrap();

    let subgraph_store = stores.network_store.subgraph_store();

    // Map the network name to our mock chain so graph-node routes triggers correctly.
    let mut blockchain_map = BlockchainMap::new();
    blockchain_map.insert(stores.network_name.clone(), chain.clone());
    let blockchain_map = Arc::new(blockchain_map);

    // FileLinkResolver loads the manifest and WASM from the build directory
    // instead of fetching from IPFS. The alias maps the Qm deployment hash to the
    // actual manifest path so that clone_for_manifest can resolve it without
    // treating the hash as a filesystem path.
    let aliases =
        std::collections::HashMap::from([(hash.to_string(), manifest_path.to_path_buf())]);
    let link_resolver: Arc<dyn graph::components::link_resolver::LinkResolver> = Arc::new(
        FileLinkResolver::new(Some(build_dir.to_path_buf()), aliases),
    );

    // IPFS client is required by the instance manager constructor but not used
    // for manifest loading (FileLinkResolver handles that).
    let ipfs_metrics = IpfsMetrics::new(&mock_registry);
    let ipfs_client = Arc::new(
        IpfsRpcClient::new_unchecked(ServerAddress::test_rpc_api(), ipfs_metrics, logger)
            .context("Failed to create IPFS client")?,
    );

    let ipfs_service = ipfs_service(
        ipfs_client,
        env_vars.mappings.max_ipfs_file_bytes,
        env_vars.mappings.ipfs_timeout,
        env_vars.mappings.ipfs_request_limit,
    );

    let arweave_resolver = Arc::new(ArweaveClient::default());
    let arweave_service = arweave_service(
        arweave_resolver.cheap_clone(),
        env_vars.mappings.ipfs_request_limit,
        graph::components::link_resolver::FileSizeLimit::MaxBytes(
            env_vars.mappings.max_ipfs_file_bytes as u64,
        ),
    );

    let sg_count = Arc::new(SubgraphCountMetric::new(mock_registry.cheap_clone()));
    let static_filters = env_vars.experimental_static_filters;

    let subgraph_instance_manager = Arc::new(graph_core::subgraph::SubgraphInstanceManager::<
        SubgraphStore,
        FlightClient,
    >::new(
        &logger_factory,
        env_vars.cheap_clone(),
        subgraph_store.clone(),
        blockchain_map.clone(),
        sg_count.cheap_clone(),
        mock_registry.clone(),
        link_resolver.cheap_clone(),
        ipfs_service,
        arweave_service,
        None,
        static_filters,
    ));

    let mut subgraph_instance_managers =
        graph_core::subgraph_provider::SubgraphInstanceManagers::new();
    subgraph_instance_managers.add(
        graph_core::subgraph_provider::SubgraphProcessingKind::Trigger,
        subgraph_instance_manager.cheap_clone(),
    );

    let subgraph_provider = Arc::new(graph_core::subgraph_provider::SubgraphProvider::new(
        &logger_factory,
        sg_count.cheap_clone(),
        subgraph_store.clone(),
        link_resolver.cheap_clone(),
        tokio_util::sync::CancellationToken::new(),
        subgraph_instance_managers,
    ));

    let load_manager = LoadManager::new(logger, Vec::new(), Vec::new(), mock_registry.clone());
    let graphql_runner = Arc::new(GraphQlRunner::new(
        logger,
        stores.network_store.clone(),
        Arc::new(load_manager),
        mock_registry.clone(),
    ));

    // The registrar handles subgraph naming and version management.
    // Uses PanicSubscriptionManager because tests don't need GraphQL subscriptions.
    let panicking_subscription_manager = Arc::new(PanicSubscriptionManager {});
    let subgraph_registrar = Arc::new(graph_core::subgraph::SubgraphRegistrar::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_provider.cheap_clone(),
        subgraph_store.clone(),
        panicking_subscription_manager,
        Option::<Arc<graph::amp::FlightClient>>::None,
        blockchain_map.clone(),
        node_id.clone(),
        SubgraphVersionSwitchingMode::Instant,
        Arc::new(Settings::default()),
        Arc::new(AmpChainNames::default()),
    ));

    SubgraphRegistrar::create_subgraph(subgraph_registrar.as_ref(), subgraph_name.clone()).await?;

    // Deploy the subgraph version (loads manifest, compiles WASM, creates schema tables).
    // start_block_override bypasses on-chain block validation when startBlock > 0.
    let deployment = SubgraphRegistrar::create_subgraph_version(
        subgraph_registrar.as_ref(),
        subgraph_name.clone(),
        hash.clone(),
        node_id.clone(),
        None,
        start_block_override,
        None,
        None,
        false,
    )
    .await?;

    Ok(TestContext {
        provider: subgraph_provider,
        store: subgraph_store,
        deployment,
        graphql_runner,
    })
}

/// Remove a subgraph deployment after a test run. Errors are ignored.
async fn cleanup(
    subgraph_store: &SubgraphStore,
    name: &SubgraphName,
    hash: &DeploymentHash,
) -> Result<()> {
    let locators = SubgraphStoreTrait::locators(subgraph_store, hash).await?;

    // Ignore errors - the subgraph might not exist on first run
    let _ = subgraph_store.remove_subgraph(name.clone()).await;

    for locator in &locators {
        // Unassign the deployment from its node first — remove_deployment
        // silently skips deletion if the deployment is still assigned.
        let _ = SubgraphStoreTrait::unassign_subgraph(subgraph_store, locator).await;
        subgraph_store.remove_deployment(locator.id.into()).await?;
    }

    Ok(())
}

/// Poll until the subgraph reaches `stop_block` or fails.
///
/// Returns `Ok(())` on success or `Err(SubgraphError)` on fatal error or timeout.
async fn wait_for_sync(
    logger: &Logger,
    store: Arc<SubgraphStore>,
    deployment: &DeploymentLocator,
    stop_block: BlockPtr,
) -> Result<(), SubgraphError> {
    // NOTE: Hardcoded timeout/interval - could be made configurable via env var
    // or CLI flag for slow subgraphs or faster iteration during development.
    const MAX_WAIT: Duration = Duration::from_secs(60);
    const WAIT_TIME: Duration = Duration::from_millis(500);

    let start = Instant::now();

    async fn flush(logger: &Logger, store: &Arc<SubgraphStore>, deployment: &DeploymentLocator) {
        if let Ok(writable) = store
            .clone()
            .writable(logger.clone(), deployment.id, Arc::new(vec![]))
            .await
        {
            let _ = writable.flush().await;
        }
    }

    flush(logger, &store, deployment).await;

    while start.elapsed() < MAX_WAIT {
        tokio::time::sleep(WAIT_TIME).await;
        flush(logger, &store, deployment).await;

        let block_ptr = match store.least_block_ptr(&deployment.hash).await {
            Ok(Some(ptr)) => ptr,
            _ => continue, // Not started yet
        };

        info!(logger, "Sync progress"; "current" => block_ptr.number, "target" => stop_block.number);

        // Check if the subgraph hit a fatal error (e.g., handler panic, deterministic error).
        let status = store.status_for_id(deployment.id).await;
        if let Some(fatal_error) = status.fatal_error {
            return Err(fatal_error);
        }

        if block_ptr.number >= stop_block.number {
            info!(logger, "Reached stop block");
            return Ok(());
        }
    }

    Err(SubgraphError {
        subgraph_id: deployment.hash.clone(),
        message: format!("Sync timeout after {}s", MAX_WAIT.as_secs()),
        block_ptr: None,
        handler: None,
        deterministic: false,
    })
}
