//! Test runner: orchestrates subgraph indexing with mock blockchain data.
//!
//! This is the core of `gnd test`. For each test file, it:
//!
//! 1. Creates a temporary PostgreSQL database (pgtemp) for complete test isolation
//! 2. Initializes graph-node stores (entity storage, block storage, chain store)
//! 3. Constructs a mock Ethereum chain that feeds pre-defined blocks
//! 4. Deploys the subgraph and starts the indexer
//! 5. Waits for all blocks to be processed (or a fatal error)
//! 6. Runs GraphQL assertions against the indexed entity state
//!
//! ## Architecture
//!
//! The runner reuses real graph-node infrastructure — the same store, WASM runtime,
//! and trigger processing code used in production. Only the blockchain layer is
//! mocked via `StaticStreamBuilder` (see [`super::block_stream`]), which feeds
//! pre-built `BlockWithTriggers` from the test JSON instead of fetching from an
//! RPC endpoint.
//!
//! This approach follows the same pattern as `gnd dev`, which also uses
//! `FileLinkResolver` and filesystem-based deployment hashes instead of IPFS.
//!
//! Noop/stub adapters (see [`super::noop`]) satisfy the `Chain` constructor's
//! trait bounds without making real network calls.

use super::assertion::run_assertions;
use super::block_stream::{MutexBlockStreamBuilder, StaticStreamBuilder};
use super::mock_chain;
use super::noop::{NoopAdapterSelector, StaticBlockRefetcher};
use super::schema::{TestFile, TestResult};
use super::trigger::build_blocks_with_triggers;
use super::TestOpt;
use crate::manifest::{load_manifest, Manifest};
use anyhow::{anyhow, Context, Result};
use graph::amp::FlightClient;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::{BlockPtr, BlockchainMap, ChainIdentifier};
use graph::cheap_clone::CheapClone;
use graph::components::link_resolver::{ArweaveClient, FileLinkResolver};
use graph::components::metrics::MetricsRegistry;
use graph::components::network_provider::{ChainName, ProviderCheckStrategy, ProviderManager};
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
use graph::slog::{info, o, Drain, Logger, OwnedKVList, Record};
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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use pgtemp::PgTempDBBuilder;

/// Node ID used for all test deployments. Visible in store metadata.
const NODE_ID: &str = "gnd-test";

// ============ Test Infrastructure Types ============

/// A slog drain that suppresses the "Store event stream ended" error message.
///
/// When a test completes and the pgtemp database is dropped, the store's
/// background subscription listener loses its connection and logs an error.
/// This is expected during cleanup and not a real problem, so we filter it
/// out to avoid confusing test output. All other log messages pass through.
struct FilterStoreEventEndedDrain<D: Drain> {
    inner: D,
}

impl<D: Drain> Drain for FilterStoreEventEndedDrain<D> {
    type Ok = Option<D::Ok>;
    type Err = D::Err;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        if record
            .msg()
            .to_string()
            .contains("Store event stream ended")
        {
            return Ok(None);
        }
        self.inner.log(record, values).map(Some)
    }
}

/// Bundles the store infrastructure needed for test execution.
///
/// Created once per test and holds the connection pools, chain store,
/// and chain head listener that the indexer needs.
struct TestStores {
    /// Network name from the subgraph manifest (e.g., "mainnet").
    /// Must match the chain config so graph-node routes triggers correctly.
    network_name: ChainName,
    /// Listens for chain head updates — needed by the Chain constructor.
    chain_head_listener: Arc<ChainHeadUpdateListener>,
    /// The top-level store (wraps subgraph store + block store).
    network_store: Arc<Store>,
    /// Per-chain block storage.
    chain_store: Arc<ChainStore>,
}

/// All the pieces needed to run a test after infrastructure setup.
///
/// Holds references to the subgraph provider (for starting indexing),
/// the store (for querying sync status), the deployment locator,
/// and the GraphQL runner (for assertions).
pub(super) struct TestContext {
    #[allow(dead_code)]
    pub(super) logger: Logger,
    /// Starts/stops subgraph indexing.
    pub(super) provider: Arc<graph_core::subgraph_provider::SubgraphProvider>,
    /// Used to check sync progress and health status.
    pub(super) store: Arc<SubgraphStore>,
    /// Identifies this specific subgraph deployment in the store.
    pub(super) deployment: DeploymentLocator,
    /// Executes GraphQL queries against the indexed data.
    pub(super) graphql_runner: Arc<GraphQlRunner<Store>>,
}

// ============ Test Execution ============

/// Extract the network name (e.g., "mainnet") from the first data source in a manifest.
///
/// The network name must match the chain configuration passed to the store,
/// otherwise graph-node won't route triggers to the correct chain.
/// Falls back to "mainnet" if not found (the common case for Ethereum subgraphs).
fn extract_network_from_manifest(manifest: &Manifest) -> Result<String> {
    let network = manifest
        .data_sources
        .first()
        .and_then(|ds| ds.network.clone())
        .unwrap_or_else(|| "mainnet".to_string());

    Ok(network)
}

/// Extract the minimum `startBlock` across all Ethereum data sources in a manifest.
///
/// When a manifest specifies `startBlock` on its data sources, graph-node
/// normally validates that the block exists on-chain during deployment.
/// In tests there is no real chain, so the caller uses this value to build
/// a `start_block_override` that bypasses validation.
///
/// Only considers Ethereum data sources (kind: "ethereum" or "ethereum/contract")
/// since gnd test only supports testing Ethereum contracts.
///
/// Returns 0 if no Ethereum data source specifies a `startBlock`.
fn extract_start_block_from_manifest(manifest: &Manifest) -> Result<u64> {
    Ok(manifest
        .data_sources
        .iter()
        .map(|ds| ds.start_block)
        .min()
        .unwrap_or(0))
}

/// Run a single test file end-to-end.
///
/// This is the main entry point called from `mod.rs` for each test file.
/// It creates isolated infrastructure (database, stores, chain), indexes
/// the mock blocks, and checks the GraphQL assertions.
///
/// Returns `TestResult::Passed` if all assertions match, or `TestResult::Failed`
/// with details about handler errors or assertion mismatches.
pub async fn run_single_test(opt: &TestOpt, test_file: &TestFile) -> Result<TestResult> {
    // Empty test with no blocks and no assertions is trivially passing.
    if test_file.blocks.is_empty() && test_file.assertions.is_empty() {
        return Ok(TestResult::Passed { assertions: vec![] });
    }

    // Resolve paths relative to the manifest location.
    let manifest_dir = opt
        .manifest
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    // The build directory contains compiled WASM, schema, and the built manifest.
    // Created by `gnd build` (which runs automatically unless --skip-build).
    let build_dir = manifest_dir.join("build");

    let manifest_filename = opt
        .manifest
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("subgraph.yaml");
    let built_manifest_path = build_dir.join(manifest_filename);
    let built_manifest_path = built_manifest_path.canonicalize().with_context(|| {
        format!(
            "Built manifest not found: {}",
            built_manifest_path.display()
        )
    })?;

    let manifest = load_manifest(&built_manifest_path)?;
    // The network name from the manifest (e.g., "mainnet") determines which
    // chain config the store uses. Must match exactly.
    let network_name: ChainName = extract_network_from_manifest(&manifest)?.into();

    // Extract the minimum startBlock from the manifest. When startBlock > 0,
    // graph-node normally validates the block exists on-chain, but our test
    // environment has no real chain. We provide a start_block_override to
    // bypass validation, and also default test block numbering to start at
    // the manifest's startBlock so blocks land in the indexed range.
    let min_start_block = extract_start_block_from_manifest(&manifest)?;

    // Convert test JSON blocks into graph-node's internal block format.
    // Default block numbering starts at the manifest's startBlock so that
    // test blocks without explicit numbers fall in the subgraph's indexed range.
    let blocks = build_blocks_with_triggers(test_file, min_start_block)?;

    // Build a start_block_override when startBlock > 0 to bypass on-chain
    // block validation (which would fail against the dummy firehose endpoint).
    // This mirrors what resolve_start_block() computes: a BlockPtr for
    // block (min_start_block - 1).
    let start_block_override = if min_start_block > 0 {
        use graph::prelude::alloy::primitives::keccak256;
        let hash = keccak256((min_start_block - 1).to_be_bytes());
        Some(BlockPtr::new(hash.into(), (min_start_block - 1) as i32))
    } else {
        None
    };

    // Create a temporary database for this test. The `_temp_db` handle must
    // be kept alive for the duration of the test — dropping it destroys the database.
    let (db_url, _temp_db) = get_database_url(opt, &build_dir)?;

    let logger = graph::log::logger(false).new(o!("test" => test_file.name.clone()));

    // Initialize stores with the network name from the manifest.
    let stores = setup_stores(&logger, &db_url, &network_name).await?;

    // Create the mock Ethereum chain that will feed our pre-built blocks.
    let chain = setup_chain(&test_file.name, blocks.clone(), &stores).await?;

    // Use the built manifest path as the deployment hash, matching gnd dev's pattern.
    // FileLinkResolver resolves the hash back to the filesystem path when loading.
    let deployment_id = built_manifest_path.display().to_string();
    let hash = DeploymentHash::new(&deployment_id).map_err(|_| {
        anyhow!(
            "Failed to create deployment hash from path: {}",
            deployment_id
        )
    })?;

    // Sanitize test name for use as a subgraph name (alphanumeric + hyphens + underscores).
    let test_name_sanitized = test_file
        .name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .collect::<String>();
    let subgraph_name =
        SubgraphName::new(format!("test/{}", test_name_sanitized)).map_err(|e| anyhow!("{}", e))?;

    // Wire up all graph-node components (instance manager, provider, registrar, etc.)
    // and deploy the subgraph.
    let ctx = setup_context(
        &logger,
        &stores,
        &chain,
        &build_dir,
        hash,
        subgraph_name.clone(),
        start_block_override,
    )
    .await?;

    // Populate eth_call cache with mock responses before starting indexer.
    // This ensures handlers can successfully retrieve mocked contract call results.
    super::eth_calls::populate_eth_call_cache(
        &logger,
        stores.chain_store.cheap_clone(),
        &blocks,
        test_file,
    )
    .await
    .context("Failed to populate eth_call cache")?;

    // Determine the target block — the indexer will process until it reaches this.
    let stop_block = if blocks.is_empty() {
        mock_chain::genesis_ptr()
    } else {
        mock_chain::final_block_ptr(&blocks).ok_or_else(|| anyhow!("No blocks to process"))?
    };

    // Start the indexer and wait for it to process all blocks.
    info!(logger, "Starting subgraph indexing"; "stop_block" => stop_block.number);

    ctx.provider
        .clone()
        .start_subgraph(ctx.deployment.clone(), Some(stop_block.number))
        .await;

    match wait_for_sync(
        &logger,
        ctx.store.clone(),
        &ctx.deployment,
        stop_block.clone(),
    )
    .await
    {
        Ok(()) => {
            // Indexing succeeded — now validate the entity state via GraphQL.
            run_assertions(&ctx, &test_file.assertions).await
        }
        Err(subgraph_error) => {
            // The subgraph handler threw a fatal error during indexing.
            // Report it as a test failure without running assertions.
            Ok(TestResult::Failed {
                handler_error: Some(subgraph_error.message),
                assertions: vec![],
            })
        }
    }
}

/// Get a PostgreSQL connection URL for the test.
///
/// If `--postgres-url` was provided, uses that directly.
/// Otherwise, on Unix, creates a temporary database via pgtemp in the build
/// directory (matching `gnd dev`'s pattern). The database is automatically
/// destroyed when `TempPgHandle` is dropped.
///
/// On non-Unix systems, `--postgres-url` is required.
fn get_database_url(opt: &TestOpt, build_dir: &Path) -> Result<(String, Option<TempPgHandle>)> {
    if let Some(url) = &opt.postgres_url {
        return Ok((url.clone(), None));
    }

    #[cfg(unix)]
    {
        if !build_dir.exists() {
            anyhow::bail!(
                "Build directory does not exist: {}. Run 'gnd build' first.",
                build_dir.display()
            );
        }

        let db = PgTempDBBuilder::new()
            .with_data_dir_prefix(build_dir)
            .persist_data(false)
            .with_initdb_arg("-E", "UTF8")
            .with_initdb_arg("--locale", "C")
            .start();

        let url = db.connection_uri().to_string();
        Ok((url, Some(TempPgHandle(db))))
    }

    #[cfg(not(unix))]
    {
        let _ = build_dir;
        Err(anyhow!(
            "On non-Unix systems, please provide --postgres-url"
        ))
    }
}

/// RAII handle that keeps a pgtemp database alive for the test's duration.
///
/// The inner `PgTempDB` is never read directly — its purpose is to prevent
/// the temporary database from being destroyed until this handle is dropped.
#[cfg(unix)]
struct TempPgHandle(#[allow(dead_code)] pgtemp::PgTempDB);

#[cfg(not(unix))]
struct TempPgHandle;

/// Initialize graph-node stores from a database URL.
///
/// Creates:
/// - A TOML config with the database URL and a chain entry for the test network
/// - A `StoreBuilder` that runs database migrations and creates connection pools
/// - A chain store for the test chain with a synthetic genesis block (hash=0x0)
///
/// Uses a filtered logger to suppress the expected "Store event stream ended"
/// error that occurs when pgtemp is dropped during cleanup.
async fn setup_stores(
    _logger: &Logger,
    db_url: &str,
    network_name: &ChainName,
) -> Result<TestStores> {
    // Minimal graph-node config: one primary shard, no chain providers.
    // The chain→shard mapping defaults to "primary" in StoreBuilder::make_store,
    // and we construct EthereumNetworkAdapters directly in setup_chain.
    let config_str = format!(
        r#"
[store]
[store.primary]
connection = "{}"
pool_size = 10

[deployment]
[[deployment.rule]]
store = "primary"
indexers = [ "default" ]

[chains]
ingestor = "default"
"#,
        db_url
    );

    let config = Config::from_str(&config_str, "default")
        .map_err(|e| anyhow!("Failed to parse config: {}", e))?;

    let mock_registry = Arc::new(MetricsRegistry::mock());
    let node_id = NodeId::new(NODE_ID).unwrap();

    // Filter out the "Store event stream ended" error that fires during
    // cleanup when pgtemp drops the database out from under the listener.
    let base_logger = graph::log::logger(false);
    let filtered_drain = FilterStoreEventEndedDrain {
        inner: base_logger.clone(),
    };
    let store_logger = Logger::root(filtered_drain.fuse(), o!());

    // StoreBuilder runs migrations and creates connection pools.
    let store_builder = StoreBuilder::new(
        &store_logger,
        &node_id,
        &config,
        None,
        mock_registry.clone(),
    )
    .await;

    let chain_head_listener = store_builder.chain_head_update_listener();
    let network_identifiers: Vec<ChainName> = vec![network_name.clone()];
    let network_store = store_builder.network_store(network_identifiers).await;

    // Synthetic chain identifier — net_version "1" with zero genesis hash.
    let ident = ChainIdentifier {
        net_version: "1".into(),
        genesis_block_hash: graph::prelude::alloy::primitives::B256::ZERO.into(),
    };

    let chain_store = network_store
        .block_store()
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
/// The chain uses:
/// - `StaticStreamBuilder`: feeds pre-defined blocks instead of RPC/Firehose
/// - `NoopAdapterSelector` / `NoopRuntimeAdapterBuilder`: stubs for unused interfaces
/// - `StaticBlockRefetcher`: no-op since there are no reorgs in tests
/// - A dummy firehose endpoint (never actually connected to)
async fn setup_chain(
    test_name: &str,
    blocks: Vec<BlockWithTriggers<Chain>>,
    stores: &TestStores,
) -> Result<Arc<Chain>> {
    let logger = graph::log::logger(false).new(o!("test" => test_name.to_string()));
    let mock_registry = Arc::new(MetricsRegistry::mock());
    let logger_factory = LoggerFactory::new(logger.clone(), None, mock_registry.clone());

    // Dummy firehose endpoint — required by Chain constructor but never used.
    // Uses 0.0.0.0:0 to prevent accidental DNS lookups if the endpoint is ever reached.
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

    let static_block_stream = Arc::new(StaticStreamBuilder { chain: blocks });
    let block_stream_builder = Arc::new(MutexBlockStreamBuilder(Mutex::new(static_block_stream)));

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
///
/// This mirrors what `gnd dev` does via the launcher, but assembled directly:
/// 1. Clean up any leftover deployment from a previous run
/// 2. Create blockchain map (just our mock chain)
/// 3. Set up link resolver (FileLinkResolver for local filesystem)
/// 4. Create the subgraph instance manager (WASM runtime, trigger processing)
/// 5. Create the subgraph provider (lifecycle management)
/// 6. Create the GraphQL runner (for assertions)
/// 7. Register and deploy the subgraph via the registrar
async fn setup_context(
    logger: &Logger,
    stores: &TestStores,
    chain: &Arc<Chain>,
    build_dir: &Path,
    hash: DeploymentHash,
    subgraph_name: SubgraphName,
    start_block_override: Option<BlockPtr>,
) -> Result<TestContext> {
    let env_vars = Arc::new(EnvVars::from_env().unwrap_or_default());
    let mock_registry = Arc::new(MetricsRegistry::mock());
    let logger_factory = LoggerFactory::new(logger.clone(), None, mock_registry.clone());
    let node_id = NodeId::new(NODE_ID).unwrap();

    let subgraph_store = stores.network_store.subgraph_store();

    // Remove any leftover deployment from a previous test run (idempotent).
    cleanup(&subgraph_store, &subgraph_name, &hash).await.ok();

    // Map the network name to our mock chain so graph-node routes triggers correctly.
    let mut blockchain_map = BlockchainMap::new();
    blockchain_map.insert(stores.network_name.clone(), chain.clone());
    let blockchain_map = Arc::new(blockchain_map);

    // FileLinkResolver loads the manifest and WASM from the build directory
    // instead of fetching from IPFS. This matches gnd dev's approach.
    let link_resolver: Arc<dyn graph::components::link_resolver::LinkResolver> =
        Arc::new(FileLinkResolver::with_base_dir(build_dir));

    // IPFS client is required by the instance manager constructor but not used
    // for manifest loading (FileLinkResolver handles that).
    let ipfs_metrics = IpfsMetrics::new(&mock_registry);
    let ipfs_client = Arc::new(
        IpfsRpcClient::new_unchecked(ServerAddress::local_rpc_api(), ipfs_metrics, logger)
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

    // The instance manager handles WASM compilation, trigger processing,
    // and entity storage for running subgraphs.
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

    // The provider manages subgraph lifecycle (start/stop indexing).
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

    // GraphQL runner for executing assertion queries against indexed data.
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
    ));

    // Register the subgraph name (e.g., "test/TransferCreatesEntity").
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
        logger: logger_factory.subgraph_logger(&deployment),
        provider: subgraph_provider,
        store: subgraph_store,
        deployment,
        graphql_runner,
    })
}

/// Remove a previous subgraph deployment and its data.
///
/// Called before each test to ensure a clean slate. Errors are ignored
/// (the deployment might not exist on first run).
async fn cleanup(
    subgraph_store: &SubgraphStore,
    name: &SubgraphName,
    hash: &DeploymentHash,
) -> Result<()> {
    let locators = SubgraphStoreTrait::locators(subgraph_store, hash).await?;

    // Ignore errors - the subgraph might not exist on first run
    let _ = subgraph_store.remove_subgraph(name.clone()).await;

    for locator in locators {
        subgraph_store.remove_deployment(locator.id.into()).await?;
    }

    Ok(())
}

/// Poll the store until the subgraph reaches the target block or fails.
///
/// Periodically flushes the store's write buffer to speed up block processing
/// (the store batches writes and flush forces them through immediately).
///
/// Returns `Ok(())` when the subgraph reaches `stop_block`, or `Err(SubgraphError)`
/// if the subgraph fails with a fatal error or times out after 60 seconds.
async fn wait_for_sync(
    logger: &Logger,
    store: Arc<SubgraphStore>,
    deployment: &DeploymentLocator,
    stop_block: BlockPtr,
) -> Result<(), SubgraphError> {
    const MAX_WAIT: Duration = Duration::from_secs(60);
    const WAIT_TIME: Duration = Duration::from_millis(500);

    let start = Instant::now();

    /// Force the store to flush its write buffer, making pending entity
    /// changes visible to queries sooner.
    async fn flush(logger: &Logger, store: &Arc<SubgraphStore>, deployment: &DeploymentLocator) {
        if let Ok(writable) = store
            .clone()
            .writable(logger.clone(), deployment.id, Arc::new(vec![]))
            .await
        {
            let _ = writable.flush().await;
        }
    }

    // Initial flush to ensure any pre-existing writes are visible.
    flush(logger, &store, deployment).await;

    while start.elapsed() < MAX_WAIT {
        tokio::time::sleep(WAIT_TIME).await;
        flush(logger, &store, deployment).await;

        // Check current indexing progress.
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

    // Timeout — return a synthetic error.
    Err(SubgraphError {
        subgraph_id: deployment.hash.clone(),
        message: format!("Sync timeout after {}s", MAX_WAIT.as_secs()),
        block_ptr: None,
        handler: None,
        deterministic: false,
    })
}
