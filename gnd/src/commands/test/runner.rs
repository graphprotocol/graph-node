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

use super::TestOpt;
use super::assertion::run_assertions;
use super::block_stream::StaticStreamBuilder;
use super::mock_arweave::MockArweaveResolver;
use super::mock_chain;
use super::mock_ipfs::MockIpfsClient;
use super::mock_runtime::TestRuntimeAdapterBuilder;
use super::mock_transport::MockTransport;
use super::noop::{NoopAdapterSelector, StaticBlockRefetcher};
use super::schema::{TestFile, TestResult};
use super::trigger::build_blocks_with_triggers;
use crate::manifest::{Manifest, load_manifest};
use anyhow::{Context, Result, anyhow, ensure};
use graph::amp::FlightClient;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::{BlockPtr, BlockchainMap, ChainIdentifier};
use graph::cheap_clone::CheapClone;
use graph::components::link_resolver::FileLinkResolver;
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
use graph::ipfs::{ContentPath, IpfsMetrics};
use graph::prelude::{
    DeploymentHash, LoggerFactory, NodeId, SubgraphCountMetric, SubgraphName, SubgraphRegistrar,
    SubgraphStore as SubgraphStoreTrait, SubgraphVersionSwitchingMode,
};
use graph::slog::{Logger, info, o};
use graph_chain_ethereum::network::{EthereumNetworkAdapter, EthereumNetworkAdapters};
use graph_chain_ethereum::{
    Chain, EthereumAdapter, NodeCapabilities, ProviderEthRpcMetrics, Transport,
    chain::ChainSettings,
};
use graph_core::polling_monitor::{arweave_service, ipfs_service};
use graph_graphql::prelude::GraphQlRunner;
use graph_node::config::{Config, Opt};
use graph_node::manager::PanicSubscriptionManager;
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::{ChainHeadUpdateListener, ChainStore, Store, SubgraphStore};
use std::collections::HashMap;
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
        // "trace" sets GRAPH_LOG=trace globally via slog_envlogger.
        _ => graph::log::logger_with_levels(true, Some("trace")),
    }
}

/// Mock file data passed to `setup_context`.
struct MockData {
    ipfs_files: HashMap<ContentPath, graph::bytes::Bytes>,
    ipfs_unresolved_tx: tokio::sync::mpsc::UnboundedSender<ContentPath>,
    arweave_files: HashMap<String, graph::bytes::Bytes>,
    arweave_unresolved_tx: tokio::sync::mpsc::UnboundedSender<String>,
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

/// Manifest data shared across all tests in a run.
pub(super) struct ManifestInfo {
    pub build_dir: PathBuf,
    /// Built manifest path. Aliased to `hash` in `FileLinkResolver` so the Qm
    /// hash resolves to a real filesystem path.
    pub manifest_path: PathBuf,
    pub network_name: ChainName,
    pub min_start_block: u64,
    /// Override for on-chain block validation when startBlock > 0.
    pub start_block_override: Option<BlockPtr>,
    pub hash: DeploymentHash,
    pub subgraph_name: SubgraphName,
    /// Event selectors (topic0) for handlers that declare `receipt: true`.
    /// Only logs whose topic0 is in this set receive a non-null receipt.
    pub receipt_required_selectors:
        std::collections::HashSet<graph::prelude::alloy::primitives::B256>,
}

/// Compute `"Qm" + hex(sha1(path + '\0' + seed))`. Seed ensures per-run uniqueness.
fn deployment_hash_from_path_and_seed(path: &Path, seed: u128) -> Result<DeploymentHash> {
    use sha1::{Digest, Sha1};

    let input = format!("{}\0{}", path.display(), seed);
    let digest = Sha1::digest(input.as_bytes());
    let qm = format!("Qm{}", hex::encode(digest));
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

    // Per-run seed for unique deployment hash and subgraph name.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let hash = deployment_hash_from_path_and_seed(&built_manifest_path, seed)?;

    // Derive subgraph name from the root dir, sanitized for SubgraphName compatibility.
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

    let receipt_required_selectors = extract_receipt_required_selectors(&manifest);

    Ok(ManifestInfo {
        build_dir,
        manifest_path: built_manifest_path,
        network_name,
        min_start_block,
        start_block_override,
        hash,
        subgraph_name,
        receipt_required_selectors,
    })
}

/// Collect topic0 selectors for handlers declaring `receipt: true`.
///
/// Covers data sources and templates. Mirrors `MappingEventHandler::topic0()`:
/// strip `"indexed "` and spaces, then keccak256.
fn extract_receipt_required_selectors(
    manifest: &Manifest,
) -> std::collections::HashSet<graph::prelude::alloy::primitives::B256> {
    use graph::prelude::alloy::primitives::keccak256;

    manifest
        .data_sources
        .iter()
        .flat_map(|ds| &ds.event_handlers)
        .chain(manifest.templates.iter().flat_map(|t| &t.event_handlers))
        .filter(|h| h.receipt)
        .map(|h| {
            let normalized = h.event.replace("indexed ", "").replace(' ', "");
            keccak256(normalized.as_bytes())
        })
        .collect()
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
    test_file_path: &Path,
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

    // Start numbering at the manifest's startBlock so implicit blocks are in range.
    let blocks = build_blocks_with_triggers(
        test_file,
        manifest_info.min_start_block,
        &manifest_info.receipt_required_selectors,
    )?;

    // Build mock IPFS file map.
    let test_file_dir = test_file_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    let mut mock_files: HashMap<ContentPath, graph::bytes::Bytes> = HashMap::new();
    for entry in &test_file.files {
        let path = ContentPath::new(&entry.cid)
            .map_err(|e| anyhow!("Invalid CID '{}' in test files: {}", entry.cid, e))?;
        let content = entry
            .resolve(&test_file_dir)
            .with_context(|| format!("Failed to resolve mock file for CID '{}'", entry.cid))?;
        mock_files.insert(path, content);
    }

    let (unresolved_tx, mut unresolved_rx) = tokio::sync::mpsc::unbounded_channel::<ContentPath>();

    let mut mock_arweave_files: HashMap<String, graph::bytes::Bytes> = HashMap::new();
    for entry in &test_file.arweave_files {
        let content = entry.resolve(&test_file_dir).with_context(|| {
            format!(
                "Failed to resolve mock Arweave file for txId '{}'",
                entry.tx_id
            )
        })?;
        mock_arweave_files.insert(entry.tx_id.clone(), content);
    }

    let (arweave_unresolved_tx, mut arweave_unresolved_rx) =
        tokio::sync::mpsc::unbounded_channel::<String>();

    // `db` must stay alive — dropping it destroys a pgtemp database.
    let db = create_test_database(opt, &manifest_info.build_dir)?;

    let logger = make_test_logger(opt.verbose).new(o!("test" => test_file.name.clone()));

    let stores = setup_stores(&logger, &db, &manifest_info.network_name).await?;

    let mock_transport = MockTransport::new(test_file, &blocks)?;
    let chain = setup_chain(&logger, blocks.clone(), &stores, mock_transport).await?;

    let mock_data = MockData {
        ipfs_files: mock_files,
        ipfs_unresolved_tx: unresolved_tx,
        arweave_files: mock_arweave_files,
        arweave_unresolved_tx,
    };

    let ctx = setup_context(&logger, &stores, &chain, manifest_info, mock_data).await?;

    // The indexer will process until it reaches this block.
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
        Ok(()) => {
            // Drain unresolved CIDs/tx IDs, deduplicating.
            let mut unresolved_ipfs: Vec<ContentPath> = Vec::new();
            while let Ok(cid) = unresolved_rx.try_recv() {
                if !unresolved_ipfs.contains(&cid) {
                    unresolved_ipfs.push(cid);
                }
            }

            let mut unresolved_arweave: Vec<String> = Vec::new();
            while let Ok(tx_id) = arweave_unresolved_rx.try_recv() {
                if !unresolved_arweave.contains(&tx_id) {
                    unresolved_arweave.push(tx_id);
                }
            }

            let mut missing_parts: Vec<String> = Vec::new();

            if !unresolved_ipfs.is_empty() {
                let list = unresolved_ipfs
                    .iter()
                    .map(|p| format!("  - {}", p))
                    .collect::<Vec<_>>()
                    .join("\n");
                missing_parts.push(format!(
                    "IPFS CIDs not found in mock 'files':\n{}\n\
                     Add the missing CID(s) to the \"files\" array in your test JSON.",
                    list
                ));
            }

            if !unresolved_arweave.is_empty() {
                let list = unresolved_arweave
                    .iter()
                    .map(|id| format!("  - {}", id))
                    .collect::<Vec<_>>()
                    .join("\n");
                missing_parts.push(format!(
                    "Arweave tx IDs not found in mock 'arweaveFiles':\n{}\n\
                     Add the missing txId(s) to the \"arweaveFiles\" array in your test JSON.",
                    list
                ));
            }

            if !missing_parts.is_empty() {
                Ok(TestResult {
                    handler_error: Some(missing_parts.join("\n\n")),
                    assertions: vec![],
                })
            } else {
                run_assertions(&ctx, &test_file.assertions).await
            }
        }
        Err(subgraph_error) => {
            // Fatal handler error — skip assertions.
            Ok(TestResult {
                handler_error: Some(format_handler_error(&subgraph_error)),
                assertions: vec![],
            })
        }
    };

    // Stop the subgraph regardless of result.
    ctx.provider
        .clone()
        .stop_subgraph(ctx.deployment.clone())
        .await;

    // For persistent DBs: clean up on pass, preserve on failure for inspection.
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

/// Create the test database: pgtemp (auto-dropped) or persistent (`--postgres-url`).
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

        // Override unix_socket_directories to /tmp: macOS temp paths can exceed
        // the 104-byte Unix socket limit, causing postgres to silently fail to start.
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

    /// Returns true if the deployment must be cleaned up after the test.
    /// Temporary databases are auto-dropped; persistent ones accumulate state.
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

    let config = Config::from_str(&config_str, &Opt::default())
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

/// Construct the mock Ethereum `Chain` with pre-built blocks.
async fn setup_chain(
    logger: &Logger,
    blocks: Vec<BlockWithTriggers<Chain>>,
    stores: &TestStores,
    mock_transport: MockTransport,
) -> Result<Arc<Chain>> {
    let mock_registry = Arc::new(MetricsRegistry::mock());
    let logger_factory = LoggerFactory::new(logger.clone(), None, mock_registry.clone());

    // Dummy firehose endpoint — required by Chain constructor, never used.
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

    let endpoint_metrics = Arc::new(EndpointMetrics::mock());
    let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));
    let rpc_client = graph::prelude::alloy::rpc::client::RpcClient::new(mock_transport, true);
    let transport = Transport::RPC(rpc_client);

    let block_stream_builder: Arc<dyn graph::blockchain::block_stream::BlockStreamBuilder<Chain>> =
        Arc::new(StaticStreamBuilder { chain: blocks });
    let dummy_adapter = Arc::new(
        EthereumAdapter::new(
            logger.clone(),
            String::new(),
            transport,
            provider_metrics,
            true,
            false,
            Arc::new(ChainSettings::from_env_defaults()),
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

    let chain_settings = Arc::new(ChainSettings::from_env_defaults());
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
        Arc::new(TestRuntimeAdapterBuilder),
        eth_adapters,
        graph::prelude::ENV_VARS.reorg_threshold(),
        true,
        chain_settings,
    );

    Ok(Arc::new(chain))
}

/// Wire up all graph-node components and deploy the subgraph.
async fn setup_context(
    logger: &Logger,
    stores: &TestStores,
    chain: &Arc<Chain>,
    manifest_info: &ManifestInfo,
    mock_data: MockData,
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

    // Route triggers to our mock chain.
    let mut blockchain_map = BlockchainMap::new();
    blockchain_map.insert(stores.network_name.clone(), chain.clone());
    let blockchain_map = Arc::new(blockchain_map);

    // FileLinkResolver loads from build dir. Alias maps the Qm hash → manifest path
    // so clone_for_manifest can resolve it without treating the hash as a path.
    let aliases =
        std::collections::HashMap::from([(hash.to_string(), manifest_path.to_path_buf())]);
    let link_resolver: Arc<dyn graph::components::link_resolver::LinkResolver> = Arc::new(
        FileLinkResolver::new(Some(build_dir.to_path_buf()), aliases),
    );

    // Mock IPFS client for file data sources (FileLinkResolver handles the manifest).
    let ipfs_metrics = IpfsMetrics::new(&mock_registry);
    let ipfs_client = Arc::new(MockIpfsClient {
        files: mock_data.ipfs_files,
        metrics: ipfs_metrics,
        unresolved_tx: mock_data.ipfs_unresolved_tx,
    });

    let ipfs_service = ipfs_service(
        ipfs_client,
        env_vars.mappings.max_ipfs_file_bytes,
        env_vars.mappings.ipfs_timeout,
        env_vars.mappings.ipfs_request_limit,
    );

    let arweave_resolver: Arc<dyn graph::components::link_resolver::ArweaveResolver> =
        Arc::new(MockArweaveResolver {
            files: mock_data.arweave_files,
            unresolved_tx: mock_data.arweave_unresolved_tx,
        });
    let arweave_service = arweave_service(
        arweave_resolver,
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
        Arc::new(graph::components::log_store::NoOpLogStore),
    ));

    // Uses PanicSubscriptionManager — tests don't need GraphQL subscriptions.
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

    // Deploy the subgraph. start_block_override bypasses on-chain block validation.
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

/// Format a `SubgraphError` for user-facing output.
fn format_handler_error(err: &SubgraphError) -> String {
    let mut parts = Vec::new();

    if let Some(handler) = &err.handler {
        parts.push(format!("in {handler}"));
    }
    if let Some(block_ptr) = &err.block_ptr {
        parts.push(format!("at block #{}", block_ptr.number));
    }

    let location = parts.join(" ");
    if location.is_empty() {
        err.message.clone()
    } else {
        format!("{location}: {}", err.message)
    }
}

/// Poll until the subgraph reaches `stop_block`, returning `Err` on fatal error or timeout.
async fn wait_for_sync(
    logger: &Logger,
    store: Arc<SubgraphStore>,
    deployment: &DeploymentLocator,
    stop_block: BlockPtr,
) -> Result<(), SubgraphError> {
    // Hardcoded; could be made configurable via env var or CLI flag.
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

        // Check for fatal errors.
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
