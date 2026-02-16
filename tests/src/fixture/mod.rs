pub mod ethereum;

use std::collections::{BTreeSet, HashMap};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Error;
use async_stream::stream;
use async_trait::async_trait;
use graph::amp;
use graph::blockchain::block_stream::{
    BlockRefetcher, BlockStream, BlockStreamBuilder, BlockStreamError, BlockStreamEvent,
    BlockWithTriggers, FirehoseCursor,
};
use graph::blockchain::{
    Block, BlockHash, BlockPtr, Blockchain, BlockchainMap, ChainIdentifier, RuntimeAdapter,
    TriggerFilterWrapper, TriggersAdapter, TriggersAdapterSelector,
};
use graph::cheap_clone::CheapClone;
use graph::components::link_resolver::{
    ArweaveClient, ArweaveClientError, ArweaveResolver, FileLinkResolver, FileSizeLimit,
    LinkResolverContext,
};
use graph::components::metrics::MetricsRegistry;
use graph::components::network_provider::ChainName;
use graph::components::store::{DeploymentLocator, EthereumCallCache, SourceableStore};
use graph::components::subgraph::{Settings, SubgraphInstanceManager as _};
use graph::data::graphql::load_manager::LoadManager;
use graph::data::query::{Query, QueryTarget};
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth};
use graph::data_source::DataSource;
use graph::endpoint::EndpointMetrics;
use graph::env::EnvVars;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints, SubgraphLimit};
use graph::futures03::{Stream, StreamExt};
use graph::http_body_util::Full;
use graph::hyper::body::Bytes;
use graph::hyper::Request;
use graph::ipfs::{IpfsClient, IpfsMetrics};
use graph::prelude::alloy::primitives::B256;
use graph::prelude::alloy::primitives::U256;
use graph::prelude::serde_json::{self, json};
use graph::prelude::{
    lazy_static, q, r, ApiVersion, BigInt, BlockNumber, DeploymentHash, GraphQlRunner as _,
    IpfsResolver, LinkResolver, LoggerFactory, NodeId, QueryError, SubgraphCountMetric,
    SubgraphName, SubgraphRegistrar, SubgraphStore as _, SubgraphVersionSwitchingMode,
    TriggerProcessor,
};
use graph_chain_ethereum::chain::RuntimeAdapterBuilder;
use graph_chain_ethereum::network::EthereumNetworkAdapters;
use graph_chain_ethereum::Chain;
use graph_core::polling_monitor::{arweave_service, ipfs_service};
use graph_node::manager::PanicSubscriptionManager;
use graph_node::{config::Config, store_builder::StoreBuilder};
use graph_runtime_wasm::RuntimeHostBuilder;
use graph_server_index_node::IndexNodeService;
use graph_store_postgres::{ChainHeadUpdateListener, ChainStore, Store, SubgraphStore};
use serde::Deserialize;
use slog::{crit, debug, info, o, Discard, Logger};
use std::env::VarError;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs::read_to_string;

const NODE_ID: &str = "default";

pub fn test_ptr(n: BlockNumber) -> BlockPtr {
    test_ptr_reorged(n, 0)
}

// Set n as the low bits and `reorg_n` as the high bits of the hash.
pub fn test_ptr_reorged(n: BlockNumber, reorg_n: u32) -> BlockPtr {
    let mut hash = B256::from(U256::from(n as u64));
    hash[0..4].copy_from_slice(&reorg_n.to_be_bytes());
    BlockPtr {
        hash: hash.into(),
        number: n,
    }
}

type GraphQlRunner = graph_graphql::prelude::GraphQlRunner<Store>;

struct CommonChainConfig {
    logger_factory: LoggerFactory,
    mock_registry: Arc<MetricsRegistry>,
    chain_store: Arc<ChainStore>,
    firehose_endpoints: FirehoseEndpoints,
}

impl CommonChainConfig {
    async fn new(test_name: &str, stores: &Stores) -> Self {
        let logger = test_logger(test_name);
        let mock_registry = Arc::new(MetricsRegistry::mock());
        let logger_factory = LoggerFactory::new(logger.cheap_clone(), None, mock_registry.clone());
        let chain_store = stores.chain_store.cheap_clone();

        let firehose_endpoints =
            FirehoseEndpoints::for_testing(vec![Arc::new(FirehoseEndpoint::new(
                "",
                "https://example.com",
                None,
                None,
                true,
                false,
                SubgraphLimit::Unlimited,
                Arc::new(EndpointMetrics::mock()),
            ))]);

        Self {
            logger_factory,
            mock_registry,
            chain_store,
            firehose_endpoints,
        }
    }
}

pub struct TestChain<C: Blockchain> {
    pub chain: Arc<C>,
    pub block_stream_builder: Arc<MutexBlockStreamBuilder<C>>,
}

impl TestChainTrait<Chain> for TestChain<Chain> {
    fn set_block_stream(&self, blocks: Vec<BlockWithTriggers<Chain>>) {
        let static_block_stream = Arc::new(StaticStreamBuilder { chain: blocks });
        *self.block_stream_builder.0.lock().unwrap() = static_block_stream;
    }

    fn chain(&self) -> Arc<Chain> {
        self.chain.clone()
    }
}

pub trait TestChainTrait<C: Blockchain> {
    fn set_block_stream(&self, blocks: Vec<BlockWithTriggers<C>>);

    fn chain(&self) -> Arc<C>;
}

pub struct TestContext {
    pub logger: Logger,
    pub provider: Arc<graph_core::subgraph_provider::SubgraphProvider>,
    pub store: Arc<SubgraphStore>,
    pub deployment: DeploymentLocator,
    pub subgraph_name: SubgraphName,
    pub instance_manager: Arc<
        graph_core::subgraph::SubgraphInstanceManager<
            graph_store_postgres::SubgraphStore,
            amp::FlightClient,
        >,
    >,
    pub link_resolver: Arc<dyn graph::components::link_resolver::LinkResolver>,
    pub arweave_resolver: Arc<dyn ArweaveResolver>,
    pub env_vars: Arc<EnvVars>,
    pub ipfs: Arc<dyn IpfsClient>,
    graphql_runner: Arc<GraphQlRunner>,
    indexing_status_service: Arc<IndexNodeService<graph_store_postgres::Store, amp::FlightClient>>,
}

#[derive(Deserialize)]
pub struct IndexingStatusBlock {
    pub number: BigInt,
}

#[derive(Deserialize)]
pub struct IndexingStatusError {
    pub deterministic: bool,
    pub block: IndexingStatusBlock,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexingStatus {
    pub health: SubgraphHealth,
    pub entity_count: BigInt,
    pub fatal_error: Option<IndexingStatusError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexingStatusForCurrentVersion {
    pub indexing_status_for_current_version: IndexingStatus,
}

impl TestContext {
    pub async fn runner(
        &self,
        stop_block: BlockPtr,
    ) -> graph_core::subgraph::SubgraphRunner<
        graph_chain_ethereum::Chain,
        RuntimeHostBuilder<graph_chain_ethereum::Chain>,
    > {
        let (logger, deployment, raw) = self.get_runner_context().await;
        let tp: Box<dyn TriggerProcessor<_, _>> =
            Box::new(graph_core::subgraph::SubgraphTriggerProcessor {});

        let deployment_status_metric = self
            .instance_manager
            .new_deployment_status_metric(&deployment);

        self.instance_manager
            .build_subgraph_runner_inner(
                logger,
                self.env_vars.cheap_clone(),
                deployment,
                raw,
                Some(stop_block.block_number()),
                tp,
                deployment_status_metric,
                true,
            )
            .await
            .unwrap()
    }

    pub async fn get_runner_context(&self) -> (Logger, DeploymentLocator, serde_yaml::Mapping) {
        let logger = self.logger.cheap_clone();
        let deployment = self.deployment.cheap_clone();

        // Stolen from the IPFS provider, there's prolly a nicer way to re-use it
        let file_bytes = self
            .link_resolver
            .cat(
                &LinkResolverContext::new(&deployment.hash, &logger),
                &deployment.hash.to_ipfs_link(),
            )
            .await
            .unwrap();

        let raw: serde_yaml::Mapping = serde_yaml::from_slice(&file_bytes).unwrap();

        (logger, deployment, raw)
    }

    pub async fn start_and_sync_to(&self, stop_block: BlockPtr) {
        // In case the subgraph has been previously started.
        self.provider
            .stop_subgraph(self.deployment.cheap_clone())
            .await;

        self.provider
            .cheap_clone()
            .start_subgraph(self.deployment.cheap_clone(), Some(stop_block.number))
            .await;

        debug!(self.logger, "TEST: syncing to {}", stop_block.number);

        wait_for_sync(
            &self.logger,
            self.store.clone(),
            &self.deployment.clone(),
            stop_block,
        )
        .await
        .unwrap();
    }

    pub async fn start_and_sync_to_error(&self, stop_block: BlockPtr) -> SubgraphError {
        // In case the subgraph has been previously started.
        self.provider
            .stop_subgraph(self.deployment.cheap_clone())
            .await;

        self.provider
            .cheap_clone()
            .start_subgraph(self.deployment.cheap_clone(), None)
            .await;

        wait_for_sync(
            &self.logger,
            self.store.clone(),
            &self.deployment.clone(),
            stop_block,
        )
        .await
        .unwrap_err()
    }

    pub async fn query(&self, query: &str) -> Result<Option<r::Value>, Vec<QueryError>> {
        let target = QueryTarget::Deployment(self.deployment.hash.clone(), ApiVersion::default());
        let query = Query::new(q::parse_query(query).unwrap().into_static(), None, false);
        let query_res = self.graphql_runner.clone().run_query(query, target).await;
        query_res.first().unwrap().duplicate().to_result()
    }

    pub async fn indexing_status(&self) -> IndexingStatus {
        let query = format!(
            r#"
            {{
                indexingStatusForCurrentVersion(subgraphName: "{}") {{
                    health
                    entityCount
                    fatalError {{
                        deterministic
                        block {{
                            number
                        }}
                    }}
                }}
            }}
            "#,
            &self.subgraph_name
        );
        let body = json!({ "query": query }).to_string();
        let req: Request<Full<Bytes>> = Request::new(body.into());
        let res = self.indexing_status_service.handle_graphql_query(req).await;
        let value = res
            .unwrap()
            .first()
            .unwrap()
            .duplicate()
            .to_result()
            .unwrap()
            .unwrap();
        let query_res: IndexingStatusForCurrentVersion =
            serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
        query_res.indexing_status_for_current_version
    }

    pub async fn rewind(&self, block_ptr_to: BlockPtr) {
        self.store
            .rewind(self.deployment.id.into(), block_ptr_to)
            .await
            .unwrap()
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Using drop to clean up the subgraph after the test is too clever
        // by half and should really be done with an explicit method,
        // something like `TestContext::cleanup(self)`.
        let store = self.store.cheap_clone();
        let subgraph_name = self.subgraph_name.clone();
        let deployment_hash = self.deployment.hash.clone();
        let logger = self.logger.cheap_clone();
        graph::spawn(async move {
            if let Err(e) = cleanup(&store, &subgraph_name, &deployment_hash).await {
                crit!(logger, "error cleaning up test subgraph"; "error" => e.to_string());
            }
        });
    }
}

pub struct Stores {
    network_name: ChainName,
    chain_head_listener: Arc<ChainHeadUpdateListener>,
    pub network_store: Arc<Store>,
    chain_store: Arc<ChainStore>,
}

graph::prelude::lazy_static! {
    /// Mutex for assuring there's only one test at a time
    /// running the `stores` function.
    pub static ref STORE_MUTEX: Mutex<()> = Mutex::new(());
}

fn test_logger(test_name: &str) -> Logger {
    crate::output::test_logger(test_name)
}

#[allow(clippy::await_holding_lock)]
pub async fn stores(test_name: &str, store_config_path: &str) -> Stores {
    let _mutex_guard = STORE_MUTEX.lock().unwrap();

    let config = {
        let config = read_to_string(store_config_path).await.unwrap();
        let db_url = match std::env::var("THEGRAPH_STORE_POSTGRES_DIESEL_URL") {
            Ok(url) => url,
            Err(VarError::NotPresent) => panic!(
                "to run end-to-end tests it is required to set \
                                            $THEGRAPH_STORE_POSTGRES_DIESEL_URL to the test db url"
            ),
            Err(e) => panic!("{}", e.to_string()),
        };
        let config = config.replace("$THEGRAPH_STORE_POSTGRES_DIESEL_URL", &db_url);
        Config::from_str(&config, "default").expect("failed to create configuration")
    };

    let logger = test_logger(test_name);
    let mock_registry: Arc<MetricsRegistry> = Arc::new(MetricsRegistry::mock());
    let node_id = NodeId::new(NODE_ID).unwrap();
    let store_builder =
        StoreBuilder::new(&logger, &node_id, &config, None, mock_registry.clone()).await;

    let network_name: ChainName = config
        .chains
        .chains
        .iter()
        .next()
        .unwrap()
        .0
        .as_str()
        .into();
    let chain_head_listener = store_builder.chain_head_update_listener();
    let network_identifiers: Vec<ChainName> = vec![network_name.clone()].into_iter().collect();
    let network_store = store_builder.network_store(network_identifiers).await;
    let ident = ChainIdentifier {
        net_version: "".into(),
        genesis_block_hash: test_ptr(0).hash,
    };
    let chain_store = network_store
        .block_store()
        .create_chain_store(&network_name, ident)
        .await
        .unwrap_or_else(|_| panic!("No chain store for {}", &network_name));

    Stores {
        network_name,
        chain_head_listener,
        network_store,
        chain_store,
    }
}

pub struct TestInfo {
    pub test_dir: String,
    pub test_name: String,
    pub subgraph_name: SubgraphName,
    pub hash: DeploymentHash,
}

#[derive(Debug)]
pub struct StaticArweaveResolver {
    content: HashMap<String, Vec<u8>>,
}

impl StaticArweaveResolver {
    pub fn new(content: HashMap<String, Vec<u8>>) -> Self {
        Self { content }
    }
}

#[async_trait]
impl ArweaveResolver for StaticArweaveResolver {
    async fn get(
        &self,
        file: &graph::data_source::offchain::Base64,
    ) -> Result<Vec<u8>, ArweaveClientError> {
        self.get_with_limit(file, &FileSizeLimit::Unlimited).await
    }

    async fn get_with_limit(
        &self,
        file: &graph::data_source::offchain::Base64,
        _limit: &FileSizeLimit,
    ) -> Result<Vec<u8>, ArweaveClientError> {
        self.content
            .get(file.as_str())
            .cloned()
            .ok_or(ArweaveClientError::UnableToCheckFileSize)
    }
}

pub async fn setup<C: Blockchain>(
    test_info: &TestInfo,
    stores: &Stores,
    chain: &impl TestChainTrait<C>,
    graft_block: Option<BlockPtr>,
    env_vars: Option<EnvVars>,
) -> TestContext {
    setup_inner(test_info, stores, chain, graft_block, env_vars, None, None).await
}

pub async fn setup_with_file_link_resolver<C: Blockchain>(
    test_info: &TestInfo,
    stores: &Stores,
    chain: &impl TestChainTrait<C>,
    graft_block: Option<BlockPtr>,
    env_vars: Option<EnvVars>,
) -> TestContext {
    let mut base_dir = PathBuf::from(test_info.test_dir.clone());
    base_dir.push("build");
    let link_resolver = Arc::new(FileLinkResolver::with_base_dir(base_dir));
    setup_inner(
        test_info,
        stores,
        chain,
        graft_block,
        env_vars,
        Some(link_resolver),
        None,
    )
    .await
}

pub async fn setup_with_arweave_resolver<C: Blockchain>(
    test_info: &TestInfo,
    stores: &Stores,
    chain: &impl TestChainTrait<C>,
    graft_block: Option<BlockPtr>,
    env_vars: Option<EnvVars>,
    arweave_resolver: Arc<dyn ArweaveResolver>,
) -> TestContext {
    setup_inner(
        test_info,
        stores,
        chain,
        graft_block,
        env_vars,
        None,
        Some(arweave_resolver),
    )
    .await
}

pub async fn setup_inner<C: Blockchain>(
    test_info: &TestInfo,
    stores: &Stores,
    chain: &impl TestChainTrait<C>,
    graft_block: Option<BlockPtr>,
    env_vars: Option<EnvVars>,
    link_resolver: Option<Arc<dyn LinkResolver>>,
    arweave_resolver: Option<Arc<dyn ArweaveResolver>>,
) -> TestContext {
    let env_vars = Arc::new(match env_vars {
        Some(ev) => ev,
        None => EnvVars::from_env().unwrap(),
    });

    let logger = test_logger(&test_info.test_name);
    let mock_registry: Arc<MetricsRegistry> = Arc::new(MetricsRegistry::mock());
    let logger_factory = LoggerFactory::new(logger.clone(), None, mock_registry.clone());
    let node_id = NodeId::new(NODE_ID).unwrap();

    // Make sure we're starting from a clean state.
    let subgraph_store = stores.network_store.subgraph_store();
    cleanup(&subgraph_store, &test_info.subgraph_name, &test_info.hash)
        .await
        .unwrap();

    let mut blockchain_map = BlockchainMap::new();
    blockchain_map.insert(stores.network_name.clone(), chain.chain());

    let static_filters = env_vars.experimental_static_filters;

    let ipfs_client: Arc<dyn IpfsClient> = Arc::new(
        graph::ipfs::IpfsRpcClient::new_unchecked(
            graph::ipfs::ServerAddress::test_rpc_api(),
            IpfsMetrics::new(&mock_registry),
            &logger,
        )
        .unwrap(),
    );

    let link_resolver = match link_resolver {
        Some(link_resolver) => link_resolver,
        None => Arc::new(IpfsResolver::new(
            ipfs_client.cheap_clone(),
            Default::default(),
        )),
    };

    let ipfs_service = ipfs_service(
        ipfs_client.cheap_clone(),
        env_vars.mappings.max_ipfs_file_bytes,
        env_vars.mappings.ipfs_timeout,
        env_vars.mappings.ipfs_request_limit,
    );

    let arweave_resolver: Arc<dyn ArweaveResolver> =
        arweave_resolver.unwrap_or_else(|| Arc::new(ArweaveClient::default()));
    let arweave_service = arweave_service(
        arweave_resolver.cheap_clone(),
        env_vars.mappings.ipfs_request_limit,
        match env_vars.mappings.max_ipfs_file_bytes {
            0 => FileSizeLimit::Unlimited,
            n => FileSizeLimit::MaxBytes(n as u64),
        },
    );
    let sg_count = Arc::new(SubgraphCountMetric::new(mock_registry.cheap_clone()));

    let blockchain_map = Arc::new(blockchain_map);

    let subgraph_instance_manager = Arc::new(graph_core::subgraph::SubgraphInstanceManager::new(
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

    // Graphql runner
    let load_manager = LoadManager::new(&logger, Vec::new(), Vec::new(), mock_registry.clone());
    let graphql_runner = Arc::new(GraphQlRunner::new(
        &logger,
        stores.network_store.clone(),
        Arc::new(load_manager),
        mock_registry.clone(),
    ));

    let indexing_status_service = Arc::new(IndexNodeService::new(
        logger.cheap_clone(),
        blockchain_map.cheap_clone(),
        stores.network_store.cheap_clone(),
        link_resolver.cheap_clone(),
        None,
    ));

    let panicking_subscription_manager = Arc::new(PanicSubscriptionManager {});

    let subgraph_registrar = Arc::new(graph_core::subgraph::SubgraphRegistrar::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_provider.cheap_clone(),
        subgraph_store.clone(),
        panicking_subscription_manager,
        Option::<Arc<amp::FlightClient>>::None,
        blockchain_map.clone(),
        node_id.clone(),
        SubgraphVersionSwitchingMode::Instant,
        Arc::new(Settings::default()),
        Arc::new(graph::components::network_provider::AmpChainNames::default()),
    ));

    SubgraphRegistrar::create_subgraph(
        subgraph_registrar.as_ref(),
        test_info.subgraph_name.clone(),
    )
    .await
    .expect("unable to create subgraph");

    let deployment = SubgraphRegistrar::create_subgraph_version(
        subgraph_registrar.as_ref(),
        test_info.subgraph_name.clone(),
        test_info.hash.clone(),
        node_id.clone(),
        None,
        None,
        graft_block,
        None,
        false,
    )
    .await
    .expect("failed to create subgraph version");

    TestContext {
        logger: logger_factory.subgraph_logger(&deployment),
        provider: subgraph_provider,
        store: subgraph_store,
        deployment,
        subgraph_name: test_info.subgraph_name.clone(),
        graphql_runner,
        instance_manager: subgraph_instance_manager,
        link_resolver,
        env_vars,
        indexing_status_service,
        ipfs: ipfs_client,
        arweave_resolver,
    }
}

pub async fn cleanup(
    subgraph_store: &SubgraphStore,
    name: &SubgraphName,
    hash: &DeploymentHash,
) -> Result<(), Error> {
    let locators = subgraph_store.locators(hash).await?;
    subgraph_store.remove_subgraph(name.clone()).await?;
    for locator in locators {
        subgraph_store.remove_deployment(locator.id.into()).await?;
    }
    Ok(())
}

pub async fn wait_for_sync(
    logger: &Logger,
    store: Arc<SubgraphStore>,
    deployment: &DeploymentLocator,
    stop_block: BlockPtr,
) -> Result<(), SubgraphError> {
    // We wait one second between checks for the subgraph to sync. That
    // means we wait up to a 30 seconds here by default.
    lazy_static! {
        static ref MAX_WAIT: Duration = Duration::from_secs(
            std::env::var("RUNNER_TESTS_WAIT_FOR_SYNC_SECS")
                .map(|val| val.parse().unwrap())
                .unwrap_or(30)
        );
    }
    const WAIT_TIME: Duration = Duration::from_secs(1);

    /// We flush here to speed up how long the write queue waits before it
    /// considers a batch complete and writable. Without flushing, we would
    /// have to wait for `GRAPH_STORE_WRITE_BATCH_DURATION` before all
    /// changes have been written to the database
    async fn flush(logger: &Logger, store: &Arc<SubgraphStore>, deployment: &DeploymentLocator) {
        store
            .clone()
            .writable(logger.clone(), deployment.id, Arc::new(vec![]))
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
    }

    let start = Instant::now();

    flush(logger, &store, deployment).await;

    while start.elapsed() < *MAX_WAIT {
        tokio::time::sleep(WAIT_TIME).await;
        flush(logger, &store, deployment).await;

        let block_ptr = match store.least_block_ptr(&deployment.hash).await {
            Ok(Some(ptr)) => ptr,
            res => {
                info!(&logger, "{:?}", res);
                continue;
            }
        };
        info!(logger, "TEST: sync status: {:?}", block_ptr);
        let status = store.status_for_id(deployment.id).await;

        if let Some(fatal_error) = status.fatal_error {
            return Err(fatal_error);
        }

        if block_ptr == stop_block {
            info!(logger, "TEST: reached stop block");
            return Ok(());
        }
    }

    // We only get here if we timed out waiting for the subgraph to reach
    // the stop block
    panic!("Sync did not complete within {}s", MAX_WAIT.as_secs());
}

struct StaticBlockRefetcher<C: Blockchain> {
    x: PhantomData<C>,
}

#[async_trait]
impl<C: Blockchain> BlockRefetcher<C> for StaticBlockRefetcher<C> {
    fn required(&self, _chain: &C) -> bool {
        false
    }

    async fn get_block(
        &self,
        _chain: &C,
        _logger: &Logger,
        _cursor: FirehoseCursor,
    ) -> Result<C::Block, Error> {
        unimplemented!("this block refetcher always returns false, get_block shouldn't be called")
    }
}

pub struct MutexBlockStreamBuilder<C: Blockchain>(pub Mutex<Arc<dyn BlockStreamBuilder<C>>>);

#[async_trait]
impl<C: Blockchain> BlockStreamBuilder<C> for MutexBlockStreamBuilder<C> {
    async fn build_firehose(
        &self,
        chain: &C,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<<C as Blockchain>::TriggerFilter>,
        unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<C>>> {
        let builder = self.0.lock().unwrap().clone();

        builder
            .build_firehose(
                chain,
                deployment,
                block_cursor,
                start_blocks,
                subgraph_current_block,
                filter,
                unified_api_version,
            )
            .await
    }

    async fn build_polling(
        &self,
        chain: &C,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilterWrapper<C>>,
        unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<C>>> {
        let builder = self.0.lock().unwrap().clone();

        builder
            .build_polling(
                chain,
                deployment,
                start_blocks,
                source_subgraph_stores,
                subgraph_current_block,
                filter,
                unified_api_version,
            )
            .await
    }
}

/// `chain` is the sequence of chain heads to be processed. If the next block to be processed in the
/// chain is not a descendant of the previous one, reorgs will be emitted until it is.
///
/// If the stream is reset, emitted reorged blocks will not be emitted again.
/// See also: static-stream-builder
struct StaticStreamBuilder<C: Blockchain> {
    chain: Vec<BlockWithTriggers<C>>,
}

#[async_trait]
impl<C: Blockchain> BlockStreamBuilder<C> for StaticStreamBuilder<C>
where
    C::TriggerData: Clone,
{
    async fn build_firehose(
        &self,
        _chain: &C,
        _deployment: DeploymentLocator,
        _block_cursor: FirehoseCursor,
        _start_blocks: Vec<graph::prelude::BlockNumber>,
        current_block: Option<graph::blockchain::BlockPtr>,
        _filter: Arc<C::TriggerFilter>,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<C>>> {
        let current_idx = current_block.map(|current_block| {
            self.chain
                .iter()
                .enumerate()
                .find(|(_, b)| b.ptr() == current_block)
                .unwrap()
                .0
        });
        Ok(Box::new(StaticStream {
            stream: Box::pin(stream_events(self.chain.clone(), current_idx)),
        }))
    }

    async fn build_polling(
        &self,
        _chain: &C,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<graph::prelude::BlockNumber>,
        _source_subgraph_stores: Vec<Arc<dyn SourceableStore>>,
        subgraph_current_block: Option<graph::blockchain::BlockPtr>,
        _filter: Arc<TriggerFilterWrapper<C>>,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<C>>> {
        let current_idx = subgraph_current_block.map(|current_block| {
            self.chain
                .iter()
                .enumerate()
                .find(|(_, b)| b.ptr() == current_block)
                .unwrap()
                .0
        });
        Ok(Box::new(StaticStream {
            stream: Box::pin(stream_events(self.chain.clone(), current_idx)),
        }))
    }
}

struct StaticStream<C: Blockchain> {
    stream: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, BlockStreamError>> + Send>>,
}

impl<C: Blockchain> BlockStream<C> for StaticStream<C> {
    fn buffer_size_hint(&self) -> usize {
        1
    }
}

impl<C: Blockchain> Stream for StaticStream<C> {
    type Item = Result<BlockStreamEvent<C>, BlockStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

fn stream_events<C: Blockchain>(
    blocks: Vec<BlockWithTriggers<C>>,
    current_idx: Option<usize>,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, BlockStreamError>>
where
    C::TriggerData: Clone,
{
    struct ForkDb<B: Block> {
        blocks: HashMap<BlockPtr, B>,
    }

    impl<B: Block> ForkDb<B> {
        fn common_ancestor(&self, a: BlockPtr, b: BlockPtr) -> Option<&B> {
            let mut a = self.blocks.get(&a).unwrap();
            let mut b = self.blocks.get(&b).unwrap();
            while a.number() > b.number() {
                a = self.blocks.get(&a.parent_ptr()?).unwrap();
            }
            while b.number() > a.number() {
                b = self.blocks.get(&b.parent_ptr()?).unwrap();
            }
            while a.hash() != b.hash() {
                a = self.blocks.get(&a.parent_ptr()?).unwrap();
                b = self.blocks.get(&b.parent_ptr()?).unwrap();
            }
            Some(a)
        }
    }

    let fork_db = ForkDb {
        blocks: blocks.iter().map(|b| (b.ptr(), b.block.clone())).collect(),
    };

    // See also: static-stream-builder
    stream! {
        let mut current_ptr = current_idx.map(|idx| blocks[idx].ptr());
        let skip = current_idx.map(|idx| idx + 1).unwrap_or(0);
        let mut blocks_iter = blocks.iter().skip(skip).peekable();
        while let Some(&block) = blocks_iter.peek() {
            if block.parent_ptr() == current_ptr {
                current_ptr = Some(block.ptr());
                blocks_iter.next(); // Block consumed, advance the iterator.
                yield Ok(BlockStreamEvent::ProcessBlock(block.clone(), FirehoseCursor::None));
            } else {
                let revert_to = fork_db.common_ancestor(block.ptr(), current_ptr.unwrap()).unwrap().ptr();
                current_ptr = Some(revert_to.clone());
                yield Ok(BlockStreamEvent::Revert(revert_to, FirehoseCursor::None));
            }
        }
    }
}

struct NoopRuntimeAdapter<C> {
    x: PhantomData<C>,
}

impl<C: Blockchain> RuntimeAdapter<C> for NoopRuntimeAdapter<C> {
    fn host_fns(&self, _ds: &DataSource<C>) -> Result<Vec<graph::blockchain::HostFn>, Error> {
        Ok(vec![])
    }
}

struct NoopRuntimeAdapterBuilder {}

impl RuntimeAdapterBuilder for NoopRuntimeAdapterBuilder {
    fn build(
        &self,
        _: Arc<EthereumNetworkAdapters>,
        _: Arc<dyn EthereumCallCache + 'static>,
        _: Arc<ChainIdentifier>,
    ) -> Arc<dyn graph::blockchain::RuntimeAdapter<graph_chain_ethereum::Chain> + 'static> {
        Arc::new(NoopRuntimeAdapter { x: PhantomData })
    }
}

pub struct NoopAdapterSelector<C> {
    pub x: PhantomData<C>,
    pub triggers_in_block_sleep: Duration,
}

impl<C: Blockchain> TriggersAdapterSelector<C> for NoopAdapterSelector<C> {
    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &<C as Blockchain>::NodeCapabilities,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn graph::blockchain::TriggersAdapter<C>>, Error> {
        // Return no triggers on data source reprocessing.
        let triggers_in_block = Arc::new(|block| {
            let logger = Logger::root(Discard, o!());
            Ok(BlockWithTriggers::new(block, Vec::new(), &logger))
        });
        Ok(Arc::new(MockTriggersAdapter {
            x: PhantomData,
            triggers_in_block,
            triggers_in_block_sleep: self.triggers_in_block_sleep,
        }))
    }
}

pub struct MockAdapterSelector<C: Blockchain> {
    pub x: PhantomData<C>,
    pub triggers_in_block_sleep: Duration,
    pub triggers_in_block:
        Arc<dyn Fn(<C as Blockchain>::Block) -> Result<BlockWithTriggers<C>, Error> + Sync + Send>,
}

impl<C: Blockchain> TriggersAdapterSelector<C> for MockAdapterSelector<C> {
    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &<C as Blockchain>::NodeCapabilities,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn graph::blockchain::TriggersAdapter<C>>, Error> {
        Ok(Arc::new(MockTriggersAdapter {
            x: PhantomData,
            triggers_in_block: self.triggers_in_block.clone(),
            triggers_in_block_sleep: self.triggers_in_block_sleep,
        }))
    }
}

struct MockTriggersAdapter<C: Blockchain> {
    x: PhantomData<C>,
    triggers_in_block_sleep: Duration,
    triggers_in_block:
        Arc<dyn Fn(<C as Blockchain>::Block) -> Result<BlockWithTriggers<C>, Error> + Sync + Send>,
}

#[async_trait]
impl<C: Blockchain> TriggersAdapter<C> for MockTriggersAdapter<C> {
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
        _root: Option<BlockHash>,
    ) -> Result<Option<<C as Blockchain>::Block>, Error> {
        todo!()
    }

    async fn load_block_ptrs_by_numbers(
        &self,
        _logger: Logger,
        _block_numbers: BTreeSet<BlockNumber>,
    ) -> Result<Vec<C::Block>, Error> {
        unimplemented!()
    }

    async fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        todo!()
    }

    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &C::TriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<C>>, BlockNumber), Error> {
        todo!()
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        block: <C as Blockchain>::Block,
        _filter: &<C as Blockchain>::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error> {
        tokio::time::sleep(self.triggers_in_block_sleep).await;

        (self.triggers_in_block)(block)
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        todo!()
    }

    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        match block.number {
            0 => Ok(None),
            n => Ok(Some(BlockPtr {
                hash: BlockHash::default(),
                number: n - 1,
            })),
        }
    }
}
