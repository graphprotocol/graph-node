pub mod ethereum;

use std::marker::PhantomData;
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;

use crate::helpers::run_cmd;
use anyhow::Error;
use async_stream::stream;
use futures::{Stream, StreamExt};
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamBuilder, BlockStreamEvent, BlockWithTriggers, FirehoseCursor,
};
use graph::blockchain::{
    Block, BlockHash, BlockPtr, Blockchain, BlockchainMap, ChainIdentifier, RuntimeAdapter,
    TriggersAdapter, TriggersAdapterSelector,
};
use graph::cheap_clone::CheapClone;
use graph::components::store::{BlockStore, DeploymentLocator};
use graph::data::graphql::effort::LoadManager;
use graph::data::query::{Query, QueryTarget};
use graph::env::ENV_VARS;
use graph::ipfs_client::IpfsClient;
use graph::prelude::ethabi::ethereum_types::H256;
use graph::prelude::{
    async_trait, r, ApiVersion, BlockNumber, DeploymentHash, GraphQlRunner as _, LoggerFactory,
    MetricsRegistry, NodeId, QueryError, SubgraphAssignmentProvider, SubgraphName,
    SubgraphRegistrar, SubgraphStore as _, SubgraphVersionSwitchingMode,
};
use graph_core::polling_monitor::ipfs_service::IpfsService;
use graph_core::{
    LinkResolver, SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider,
    SubgraphInstanceManager, SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_graphql::prelude::GraphQlRunner;
use graph_mock::MockMetricsRegistry;
use graph_node::manager::PanicSubscriptionManager;
use graph_node::{config::Config, store_builder::StoreBuilder};
use graph_store_postgres::{ChainHeadUpdateListener, ChainStore, Store, SubgraphStore};
use slog::{crit, info, Logger};
use std::env::VarError;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs::read_to_string;

const NODE_ID: &str = "default";

pub async fn build_subgraph(dir: &str) -> DeploymentHash {
    build_subgraph_with_yarn_cmd(dir, "deploy:test").await
}

pub async fn build_subgraph_with_yarn_cmd(dir: &str, yarn_cmd: &str) -> DeploymentHash {
    // Test that IPFS is up.
    IpfsClient::localhost()
        .test()
        .await
        .expect("Could not connect to IPFS, make sure it's running at port 5001");

    // Make sure dependencies are present.
    run_cmd(
        Command::new("yarn")
            .arg("install")
            .arg("--mutex")
            .arg("file:.yarn-mutex")
            .current_dir("./integration-tests"),
    );

    // Run codegen.
    run_cmd(Command::new("yarn").arg("codegen").current_dir(&dir));

    // Run `deploy` for the side effect of uploading to IPFS, the graph node url
    // is fake and the actual deploy call is meant to fail.
    let deploy_output = run_cmd(
        Command::new("yarn")
            .arg(yarn_cmd)
            .env("IPFS_URI", "http://127.0.0.1:5001")
            .env("GRAPH_NODE_ADMIN_URI", "http://localhost:0")
            .current_dir(dir),
    );

    // Hack to extract deployment id from `graph deploy` output.
    const ID_PREFIX: &str = "Build completed: ";
    let mut line = deploy_output
        .lines()
        .find(|line| line.contains(ID_PREFIX))
        .expect("found no matching line");
    if !line.starts_with(ID_PREFIX) {
        line = &line[5..line.len() - 5]; // workaround for colored output
    }
    DeploymentHash::new(line.trim_start_matches(ID_PREFIX)).unwrap()
}

pub fn test_ptr(n: BlockNumber) -> BlockPtr {
    BlockPtr {
        hash: H256::from_low_u64_be(n as u64).into(),
        number: n,
    }
}
pub struct TestContext {
    pub logger: Logger,
    pub provider: Arc<
        IpfsSubgraphAssignmentProvider<
            SubgraphInstanceManager<graph_store_postgres::SubgraphStore>,
        >,
    >,
    pub store: Arc<SubgraphStore>,
    pub deployment: DeploymentLocator,
    pub subgraph_name: SubgraphName,
    graphql_runner: Arc<GraphQlRunner<Store, PanicSubscriptionManager>>,
}

impl TestContext {
    pub async fn start_and_sync_to(&self, stop_block: BlockPtr) {
        self.provider
            .start(self.deployment.clone(), Some(stop_block.number))
            .await
            .expect("unable to start subgraph");

        wait_for_sync(&self.logger, &self.store, &self.deployment.hash, stop_block)
            .await
            .unwrap();
    }

    pub async fn query(&self, query: &str) -> Result<Option<r::Value>, Vec<QueryError>> {
        let target = QueryTarget::Deployment(self.deployment.hash.clone(), ApiVersion::default());

        self.graphql_runner
            .clone()
            .run_query(
                Query::new(
                    graphql_parser::parse_query(query).unwrap().into_static(),
                    None,
                ),
                target,
            )
            .await
            .first()
            .unwrap()
            .duplicate()
            .to_result()
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if let Err(e) = cleanup(&self.store, &self.subgraph_name, &self.deployment.hash) {
            crit!(self.logger, "error cleaning up test subgraph"; "error" => e.to_string());
        }
    }
}

pub struct Stores {
    network_name: String,
    chain_head_listener: Arc<ChainHeadUpdateListener>,
    network_store: Arc<Store>,
    chain_store: Arc<ChainStore>,
}

graph::prelude::lazy_static! {
    /// Mutex for assuring there's only one test at a time
    /// running the `stores` function.
    pub static ref STORE_MUTEX: Mutex<()> = Mutex::new(());
}

pub async fn stores(store_config_path: &str) -> Stores {
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

    let logger = graph::log::logger(true);
    let mock_registry: Arc<dyn MetricsRegistry> = Arc::new(MockMetricsRegistry::new());
    let node_id = NodeId::new(NODE_ID).unwrap();
    let store_builder =
        StoreBuilder::new(&logger, &node_id, &config, None, mock_registry.clone()).await;

    let network_name: String = config.chains.chains.iter().next().unwrap().0.to_string();
    let chain_head_listener = store_builder.chain_head_update_listener();
    let network_identifiers = vec![(
        network_name.clone(),
        (vec![ChainIdentifier {
            net_version: "".into(),
            genesis_block_hash: test_ptr(0).hash,
        }]),
    )];
    let network_store = store_builder.network_store(network_identifiers);
    let chain_store = network_store
        .block_store()
        .chain_store(network_name.as_ref())
        .expect(format!("No chain store for {}", &network_name).as_ref());

    Stores {
        network_name,
        chain_head_listener,
        network_store,
        chain_store,
    }
}

pub async fn setup<C: Blockchain>(
    subgraph_name: SubgraphName,
    hash: &DeploymentHash,
    stores: &Stores,
    chain: Arc<C>,
    graft_block: Option<BlockPtr>,
) -> TestContext {
    let logger = graph::log::logger(true);
    let logger_factory = LoggerFactory::new(logger.clone(), None);
    let mock_registry: Arc<dyn MetricsRegistry> = Arc::new(MockMetricsRegistry::new());
    let node_id = NodeId::new(NODE_ID).unwrap();

    // Make sure we're starting from a clean state.
    let subgraph_store = stores.network_store.subgraph_store();
    cleanup(&subgraph_store, &subgraph_name, hash).unwrap();

    let mut blockchain_map = BlockchainMap::new();
    blockchain_map.insert(stores.network_name.clone(), chain);

    let static_filters = ENV_VARS.experimental_static_filters;

    let ipfs = IpfsClient::localhost();
    let link_resolver = Arc::new(LinkResolver::new(
        vec![ipfs.cheap_clone()],
        Default::default(),
    ));
    let ipfs_service = IpfsService::new(
        ipfs,
        ENV_VARS.mappings.max_ipfs_file_bytes as u64,
        ENV_VARS.mappings.ipfs_timeout,
        ENV_VARS.mappings.max_ipfs_concurrent_requests,
    );

    let blockchain_map = Arc::new(blockchain_map);
    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger_factory,
        subgraph_store.clone(),
        blockchain_map.clone(),
        mock_registry.clone(),
        link_resolver.cheap_clone(),
        ipfs_service,
        static_filters,
    );

    // Graphql runner
    let subscription_manager = Arc::new(PanicSubscriptionManager {});
    let load_manager = LoadManager::new(&logger, Vec::new(), mock_registry.clone());
    let graphql_runner = Arc::new(GraphQlRunner::new(
        &logger,
        stores.network_store.clone(),
        subscription_manager.clone(),
        Arc::new(load_manager),
        mock_registry.clone(),
    ));

    // Create IPFS-based subgraph provider
    let subgraph_provider = Arc::new(IpfsSubgraphAssignmentProvider::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_instance_manager,
    ));

    let panicking_subscription_manager = Arc::new(PanicSubscriptionManager {});

    let subgraph_registrar = Arc::new(IpfsSubgraphRegistrar::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_provider.clone(),
        subgraph_store.clone(),
        panicking_subscription_manager,
        blockchain_map.clone(),
        node_id.clone(),
        SubgraphVersionSwitchingMode::Instant,
    ));

    SubgraphRegistrar::create_subgraph(subgraph_registrar.as_ref(), subgraph_name.clone())
        .await
        .expect("unable to create subgraph");

    let deployment = SubgraphRegistrar::create_subgraph_version(
        subgraph_registrar.as_ref(),
        subgraph_name.clone(),
        hash.clone(),
        node_id.clone(),
        None,
        None,
        graft_block,
    )
    .await
    .expect("failed to create subgraph version");

    TestContext {
        logger: logger_factory.subgraph_logger(&deployment),
        provider: subgraph_provider,
        store: subgraph_store,
        deployment,
        subgraph_name,
        graphql_runner,
    }
}

pub fn cleanup(
    subgraph_store: &SubgraphStore,
    name: &SubgraphName,
    hash: &DeploymentHash,
) -> Result<(), Error> {
    let locators = subgraph_store.locators(hash)?;
    subgraph_store.remove_subgraph(name.clone())?;
    for locator in locators {
        subgraph_store.remove_deployment(locator.id.into())?;
    }
    Ok(())
}

pub async fn wait_for_sync(
    logger: &Logger,
    store: &SubgraphStore,
    hash: &DeploymentHash,
    stop_block: BlockPtr,
) -> Result<(), Error> {
    let mut err_count = 0;
    while err_count < 10 {
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let block_ptr = match store.least_block_ptr(&hash).await {
            Ok(Some(ptr)) => ptr,
            res => {
                info!(&logger, "{:?}", res);
                err_count += 1;
                continue;
            }
        };

        if block_ptr == stop_block {
            break;
        }

        if !store.is_healthy(&hash).await.unwrap() {
            return Err(anyhow::anyhow!("subgraph failed unexpectedly"));
        }
    }

    Ok(())
}

/// `chain` is the sequence of chain heads to be processed. If the next block to be processed in the
/// chain is not a descendant of the previous one, reorgs will be emitted until it is.
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
                .0 as usize
        });
        Ok(Box::new(StaticStream {
            stream: Box::pin(stream_events(self.chain.clone(), current_idx)),
        }))
    }

    async fn build_polling(
        &self,
        _chain: Arc<C>,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<graph::prelude::BlockNumber>,
        _subgraph_current_block: Option<graph::blockchain::BlockPtr>,
        _filter: Arc<C::TriggerFilter>,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<C>>> {
        unimplemented!("only firehose mode should be used for tests")
    }
}

struct StaticStream<C: Blockchain> {
    stream: Pin<Box<dyn Stream<Item = Result<BlockStreamEvent<C>, Error>> + Send>>,
}

impl<C: Blockchain> BlockStream<C> for StaticStream<C> {}

impl<C: Blockchain> Stream for StaticStream<C> {
    type Item = Result<BlockStreamEvent<C>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

fn stream_events<C: Blockchain>(
    blocks: Vec<BlockWithTriggers<C>>,
    current_idx: Option<usize>,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, Error>>
where
    C::TriggerData: Clone,
{
    // See also: static-stream-builder
    stream! {
        let current_block = current_idx.map(|idx| &blocks[idx]);
        let mut current_ptr = current_block.map(|b| b.ptr());
        let mut current_parent_ptr = current_block.and_then(|b| b.parent_ptr());
        let skip = current_idx.map(|idx| idx + 1).unwrap_or(0);
        let mut blocks_iter = blocks.iter().skip(skip).peekable();
        while let Some(&block) = blocks_iter.peek() {
            if block.parent_ptr() == current_ptr {
                current_ptr = Some(block.ptr());
                current_parent_ptr = block.parent_ptr();
                blocks_iter.next(); // Block consumed, advance the iterator.
                yield Ok(BlockStreamEvent::ProcessBlock(block.clone(), FirehoseCursor::None));
            } else {
                let revert_to = current_parent_ptr.unwrap();
                current_ptr = Some(revert_to.clone());
                current_parent_ptr = blocks
                    .iter()
                    .find(|b| b.ptr() == revert_to)
                    .unwrap()
                    .block
                    .parent_ptr();
                yield Ok(BlockStreamEvent::Revert(revert_to, FirehoseCursor::None));
            }
        }
    }
}

struct NoopRuntimeAdapter<C> {
    x: PhantomData<C>,
}

impl<C: Blockchain> RuntimeAdapter<C> for NoopRuntimeAdapter<C> {
    fn host_fns(
        &self,
        _ds: &<C as Blockchain>::DataSource,
    ) -> Result<Vec<graph::blockchain::HostFn>, Error> {
        Ok(vec![])
    }
}

struct NoopAdapterSelector<C> {
    x: PhantomData<C>,
}

impl<C: Blockchain> TriggersAdapterSelector<C> for NoopAdapterSelector<C> {
    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &<C as Blockchain>::NodeCapabilities,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn graph::blockchain::TriggersAdapter<C>>, Error> {
        Ok(Arc::new(NoopTriggersAdapter { x: PhantomData }))
    }
}

struct NoopTriggersAdapter<C> {
    x: PhantomData<C>,
}

#[async_trait]
impl<C: Blockchain> TriggersAdapter<C> for NoopTriggersAdapter<C> {
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<<C as Blockchain>::Block>, Error> {
        todo!()
    }

    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &<C as Blockchain>::TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<C>>, Error> {
        todo!()
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        block: <C as Blockchain>::Block,
        _filter: &<C as Blockchain>::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error> {
        // Return no triggers on data source reprocessing.
        Ok(BlockWithTriggers::new(block, Vec::new()))
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
