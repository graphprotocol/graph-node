use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::anyhow;
use anyhow::Error;
use async_stream::stream;
use ethereum::trigger::{EthereumBlockTriggerType, EthereumTrigger};
use futures03::{Stream, StreamExt};
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamBuilder, BlockStreamEvent, BlockWithTriggers,
};
use graph::blockchain::{
    Block, BlockHash, BlockPtr, Blockchain, BlockchainMap, ChainIdentifier, RuntimeAdapter,
    TriggersAdapter, TriggersAdapterSelector,
};
use graph::cheap_clone::CheapClone;
use graph::components::store::{BlockStore, DeploymentId, DeploymentLocator};
use graph::env::{EnvVars, ENV_VARS};
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints};
use graph::ipfs_client::IpfsClient;
use graph::prelude::ethabi::ethereum_types::{H256, U64};
use graph::prelude::{
    async_trait, BlockNumber, DeploymentHash, LightEthereumBlock, LoggerFactory, MetricsRegistry,
    NodeId, SubgraphAssignmentProvider, SubgraphName, SubgraphRegistrar, SubgraphStore,
    SubgraphVersionSwitchingMode,
};
use graph_chain_ethereum::{self as ethereum, Chain};
use graph_core::{
    LinkResolver, SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider,
    SubgraphInstanceManager, SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_mock::MockMetricsRegistry;
use graph_node::manager::PanicSubscriptionManager;
use graph_node::{
    config::{Config, Opt},
    store_builder::StoreBuilder,
};
use slog::{debug, info, Logger};
use std::env;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_runner() -> anyhow::Result<()> {
    let subgraph_name = SubgraphName::new("test1")
        .expect("Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'");

    let deployment = DeploymentLocator {
        id: DeploymentId::new(1),
        // ethereum-blocks
        hash: DeploymentHash::new("QmUuqWUHTNaHkq6d333n7gCrZbwzLN2yophaHeeK9x6BjJ")
            .expect("unable to parse hash"),
    };

    let start_block =
        graph_chain_ethereum::chain::BlockFinality::Final(Arc::new(LightEthereumBlock {
            hash: Some(H256::from_low_u64_be(1)),
            number: Some(U64::from(1)),
            ..Default::default()
        }));
    let stop_block = 2;

    let ctx = setup(
        subgraph_name.clone(),
        &deployment,
        start_block.clone(),
        vec![
            BlockStreamEvent::ProcessBlock(
                BlockWithTriggers::<graph_chain_ethereum::chain::Chain> {
                    block: start_block.clone(),
                    trigger_data: vec![],
                },
                None,
            ),
            BlockStreamEvent::ProcessBlock(
                BlockWithTriggers::<graph_chain_ethereum::chain::Chain> {
                    block: graph_chain_ethereum::chain::BlockFinality::Final(Arc::new(
                        LightEthereumBlock {
                            hash: Some(H256::from_low_u64_be(2)),
                            number: Some(U64::from(2)),
                            ..Default::default()
                        },
                    )),
                    trigger_data: vec![EthereumTrigger::Block(
                        BlockPtr {
                            hash: H256::from_low_u64_be(2).into(),
                            number: 2,
                        },
                        EthereumBlockTriggerType::Every,
                    )],
                },
                None,
            ),
        ],
    )
    .await;

    let provider = ctx.provider.clone();
    let store = ctx.store.clone();

    let logger = ctx.logger_factory.subgraph_logger(&deployment);

    SubgraphAssignmentProvider::start(provider.as_ref(), deployment.clone(), Some(stop_block))
        .await
        .expect("unabel to start subgraph");

    loop {
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let block_ptr = match store.least_block_ptr(&deployment.hash).await {
            Ok(Some(ptr)) => ptr,
            res => {
                info!(&logger, "{:?}", res);
                continue;
            }
        };

        debug!(&logger, "subgraph block: {:?}", block_ptr);

        if block_ptr.number >= stop_block {
            info!(
                &logger,
                "subgraph now at block {}, reached stop block {}", block_ptr.number, stop_block
            );
            break;
        }
    }

    // FIXME: wait for instance manager to stop.
    // If we remove the subgraph first, it will panic on:
    // 1504c9d8-36e4-45bb-b4f2-71cf58789ed9
    tokio::time::sleep(Duration::from_millis(4000)).await;

    info!(&logger, "Removing subgraph {}", subgraph_name);
    store.clone().remove_subgraph(subgraph_name)?;

    Ok(())
}

struct TestContext {
    logger_factory: LoggerFactory,
    provider: Arc<
        IpfsSubgraphAssignmentProvider<
            SubgraphInstanceManager<graph_store_postgres::SubgraphStore>,
        >,
    >,
    store: Arc<dyn SubgraphStore>,
}

async fn setup(
    subgraph_name: SubgraphName,
    deployment: &DeploymentLocator,
    start_block: <Chain as Blockchain>::Block,
    events: Vec<BlockStreamEvent<Chain>>,
) -> TestContext {
    let logger = Logger::root(slog::Discard, slog::o!());
    let logger_factory = LoggerFactory::new(logger.clone(), None);
    let mut opt = Opt::default();
    let postgres_url = env::var("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .unwrap_or("postgresql://graph-node:let-me-in@127.0.0.1:5432/graph-node".into());

    opt.postgres_url = Some(postgres_url);
    let node_id = NodeId::new(opt.node_id.clone()).expect("invalid node_id");
    let config = Config::load(&logger, &opt).expect("failed to create configuration");

    let mock_registry: Arc<dyn MetricsRegistry> = Arc::new(MockMetricsRegistry::new());

    let network_name: String = "mainnet".into();

    let store_builder =
        StoreBuilder::new(&logger, &node_id, &config, None, mock_registry.clone()).await;

    let ipfs =
        IpfsClient::new("https://api.thegraph.com/ipfs/").expect("failed to start ipfs client");

    let link_resolver = Arc::new(LinkResolver::new(vec![ipfs], Arc::new(EnvVars::default())));

    let chain_head_update_listener = store_builder.chain_head_update_listener();

    let network_identifiers: Vec<(String, Vec<ChainIdentifier>)> = vec![(
        "mainnet".into(),
        (vec![ChainIdentifier {
            net_version: "".into(),
            genesis_block_hash: start_block.hash(),
        }]),
    )];
    let network_store = store_builder.network_store(network_identifiers);

    let subgraph_store = network_store.subgraph_store();
    let chain_store = network_store
        .block_store()
        .chain_store(network_name.as_ref())
        .expect(format!("No chain store for {}", &network_name).as_ref());

    // This is needed bacause the stream builder only works for firehose and this will only be called if there
    // are > 1 firehose endpoints. The endpoint itself is never used because it's mocked.
    let firehose_endpoints: FirehoseEndpoints = vec![Arc::new(
        FirehoseEndpoint::new(logger.clone(), "", "https://example.com", None, true)
            .await
            .expect("unable to create endpoint"),
    )]
    .into();

    let chain = ethereum::Chain::new(
        logger_factory.clone(),
        network_name.clone(),
        node_id.clone(),
        mock_registry.clone(),
        chain_store.cheap_clone(),
        chain_store,
        firehose_endpoints,
        ethereum::network::EthereumNetworkAdapters { adapters: vec![] },
        chain_head_update_listener,
        Arc::new(StaticStreamBuilder { events }),
        Arc::new(NoopAdapterSelector {}),
        Arc::new(NoopRuntimeAdapter {}),
        ethereum::ENV_VARS.reorg_threshold,
        // We assume the tested chain is always ingestible for now
        true,
    );

    let mut blockchain_map = BlockchainMap::new();
    blockchain_map.insert(network_name.clone(), Arc::new(chain));

    let static_filters = ENV_VARS.experimental_static_filters;

    let blockchain_map = Arc::new(blockchain_map);
    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger_factory,
        subgraph_store.clone(),
        blockchain_map.clone(),
        mock_registry.clone(),
        link_resolver.cheap_clone(),
        static_filters,
    );

    // Create IPFS-based subgraph provider
    let subgraph_provider: Arc<
        IpfsSubgraphAssignmentProvider<
            SubgraphInstanceManager<graph_store_postgres::SubgraphStore>,
        >,
    > = Arc::new(IpfsSubgraphAssignmentProvider::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_instance_manager,
    ));

    let panicking_subscription_manager = Arc::new(PanicSubscriptionManager {});

    let subgraph_registrar: Arc<
        IpfsSubgraphRegistrar<
            IpfsSubgraphAssignmentProvider<
                SubgraphInstanceManager<graph_store_postgres::SubgraphStore>,
            >,
            graph_store_postgres::SubgraphStore,
            PanicSubscriptionManager,
        >,
    > = Arc::new(IpfsSubgraphRegistrar::new(
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

    SubgraphRegistrar::create_subgraph_version(
        subgraph_registrar.as_ref(),
        subgraph_name.clone(),
        deployment.hash.clone(),
        node_id.clone(),
        None,
        Some(BlockPtr {
            hash: start_block.hash(),
            number: start_block.number(),
        }),
    )
    .await
    .expect("failed to create subgraph version");

    TestContext {
        logger_factory,
        provider: subgraph_provider,
        store: subgraph_store,
    }
}

struct StaticStreamBuilder<C: Blockchain> {
    events: Vec<BlockStreamEvent<C>>,
}

#[async_trait]
impl<C: Blockchain> BlockStreamBuilder<C> for StaticStreamBuilder<C>
where
    C::TriggerData: Clone,
{
    fn build_firehose(
        &self,
        _chain: &C,
        _deployment: DeploymentLocator,
        _block_cursor: Option<String>,
        _start_blocks: Vec<graph::prelude::BlockNumber>,
        _subgraph_current_block: Option<graph::blockchain::BlockPtr>,
        _filter: Arc<C::TriggerFilter>,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> anyhow::Result<Box<dyn BlockStream<C>>> {
        Ok(Box::new(StaticStream {
            stream: Box::pin(stream_events(self.events.clone())),
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
        Ok(Box::new(StaticStream {
            stream: Box::pin(stream_events(self.events.clone())),
        }))
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
    blocks: Vec<BlockStreamEvent<C>>,
) -> impl Stream<Item = Result<BlockStreamEvent<C>, Error>> {
    stream! {
        for event in blocks.into_iter() {
            yield Ok(event);
        }

        loop {
            yield Err(anyhow!("no more blocks"))
        }
    }
}

struct NoopRuntimeAdapter {}

impl RuntimeAdapter<Chain> for NoopRuntimeAdapter {
    fn host_fns(
        &self,
        _ds: &<Chain as Blockchain>::DataSource,
    ) -> Result<Vec<graph::blockchain::HostFn>, Error> {
        Ok(vec![])
    }
}

struct NoopAdapterSelector {}

impl TriggersAdapterSelector<Chain> for NoopAdapterSelector {
    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &<graph_chain_ethereum::Chain as Blockchain>::NodeCapabilities,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn graph::blockchain::TriggersAdapter<Chain>>, Error> {
        Ok(Arc::new(NoopTriggersAdapter {}))
    }
}

struct NoopTriggersAdapter {}

#[async_trait]
impl TriggersAdapter<Chain> for NoopTriggersAdapter {
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<<graph_chain_ethereum::Chain as Blockchain>::Block>, Error> {
        todo!()
    }

    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &<graph_chain_ethereum::Chain as Blockchain>::TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, Error> {
        todo!()
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        _block: <graph_chain_ethereum::Chain as Blockchain>::Block,
        _filter: &<graph_chain_ethereum::Chain as Blockchain>::TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        todo!()
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
