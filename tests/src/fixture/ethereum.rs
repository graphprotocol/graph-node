use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use super::{
    test_ptr, CommonChainConfig, MutexBlockStreamBuilder, NoopAdapterSelector,
    NoopRuntimeAdapterBuilder, StaticBlockRefetcher, StaticStreamBuilder, Stores, TestChain,
};
use async_trait::async_trait;
use futures_core::Stream;
use graph::blockchain::block_stream::BlockStreamEvent;
use graph::blockchain::client::ChainClient;
use graph::blockchain::{
    self, BlockPtr, Blockchain, BlockchainKind, DataSource, DataSourceTemplate, NoopDecoderHook,
    RuntimeAdapter, TriggersAdapter, TriggersAdapterSelector, UnresolvedDataSourceTemplate,
};
use graph::cheap_clone::CheapClone;
use graph::components::store::{
    DeploymentCursorTracker, DeploymentId, DeploymentLocator,
    SubscriptionManager as SubscriptionManagerTrait,
};
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::data_source::{
    DataSource as DataSourceEnum, UnresolvedDataSourceTemplate as UnresolvedDataSourceTemplateEnum,
};
use graph::data_source::{
    DataSourceTemplate as DataSourceTemplateEnum, TriggerWithHandler,
    UnresolvedDataSource as UnresolvedDataSourceEnum,
};
use graph::prelude::ethabi::ethereum_types::H256;
use graph::prelude::web3::types::{Address, Log, Transaction, H160};
use graph::prelude::{
    ethabi, tiny_keccak, BlockHash, BlockNumber, ChainStore, CreateSubgraphResult, DeploymentHash,
    LightEthereumBlock, LinkResolver, LogCode, NodeId,
    SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait,
    SubgraphInstanceManager as SubgraphInstanceManagerTrait, SubgraphName,
    SubgraphRegistrar as SubgraphRegistrarTrait, SubgraphRegistrarError,
    SubgraphStore as SubgraphStoreTrait, ENV_VARS,
};
use graph::{blockchain::block_stream::BlockWithTriggers, prelude::ethabi::ethereum_types::U64};
use graph_chain_ethereum::network::EthereumNetworkAdapters;
use graph_chain_ethereum::trigger::LogRef;
use graph_chain_ethereum::{
    chain::BlockFinality,
    trigger::{EthereumBlockTriggerType, EthereumTrigger},
};
use graph_chain_ethereum::{Chain, NodeCapabilities};
use graph_core::{
    create_subgraph_version, SubgraphInstanceManager, SubgraphRegistrar, SubgraphTriggerProcessor,
};
use serde::{Deserialize, Deserializer};
use slog::{info, Logger};

/// Wrap eth as our test chain
pub struct WrappedEthChain(Arc<Chain>, pub Mutex<Arc<StaticStreamBuilder<Chain>>>);

impl std::fmt::Debug for WrappedEthChain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self.0))
    }
}

impl WrappedEthChain {
    pub fn new(chain: Arc<Chain>, stream_builder: Arc<StaticStreamBuilder<Chain>>) -> Self {
        WrappedEthChain(chain, Mutex::new(stream_builder))
    }

    pub fn inner_chain(&self) -> Arc<Chain> {
        self.0.clone()
    }

    pub fn directly_resolve_block_from_number(&self, number: BlockNumber) -> Option<BlockPtr> {
        let block_stream = self.1.lock().unwrap().chain.clone();
        let mut blocks = block_stream.iter().map(|b| b.ptr());
        blocks.find_map(|b| {
            if b.number == number {
                Some(b.clone())
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct WrappedDataSource(DataSourceEnum<Chain>);

pub struct WrappedUnresolvedDataSource(UnresolvedDataSourceEnum<Chain>);

#[derive(Default)]
pub struct WrappedUnresolvedDataSourceTemplate(UnresolvedDataSourceTemplateEnum<Chain>);

#[derive(Debug)]
pub struct WrappedDataSourceTemplate(DataSourceTemplateEnum<Chain>);

#[derive(Clone, Debug, Default)]
pub struct WrappedTriggerFilter(graph_chain_ethereum::TriggerFilter);

impl WrappedTriggerFilter {
    pub fn inner(&self) -> &graph_chain_ethereum::TriggerFilter {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WrappedNodeCapabilities(NodeCapabilities);

#[derive(Debug)]
pub struct WrappedChainClient(());

pub struct WrappedRuntimeAdapter(Arc<dyn RuntimeAdapter<Chain>>);
impl RuntimeAdapter<WrappedEthChain> for WrappedRuntimeAdapter {
    fn host_fns(
        &self,
        ds: &<WrappedEthChain as Blockchain>::DataSource,
    ) -> Result<Vec<blockchain::HostFn>, anyhow::Error> {
        RuntimeAdapter::<Chain>::host_fns(
            self.0.deref(),
            match &ds.0 {
                DataSourceEnum::Onchain(onchain) => onchain,
                _ => todo!("only onchain ds are supported"),
            },
        )
    }
}

pub struct WrappedTriggersAdapter(Arc<dyn blockchain::TriggersAdapter<Chain>>);

pub struct WrappedBlockStream(Box<dyn blockchain::block_stream::BlockStream<Chain>>);

impl Stream for WrappedBlockStream {
    type Item = Result<
        blockchain::block_stream::BlockStreamEvent<WrappedEthChain>,
        blockchain::block_stream::BlockStreamError,
    >;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let inner = Pin::new(&mut self.get_mut().0);
        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(result))) => {
                let mapped = match result {
                    BlockStreamEvent::Revert(block_ptr, firehose_cursor) => {
                        BlockStreamEvent::Revert(block_ptr, firehose_cursor)
                    }
                    BlockStreamEvent::ProcessBlock(block_with_triggers, firehose_cursor) => {
                        let BlockWithTriggers {
                            block,
                            trigger_data,
                        } = block_with_triggers;
                        BlockStreamEvent::ProcessBlock(
                            BlockWithTriggers {
                                block,
                                trigger_data,
                            },
                            firehose_cursor,
                        )
                    }
                    BlockStreamEvent::ProcessWasmBlock(
                        block_ptr,
                        block_time,
                        wasm,
                        a_string,
                        firehose_cursor,
                    ) => BlockStreamEvent::ProcessWasmBlock(
                        block_ptr,
                        block_time,
                        wasm,
                        a_string,
                        firehose_cursor,
                    ),
                };
                Poll::Ready(Some(Ok(mapped)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl blockchain::block_stream::BlockStream<WrappedEthChain> for WrappedBlockStream {
    fn buffer_size_hint(&self) -> usize {
        self.0.buffer_size_hint()
    }
}

#[async_trait]
impl blockchain::TriggersAdapter<WrappedEthChain> for WrappedTriggersAdapter {
    async fn ancestor_block(
        &self,
        ptr: BlockPtr,
        offset: BlockNumber,
        root: Option<BlockHash>,
    ) -> Result<Option<<WrappedEthChain as Blockchain>::Block>, anyhow::Error> {
        self.0.ancestor_block(ptr, offset, root).await
    }
    async fn scan_triggers(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &WrappedTriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<WrappedEthChain>>, BlockNumber), anyhow::Error> {
        let (blocks, num) = self.0.scan_triggers(from, to, &filter.0).await?;
        let mut blocks_with_triggers = Vec::new();
        for block in blocks {
            let BlockWithTriggers {
                block,
                trigger_data,
            } = block;
            blocks_with_triggers.push(BlockWithTriggers {
                block,
                trigger_data,
            })
        }
        Ok((blocks_with_triggers, num))
    }
    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: <WrappedEthChain as Blockchain>::Block,
        filter: &<WrappedEthChain as Blockchain>::TriggerFilter,
    ) -> Result<BlockWithTriggers<WrappedEthChain>, anyhow::Error> {
        let inner = self.0.triggers_in_block(logger, block, &filter.0).await?;
        let BlockWithTriggers {
            block,
            trigger_data,
        } = inner;
        Ok(BlockWithTriggers {
            block,
            trigger_data,
        })
    }

    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, anyhow::Error> {
        self.0.is_on_main_chain(ptr).await
    }
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, anyhow::Error> {
        self.0.parent_ptr(block).await
    }
}

impl std::fmt::Display for WrappedNodeCapabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("WrappedNodeCapabilities({})", self.0))
    }
}

impl blockchain::NodeCapabilities<WrappedEthChain> for WrappedNodeCapabilities {
    fn from_data_sources(data_sources: &[<WrappedEthChain as Blockchain>::DataSource]) -> Self {
        let ds = data_sources
            .into_iter()
            .map(|source| match source.0.clone() {
                DataSourceEnum::Onchain(onchain) => onchain,
                DataSourceEnum::Offchain(_) => {
                    todo!("only onchain data sources are supported")
                }
            })
            .collect::<Vec<_>>();
        let inner =
            <NodeCapabilities as blockchain::NodeCapabilities<Chain>>::from_data_sources(&ds);
        WrappedNodeCapabilities(inner)
    }
}

impl blockchain::TriggerFilter<WrappedEthChain> for WrappedTriggerFilter {
    fn extend_with_template(
        &mut self,
        data_source: impl Iterator<Item = <WrappedEthChain as Blockchain>::DataSourceTemplate>,
    ) {
        self.0
            .extend_with_template(data_source.map(|filter| match filter.0 {
                DataSourceTemplateEnum::Onchain(onchain) => onchain,
                DataSourceTemplateEnum::Offchain(_) => {
                    todo!("only onchain templates are supported")
                }
            }))
    }

    fn extend<'a>(
        &mut self,
        data_sources: impl Iterator<Item = &'a <WrappedEthChain as Blockchain>::DataSource> + Clone,
    ) {
        self.0.extend(data_sources.map(|source| match &source.0 {
            DataSourceEnum::Onchain(onchain) => onchain,
            DataSourceEnum::Offchain(_) => todo!("only onchain data sources supported"),
        }))
    }

    fn node_capabilities(&self) -> <WrappedEthChain as Blockchain>::NodeCapabilities {
        WrappedNodeCapabilities(self.0.node_capabilities())
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        self.0.to_firehose_filter()
    }
}

impl<'de> Deserialize<'de> for WrappedUnresolvedDataSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = UnresolvedDataSourceEnum::deserialize(deserializer)?;
        Ok(WrappedUnresolvedDataSource(inner))
    }
}

impl<'de> Deserialize<'de> for WrappedUnresolvedDataSourceTemplate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = UnresolvedDataSourceTemplateEnum::deserialize(deserializer)?;
        Ok(WrappedUnresolvedDataSourceTemplate(inner))
    }
}
impl Clone for WrappedDataSourceTemplate {
    fn clone(&self) -> Self {
        let inner = self.0.clone();
        WrappedDataSourceTemplate(inner)
    }
}

impl Clone for WrappedUnresolvedDataSourceTemplate {
    fn clone(&self) -> Self {
        let inner = match &self.0 {
            UnresolvedDataSourceTemplateEnum::Onchain(template) => {
                UnresolvedDataSourceTemplateEnum::Onchain(template.clone())
            }
            UnresolvedDataSourceTemplateEnum::Offchain(template) => {
                UnresolvedDataSourceTemplateEnum::Offchain(template.clone())
            }
        };
        WrappedUnresolvedDataSourceTemplate(inner)
    }
}

impl DataSourceTemplate<WrappedEthChain> for WrappedDataSourceTemplate {
    fn api_version(&self) -> graph::semver::Version {
        self.0.api_version()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        self.0.runtime()
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn manifest_idx(&self) -> u32 {
        self.0.manifest_idx()
    }

    fn kind(&self) -> &str {
        self.0.as_onchain().unwrap().kind()
    }
}

#[async_trait]
impl UnresolvedDataSourceTemplate<WrappedEthChain> for WrappedUnresolvedDataSourceTemplate {
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<WrappedDataSourceTemplate, anyhow::Error> {
        let inner = match self.0 {
            UnresolvedDataSourceTemplateEnum::Onchain(inner) => inner,
            UnresolvedDataSourceTemplateEnum::Offchain(_unresolved_data_source_template) => {
                unreachable!()
            }
        };
        let resolved = inner.resolve(resolver, logger, manifest_idx).await?;
        Ok(WrappedDataSourceTemplate(DataSourceTemplateEnum::Onchain(
            resolved,
        )))
    }
}

impl DataSource<WrappedEthChain> for WrappedDataSource {
    fn from_template_info(
        info: graph::prelude::InstanceDSTemplateInfo,
        template: &DataSourceTemplateEnum<WrappedEthChain>,
    ) -> Result<Self, anyhow::Error> {
        let inner = match template {
            DataSourceTemplateEnum::Onchain(inner) => inner,
            DataSourceTemplateEnum::Offchain(_) => unreachable!(),
        };

        let inner = match inner.0.clone() {
            DataSourceTemplateEnum::Onchain(inner) => inner,
            DataSourceTemplateEnum::Offchain(_) => unreachable!(),
        };

        let inner = DataSourceTemplateEnum::Onchain(inner);
        let ds = DataSource::from_template_info(info, &inner)?;
        Ok(WrappedDataSource(DataSourceEnum::Onchain(ds)))
    }

    fn from_stored_dynamic_data_source(
        template: &WrappedDataSourceTemplate,
        stored: graph::components::store::StoredDynamicDataSource,
    ) -> Result<Self, anyhow::Error> {
        let inner = match template.0.clone() {
            DataSourceTemplateEnum::Onchain(inner) => inner,
            DataSourceTemplateEnum::Offchain(_data_source_template) => todo!(),
        };
        //let inner = DataSourceTemplateEnum::Onchain(inner);
        let ds = DataSource::<Chain>::from_stored_dynamic_data_source(&inner, stored)?;
        Ok(WrappedDataSource(DataSourceEnum::Onchain(ds)))
    }

    fn address(&self) -> Option<&[u8]> {
        self.0.as_onchain()?.address()
    }

    fn start_block(&self) -> graph::prelude::BlockNumber {
        self.0.as_onchain().unwrap().start_block()
    }

    fn end_block(&self) -> Option<graph::prelude::BlockNumber> {
        self.0.as_onchain()?.end_block()
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn kind(&self) -> &str {
        self.0.as_onchain().unwrap().kind()
    }

    fn network(&self) -> Option<&str> {
        self.0.as_onchain()?.network()
    }

    fn context(&self) -> Arc<Option<graph::prelude::DataSourceContext>> {
        self.0.context()
    }

    fn creation_block(&self) -> Option<graph::prelude::BlockNumber> {
        self.0.creation_block()
    }

    fn api_version(&self) -> graph::semver::Version {
        self.0.api_version()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        self.0.runtime()
    }

    fn handler_kinds(&self) -> std::collections::HashSet<&str> {
        self.0.handler_kinds()
    }

    fn match_and_decode(
        &self,
        trigger: &<Chain as Blockchain>::TriggerData,
        block: &Arc<<Chain as Blockchain>::Block>,
        logger: &slog::Logger,
    ) -> Result<Option<TriggerWithHandler<<Chain as Blockchain>::MappingTrigger>>, anyhow::Error>
    {
        self.0
            .as_onchain()
            .unwrap()
            .match_and_decode(trigger, block, logger)
    }

    fn is_duplicate_of(&self, other: &Self) -> bool {
        self.0.is_duplicate_of(&other.0)
    }

    fn as_stored_dynamic_data_source(&self) -> graph::components::store::StoredDynamicDataSource {
        self.0.as_stored_dynamic_data_source()
    }

    fn validate(&self, spec_version: &graph::semver::Version) -> Vec<anyhow::Error> {
        self.0.validate(spec_version)
    }
}

#[async_trait]
impl blockchain::UnresolvedDataSource<WrappedEthChain> for WrappedUnresolvedDataSource {
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<WrappedDataSource, anyhow::Error> {
        println!("UnresolvedDataSource<WrappedEthChain>::resolve");
        let inner = self.0;
        let inner = inner.resolve(resolver, logger, manifest_idx).await?;
        Ok(WrappedDataSource(inner))
    }
}

#[async_trait]
impl Blockchain for WrappedEthChain {
    const KIND: BlockchainKind = Chain::KIND;
    const ALIASES: &'static [&'static str] = Chain::ALIASES;

    type Client = Arc<ChainClient<Chain>>;

    type Block = <graph_chain_ethereum::Chain as Blockchain>::Block;

    type DataSource = WrappedDataSource;
    type DataSourceTemplate = WrappedDataSourceTemplate;

    type UnresolvedDataSource = WrappedUnresolvedDataSource;
    type UnresolvedDataSourceTemplate = WrappedUnresolvedDataSourceTemplate;
    type TriggerData = <Chain as Blockchain>::TriggerData;
    type MappingTrigger = <Chain as Blockchain>::MappingTrigger;
    type TriggerFilter = WrappedTriggerFilter;
    type NodeCapabilities = WrappedNodeCapabilities;
    type DecoderHook = NoopDecoderHook;

    fn triggers_adapter(
        &self,
        log: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapter<Self>>, anyhow::Error> {
        let inner = self
            .0
            .triggers_adapter(log, &capabilities.0, unified_api_version)?;
        let wrapped = Arc::new(WrappedTriggersAdapter(inner));
        Ok(wrapped)
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.0.chain_store()
    }

    fn is_refetch_block_required(&self) -> bool {
        self.0.is_refetch_block_required()
    }

    fn runtime(&self) -> anyhow::Result<(Arc<dyn RuntimeAdapter<Self>>, Self::DecoderHook)> {
        let (inner, _decoderhook) = self.0.runtime()?;
        let wrapped = WrappedRuntimeAdapter(inner);
        Ok((Arc::new(wrapped), NoopDecoderHook))
    }

    fn chain_client(&self) -> Arc<ChainClient<Self>> {
        let client = self.0.chain_client();
        let wrapped = match client.deref() {
            ChainClient::Firehose(firehose_endpoints) => {
                ChainClient::new_firehose(firehose_endpoints.clone())
            }
            _ => todo!("only firehose is supported"),
        };
        let wrapped = Arc::new(wrapped);
        wrapped
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, blockchain::IngestorError> {
        info!(&logger, "block_pointer_from_number - WrappedEthChain is directly resolving blocks from the BlockStreamBuilder"; "number" => number);
        if let Some(block_ptr) = self.directly_resolve_block_from_number(number) {
            return Ok(block_ptr);
        }
        self.0.block_pointer_from_number(logger, number).await
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        store: impl DeploymentCursorTracker,
        start_blocks: Vec<BlockNumber>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn blockchain::block_stream::BlockStream<Self>>, anyhow::Error> {
        let inner = self
            .0
            .new_block_stream(
                deployment,
                store,
                start_blocks,
                Arc::new(filter.0.clone()),
                unified_api_version,
            )
            .await?;
        let wrapped = Box::new(WrappedBlockStream(inner));
        Ok(wrapped)
    }

    async fn refetch_firehose_block(
        &self,
        logger: &Logger,
        cursor: blockchain::block_stream::FirehoseCursor,
    ) -> Result<Self::Block, anyhow::Error> {
        self.0.refetch_firehose_block(logger, cursor).await
    }

    async fn block_ingestor(&self) -> anyhow::Result<Box<dyn blockchain::BlockIngestor>> {
        self.0.block_ingestor().await
    }
}

pub async fn chain(
    test_name: &str,
    blocks: Vec<BlockWithTriggers<Chain>>,
    stores: &Stores,
    triggers_adapter: Option<Arc<dyn TriggersAdapterSelector<Chain>>>,
) -> TestChain<Chain, Chain> {
    let (_, block_stream_builder, chain) =
        create_chain(triggers_adapter, test_name, stores, blocks).await;

    TestChain {
        chain: Arc::new(chain),
        block_stream_builder,
    }
}

pub async fn wrapped_chain(
    test_name: &str,
    blocks: Vec<BlockWithTriggers<Chain>>,
    stores: &Stores,
    triggers_adapter: Option<Arc<dyn TriggersAdapterSelector<Chain>>>,
) -> TestChain<WrappedEthChain, Chain> {
    let (static_block_stream, block_stream_builder, chain) =
        create_chain(triggers_adapter, test_name, stores, blocks).await;

    TestChain {
        chain: Arc::new(WrappedEthChain::new(Arc::new(chain), static_block_stream)),
        block_stream_builder,
    }
}

async fn create_chain(
    triggers_adapter: Option<Arc<dyn TriggersAdapterSelector<Chain>>>,
    test_name: &str,
    stores: &Stores,
    blocks: Vec<BlockWithTriggers<Chain>>,
) -> (
    Arc<StaticStreamBuilder<Chain>>,
    Arc<MutexBlockStreamBuilder<Chain>>,
    Chain,
) {
    let triggers_adapter = triggers_adapter.unwrap_or(Arc::new(NoopAdapterSelector {
        triggers_in_block_sleep: Duration::ZERO,
        x: PhantomData,
    }));

    let CommonChainConfig {
        logger_factory,
        mock_registry,
        chain_store,
        firehose_endpoints,
        node_id,
    } = CommonChainConfig::new(test_name, stores).await;

    let client = Arc::new(ChainClient::<Chain>::new_firehose(firehose_endpoints));

    let static_block_stream = Arc::new(StaticStreamBuilder { chain: blocks });
    let block_stream_builder = Arc::new(MutexBlockStreamBuilder(Mutex::new(
        static_block_stream.clone(),
    )));

    let eth_adapters = Arc::new(EthereumNetworkAdapters::empty_for_testing());

    let chain = Chain::new(
        logger_factory,
        stores.network_name.clone(),
        node_id,
        mock_registry,
        chain_store.cheap_clone(),
        chain_store,
        client,
        stores.chain_head_listener.cheap_clone(),
        block_stream_builder.clone(),
        Arc::new(StaticBlockRefetcher { x: PhantomData }),
        triggers_adapter,
        Arc::new(NoopRuntimeAdapterBuilder {}),
        eth_adapters,
        ENV_VARS.reorg_threshold,
        ENV_VARS.ingestor_polling_interval,
        // We assume the tested chain is always ingestible for now
        true,
    );
    (static_block_stream, block_stream_builder, chain)
}

pub fn genesis() -> BlockWithTriggers<graph_chain_ethereum::Chain> {
    let ptr = test_ptr(0);
    BlockWithTriggers::<graph_chain_ethereum::Chain> {
        block: BlockFinality::Final(Arc::new(LightEthereumBlock {
            hash: Some(H256::from_slice(ptr.hash.as_slice())),
            number: Some(U64::from(ptr.number)),
            ..Default::default()
        })),
        trigger_data: vec![EthereumTrigger::Block(ptr, EthereumBlockTriggerType::End)],
    }
}

pub fn generate_empty_blocks_for_range(
    parent_ptr: BlockPtr,
    start: i32,
    end: i32,
    add_to_hash: u64, // Use to differentiate forks
) -> Vec<BlockWithTriggers<Chain>> {
    let mut blocks: Vec<BlockWithTriggers<Chain>> = vec![];

    for i in start..(end + 1) {
        let parent_ptr = blocks.last().map(|b| b.ptr()).unwrap_or(parent_ptr.clone());
        let ptr = BlockPtr {
            number: i,
            hash: H256::from_low_u64_be(i as u64 + add_to_hash).into(),
        };
        blocks.push(empty_block(parent_ptr, ptr));
    }

    blocks
}

pub fn empty_block(parent_ptr: BlockPtr, ptr: BlockPtr) -> BlockWithTriggers<Chain> {
    assert!(ptr != parent_ptr);
    assert!(ptr.number > parent_ptr.number);

    // A 0x000.. transaction is used so `push_test_log` can use it
    let transactions = vec![Transaction {
        hash: H256::zero(),
        block_hash: Some(H256::from_slice(ptr.hash.as_slice())),
        block_number: Some(ptr.number.into()),
        transaction_index: Some(0.into()),
        from: Some(H160::zero()),
        to: Some(H160::zero()),
        ..Default::default()
    }];

    BlockWithTriggers::<graph_chain_ethereum::Chain> {
        block: BlockFinality::Final(Arc::new(LightEthereumBlock {
            hash: Some(H256::from_slice(ptr.hash.as_slice())),
            number: Some(U64::from(ptr.number)),
            parent_hash: H256::from_slice(parent_ptr.hash.as_slice()),
            transactions,
            ..Default::default()
        })),
        trigger_data: vec![EthereumTrigger::Block(ptr, EthereumBlockTriggerType::End)],
    }
}

pub fn push_test_log(block: &mut BlockWithTriggers<Chain>, payload: impl Into<String>) {
    let log = Arc::new(Log {
        address: Address::zero(),
        topics: vec![tiny_keccak::keccak256(b"TestEvent(string)").into()],
        data: ethabi::encode(&[ethabi::Token::String(payload.into())]).into(),
        block_hash: Some(H256::from_slice(block.ptr().hash.as_slice())),
        block_number: Some(block.ptr().number.into()),
        transaction_hash: Some(H256::from_low_u64_be(0)),
        transaction_index: Some(0.into()),
        log_index: Some(0.into()),
        transaction_log_index: Some(0.into()),
        log_type: None,
        removed: None,
    });
    block
        .trigger_data
        .push(EthereumTrigger::Log(LogRef::FullLog(log, None)))
}

pub fn push_test_command(
    block: &mut BlockWithTriggers<Chain>,
    test_command: impl Into<String>,
    data: impl Into<String>,
) {
    let log = Arc::new(Log {
        address: Address::zero(),
        topics: vec![tiny_keccak::keccak256(b"TestEvent(string,string)").into()],
        data: ethabi::encode(&[
            ethabi::Token::String(test_command.into()),
            ethabi::Token::String(data.into()),
        ])
        .into(),
        block_hash: Some(H256::from_slice(block.ptr().hash.as_slice())),
        block_number: Some(block.ptr().number.into()),
        transaction_hash: Some(H256::from_low_u64_be(0)),
        transaction_index: Some(0.into()),
        log_index: Some(0.into()),
        transaction_log_index: Some(0.into()),
        log_type: None,
        removed: None,
    });
    block
        .trigger_data
        .push(EthereumTrigger::Log(LogRef::FullLog(log, None)))
}

pub fn push_test_polling_trigger(block: &mut BlockWithTriggers<Chain>) {
    block.trigger_data.push(EthereumTrigger::Block(
        block.ptr(),
        EthereumBlockTriggerType::End,
    ))
}

#[derive(Clone)]
pub struct WrappedSubgraphInstanceManager<S: SubgraphStoreTrait> {
    pub inner: Arc<SubgraphInstanceManager<S>>,
}

#[async_trait]
impl<S: SubgraphStoreTrait> SubgraphInstanceManagerTrait for WrappedSubgraphInstanceManager<S> {
    async fn start_subgraph(
        self: Arc<Self>,
        deployment: DeploymentLocator,
        manifest: serde_yaml::Mapping,
        stop_block: Option<BlockNumber>,
    ) {
        let inner = self.inner.cheap_clone();
        let logger = inner.subgraph_logger(&deployment);
        graph::prelude::info!(logger, "WrappedSubgraphInstanceManager::start_subgraph");
        let err_logger = inner.subgraph_logger(&deployment);

        let subgraph_start_future = async move {
            let runner = inner
                .build_subgraph_runner::<WrappedEthChain>(
                    logger.clone(),
                    inner.get_env_vars(),
                    deployment.clone(),
                    manifest,
                    stop_block,
                    Box::new(SubgraphTriggerProcessor {}),
                )
                .await?;
            inner
                .start_subgraph_thread(logger, deployment, runner)
                .await
        };
        graph::spawn(async move {
            match subgraph_start_future.await {
                Ok(()) => {}
                Err(err) => graph::prelude::error!(
                    err_logger,
                    "Failed to start subgraph";
                    "error" => format!("{:#}", err),
                    "code" => LogCode::SubgraphStartFailure
                ),
            }
        });
    }

    async fn stop_subgraph(&self, deployment: DeploymentLocator) {
        let inner = self.inner.cheap_clone();
        inner.stop_subgraph(deployment).await
    }
}

pub struct WrappedSubgraphRegistrar<P, S, SM>(pub SubgraphRegistrar<P, S, SM>);

#[async_trait]
impl<P, S, SM> SubgraphRegistrarTrait for WrappedSubgraphRegistrar<P, S, SM>
where
    P: SubgraphAssignmentProviderTrait,
    S: SubgraphStoreTrait,
    SM: SubscriptionManagerTrait,
{
    async fn create_subgraph(
        &self,
        name: SubgraphName,
    ) -> Result<CreateSubgraphResult, SubgraphRegistrarError> {
        SubgraphRegistrarTrait::create_subgraph(&self.0, name.clone()).await
    }

    async fn create_subgraph_version(
        &self,
        name: SubgraphName,
        hash: DeploymentHash,
        node_id: NodeId,
        debug_fork: Option<DeploymentHash>,
        start_block_override: Option<BlockPtr>,
        graft_block_override: Option<BlockPtr>,
        history_blocks: Option<i32>,
    ) -> Result<DeploymentLocator, SubgraphRegistrarError> {
        // We don't have a location for the subgraph yet; that will be
        // assigned when we deploy for real. For logging purposes, make up a
        // fake locator
        let logger = self
            .0
            .subgraph_logger(&DeploymentLocator::new(DeploymentId(0), hash.clone()));

        let raw: serde_yaml::Mapping = self.0.resolve_raw_manifest(&hash).await?;

        // Give priority to deployment specific history_blocks value.
        let history_blocks = history_blocks.or(self
            .0
            .get_settings()
            .for_name(&name)
            .map(|c| c.history_blocks));

        let auto_graft_sync_depth = if self.0.store.auto_graft_sync() {
            Some(0)
        } else {
            None
        };

        let deployment_locator = create_subgraph_version::<WrappedEthChain, _, _>(
            &logger,
            self.0.store.clone(),
            self.0.chains.cheap_clone(),
            name.clone(),
            hash.cheap_clone(),
            start_block_override,
            graft_block_override,
            raw,
            node_id,
            debug_fork,
            self.0.version_switching_mode,
            &self.0.resolver,
            history_blocks,
            auto_graft_sync_depth,
            self.0.provider.clone(),
        )
        .await?;

        graph::prelude::debug!(
            &logger,
            "Wrote new subgraph version to store";
            "subgraph_name" => name.to_string(),
            "subgraph_hash" => hash.to_string(),
        );

        Ok(deployment_locator)
    }

    async fn remove_subgraph(&self, name: SubgraphName) -> Result<(), SubgraphRegistrarError> {
        SubgraphRegistrarTrait::remove_subgraph(&self.0, name).await
    }

    async fn reassign_subgraph(
        &self,
        hash: &DeploymentHash,
        node_id: &NodeId,
    ) -> Result<(), SubgraphRegistrarError> {
        SubgraphRegistrarTrait::reassign_subgraph(&self.0, hash, node_id).await
    }

    async fn pause_subgraph(&self, hash: &DeploymentHash) -> Result<(), SubgraphRegistrarError> {
        SubgraphRegistrarTrait::pause_subgraph(&self.0, hash).await
    }

    async fn resume_subgraph(&self, hash: &DeploymentHash) -> Result<(), SubgraphRegistrarError> {
        SubgraphRegistrarTrait::resume_subgraph(&self.0, hash).await
    }
}
