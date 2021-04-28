use std::{pin::Pin, sync::Arc, task::Context};

use anyhow::Error;
use graph::{
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamEvent, BlockWithTriggers, ScanTriggersError, TriggersAdapter,
        },
        Block, BlockHash, Blockchain, DataSource, IngestorAdapter as IngestorAdapterTrait,
        IngestorError, Manifest, TriggerFilter,
    },
    cheap_clone::CheapClone,
    prelude::{
        async_trait, error, serde_yaml, web3::types::H256, BlockNumber, BlockPtr, ChainStore,
        DeploymentHash, EthereumAdapter, Future01CompatExt, LinkResolver, Logger,
    },
    runtime::{AscType, DeterministicHostError},
    tokio_stream::Stream,
};

pub struct Chain {
    adapter: Arc<IngestorAdapter>,
}

impl Chain {
    pub fn new(
        logger: Logger,
        chain_store: Arc<dyn ChainStore>,
        eth_adapter: Arc<dyn EthereumAdapter>,
        ancestor_count: BlockNumber,
    ) -> Self {
        let adapter = Arc::new(IngestorAdapter {
            eth_adapter,
            logger,
            ancestor_count,
            chain_store,
        });
        Chain { adapter }
    }
}

impl Blockchain for Chain {
    type Block = DummyBlock;

    type DataSource = DummyDataSource;

    type DataSourceTemplate = DummyDataSourceTemplate;

    type Manifest = DummyManifest;

    type TriggersAdapter = DummyTriggerAdapter;

    type BlockStream = DummyBlockStream;

    type TriggerData = DummyTriggerData;

    type MappingTrigger = DummyMappingTrigger;

    type TriggerFilter = DummyTriggerFilter;

    type NodeCapabilities = DummyNodeCapabilities;

    type IngestorAdapter = IngestorAdapter;

    fn reorg_threshold() -> u32 {
        todo!()
    }

    fn triggers_adapter(
        &self,
        _network: &str,
        _capabilities: Self::NodeCapabilities,
    ) -> Arc<Self::TriggersAdapter> {
        todo!()
    }

    fn new_block_stream(
        &self,
        _current_head: BlockPtr,
        _filter: Self::TriggerFilter,
    ) -> Result<Self::BlockStream, Error> {
        todo!()
    }

    fn ingestor_adapter(&self) -> Arc<Self::IngestorAdapter> {
        self.adapter.clone()
    }
}

pub struct DummyBlock;

impl Block for DummyBlock {
    fn ptr(&self) -> BlockPtr {
        todo!()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        todo!()
    }
}

pub struct DummyDataSource;

impl DataSource<Chain> for DummyDataSource {
    fn match_and_decode(
        &self,
        _trigger: &DummyTriggerData,
        _block: &DummyBlock,
        _logger: &Logger,
    ) -> Result<Option<DummyMappingTrigger>, Error> {
        todo!()
    }
}

pub struct DummyDataSourceTemplate;

pub struct DummyManifest;

#[async_trait]
impl Manifest<Chain> for DummyManifest {
    async fn resolve_from_raw(
        _id: DeploymentHash,
        _raw: serde_yaml::Mapping,
        _resolver: &impl LinkResolver,
        _logger: &Logger,
    ) -> Result<Self, Error> {
        todo!()
    }

    fn data_sources(&self) -> &[DummyDataSource] {
        todo!()
    }

    fn templates(&self) -> &[DummyDataSourceTemplate] {
        todo!()
    }
}

pub struct DummyTriggerAdapter;

#[async_trait]
impl TriggersAdapter<Chain> for DummyTriggerAdapter {
    async fn scan_triggers(
        &self,
        _chain_base: BlockPtr,
        _step_size: u32,
        _filter: DummyTriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, ScanTriggersError> {
        todo!()
    }

    async fn triggers_in_block(
        &self,
        _block: DummyBlock,
        _filter: DummyTriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        todo!()
    }
}

pub struct DummyBlockStream;

impl Stream for DummyBlockStream {
    type Item = Result<BlockStreamEvent<Chain>, Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl BlockStream<Chain> for DummyBlockStream {}

pub struct DummyTriggerData;

pub struct DummyMappingTrigger;

impl AscType for DummyMappingTrigger {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        todo!()
    }

    fn from_asc_bytes(_asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
        todo!()
    }
}

pub struct DummyTriggerFilter;

impl Default for DummyTriggerFilter {
    fn default() -> Self {
        todo!()
    }
}

impl TriggerFilter<Chain> for DummyTriggerFilter {
    fn extend<'a>(&mut self, _data_sources: impl Iterator<Item = &'a DummyDataSource>) {
        todo!()
    }
}

pub struct DummyNodeCapabilities;

pub struct IngestorAdapter {
    logger: Logger,
    ancestor_count: i32,
    eth_adapter: Arc<dyn EthereumAdapter>,
    chain_store: Arc<dyn ChainStore>,
}

#[async_trait]
impl IngestorAdapterTrait<Chain> for IngestorAdapter {
    async fn latest_block(&self) -> Result<BlockPtr, IngestorError> {
        self.eth_adapter
            .latest_block_header(&self.logger)
            .compat()
            .await
            .map(|block| block.into())
    }

    async fn ingest_block(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockHash>, IngestorError> {
        // TODO: H256::from_slice can panic
        let block_hash = H256::from_slice(block_hash.as_slice());

        // Get the fully populated block
        let block = self
            .eth_adapter
            .block_by_hash(&self.logger, block_hash)
            .compat()
            .await?
            .ok_or_else(|| IngestorError::BlockUnavailable(block_hash))?;
        let block = self
            .eth_adapter
            .load_full_block(&self.logger, block)
            .compat()
            .await?;

        // Store it in the database and try to advance the chain head pointer
        self.chain_store.upsert_block(block).await?;

        self.chain_store
            .cheap_clone()
            .attempt_chain_head_update(self.ancestor_count)
            .await
            .map(|missing| missing.map(|h256| h256.into()))
            .map_err(|e| {
                error!(self.logger, "failed to update chain head");
                IngestorError::Unknown(e)
            })
    }

    fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        self.chain_store.chain_head_ptr()
    }

    fn cleanup_cached_blocks(&self) -> Result<Option<(i32, usize)>, Error> {
        self.chain_store.cleanup_cached_blocks(self.ancestor_count)
    }
}
