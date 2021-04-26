use std::{pin::Pin, sync::Arc, task::Context};

use anyhow::Error;
use graph::{
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamEvent, BlockWithTriggers, ScanTriggersError, TriggersAdapter,
        },
        Block, Blockchain, DataSource, IngestorAdapter, Manifest, TriggerFilter,
    },
    prelude::{async_trait, serde_yaml, BlockPtr, DeploymentHash, LinkResolver, Logger},
    runtime::{AscType, DeterministicHostError},
    tokio_stream::Stream,
};

struct Chain;

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

    type IngestorAdapter = DummyIngestorAdapter;

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
}

struct DummyBlock;

impl Block for DummyBlock {
    fn ptr(&self) -> BlockPtr {
        todo!()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        todo!()
    }
}

struct DummyDataSource;

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

struct DummyDataSourceTemplate;

struct DummyManifest;

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

struct DummyTriggerAdapter;

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

struct DummyBlockStream;

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

struct DummyTriggerData;

struct DummyMappingTrigger;

impl AscType for DummyMappingTrigger {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        todo!()
    }

    fn from_asc_bytes(_asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
        todo!()
    }
}

struct DummyTriggerFilter;

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

struct DummyNodeCapabilities;

struct DummyIngestorAdapter;

impl IngestorAdapter<Chain> for DummyIngestorAdapter {
    fn head_block(&self) -> DummyBlock {
        todo!()
    }

    fn ingest_block(&self, _block: &DummyBlock) -> Result<(), Error> {
        todo!()
    }
}
