//! Noop/stub trait implementations for the mock `Chain`.
//!
//! These types satisfy the trait bounds required by the `Chain` constructor
//! but are never called during normal test execution because:
//! - Triggers are provided directly via `StaticStreamBuilder` (no scanning needed)
//! - Runtime host functions (eth_call etc.) are not available in mock tests
//!   (subgraphs that use `ethereum.call()` will fail — this is a known V1 limitation)

use async_trait::async_trait;
use graph::blockchain::block_stream::{BlockRefetcher, BlockWithTriggers, FirehoseCursor};
use graph::blockchain::{
    BlockPtr, Blockchain, ChainIdentifier, RuntimeAdapter as RuntimeAdapterTrait, TriggersAdapter,
    TriggersAdapterSelector,
};
use graph::components::store::{DeploymentLocator, EthereumCallCache};
use graph::data_source::DataSource;
use graph::prelude::{BlockHash, BlockNumber, Error};
use graph::slog::{Discard, Logger};
use graph_chain_ethereum::chain::RuntimeAdapterBuilder;
use graph_chain_ethereum::network::EthereumNetworkAdapters;
use graph_chain_ethereum::Chain;
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;

use graph::slog::o;

// ============ Block Refetcher ============

/// Block refetcher that never refetches.
///
/// In production, block refetching handles reorgs by re-fetching blocks from
/// the chain. In tests, all blocks are pre-defined and there are no reorgs,
/// so this is a noop. `required()` returns false so it's never called.
pub(super) struct StaticBlockRefetcher<C: Blockchain> {
    pub _phantom: PhantomData<C>,
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
        unimplemented!("StaticBlockRefetcher should never be called")
    }
}

// ============ Runtime Adapters ============

/// Returns empty host functions — chain-specific runtime extensions
/// (like eth_call) are not available in mock tests.
struct NoopRuntimeAdapter<C> {
    _phantom: PhantomData<C>,
}

impl<C: Blockchain> RuntimeAdapterTrait<C> for NoopRuntimeAdapter<C> {
    fn host_fns(&self, _ds: &DataSource<C>) -> Result<Vec<graph::blockchain::HostFn>, Error> {
        Ok(vec![])
    }
}

/// Builds `NoopRuntimeAdapter` instances for the Chain constructor.
pub(super) struct NoopRuntimeAdapterBuilder;

impl RuntimeAdapterBuilder for NoopRuntimeAdapterBuilder {
    fn build(
        &self,
        _eth_adapters: Arc<EthereumNetworkAdapters>,
        _call_cache: Arc<dyn EthereumCallCache>,
        _chain_identifier: Arc<ChainIdentifier>,
    ) -> Arc<dyn RuntimeAdapterTrait<Chain>> {
        Arc::new(NoopRuntimeAdapter {
            _phantom: PhantomData,
        })
    }
}

// ============ Triggers Adapters ============

/// Always returns `NoopTriggersAdapter` regardless of deployment or capabilities.
pub(super) struct NoopAdapterSelector<C> {
    pub _phantom: PhantomData<C>,
}

impl<C: Blockchain> TriggersAdapterSelector<C> for NoopAdapterSelector<C> {
    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &<C as Blockchain>::NodeCapabilities,
        _unified_api_version: graph::data::subgraph::UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapter<C>>, Error> {
        Ok(Arc::new(NoopTriggersAdapter {
            _phantom: PhantomData,
        }))
    }
}

/// A triggers adapter that returns empty/default results for all methods.
///
/// Since we feed pre-built triggers via `StaticStreamBuilder`, the adapter's
/// scanning and fetching methods are never called during normal test execution.
/// The methods that are called (like `parent_ptr` for chain traversal) return
/// sensible defaults.
struct NoopTriggersAdapter<C: Blockchain> {
    _phantom: PhantomData<C>,
}

#[async_trait]
impl<C: Blockchain> TriggersAdapter<C> for NoopTriggersAdapter<C> {
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
        _root: Option<BlockHash>,
    ) -> Result<Option<<C as Blockchain>::Block>, Error> {
        Ok(None)
    }

    async fn load_block_ptrs_by_numbers(
        &self,
        _logger: Logger,
        _block_numbers: BTreeSet<BlockNumber>,
    ) -> Result<Vec<C::Block>, Error> {
        Ok(vec![])
    }

    async fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        Ok(None)
    }

    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &C::TriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<C>>, BlockNumber), Error> {
        Ok((vec![], 0))
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        block: <C as Blockchain>::Block,
        _filter: &<C as Blockchain>::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error> {
        let logger = Logger::root(Discard, o!());
        Ok(BlockWithTriggers::new(block, Vec::new(), &logger))
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        Ok(true)
    }

    /// Returns a synthetic parent pointer for chain traversal.
    /// Block 0 has no parent; all others point to block N-1 with a default hash.
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
