//! Noop/stub trait implementations for the mock `Chain`.
//!
//! These types satisfy the trait bounds required by the `Chain` constructor
//! but are never called during normal test execution because:
//! - Triggers are provided directly via `StaticStreamBuilder` (no scanning needed)
//! - The real `EthereumRuntimeAdapterBuilder` is used for host functions
//!   (ethereum.call, ethereum.getBalance, ethereum.hasCode), backed by the call cache

use async_trait::async_trait;
use graph::blockchain::block_stream::{BlockRefetcher, BlockWithTriggers, FirehoseCursor};
use graph::blockchain::{BlockPtr, Blockchain, TriggersAdapter, TriggersAdapterSelector};
use graph::components::store::DeploymentLocator;
use graph::prelude::{BlockHash, BlockNumber, Error};
use graph::slog::{Discard, Logger};
use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;

use graph::slog::o;

/// No-op block refetcher. Tests have no reorgs, so refetching is never needed.
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
        Err(anyhow::anyhow!(
            "StaticBlockRefetcher::get_block should never be called — block refetching is disabled in test mode"
        ))
    }
}

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

/// Returns empty/default results. Never called since triggers come from `StaticStreamBuilder`.
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
