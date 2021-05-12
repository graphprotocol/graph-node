use anyhow::Error;
use async_trait::async_trait;
use futures::Stream;
use std::cmp;
use std::collections::VecDeque;
use std::mem;
use std::time::Duration;

use crate::components::store::BlockNumber;
use crate::components::store::WritableStore;
use crate::{prelude::*, prometheus::labels};

use super::{Block, BlockPtr, Blockchain};

#[cfg(debug_assertions)]
use fail::fail_point;

pub struct BlockWithTriggers<C: Blockchain> {
    pub block: C::Block,
    pub trigger_data: Vec<C::TriggerData>,
}

impl<C: Blockchain> BlockWithTriggers<C> {
    pub fn new(block: C::Block, trigger_data: Vec<C::TriggerData>) -> Self {
        Self {
            block,
            trigger_data,
        }
    }

    pub fn trigger_count(&self) -> usize {
        self.trigger_data.len()
    }

    pub fn ptr(&self) -> BlockPtr {
        self.block.ptr()
    }
}

pub enum ScanTriggersError {
    // The chain base could not be found. It should be reverted.
    ChainBaseNotFound,
    Unknown(Error),
}

#[async_trait]
pub trait TriggersAdapter<C: Blockchain>: Send + Sync {
    // Return the block that is `offset` blocks before the block pointed to
    // by `ptr` from the local cache. An offset of 0 means the block itself,
    // an offset of 1 means the block's parent etc. If the block is not in
    // the local cache, return `None`
    fn ancestor_block(&self, ptr: BlockPtr, offset: BlockNumber)
        -> Result<Option<C::Block>, Error>;

    // Returns a sequence of blocks in increasing order of block number.
    // Each block will include all of its triggers that match the given `filter`.
    // The sequence may omit blocks that contain no triggers,
    // but all returned blocks must part of a same chain starting at `chain_base`.
    // At least one block will be returned, even if it contains no triggers.
    // `step_size` is the suggested number blocks to be scanned.
    async fn scan_triggers(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &C::TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<C>>, Error>;

    // Used for reprocessing blocks when creating a data source.
    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: C::Block,
        filter: &C::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error>;

    /// Return `true` if the block with the given hash and number is on the
    /// main chain, i.e., the chain going back from the current chain head.
    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error>;

    /// Get pointer to parent of `block`. This is called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<BlockPtr, Error>;
}

pub enum BlockStreamEvent<C: Blockchain> {
    // ETHDEP: The meaning of the ptr needs to be clarified. Right now, it
    // has the same meaning as the pointer in `NextBlocks::Revert`, and it's
    // not clear whether that pointer should become the new subgraph head
    // pointer, or if we should revert that block, and make the block's
    // parent the new subgraph head. To not risk introducing bugs, for now,
    // we take it to mean whatever `NextBlocks::Revert` means
    Revert(BlockPtr),
    ProcessBlock(BlockWithTriggers<C>),
}

#[derive(Clone)]
pub struct BlockStreamMetrics {
    pub deployment_head: Box<Gauge>,
    pub reverted_blocks: Box<Gauge>,
    pub stopwatch: StopwatchMetrics,
}

impl BlockStreamMetrics {
    pub fn new(
        registry: Arc<impl MetricsRegistry>,
        deployment_id: &DeploymentHash,
        network: String,
        stopwatch: StopwatchMetrics,
    ) -> Self {
        let reverted_blocks = registry
            .new_deployment_gauge(
                "deployment_reverted_blocks",
                "Track the last reverted block for a subgraph deployment",
                deployment_id.as_str(),
            )
            .expect("Failed to create `deployment_reverted_blocks` gauge");
        let labels = labels! { String::from("deployment") => deployment_id.to_string(), String::from("network") => network };
        let deployment_head = registry
            .new_gauge(
                "deployment_head",
                "Track the head block number for a deployment",
                labels,
            )
            .expect("failed to create `deployment_head` gauge");
        Self {
            deployment_head,
            reverted_blocks,
            stopwatch,
        }
    }
}

enum BlockStreamState<C>
where
    C: Blockchain,
{
    /// Starting or restarting reconciliation.
    ///
    /// Valid next states: Reconciliation
    BeginReconciliation,

    /// The BlockStream is reconciling the subgraph store state with the chain store state.
    ///
    /// Valid next states: YieldingBlocks, Idle, BeginReconciliation (in case of revert)
    Reconciliation(Box<dyn Future<Item = NextBlocks<C>, Error = Error> + Send>),

    /// The BlockStream is emitting blocks that must be processed in order to bring the subgraph
    /// store up to date with the chain store.
    ///
    /// Valid next states: BeginReconciliation
    YieldingBlocks(VecDeque<BlockWithTriggers<C>>),

    /// The BlockStream experienced an error and is pausing before attempting to produce
    /// blocks again.
    ///
    /// Valid next states: BeginReconciliation
    RetryAfterDelay(Box<dyn Future<Item = (), Error = Error> + Send>),

    /// The BlockStream has reconciled the subgraph store and chain store states.
    /// No more work is needed until a chain head update.
    ///
    /// Valid next states: BeginReconciliation
    Idle,

    /// Not a real state, only used when going from one state to another.
    Transition,
}

/// A single next step to take in reconciling the state of the subgraph store with the state of the
/// chain store.
enum ReconciliationStep<C>
where
    C: Blockchain,
{
    /// Revert the current block pointed at by the subgraph pointer. The pointer is to the current
    /// subgraph head, and a single block will be reverted so the new head will be the parent of the
    /// current one.
    Revert(BlockPtr),

    /// Move forwards, processing one or more blocks. Second element is the block range size.
    ProcessDescendantBlocks(Vec<BlockWithTriggers<C>>, BlockNumber),

    /// This step is a no-op, but we need to check again for a next step.
    Retry,

    /// Subgraph pointer now matches chain head pointer.
    /// Reconciliation is complete.
    Done,
}

struct BlockStreamContext<C>
where
    C: Blockchain,
{
    subgraph_store: Arc<dyn WritableStore>,
    chain_store: Arc<dyn ChainStore>,
    adapter: Arc<C::TriggersAdapter>,
    node_id: NodeId,
    subgraph_id: DeploymentHash,
    // This is not really a block number, but the (unsigned) difference
    // between two block numbers
    reorg_threshold: BlockNumber,
    filter: C::TriggerFilter,
    start_blocks: Vec<BlockNumber>,
    logger: Logger,
    metrics: Arc<BlockStreamMetrics>,
    previous_triggers_per_block: f64,
    // Not a BlockNumber, but the difference between two block numbers
    previous_block_range_size: BlockNumber,
    // Not a BlockNumber, but the difference between two block numbers
    max_block_range_size: BlockNumber,
    target_triggers_per_block_range: u64,
}

impl<C: Blockchain> Clone for BlockStreamContext<C> {
    fn clone(&self) -> Self {
        Self {
            subgraph_store: self.subgraph_store.cheap_clone(),
            chain_store: self.chain_store.cheap_clone(),
            adapter: self.adapter.clone(),
            node_id: self.node_id.clone(),
            subgraph_id: self.subgraph_id.clone(),
            reorg_threshold: self.reorg_threshold,
            filter: self.filter.clone(),
            start_blocks: self.start_blocks.clone(),
            logger: self.logger.clone(),
            metrics: self.metrics.clone(),
            previous_triggers_per_block: self.previous_triggers_per_block,
            previous_block_range_size: self.previous_block_range_size,
            max_block_range_size: self.max_block_range_size,
            target_triggers_per_block_range: self.target_triggers_per_block_range,
        }
    }
}

/// Notifications about the chain head advancing. The block ingestor sends
/// an update on this stream whenever the head of the underlying chain
/// changes. The updates have no payload, receivers should call
/// `Store::chain_head_ptr` to check what the latest block is.
pub type ChainHeadUpdateStream = Box<dyn Stream<Item = (), Error = ()> + Send>;

pub trait ChainHeadUpdateListener: Send + Sync + 'static {
    /// Subscribe to chain head updates for the given network.
    fn subscribe(&self, network: String) -> ChainHeadUpdateStream;
}

pub struct BlockStream<C: Blockchain> {
    state: BlockStreamState<C>,
    consecutive_err_count: u32,
    chain_head_update_stream: ChainHeadUpdateStream,
    ctx: BlockStreamContext<C>,
}

// This is the same as `ReconciliationStep` but without retries.
enum NextBlocks<C>
where
    C: Blockchain,
{
    /// Blocks and range size
    Blocks(VecDeque<BlockWithTriggers<C>>, BlockNumber),

    /// Revert the current block pointed at by the subgraph pointer.
    Revert(BlockPtr),
    Done,
}

impl<C> BlockStream<C>
where
    C: Blockchain,
{
    pub fn new(
        subgraph_store: Arc<dyn WritableStore>,
        chain_store: Arc<dyn ChainStore>,
        chain_head_update_stream: ChainHeadUpdateStream,
        adapter: Arc<C::TriggersAdapter>,
        node_id: NodeId,
        subgraph_id: DeploymentHash,
        filter: C::TriggerFilter,
        start_blocks: Vec<BlockNumber>,
        reorg_threshold: BlockNumber,
        logger: Logger,
        metrics: Arc<BlockStreamMetrics>,
        max_block_range_size: BlockNumber,
        target_triggers_per_block_range: u64,
    ) -> Self {
        BlockStream {
            state: BlockStreamState::BeginReconciliation,
            consecutive_err_count: 0,
            chain_head_update_stream,
            ctx: BlockStreamContext {
                subgraph_store,
                chain_store,
                adapter,
                node_id,
                subgraph_id,
                reorg_threshold,
                logger,
                filter,
                start_blocks,
                metrics,

                // A high number here forces a slow start, with a range of 1.
                previous_triggers_per_block: 1_000_000.0,
                previous_block_range_size: 1,
                max_block_range_size,
                target_triggers_per_block_range,
            },
        }
    }
}

impl<C> BlockStreamContext<C>
where
    C: Blockchain,
{
    /// Perform reconciliation steps until there are blocks to yield or we are up-to-date.
    async fn next_blocks(&self) -> Result<NextBlocks<C>, Error> {
        let ctx = self.clone();

        loop {
            match ctx.get_next_step().await? {
                ReconciliationStep::ProcessDescendantBlocks(next_blocks, range_size) => {
                    return Ok(NextBlocks::Blocks(
                        next_blocks.into_iter().collect(),
                        range_size,
                    ));
                }
                ReconciliationStep::Retry => {
                    continue;
                }
                ReconciliationStep::Done => {
                    // Reconciliation is complete, so try to mark subgraph as Synced
                    ctx.update_subgraph_synced_status()?;

                    return Ok(NextBlocks::Done);
                }
                ReconciliationStep::Revert(block) => return Ok(NextBlocks::Revert(block)),
            }
        }
    }

    /// Determine the next reconciliation step. Does not modify Store or ChainStore.
    async fn get_next_step(&self) -> Result<ReconciliationStep<C>, Error> {
        let ctx = self.clone();
        let start_blocks = self.start_blocks.clone();
        let max_block_range_size = self.max_block_range_size;

        // Get pointers from database for comparison
        let head_ptr_opt = ctx.chain_store.chain_head_ptr()?;
        let subgraph_ptr = ctx.subgraph_store.block_ptr()?;

        // If chain head ptr is not set yet
        let head_ptr = match head_ptr_opt {
            Some(head_ptr) => head_ptr,

            // Don't do any reconciliation until the chain store has more blocks
            None => {
                return Ok(ReconciliationStep::Done);
            }
        };

        trace!(
            ctx.logger, "Chain head pointer";
            "hash" => format!("{:?}", head_ptr.hash),
            "number" => &head_ptr.number
        );
        trace!(
            ctx.logger, "Subgraph pointer";
            "hash" => format!("{:?}", subgraph_ptr.as_ref().map(|block| &block.hash)),
            "number" => subgraph_ptr.as_ref().map(|block| block.number),
        );

        // Make sure not to include genesis in the reorg threshold.
        let reorg_threshold = ctx.reorg_threshold.min(head_ptr.number);

        // Only continue if the subgraph block ptr is behind the head block ptr.
        // subgraph_ptr > head_ptr shouldn't happen, but if it does, it's safest to just stop.
        if let Some(ptr) = &subgraph_ptr {
            if ptr.number >= head_ptr.number {
                return Ok(ReconciliationStep::Done);
            }

            self.metrics.deployment_head.set(ptr.number as f64);
        }

        // Subgraph ptr is behind head ptr.
        // Let's try to move the subgraph ptr one step in the right direction.
        // First question: which direction should the ptr be moved?
        //
        // We will use a different approach to deciding the step direction depending on how far
        // the subgraph ptr is behind the head ptr.
        //
        // Normally, we need to worry about chain reorganizations -- situations where the
        // Ethereum client discovers a new longer chain of blocks different from the one we had
        // processed so far, forcing us to rollback one or more blocks we had already
        // processed.
        // We can't assume that blocks we receive are permanent.
        //
        // However, as a block receives more and more confirmations, eventually it becomes safe
        // to assume that that block will be permanent.
        // The probability of a block being "uncled" approaches zero as more and more blocks
        // are chained on after that block.
        // Eventually, the probability is so low, that a block is effectively permanent.
        // The "effectively permanent" part is what makes blockchains useful.
        // See here for more discussion:
        // https://blog.ethereum.org/2016/05/09/on-settlement-finality/
        //
        // Accordingly, if the subgraph ptr is really far behind the head ptr, then we can
        // trust that the Ethereum node knows what the real, permanent block is for that block
        // number.
        // We'll define "really far" to mean "greater than reorg_threshold blocks".
        //
        // If the subgraph ptr is not too far behind the head ptr (i.e. less than
        // reorg_threshold blocks behind), then we have to allow for the possibility that the
        // block might be on the main chain now, but might become uncled in the future.
        //
        // Most importantly: Our ability to make this assumption (or not) will determine what
        // Ethereum RPC calls can give us accurate data without race conditions.
        // (This is mostly due to some unfortunate API design decisions on the Ethereum side)
        if subgraph_ptr.is_none()
            || (head_ptr.number - subgraph_ptr.as_ref().unwrap().number) > reorg_threshold
        {
            // Since we are beyond the reorg threshold, the Ethereum node knows what block has
            // been permanently assigned this block number.
            // This allows us to ask the node: does subgraph_ptr point to a block that was
            // permanently accepted into the main chain, or does it point to a block that was
            // uncled?
            let is_on_main_chain = match &subgraph_ptr {
                Some(ptr) => ctx.adapter.is_on_main_chain(ptr.clone()).await?,
                None => true,
            };
            if !is_on_main_chain {
                // The subgraph ptr points to a block that was uncled.
                // We need to revert this block.
                //
                // Note: We can safely unwrap the subgraph ptr here, because
                // if it was `None`, `is_on_main_chain` would be true.
                return Ok(ReconciliationStep::Revert(subgraph_ptr.unwrap()));
            }

            // The subgraph ptr points to a block on the main chain.
            // This means that the last block we processed does not need to be
            // reverted.
            // Therefore, our direction of travel will be forward, towards the
            // chain head.

            // As an optimization, instead of advancing one block, we will use an
            // Ethereum RPC call to find the first few blocks that have event(s) we
            // are interested in that lie within the block range between the subgraph ptr
            // and either the next data source start_block or the reorg threshold.
            // Note that we use block numbers here.
            // This is an artifact of Ethereum RPC limitations.
            // It is only safe to use block numbers because we are beyond the reorg
            // threshold.

            // Start with first block after subgraph ptr; if the ptr is None,
            // then we start with the genesis block
            let from = subgraph_ptr.map_or(0, |ptr| ptr.number + 1);

            // Get the next subsequent data source start block to ensure the block
            // range is aligned with data source. This is not necessary for
            // correctness, but it avoids an ineffecient situation such as the range
            // being 0..100 and the start block for a data source being 99, then
            // `calls_in_block_range` would request unecessary traces for the blocks
            // 0 to 98 because the start block is within the range.
            let next_start_block: BlockNumber = start_blocks
                .into_iter()
                .filter(|block_num| block_num > &from)
                .min()
                .unwrap_or(BLOCK_NUMBER_MAX);

            // End either just before the the next data source start_block or just
            // prior to the reorg threshold. It isn't safe to go farther than the
            // reorg threshold due to race conditions.
            let to_limit = cmp::min(head_ptr.number - reorg_threshold, next_start_block - 1);

            // Calculate the range size according to the target number of triggers,
            // respecting the global maximum and also not increasing too
            // drastically from the previous block range size.
            //
            // An example of the block range dynamics:
            // - Start with a block range of 1, target of 1000.
            // - Scan 1 block:
            //   0 triggers found, max_range_size = 10, range_size = 10
            // - Scan 10 blocks:
            //   2 triggers found, 0.2 per block, range_size = 1000 / 0.2 = 5000
            // - Scan 5000 blocks:
            //   10000 triggers found, 2 per block, range_size = 1000 / 2 = 500
            // - Scan 500 blocks:
            //   1000 triggers found, 2 per block, range_size = 1000 / 2 = 500
            let range_size_upper_limit =
                max_block_range_size.min(ctx.previous_block_range_size * 10);
            let range_size = if ctx.previous_triggers_per_block == 0.0 {
                range_size_upper_limit
            } else {
                (self.target_triggers_per_block_range as f64 / ctx.previous_triggers_per_block)
                    .max(1.0)
                    .min(range_size_upper_limit as f64) as BlockNumber
            };
            let to = cmp::min(from + range_size - 1, to_limit);

            let section = ctx.metrics.stopwatch.start_section("scan_blocks");
            info!(
                ctx.logger,
                "Scanning blocks [{}, {}]", from, to;
                "range_size" => range_size
            );

            let blocks = self.adapter.scan_triggers(from, to, &self.filter).await?;

            section.end();
            Ok(ReconciliationStep::ProcessDescendantBlocks(
                blocks, range_size,
            ))
        } else {
            // The subgraph ptr is not too far behind the head ptr.
            // This means a few things.
            //
            // First, because we are still within the reorg threshold,
            // we can't trust the Ethereum RPC methods that use block numbers.
            // Block numbers in this region are not yet immutable pointers to blocks;
            // the block associated with a particular block number on the Ethereum node could
            // change under our feet at any time.
            //
            // Second, due to how the BlockIngestor is designed, we get a helpful guarantee:
            // the head block and at least its reorg_threshold most recent ancestors will be
            // present in the block store.
            // This allows us to work locally in the block store instead of relying on
            // Ethereum RPC calls, so that we are not subject to the limitations of the RPC
            // API.

            // To determine the step direction, we need to find out if the subgraph ptr refers
            // to a block that is an ancestor of the head block.
            // We can do so by walking back up the chain from the head block to the appropriate
            // block number, and checking to see if the block we found matches the
            // subgraph_ptr.

            let subgraph_ptr =
                subgraph_ptr.expect("subgraph block pointer should not be `None` here");

            #[cfg(debug_assertions)]
            if test_reorg(subgraph_ptr.clone()) {
                return Ok(ReconciliationStep::Revert(subgraph_ptr));
            }

            // Precondition: subgraph_ptr.number < head_ptr.number
            // Walk back to one block short of subgraph_ptr.number
            let offset = head_ptr.number - subgraph_ptr.number - 1;

            // In principle this block should be in the store, but we have seen this error for deep
            // reorgs in ropsten.
            let head_ancestor_opt = self.adapter.ancestor_block(head_ptr, offset)?;

            match head_ancestor_opt {
                None => {
                    // Block is missing in the block store.
                    // This generally won't happen often, but can happen if the head ptr has
                    // been updated since we retrieved the head ptr, and the block store has
                    // been garbage collected.
                    // It's easiest to start over at this point.
                    Ok(ReconciliationStep::Retry)
                }
                Some(head_ancestor) => {
                    // We stopped one block short, so we'll compare the parent hash to the
                    // subgraph ptr.
                    if head_ancestor.parent_hash().as_ref() == Some(&subgraph_ptr.hash) {
                        // The subgraph ptr is an ancestor of the head block.
                        // We cannot use an RPC call here to find the first interesting block
                        // due to the race conditions previously mentioned,
                        // so instead we will advance the subgraph ptr by one block.
                        // Note that head_ancestor is a child of subgraph_ptr.
                        let block = self
                            .adapter
                            .triggers_in_block(&self.logger, head_ancestor, &self.filter)
                            .await?;
                        Ok(ReconciliationStep::ProcessDescendantBlocks(vec![block], 1))
                    } else {
                        // The subgraph ptr is not on the main chain.
                        // We will need to step back (possibly repeatedly) one block at a time
                        // until we are back on the main chain.
                        Ok(ReconciliationStep::Revert(subgraph_ptr))
                    }
                }
            }
        }
    }

    /// Set subgraph deployment entity synced flag if and only if the subgraph block pointer is
    /// caught up to the head block pointer.
    fn update_subgraph_synced_status(&self) -> Result<(), Error> {
        let head_ptr_opt = self.chain_store.chain_head_ptr()?;
        let subgraph_ptr = self.subgraph_store.block_ptr()?;

        if head_ptr_opt != subgraph_ptr || head_ptr_opt.is_none() || subgraph_ptr.is_none() {
            // Not synced yet
            Ok(())
        } else {
            // Synced

            // Stop recording time-to-sync metrics.
            self.metrics.stopwatch.disable();

            self.subgraph_store.deployment_synced()
        }
    }
}

impl<C: Blockchain> Stream for BlockStream<C> {
    type Item = BlockStreamEvent<C>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut state = BlockStreamState::Transition;
        mem::swap(&mut self.state, &mut state);

        let result = loop {
            match state {
                BlockStreamState::BeginReconciliation => {
                    // Start the reconciliation process by asking for blocks
                    let ctx = self.ctx.clone();
                    let fut = async move { ctx.next_blocks().await };
                    state = BlockStreamState::Reconciliation(Box::new(fut.boxed().compat()));
                }

                // Waiting for the reconciliation to complete or yield blocks
                BlockStreamState::Reconciliation(mut next_blocks_future) => {
                    match next_blocks_future.poll() {
                        // Reconciliation found blocks to process
                        Ok(Async::Ready(NextBlocks::Blocks(next_blocks, block_range_size))) => {
                            // We had only one error, so we infer that reducing the range size is
                            // what fixed it. Reduce the max range size to prevent future errors.
                            // See: 018c6df4-132f-4acc-8697-a2d64e83a9f0
                            if self.consecutive_err_count == 1 {
                                // Reduce the max range size by 10%, but to no less than 10.
                                self.ctx.max_block_range_size =
                                    (self.ctx.max_block_range_size * 9 / 10).max(10);
                            }
                            self.consecutive_err_count = 0;

                            let total_triggers =
                                next_blocks.iter().map(|b| b.trigger_count()).sum::<usize>();
                            self.ctx.previous_triggers_per_block =
                                total_triggers as f64 / block_range_size as f64;
                            self.ctx.previous_block_range_size = block_range_size;
                            if total_triggers > 0 {
                                debug!(self.ctx.logger, "Processing {} triggers", total_triggers);
                            }

                            // Switch to yielding state until next_blocks is depleted
                            state = BlockStreamState::YieldingBlocks(next_blocks);

                            // Yield the first block in next_blocks
                            continue;
                        }

                        // Reconciliation completed. We're caught up to chain head.
                        Ok(Async::Ready(NextBlocks::Done)) => {
                            // Reset error count
                            self.consecutive_err_count = 0;

                            // Switch to idle
                            state = BlockStreamState::Idle;

                            // Poll for chain head update
                            continue;
                        }

                        Ok(Async::Ready(NextBlocks::Revert(block))) => {
                            state = BlockStreamState::BeginReconciliation;
                            break Ok(Async::Ready(Some(BlockStreamEvent::Revert(block))));
                        }

                        Ok(Async::NotReady) => {
                            // Nothing to change or yield yet.
                            state = BlockStreamState::Reconciliation(next_blocks_future);
                            break Ok(Async::NotReady);
                        }

                        Err(e) => {
                            // Reset the block range size in an attempt to recover from the error.
                            // See also: 018c6df4-132f-4acc-8697-a2d64e83a9f0
                            self.ctx.previous_block_range_size = 1;
                            self.consecutive_err_count += 1;

                            // Pause before trying again
                            let secs = (5 * self.consecutive_err_count).max(120) as u64;
                            state = BlockStreamState::RetryAfterDelay(Box::new(
                                tokio::time::delay_for(Duration::from_secs(secs))
                                    .map(Ok)
                                    .boxed()
                                    .compat(),
                            ));
                            break Err(e);
                        }
                    }
                }

                // Yielding blocks from reconciliation process
                BlockStreamState::YieldingBlocks(mut next_blocks) => {
                    match next_blocks.pop_front() {
                        // Yield one block
                        Some(next_block) => {
                            state = BlockStreamState::YieldingBlocks(next_blocks);
                            break Ok(Async::Ready(Some(BlockStreamEvent::ProcessBlock(
                                next_block,
                            ))));
                        }

                        // Done yielding blocks
                        None => {
                            state = BlockStreamState::BeginReconciliation;
                        }
                    }
                }

                // Pausing after an error, before looking for more blocks
                BlockStreamState::RetryAfterDelay(mut delay) => match delay.poll() {
                    Ok(Async::Ready(())) | Err(_) => {
                        state = BlockStreamState::BeginReconciliation;
                    }

                    Ok(Async::NotReady) => {
                        state = BlockStreamState::RetryAfterDelay(delay);
                        break Ok(Async::NotReady);
                    }
                },

                // Waiting for a chain head update
                BlockStreamState::Idle => {
                    match self.chain_head_update_stream.poll() {
                        // Chain head was updated
                        Ok(Async::Ready(Some(()))) => {
                            state = BlockStreamState::BeginReconciliation;
                        }

                        // Chain head update stream ended
                        Ok(Async::Ready(None)) => {
                            // Should not happen
                            return Err(anyhow::anyhow!(
                                "chain head update stream ended unexpectedly"
                            ));
                        }

                        Ok(Async::NotReady) => {
                            // Stay idle
                            state = BlockStreamState::Idle;
                            break Ok(Async::NotReady);
                        }

                        // mpsc channel failed
                        Err(()) => {
                            // Should not happen
                            return Err(anyhow::anyhow!("chain head update Receiver failed"));
                        }
                    }
                }

                // This will only happen if this poll function fails to complete normally then is
                // called again.
                BlockStreamState::Transition => unreachable!(),
            }
        };

        self.state = state;

        result
    }
}

// This always returns `false` in a normal build. A test may configure reorg by enabling
// "test_reorg" fail point with the number of the block that should be reorged.
#[cfg(debug_assertions)]
#[allow(unused_variables)]
fn test_reorg(ptr: BlockPtr) -> bool {
    fail_point!("test_reorg", |reorg_at| {
        use std::str::FromStr;

        static REORGED: std::sync::Once = std::sync::Once::new();

        if REORGED.is_completed() {
            return false;
        }
        let reorg_at = BlockNumber::from_str(&reorg_at.unwrap()).unwrap();
        let should_reorg = ptr.number == reorg_at;
        if should_reorg {
            REORGED.call_once(|| {})
        }
        should_reorg
    });

    false
}
