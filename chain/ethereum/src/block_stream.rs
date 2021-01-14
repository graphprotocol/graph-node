use std::cmp;
use std::collections::VecDeque;
use std::mem;
use std::time::Duration;

use graph::components::{
    ethereum::{blocks_with_triggers, triggers_in_block, EthereumNetworks, NodeCapabilities},
    store::BlockStore,
};
use graph::prelude::{
    BlockStream as BlockStreamTrait, BlockStreamBuilder as BlockStreamBuilderTrait, *,
};

#[cfg(debug_assertions)]
use fail::fail_point;

lazy_static! {
    /// Maximum number of blocks to request in each chunk.
    static ref MAX_BLOCK_RANGE_SIZE: u64 = std::env::var("GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE")
        .unwrap_or("2000".into())
        .parse::<u64>()
        .expect("invalid GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE");

    /// Ideal number of triggers in a range. The range size will adapt to try to meet this.
    static ref TARGET_TRIGGERS_PER_BLOCK_RANGE: u64 = std::env::var("GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE")
        .unwrap_or("100".into())
        .parse::<u64>()
        .expect("invalid GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE");
}

enum BlockStreamState {
    /// Starting or restarting reconciliation.
    ///
    /// Valid next states: Reconciliation
    BeginReconciliation,

    /// The BlockStream is reconciling the subgraph store state with the chain store state.
    ///
    /// Valid next states: YieldingBlocks, Idle, BeginReconciliation (in case of revert)
    Reconciliation(Box<dyn Future<Item = NextBlocks, Error = Error> + Send>),

    /// The BlockStream is emitting blocks that must be processed in order to bring the subgraph
    /// store up to date with the chain store.
    ///
    /// Valid next states: BeginReconciliation
    YieldingBlocks(VecDeque<EthereumBlockWithTriggers>),

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
enum ReconciliationStep {
    /// Revert the current block pointed at by the subgraph pointer. The pointer is to the current
    /// subgraph head, and a single block will be reverted so the new head will be the parent of the
    /// current one.
    Revert(EthereumBlockPointer),

    /// Move forwards, processing one or more blocks. Second element is the block range size.
    ProcessDescendantBlocks(Vec<EthereumBlockWithTriggers>, u64),

    /// This step is a no-op, but we need to check again for a next step.
    Retry,

    /// Subgraph pointer now matches chain head pointer.
    /// Reconciliation is complete.
    Done,
}

struct BlockStreamContext<S, C> {
    subgraph_store: Arc<S>,
    chain_store: Arc<C>,
    eth_adapter: Arc<dyn EthereumAdapter>,
    node_id: NodeId,
    subgraph_id: SubgraphDeploymentId,
    reorg_threshold: u64,
    log_filter: EthereumLogFilter,
    call_filter: EthereumCallFilter,
    block_filter: EthereumBlockFilter,
    start_blocks: Vec<u64>,
    include_calls_in_blocks: bool,
    logger: Logger,
    metrics: Arc<BlockStreamMetrics>,
    previous_triggers_per_block: f64,
    previous_block_range_size: u64,
    max_block_range_size: u64,
}

impl<S, C> Clone for BlockStreamContext<S, C> {
    fn clone(&self) -> Self {
        Self {
            subgraph_store: self.subgraph_store.cheap_clone(),
            chain_store: self.chain_store.cheap_clone(),
            eth_adapter: self.eth_adapter.cheap_clone(),
            node_id: self.node_id.clone(),
            subgraph_id: self.subgraph_id.clone(),
            reorg_threshold: self.reorg_threshold,
            log_filter: self.log_filter.clone(),
            call_filter: self.call_filter.clone(),
            block_filter: self.block_filter.clone(),
            start_blocks: self.start_blocks.clone(),
            include_calls_in_blocks: self.include_calls_in_blocks,
            logger: self.logger.clone(),
            metrics: self.metrics.clone(),
            previous_triggers_per_block: self.previous_triggers_per_block,
            previous_block_range_size: self.previous_block_range_size,
            max_block_range_size: self.max_block_range_size,
        }
    }
}

pub struct BlockStream<S, C> {
    state: BlockStreamState,
    consecutive_err_count: u32,
    chain_head_update_stream: ChainHeadUpdateStream,
    ctx: BlockStreamContext<S, C>,
}

// This is the same as `ReconciliationStep` but without retries.
enum NextBlocks {
    /// Blocks and range size
    Blocks(VecDeque<EthereumBlockWithTriggers>, u64),

    /// Revert the current block pointed at by the subgraph pointer.
    Revert(EthereumBlockPointer),
    Done,
}

impl<S, C> BlockStream<S, C>
where
    S: SubgraphStore,
    C: ChainStore,
{
    pub fn new(
        subgraph_store: Arc<S>,
        chain_store: Arc<C>,
        eth_adapter: Arc<dyn EthereumAdapter>,
        node_id: NodeId,
        subgraph_id: SubgraphDeploymentId,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        start_blocks: Vec<u64>,
        include_calls_in_blocks: bool,
        reorg_threshold: u64,
        logger: Logger,
        metrics: Arc<BlockStreamMetrics>,
    ) -> Self {
        BlockStream {
            state: BlockStreamState::BeginReconciliation,
            consecutive_err_count: 0,
            chain_head_update_stream: chain_store.chain_head_updates(),
            ctx: BlockStreamContext {
                subgraph_store,
                chain_store,
                eth_adapter,
                node_id,
                subgraph_id,
                reorg_threshold,
                logger,
                log_filter,
                call_filter,
                block_filter,
                start_blocks,
                include_calls_in_blocks,
                metrics,

                // A high number here forces a slow start, with a range of 1.
                previous_triggers_per_block: 1_000_000.0,
                previous_block_range_size: 1,
                max_block_range_size: *MAX_BLOCK_RANGE_SIZE,
            },
        }
    }
}

impl<S, C> BlockStreamContext<S, C>
where
    S: SubgraphStore,
    C: ChainStore,
{
    /// Perform reconciliation steps until there are blocks to yield or we are up-to-date.
    fn next_blocks(&self) -> Box<dyn Future<Item = NextBlocks, Error = Error> + Send> {
        let ctx = self.clone();

        Box::new(future::loop_fn((), move |()| {
            let ctx1 = ctx.clone();
            let ctx2 = ctx.clone();

            ctx1.get_next_step().and_then(move |outcome| match outcome {
                ReconciliationStep::ProcessDescendantBlocks(next_blocks, range_size) => {
                    Ok(future::Loop::Break(NextBlocks::Blocks(
                        next_blocks.into_iter().collect(),
                        range_size,
                    )))
                }
                ReconciliationStep::Retry => Ok(future::Loop::Continue(())),
                ReconciliationStep::Done => {
                    // Reconciliation is complete, so try to mark subgraph as Synced
                    ctx2.update_subgraph_synced_status()?;

                    Ok(future::Loop::Break(NextBlocks::Done))
                }
                ReconciliationStep::Revert(block) => {
                    Ok(future::Loop::Break(NextBlocks::Revert(block)))
                }
            })
        }))
    }

    /// Determine the next reconciliation step. Does not modify Store or ChainStore.
    fn get_next_step(&self) -> impl Future<Item = ReconciliationStep, Error = Error> + Send {
        let ctx = self.clone();
        let log_filter = self.log_filter.clone();
        let call_filter = self.call_filter.clone();
        let block_filter = self.block_filter.clone();
        let start_blocks = self.start_blocks.clone();
        let max_block_range_size = self.max_block_range_size;

        // Get pointers from database for comparison
        let head_ptr_opt = ctx.chain_store.chain_head_ptr().unwrap();
        let subgraph_ptr = ctx.subgraph_store.block_ptr(&ctx.subgraph_id).unwrap();

        // If chain head ptr is not set yet
        if head_ptr_opt.is_none() {
            // Don't do any reconciliation until the chain store has more blocks
            return Box::new(future::ok(ReconciliationStep::Done))
                as Box<dyn Future<Item = _, Error = _> + Send>;
        }

        let head_ptr = head_ptr_opt.unwrap();

        trace!(
            ctx.logger, "Chain head pointer";
            "hash" => format!("{:?}", head_ptr.hash),
            "number" => &head_ptr.number
        );
        trace!(
            ctx.logger, "Subgraph pointer";
            "hash" => format!("{:?}", subgraph_ptr.map(|block| block.hash)),
            "number" => subgraph_ptr.map(|block| block.number),
        );

        // Make sure not to include genesis in the reorg threshold.
        let reorg_threshold = ctx.reorg_threshold.min(head_ptr.number);

        // Only continue if the subgraph block ptr is behind the head block ptr.
        // subgraph_ptr > head_ptr shouldn't happen, but if it does, it's safest to just stop.
        if let Some(ptr) = subgraph_ptr {
            if ptr.number >= head_ptr.number {
                return Box::new(future::ok(ReconciliationStep::Done))
                    as Box<dyn Future<Item = _, Error = _> + Send>;
            }

            self.metrics
                .blocks_behind
                .set((head_ptr.number - ptr.number) as f64);
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
            || (head_ptr.number - subgraph_ptr.unwrap().number) > reorg_threshold
        {
            // Since we are beyond the reorg threshold, the Ethereum node knows what block has
            // been permanently assigned this block number.
            // This allows us to ask the node: does subgraph_ptr point to a block that was
            // permanently accepted into the main chain, or does it point to a block that was
            // uncled?
            Box::new(
                subgraph_ptr
                    .map_or(
                        Box::new(future::ok(true)) as Box<dyn Future<Item = _, Error = _> + Send>,
                        |ptr| {
                            ctx.eth_adapter.is_on_main_chain(
                                &ctx.logger,
                                ctx.metrics.ethrpc_metrics.clone(),
                                ctx.chain_store.clone(),
                                ptr,
                            )
                        },
                    )
                    .and_then(
                        move |is_on_main_chain| -> Box<dyn Future<Item = _, Error = _> + Send> {
                            if !is_on_main_chain {
                                // The subgraph ptr points to a block that was uncled.
                                // We need to revert this block.
                                //
                                // Note: We can safely unwrap the subgraph ptr here, because
                                // if it was `None`, `is_on_main_chain` would be true.
                                return Box::new(future::ok(ReconciliationStep::Revert(
                                    subgraph_ptr.unwrap(),
                                )));
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

                            // Get the next subsequent data source start block to ensure the block range
                            // is aligned with data source.
                            let next_start_block: u64 = start_blocks
                                .into_iter()
                                .filter(|block_num| block_num > &from)
                                .min()
                                .unwrap_or(std::u64::MAX);

                            // End either just before the the next data source start_block or
                            // just prior to the reorg threshold. It isn't safe to go any farther
                            // due to race conditions.
                            let to_limit =
                                cmp::min(head_ptr.number - reorg_threshold, next_start_block - 1);

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
                                (*TARGET_TRIGGERS_PER_BLOCK_RANGE as f64
                                    / ctx.previous_triggers_per_block)
                                    .max(1.0)
                                    .min(range_size_upper_limit as f64)
                                    as u64
                            };
                            let to = cmp::min(from + range_size - 1, to_limit);

                            let section = ctx.metrics.stopwatch.start_section("scan_blocks");
                            info!(
                                ctx.logger,
                                "Scanning blocks [{}, {}]", from, to;
                                "range_size" => range_size
                            );
                            Box::new(
                                blocks_with_triggers(
                                    ctx.eth_adapter,
                                    ctx.logger.clone(),
                                    ctx.chain_store.clone(),
                                    ctx.metrics.ethrpc_metrics.clone(),
                                    from,
                                    to,
                                    log_filter.clone(),
                                    call_filter.clone(),
                                    block_filter.clone(),
                                )
                                .map_ok(move |blocks| {
                                    section.end();
                                    ReconciliationStep::ProcessDescendantBlocks(blocks, range_size)
                                })
                                .boxed()
                                .compat(),
                            )
                        },
                    ),
            )
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
            if test_reorg(subgraph_ptr) {
                return Box::new(future::ok(ReconciliationStep::Revert(subgraph_ptr)));
            }

            // Precondition: subgraph_ptr.number < head_ptr.number
            // Walk back to one block short of subgraph_ptr.number
            let offset = head_ptr.number - subgraph_ptr.number - 1;
            let head_ancestor_opt = ctx.chain_store.ancestor_block(head_ptr, offset).unwrap();
            let logger = self.logger.clone();
            match head_ancestor_opt {
                None => {
                    // Block is missing in the block store.
                    // This generally won't happen often, but can happen if the head ptr has
                    // been updated since we retrieved the head ptr, and the block store has
                    // been garbage collected.
                    // It's easiest to start over at this point.
                    Box::new(future::ok(ReconciliationStep::Retry))
                }
                Some(head_ancestor) => {
                    // We stopped one block short, so we'll compare the parent hash to the
                    // subgraph ptr.
                    if head_ancestor.block.parent_hash == subgraph_ptr.hash {
                        // The subgraph ptr is an ancestor of the head block.
                        // We cannot use an RPC call here to find the first interesting block
                        // due to the race conditions previously mentioned,
                        // so instead we will advance the subgraph ptr by one block.
                        // Note that head_ancestor is a child of subgraph_ptr.
                        let eth_adapter = self.eth_adapter.clone();

                        let block_with_calls = if !self.include_calls_in_blocks
                            || head_ancestor.transaction_receipts.is_empty()
                        {
                            Box::new(future::ok(EthereumBlockWithCalls {
                                ethereum_block: head_ancestor,
                                calls: vec![],
                            }))
                                as Box<dyn Future<Item = _, Error = _> + Send>
                        } else {
                            Box::new(
                                ctx.eth_adapter
                                    .calls_in_block(
                                        &logger,
                                        ctx.metrics.ethrpc_metrics.clone(),
                                        head_ancestor.block.number.unwrap().as_u64(),
                                        head_ancestor.block.hash.unwrap(),
                                    )
                                    .map(move |calls| EthereumBlockWithCalls {
                                        ethereum_block: head_ancestor,
                                        calls,
                                    }),
                            )
                        };

                        Box::new(
                            block_with_calls
                                .and_then(move |block| {
                                    triggers_in_block(
                                        eth_adapter,
                                        logger,
                                        ctx.chain_store.clone(),
                                        ctx.metrics.ethrpc_metrics.clone(),
                                        log_filter.clone(),
                                        call_filter.clone(),
                                        block_filter.clone(),
                                        BlockFinality::NonFinal(block),
                                    )
                                    .boxed()
                                    .compat()
                                })
                                .map(move |block| {
                                    ReconciliationStep::ProcessDescendantBlocks(vec![block], 1)
                                }),
                        )
                    } else {
                        // The subgraph ptr is not on the main chain.
                        // We will need to step back (possibly repeatedly) one block at a time
                        // until we are back on the main chain.
                        Box::new(future::ok(ReconciliationStep::Revert(subgraph_ptr)))
                    }
                }
            }
        }
    }

    /// Set subgraph deployment entity synced flag if and only if the subgraph block pointer is
    /// caught up to the head block pointer.
    fn update_subgraph_synced_status(&self) -> Result<(), Error> {
        let head_ptr_opt = self.chain_store.chain_head_ptr()?;
        let subgraph_ptr = self.subgraph_store.block_ptr(&self.subgraph_id)?;

        if head_ptr_opt != subgraph_ptr || head_ptr_opt.is_none() || subgraph_ptr.is_none() {
            // Not synced yet
            Ok(())
        } else {
            // Synced

            // Stop recording time-to-sync metrics.
            self.metrics.stopwatch.disable();

            self.subgraph_store.deployment_synced(&self.subgraph_id)
        }
    }
}

impl<S: SubgraphStore, C: ChainStore> BlockStreamTrait for BlockStream<S, C> {}

impl<S: SubgraphStore, C: ChainStore> Stream for BlockStream<S, C> {
    type Item = BlockStreamEvent;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut state = BlockStreamState::Transition;
        mem::swap(&mut self.state, &mut state);

        let result = loop {
            match state {
                BlockStreamState::BeginReconciliation => {
                    // Start the reconciliation process by asking for blocks
                    state = BlockStreamState::Reconciliation(self.ctx.next_blocks());
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
                                info!(
                                    self.ctx.logger,
                                    "Maximum range size reduced to {}",
                                    self.ctx.max_block_range_size
                                );
                            }
                            self.consecutive_err_count = 0;

                            let total_triggers =
                                next_blocks.iter().map(|b| b.triggers.len()).sum::<usize>();
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
                            break Ok(Async::Ready(Some(BlockStreamEvent::Block(next_block))));
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

pub struct BlockStreamBuilder<S, B, M> {
    subgraph_store: Arc<S>,
    block_store: Arc<B>,
    eth_networks: EthereumNetworks,
    node_id: NodeId,
    reorg_threshold: u64,
    metrics_registry: Arc<M>,
}

impl<S, B, M> Clone for BlockStreamBuilder<S, B, M> {
    fn clone(&self) -> Self {
        BlockStreamBuilder {
            subgraph_store: self.subgraph_store.clone(),
            block_store: self.block_store.clone(),
            eth_networks: self.eth_networks.clone(),
            node_id: self.node_id.clone(),
            reorg_threshold: self.reorg_threshold,
            metrics_registry: self.metrics_registry.clone(),
        }
    }
}

impl<S, B, M> BlockStreamBuilder<S, B, M>
where
    S: SubgraphStore,
    B: BlockStore,
    M: MetricsRegistry,
{
    pub fn new(
        subgraph_store: Arc<S>,
        block_store: Arc<B>,
        eth_networks: EthereumNetworks,
        node_id: NodeId,
        reorg_threshold: u64,
        metrics_registry: Arc<M>,
    ) -> Self {
        BlockStreamBuilder {
            subgraph_store,
            block_store,
            eth_networks,
            node_id,
            reorg_threshold,
            metrics_registry,
        }
    }
}

impl<S, B, M> BlockStreamBuilderTrait for BlockStreamBuilder<S, B, M>
where
    S: SubgraphStore,
    B: BlockStore,
    M: MetricsRegistry,
{
    type Stream = BlockStream<S, B::ChainStore>;

    fn build(
        &self,
        logger: Logger,
        deployment_id: SubgraphDeploymentId,
        network_name: String,
        start_blocks: Vec<u64>,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        include_calls_in_blocks: bool,
        metrics: Arc<BlockStreamMetrics>,
    ) -> Self::Stream {
        let logger = logger.new(o!(
            "component" => "BlockStream",
        ));

        let chain_store = self
            .block_store
            .chain_store(&network_name)
            .expect(&format!(
                "no store that supports network: {}",
                &network_name
            ))
            .clone();

        let requirements = NodeCapabilities {
            archive: false,
            traces: include_calls_in_blocks,
        };

        let eth_adapter = self
            .eth_networks
            .adapter_with_capabilities(network_name.clone(), &requirements)
            .expect(&format!(
                "no eth adapter that supports network: {} with {}",
                &network_name, &requirements
            ));

        // Create the actual subgraph-specific block stream
        BlockStream::new(
            self.subgraph_store.clone(),
            chain_store,
            eth_adapter.clone(),
            self.node_id.clone(),
            deployment_id,
            log_filter,
            call_filter,
            block_filter,
            start_blocks,
            include_calls_in_blocks,
            self.reorg_threshold,
            logger,
            metrics,
        )
    }
}

// This always returns `false` in a normal build. A test may configure reorg by enabling
// "test_reorg" fail point with the number of the block that should be reorged.
#[cfg(debug_assertions)]
#[allow(unused_variables)]
fn test_reorg(ptr: EthereumBlockPointer) -> bool {
    fail_point!("test_reorg", |reorg_at| {
        use std::str::FromStr;

        static REORGED: std::sync::Once = std::sync::Once::new();

        if REORGED.is_completed() {
            return false;
        }
        let reorg_at = u64::from_str(&reorg_at.unwrap()).unwrap();
        let should_reorg = ptr.number == reorg_at;
        if should_reorg {
            REORGED.call_once(|| {})
        }
        should_reorg
    });

    false
}
