use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use graph::components::forward;
use graph::prelude::{
    BlockStream as BlockStreamTrait, BlockStreamBuilder as BlockStreamBuilderTrait, *,
};
use graph::util::ethereum::string_to_h256;
use graph::web3::types::*;

const REORG_THRESHOLD: u64 = 50;

enum BlockStreamState {
    /// The BlockStream is new and has not yet been polled.
    ///
    /// Valid next states: Reconciliation
    New,

    /// The BlockStream is reconciling the subgraph store state with the chain store state.
    ///
    /// Valid next states: YieldingBlocks, Idle
    Reconciliation(
        Box<
            Future<
                    Item = Option<Box<Stream<Item = EthereumBlock, Error = Error> + Send>>,
                    Error = Error,
                > + Send,
        >,
    ),

    /// The BlockStream is emitting blocks that must be processed in order to bring the subgraph
    /// store up to date with the chain store.
    ///
    /// Valid next states: Reconciliation
    YieldingBlocks(Box<Stream<Item = EthereumBlock, Error = Error> + Send>),

    /// The BlockStream has reconciled the subgraph store and chain store states.
    /// No more work is needed until a chain head update.
    ///
    /// Valid next states: Reconciliation
    Idle,

    /// Not a real state, only used when going from one state to another.
    Transition,
}

/// A single next step to take in reconciling the state of the subgraph store with the state of the
/// chain store.
enum ReconciliationStep {
    /// Revert the current block pointed at by the subgraph pointer.
    RevertBlock(EthereumBlockPointer),

    /// Move forwards, processing one or more blocks.
    ProcessDescendantBlocks {
        from: EthereumBlockPointer,
        descendant_blocks: Vec<EthereumBlock>,
    },

    /// Skip forwards from `from` to `to`, with no processing needed.
    AdvanceToDescendantBlock {
        from: EthereumBlockPointer,
        to: EthereumBlockPointer,
    },

    /// This step is a no-op, but we need to check again for a next step.
    Retry,

    /// Subgraph pointer now matches chain head pointer.
    /// Reconciliation is complete.
    Done,
}

/// The result of performing a single ReconciliationStep.
enum ReconciliationStepOutcome {
    /// These blocks must be processed before reconciliation can continue.
    YieldBlocks(Box<Stream<Item = EthereumBlock, Error = Error> + Send>),

    /// Continue to the next reconciliation step.
    MoreSteps,

    /// Subgraph pointer now matches chain head pointer.
    /// Reconciliation is complete.
    Done,
}

struct BlockStreamContext<S, C, E> {
    subgraph_store: Arc<S>,
    chain_store: Arc<C>,
    eth_adapter: Arc<E>,
    subgraph_name: String,
    subgraph_id: String,
    logger: Logger,
}

impl<S, C, E> Clone for BlockStreamContext<S, C, E> {
    fn clone(&self) -> Self {
        Self {
            subgraph_store: self.subgraph_store.clone(),
            chain_store: self.chain_store.clone(),
            eth_adapter: self.eth_adapter.clone(),
            subgraph_name: self.subgraph_name.clone(),
            subgraph_id: self.subgraph_id.clone(),
            logger: self.logger.clone(),
        }
    }
}

pub struct BlockStream<S, C, E> {
    state: Mutex<BlockStreamState>,
    consecutive_err_count: u32,
    log_filter: EthereumLogFilter,
    chain_head_update_sink: Sender<ChainHeadUpdate>,
    chain_head_update_stream: Receiver<ChainHeadUpdate>,
    ctx: BlockStreamContext<S, C, E>,
}

impl<S, C, E> BlockStream<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
    pub fn new(
        subgraph_store: Arc<S>,
        chain_store: Arc<C>,
        eth_adapter: Arc<E>,
        subgraph_name: String,
        subgraph_id: String,
        log_filter: EthereumLogFilter,
        logger: Logger,
    ) -> Self {
        let logger = logger.new(o!(
            "component" => "BlockStream",
            "subgraph_name" => format!("{}", subgraph_name),
            "subgraph_id" => format!("{}", subgraph_id),
        ));

        let (chain_head_update_sink, chain_head_update_stream) = channel(100);

        BlockStream {
            state: Mutex::new(BlockStreamState::New),
            consecutive_err_count: 0,
            log_filter,
            chain_head_update_sink,
            chain_head_update_stream,
            ctx: BlockStreamContext {
                subgraph_store,
                chain_store,
                eth_adapter,
                subgraph_name,
                subgraph_id,
                logger,
            },
        }
    }
}

impl<S, C, E> BlockStreamContext<S, C, E>
where
    S: Store + 'static,
    C: ChainStore + 'static,
    E: EthereumAdapter,
{
    fn next_blocks(
        &self,
        log_filter: EthereumLogFilter,
    ) -> Box<
        Future<
                Item = Option<Box<Stream<Item = EthereumBlock, Error = Error> + Send>>,
                Error = Error,
            > + Send,
    > {
        let ctx = self.clone();

        // Perform reconciliation steps until there are blocks to yield or we are up-to-date.
        Box::new(future::loop_fn((), move |()| {
            let ctx = ctx.clone();

            // Determine the next step.
            ctx.get_next_step(log_filter.clone())

                // Do the next step.
                .and_then(move |step| ctx.do_step(step))

                // Check outcome.
                // Exit loop if done or there are blocks to process.
                .map(|outcome| {
                    match outcome {
                        ReconciliationStepOutcome::YieldBlocks(next_blocks) => {
                            future::Loop::Break(Some(next_blocks))
                        },
                        ReconciliationStepOutcome::MoreSteps => {
                            future::Loop::Continue(())
                        },
                        ReconciliationStepOutcome::Done => {
                            future::Loop::Break(None)
                        },
                    }
                })
        }))
    }

    fn get_next_step(
        &self,
        log_filter: EthereumLogFilter,
    ) -> impl Future<Item = ReconciliationStep, Error = Error> + Send {
        let ctx = self.clone();

        debug!(ctx.logger, "Identify next step");

        // Get pointers from database for comparison
        let head_ptr_opt = ctx.chain_store.chain_head_ptr().unwrap();
        let subgraph_ptr = ctx
            .subgraph_store
            .block_ptr(ctx.subgraph_id.clone())
            .unwrap();

        // If chain head ptr is not set yet
        if head_ptr_opt.is_none() {
            // Don't do any reconciliation until the chain store has more blocks
            return Box::new(future::ok(ReconciliationStep::Done))
                as Box<Future<Item = _, Error = _> + Send>;
        }

        let head_ptr = head_ptr_opt.unwrap();

        debug!(
            ctx.logger, "Chain head pointer";
            "hash" => format!("{:?}", head_ptr.hash),
            "number" => &head_ptr.number
        );
        debug!(
            ctx.logger, "Subgraph pointer";
            "hash" => format!("{:?}", subgraph_ptr.hash),
            "number" => &subgraph_ptr.number
        );

        // Only continue if the subgraph block ptr is behind the head block ptr.
        // subgraph_ptr > head_ptr shouldn't happen, but if it does, it's safest to just stop.
        if subgraph_ptr.number >= head_ptr.number {
            return Box::new(future::ok(ReconciliationStep::Done))
                as Box<Future<Item = _, Error = _> + Send>;
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
        // We'll define "really far" to mean "greater than REORG_THRESHOLD blocks".
        //
        // If the subgraph ptr is not too far behind the head ptr (i.e. less than
        // REORG_THRESHOLD blocks behind), then we have to allow for the possibility that the
        // block might be on the main chain now, but might become uncled in the future.
        //
        // Most importantly: Our ability to make this assumption (or not) will determine what
        // Ethereum RPC calls can give us accurate data without race conditions.
        // (This is mostly due to some unfortunate API design decisions on the Ethereum side)
        if (head_ptr.number - subgraph_ptr.number) > REORG_THRESHOLD {
            // Since we are beyond the reorg threshold, the Ethereum node knows what block has
            // been permanently assigned this block number.
            // This allows us to ask the node: does subgraph_ptr point to a block that was
            // permanently accepted into the main chain, or does it point to a block that was
            // uncled?
            Box::new(ctx.eth_adapter
                .is_on_main_chain(subgraph_ptr)
                .and_then(move |is_on_main_chain| -> Box<Future<Item = _, Error = _> + Send> {
                    if is_on_main_chain {
                        // The subgraph ptr points to a block on the main chain.
                        // This means that the last block we processed does not need to be
                        // reverted.
                        // Therefore, our direction of travel will be forward, towards the
                        // chain head.

                        // As an optimization, instead of advancing one block, we will use an
                        // Ethereum RPC call to find the first few blocks between the subgraph
                        // ptr and the reorg threshold that has event(s) we are interested in.
                        // Note that we use block numbers here.
                        // This is an artifact of Ethereum RPC limitations.
                        // It is only safe to use block numbers because we are beyond the reorg
                        // threshold.

                        // Start with first block after subgraph ptr
                        let from = subgraph_ptr.number + 1;

                        // End just prior to reorg threshold.
                        // It isn't safe to go any farther due to race conditions.
                        let to = head_ptr.number - REORG_THRESHOLD;

                        debug!(ctx.logger, "Finding next blocks with relevant events...");
                        Box::new(
                        ctx.eth_adapter
                            .find_first_blocks_with_logs(from, to, log_filter.clone())
                            .and_then(move |descendant_ptrs| -> Box<Future<Item = _, Error = _> + Send> {
                                debug!(ctx.logger, "Done finding next blocks.");

                                if descendant_ptrs.is_empty() {
                                    // No matching events in range.
                                    // Therefore, we can update the subgraph ptr without any
                                    // changes to the entity data.

                                    // We need to look up what block hash corresponds to the
                                    // block number `to`.
                                    // Again, this is only safe from race conditions due to
                                    // being beyond the reorg threshold.
                                    Box::new(ctx.eth_adapter
                                        .block_hash_by_block_number(to)
                                        .and_then(move |to_block_hash_opt| {
                                            to_block_hash_opt
                                                .ok_or_else(|| {
                                                    format_err!("Ethereum node could not find block with number {}", to)
                                                })
                                                .map(|to_block_hash| {
                                                    ReconciliationStep::AdvanceToDescendantBlock {
                                                        from: subgraph_ptr,
                                                        to: (to_block_hash, to).into(),
                                                    }
                                                })
                                        }))
                                } else {
                                    // The next few interesting blocks are at descendant_ptrs.
                                    // In particular, descendant_ptrs is a list of all blocks
                                    // between subgraph_ptr and descendant_ptrs.last() that
                                    // contain relevant events.
                                    // This will allow us to advance the subgraph_ptr to
                                    // descendant_ptrs.last() while being confident that we did
                                    // not miss any relevant events.

                                    // Load the blocks
                                    debug!(
                                        ctx.logger,
                                        "Found {} block(s) with events. Loading blocks...",
                                        descendant_ptrs.len()
                                    );
                                    Box::new(stream::futures_ordered(
                                        descendant_ptrs.into_iter().map(|descendant_ptr| {
                                            ctx.load_block(descendant_ptr.hash)
                                        }),
                                    ).collect()
                                        .map(move |descendant_blocks| {
                                            // Proceed to those blocks
                                            ReconciliationStep::ProcessDescendantBlocks {
                                                from: subgraph_ptr,
                                                descendant_blocks,
                                            }
                                        })
                                    )
                                }
                            })
                        )
                    } else {
                        // The subgraph ptr points to a block that was uncled.
                        // We need to revert this block.
                        Box::new(future::ok(ReconciliationStep::RevertBlock(subgraph_ptr)))
                    }
                })
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
            // the head block and at least its REORG_THRESHOLD most recent ancestors will be
            // present in the block store.
            // This allows us to work locally in the block store instead of relying on
            // Ethereum RPC calls, so that we are not subject to the limitations of the RPC
            // API.

            // To determine the step direction, we need to find out if the subgraph ptr refers
            // to a block that is an ancestor of the head block.
            // We can do so by walking back up the chain from the head block to the appropriate
            // block number, and checking to see if the block we found matches the
            // subgraph_ptr.

            // Precondition: subgraph_ptr.number < head_ptr.number
            // Walk back to one block short of subgraph_ptr.number
            let offset = head_ptr.number - subgraph_ptr.number - 1;
            let head_ancestor_opt = ctx.chain_store.ancestor_block(head_ptr, offset).unwrap();
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
                        Box::new(future::ok(ReconciliationStep::ProcessDescendantBlocks {
                            from: subgraph_ptr,
                            descendant_blocks: vec![head_ancestor],
                        }))
                    } else {
                        // The subgraph ptr is not on the main chain.
                        // We will need to step back (possibly repeatedly) one block at a time
                        // until we are back on the main chain.
                        Box::new(future::ok(ReconciliationStep::RevertBlock(subgraph_ptr)))
                    }
                }
            }
        }
    }

    fn do_step(
        &self,
        step: ReconciliationStep,
    ) -> Box<Future<Item = ReconciliationStepOutcome, Error = Error> + Send> {
        let ctx = self.clone();

        // We now know where to take the subgraph ptr.
        match step {
            ReconciliationStep::Retry => Box::new(future::ok(ReconciliationStepOutcome::MoreSteps)),
            ReconciliationStep::Done => Box::new(future::ok(ReconciliationStepOutcome::Done)),
            ReconciliationStep::RevertBlock(subgraph_ptr) => {
                // We would like to move to the parent of the current block.
                // This means we need to revert this block.

                // First, load the block in order to get the parent hash.
                Box::new(ctx.load_block(subgraph_ptr.hash).and_then(move |block| {
                    debug!(
                        ctx.logger,
                        "Reverting block to get back to main chain";
                        "block_number" => format!("{}", block.block.number.unwrap()),
                        "block_hash" => format!("{}", block.block.hash.unwrap())
                    );

                    // Produce pointer to parent block (using parent hash).
                    let parent_ptr = EthereumBlockPointer::to_parent(&block);

                    // Revert entity changes from this block, and update subgraph ptr.
                    future::result(ctx.subgraph_store
                        .revert_block_operations(ctx.subgraph_id.clone(), subgraph_ptr, parent_ptr)
                        .map_err(Error::from)
                        .map(|()| {
                            // At this point, the loop repeats, and we try to move the subgraph ptr another
                            // step in the right direction.
                            ReconciliationStepOutcome::MoreSteps
                        })
                    )
                }))
            }
            ReconciliationStep::AdvanceToDescendantBlock { from, to } => {
                debug!(
                    ctx.logger,
                    "Skipping {} block(s) with no relevant events...",
                    to.number - from.number
                );

                Box::new(
                    future::result(ctx.subgraph_store.set_block_ptr_with_no_changes(
                        ctx.subgraph_id.clone(),
                        from,
                        to,
                    )).map(|()| ReconciliationStepOutcome::MoreSteps),
                )
            }
            ReconciliationStep::ProcessDescendantBlocks {
                from,
                descendant_blocks,
            } => {
                let descendant_block_count = descendant_blocks.len();
                debug!(
                    ctx.logger,
                    "Processing {} block(s) in block range {}-{}...",
                    descendant_block_count,
                    from.number + 1,
                    descendant_blocks.last().unwrap().block.number.unwrap()
                );

                let mut subgraph_ptr = from;

                // Advance the subgraph ptr to each of the specified descendants and yield each
                // block with relevant events.
                Box::new(future::ok(ReconciliationStepOutcome::YieldBlocks(
                    Box::new(stream::iter_ok(descendant_blocks.into_iter()).map(
                        move |descendant_block| {
                            // First, check if there are blocks between subgraph_ptr and
                            // descendant_block.
                            let descendant_parent_ptr =
                                EthereumBlockPointer::to_parent(&descendant_block);
                            if subgraph_ptr != descendant_parent_ptr {
                                // descendant_block is not a direct child.
                                // Therefore, there are blocks that are irrelevant to this subgraph
                                // that we can skip.

                                debug!(
                                    ctx.logger,
                                    "Skipping {} block(s) with no relevant events...",
                                    descendant_parent_ptr.number - subgraph_ptr.number
                                );

                                // Update subgraph_ptr in store to skip the irrelevant blocks.
                                ctx.subgraph_store
                                    .set_block_ptr_with_no_changes(
                                        ctx.subgraph_id.clone(),
                                        subgraph_ptr,
                                        descendant_parent_ptr,
                                    ).unwrap();
                            }

                            // Update our copy of the subgraph ptr to reflect the
                            // value it will have after descendant_block is
                            // processed.
                            subgraph_ptr = (&descendant_block).into();

                            descendant_block
                        },
                    )) as Box<Stream<Item = _, Error = _> + Send>,
                )))
            }
        }
    }

    fn load_block(
        &self,
        block_hash: H256,
    ) -> impl Future<Item = EthereumBlock, Error = Error> + Send {
        let ctx = self.clone();

        // Check store first
        future::result(ctx.chain_store.block(block_hash)).and_then(
            move |local_block_opt| -> Box<Future<Item = _, Error = _> + Send> {
                // If found in store
                if let Some(local_block) = local_block_opt {
                    Box::new(future::ok(local_block))
                } else {
                    // Request from Ethereum node instead
                    Box::new(
                        ctx.eth_adapter
                            .block_by_hash(block_hash)
                            .and_then(move |block_opt| {
                                block_opt.ok_or_else(move || {
                                    format_err!(
                                        "Ethereum node could not find block with hash {}",
                                        block_hash
                                    )
                                })
                            }).and_then(move |block| {
                                // Cache in store for later
                                ctx.chain_store
                                    .upsert_blocks(stream::once(Ok(block.clone())))
                                    .map(move |()| block)
                            }),
                    )
                }
            },
        )
    }
}

impl<S, C, E> BlockStreamTrait for BlockStream<S, C, E>
where
    S: Store + 'static,
    C: ChainStore + 'static,
    E: EthereumAdapter,
{}

impl<S, C, E> Stream for BlockStream<S, C, E>
where
    S: Store + 'static,
    C: ChainStore + 'static,
    E: EthereumAdapter,
{
    type Item = EthereumBlock;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut state_lock = self.state.lock().unwrap();

        let mut state = BlockStreamState::Transition;
        mem::swap(&mut *state_lock, &mut state);

        let poll = loop {
            match state {
                // First time being polled
                BlockStreamState::New => {
                    // Start the reconciliation process by asking for blocks
                    let next_blocks_future = self.ctx.next_blocks(self.log_filter.clone());
                    state = BlockStreamState::Reconciliation(next_blocks_future);

                    // Poll the next_blocks() future
                    continue;
                }

                // Waiting for the reconciliation to complete or yield blocks
                BlockStreamState::Reconciliation(mut next_blocks_future) => {
                    match next_blocks_future.poll() {
                        // Reconciliation found blocks to process
                        Ok(Async::Ready(Some(next_blocks))) => {
                            // Switch to yielding state until next_blocks is depleted
                            state = BlockStreamState::YieldingBlocks(next_blocks);

                            // Yield the first block in next_blocks
                            continue;
                        }

                        // Reconciliation completed. We're caught up to chain head.
                        Ok(Async::Ready(None)) => {
                            // Reset error count
                            self.consecutive_err_count = 0;

                            // Switch to idle
                            state = BlockStreamState::Idle;

                            // Poll for chain head update
                            continue;
                        }

                        Ok(Async::NotReady) => {
                            // Nothing to change or yield yet.
                            state = BlockStreamState::Reconciliation(next_blocks_future);
                            break Async::NotReady;
                        }

                        Err(e) => {
                            self.consecutive_err_count += 1;

                            // If too many errors without progress, give up
                            if self.consecutive_err_count >= 10 {
                                return Err(e);
                            }

                            warn!(
                                self.ctx.logger,
                                "Trying again after error in block stream reconcile: {}", e
                            );

                            // Try again by restarting reconciliation
                            let next_blocks_future = self.ctx.next_blocks(self.log_filter.clone());
                            state = BlockStreamState::Reconciliation(next_blocks_future);

                            // Poll the next_blocks() future
                            continue;
                        }
                    }
                }

                // Yielding blocks from reconciliation process
                BlockStreamState::YieldingBlocks(mut next_blocks) => {
                    match next_blocks.poll() {
                        // Yield one block
                        Ok(Async::Ready(Some(next_block))) => {
                            state = BlockStreamState::YieldingBlocks(next_blocks);
                            break Async::Ready(Some(next_block));
                        }

                        // Done yielding blocks
                        Ok(Async::Ready(None)) => {
                            // Reset error count
                            self.consecutive_err_count = 0;

                            // Restart reconciliation until more blocks or done
                            let next_blocks_future = self.ctx.next_blocks(self.log_filter.clone());
                            state = BlockStreamState::Reconciliation(next_blocks_future);

                            // Poll the next_blocks() future
                            continue;
                        }

                        Ok(Async::NotReady) => {
                            // Nothing to change or yield yet
                            state = BlockStreamState::YieldingBlocks(next_blocks);
                            break Async::NotReady;
                        }

                        Err(e) => {
                            self.consecutive_err_count += 1;

                            // If too many errors without progress, give up
                            if self.consecutive_err_count >= 10 {
                                return Err(e);
                            }

                            warn!(
                                self.ctx.logger,
                                "Trying again after error yielding blocks to block stream: {}", e
                            );

                            // Try again by restarting reconciliation
                            let next_blocks_future = self.ctx.next_blocks(self.log_filter.clone());
                            state = BlockStreamState::Reconciliation(next_blocks_future);

                            // Poll the next_blocks() future
                            continue;
                        }
                    }
                }

                // Waiting for a chain head update
                BlockStreamState::Idle => {
                    match self.chain_head_update_stream.poll() {
                        // Chain head was updated
                        Ok(Async::Ready(Some(_chain_head_update))) => {
                            // Start reconciliation process
                            let next_blocks_future = self.ctx.next_blocks(self.log_filter.clone());
                            state = BlockStreamState::Reconciliation(next_blocks_future);

                            // Poll the next_blocks() future
                            continue;
                        }

                        // Chain head update stream ended
                        Ok(Async::Ready(None)) => {
                            // Should not happen
                            bail!("chain head update stream ended unexpectedly");
                        }

                        Ok(Async::NotReady) => {
                            // Stay idle
                            state = BlockStreamState::Idle;
                            break Async::NotReady;
                        }

                        // mpsc channel failed
                        Err(()) => {
                            // Should not happen
                            bail!("chain head update Receiver failed");
                        }
                    }
                }

                // This will only happen if this poll function fails to complete normally then is
                // called again.
                BlockStreamState::Transition => unreachable!(),
            }
        };

        mem::replace(&mut *state_lock, state);

        Ok(poll)
    }
}

impl<S, C, E> EventConsumer<ChainHeadUpdate> for BlockStream<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
    fn event_sink(&self) -> Box<Sink<SinkItem = ChainHeadUpdate, SinkError = ()> + Send> {
        let logger = self.ctx.logger.clone();

        Box::new(self.chain_head_update_sink.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}

pub struct BlockStreamBuilder<S, C, E> {
    subgraph_store: Arc<S>,
    chain_store: Arc<C>,
    eth_adapter: Arc<E>,
}

impl<S, C, E> Clone for BlockStreamBuilder<S, C, E> {
    fn clone(&self) -> Self {
        BlockStreamBuilder {
            subgraph_store: self.subgraph_store.clone(),
            chain_store: self.chain_store.clone(),
            eth_adapter: self.eth_adapter.clone(),
        }
    }
}

impl<S, C, E> BlockStreamBuilder<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
    pub fn new(subgraph_store: Arc<S>, chain_store: Arc<C>, eth_adapter: Arc<E>) -> Self {
        BlockStreamBuilder {
            subgraph_store,
            chain_store,
            eth_adapter,
        }
    }
}

impl<S, C, E> BlockStreamBuilderTrait for BlockStreamBuilder<S, C, E>
where
    S: Store + 'static,
    C: ChainStore + 'static,
    E: EthereumAdapter,
{
    type Stream = BlockStream<S, C, E>;

    fn from_subgraph(
        &self,
        name: String,
        manifest: &SubgraphManifest,
        logger: Logger,
    ) -> Self::Stream {
        // Add entry to subgraphs table in Store
        let genesis_block_ptr = self.chain_store.genesis_block_ptr().unwrap();
        self.subgraph_store
            .add_subgraph_if_missing(manifest.id.clone(), genesis_block_ptr)
            .unwrap();

        // Listen for chain head block updates
        let mut chain_head_update_listener = self.chain_store.chain_head_updates();

        // Create the actual subgraph-specific block stream
        let log_filter = create_log_filter_from_subgraph(manifest);
        let block_stream = BlockStream::new(
            self.subgraph_store.clone(),
            self.chain_store.clone(),
            self.eth_adapter.clone(),
            name,
            manifest.id.clone(),
            log_filter,
            logger,
        );

        // Forward chain head updates from the listener to the block stream
        tokio::spawn(forward(&mut chain_head_update_listener, &block_stream).unwrap());

        // Start listening for chain head updates
        chain_head_update_listener.start();

        // Leak the chain update listener; we'll terminate it by closing the
        // block stream's chain head update sink
        std::mem::forget(chain_head_update_listener);

        block_stream
    }
}

fn create_log_filter_from_subgraph(manifest: &SubgraphManifest) -> EthereumLogFilter {
    manifest
        .data_sources
        .iter()
        .flat_map(|data_source| {
            let contract_addr = data_source.source.address;
            data_source
                .mapping
                .event_handlers
                .iter()
                .map(move |event_handler| {
                    let event_sig = string_to_h256(&event_handler.event);
                    (contract_addr, event_sig)
                })
        }).collect::<EthereumLogFilter>()
}
