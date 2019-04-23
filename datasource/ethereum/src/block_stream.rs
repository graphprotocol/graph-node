use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::cmp;
use std::collections::{HashSet, HashMap};
use std::env;
use std::mem;
use std::sync::Mutex;
use std::cmp::Ordering;
use tiny_keccak::keccak256;

use graph::data::subgraph::schema::{
    SubgraphDeploymentEntity, SubgraphEntity, SubgraphVersionEntity,
};
use graph::prelude::{
    BlockStream as BlockStreamTrait, BlockStreamBuilder as BlockStreamBuilderTrait, *,
};
use graph::web3::types::*;

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
                    Item = Option<Box<Stream<Item = EthereumBlockWithTriggers, Error = Error> + Send>>,
                    Error = Error,
                > + Send,
        >,
    ),

    /// The BlockStream is emitting blocks that must be processed in order to bring the subgraph
    /// store up to date with the chain store.
    ///
    /// Valid next states: Reconciliation
    YieldingBlocks(Box<Stream<Item = EthereumBlockWithTriggers, Error = Error> + Send>),

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
        log_filter_opt: Option<EthereumLogFilter>,
        call_filter_opt: Option<EthereumCallFilter>,
        block_filter_opt: Option<EthereumBlockFilter>,
        descendant_blocks: Box<Stream<Item = EthereumBlockWithCalls, Error = Error> + Send>,
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
    YieldBlocks(Box<Stream<Item = EthereumBlockWithTriggers, Error = Error> + Send>),

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
    node_id: NodeId,
    subgraph_id: SubgraphDeploymentId,
    reorg_threshold: u64,
    logger: Logger,
    include_calls_in_blocks: bool,
}

impl<S, C, E> Clone for BlockStreamContext<S, C, E> {
    fn clone(&self) -> Self {
        Self {
            subgraph_store: self.subgraph_store.clone(),
            chain_store: self.chain_store.clone(),
            eth_adapter: self.eth_adapter.clone(),
            node_id: self.node_id.clone(),
            subgraph_id: self.subgraph_id.clone(),
            reorg_threshold: self.reorg_threshold,
            logger: self.logger.clone(),
            include_calls_in_blocks: self.include_calls_in_blocks.clone(),
        }
    }
}

pub struct BlockStream<S, C, E> {
    state: Mutex<BlockStreamState>,
    consecutive_err_count: u32,
    log_filter: Option<EthereumLogFilter>,
    call_filter: Option<EthereumCallFilter>,
    block_filter: Option<EthereumBlockFilter>,
    chain_head_update_sink: Sender<ChainHeadUpdate>,
    chain_head_update_stream: Receiver<ChainHeadUpdate>,
    _chain_head_update_guard: CancelGuard,
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
        chain_head_update_guard: CancelGuard,
        node_id: NodeId,
        subgraph_id: SubgraphDeploymentId,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        reorg_threshold: u64,
        logger: Logger,
    ) -> Self {
        let (chain_head_update_sink, chain_head_update_stream) = channel(100);

        let mut include_calls_in_blocks = false;
        if call_filter.is_some() {
            include_calls_in_blocks = true
        }
        if block_filter.is_some() && block_filter.clone().unwrap().contract_addresses.len() > 0 {
            include_calls_in_blocks = true
        }

        BlockStream {
            state: Mutex::new(BlockStreamState::New),
            consecutive_err_count: 0,
            log_filter,
            call_filter,
            block_filter,
            chain_head_update_sink,
            chain_head_update_stream,
            _chain_head_update_guard: chain_head_update_guard,
            ctx: BlockStreamContext {
                subgraph_store,
                chain_store,
                eth_adapter,
                node_id,
                subgraph_id,
                reorg_threshold,
                logger,
                include_calls_in_blocks,
            },
        }
    }
}

impl<S, C, E> BlockStreamContext<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
    /// Perform reconciliation steps until there are blocks to yield or we are up-to-date.
    fn next_blocks(
        &self,
        log_filter: Option<EthereumLogFilter>,
        call_filter: Option<EthereumCallFilter>,
        block_filter: Option<EthereumBlockFilter>,
    ) -> Box<
        Future<
                Item = Option<Box<Stream<Item = EthereumBlockWithTriggers, Error = Error> + Send>>,
                Error = Error,
            > + Send,
    > {
        let ctx = self.clone();

        Box::new(future::loop_fn((), move |()| {
            let ctx1 = ctx.clone();
            let ctx2 = ctx.clone();
            let ctx3 = ctx.clone();
            let log_filter = log_filter.clone();
            let call_filter = call_filter.clone();
            let block_filter = block_filter.clone();

            // Update progress metrics
            future::result(ctx1.update_subgraph_block_count())
                // Determine the next step.
                .and_then(move |()| ctx1.get_next_step(log_filter, call_filter, block_filter))
                // Do the next step.
                .and_then(move |step| ctx2.do_step(step))
                // Check outcome.
                // Exit loop if done or there are blocks to process.
                .and_then(move |outcome| match outcome {
                    ReconciliationStepOutcome::YieldBlocks(next_blocks) => {
                        Ok(future::Loop::Break(Some(next_blocks)))
                    }
                    ReconciliationStepOutcome::MoreSteps => Ok(future::Loop::Continue(())),
                    ReconciliationStepOutcome::Done => {
                        // Reconciliation is complete, so try to mark subgraph as Synced
                        ctx3.update_subgraph_synced_status()?;

                        Ok(future::Loop::Break(None))
                    }
                })
        }))
    }

    /// Determine the next reconciliation step. Does not modify Store or ChainStore.
    fn get_next_step(
        &self,
        log_filter: Option<EthereumLogFilter>,
        call_filter: Option<EthereumCallFilter>,
        block_filter: Option<EthereumBlockFilter>,
    ) -> impl Future<Item = ReconciliationStep, Error = Error> + Send {
        let ctx = self.clone();
        let reorg_threshold = ctx.reorg_threshold;

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

        trace!(
            ctx.logger, "Chain head pointer";
            "hash" => format!("{:?}", head_ptr.hash),
            "number" => &head_ptr.number
        );
        trace!(
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
        // We'll define "really far" to mean "greater than reorg_threshold blocks".
        //
        // If the subgraph ptr is not too far behind the head ptr (i.e. less than
        // reorg_threshold blocks behind), then we have to allow for the possibility that the
        // block might be on the main chain now, but might become uncled in the future.
        //
        // Most importantly: Our ability to make this assumption (or not) will determine what
        // Ethereum RPC calls can give us accurate data without race conditions.
        // (This is mostly due to some unfortunate API design decisions on the Ethereum side)
        if (head_ptr.number - subgraph_ptr.number) > reorg_threshold {
            // Since we are beyond the reorg threshold, the Ethereum node knows what block has
            // been permanently assigned this block number.
            // This allows us to ask the node: does subgraph_ptr point to a block that was
            // permanently accepted into the main chain, or does it point to a block that was
            // uncled?
            Box::new(ctx.eth_adapter
                .is_on_main_chain(&ctx.logger, subgraph_ptr)
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
                        let to_limit = head_ptr.number - reorg_threshold;

                        // But also avoid having too large a range to ensure subgraph block ptr is
                        // updated frequently.
                        let to = cmp::min(from + (100_000 - 1), to_limit);

                        debug!(ctx.logger, "Finding next blocks with relevant events...");
                        Box::new(
                            ctx.eth_adapter
                                .blocks_with_triggers(
                                    &ctx.logger,
                                    from,
                                    to,
                                    log_filter.clone(),
                                    call_filter.clone(),
                                    block_filter.clone(),
                                )
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
                                                 .block_hash_by_block_number(&ctx.logger, to)
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
                                            "Found {} block(s) with events.",
                                            descendant_ptrs.len()
                                        );
                                        let descendant_hashes = descendant_ptrs.into_iter().map(|ptr| ptr.hash).collect();
                                        Box::new(future::ok(
                                            // Proceed to those blocks
                                            ReconciliationStep::ProcessDescendantBlocks {
                                                from: subgraph_ptr,
                                                log_filter_opt: log_filter.clone(),
                                                call_filter_opt: call_filter.clone(),
                                                block_filter_opt: block_filter.clone(),
                                                descendant_blocks: Box::new(ctx.load_blocks(descendant_hashes)),
                                            }
                                        ))
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

            // Precondition: subgraph_ptr.number < head_ptr.number
            // Walk back to one block short of subgraph_ptr.number
            let offset = head_ptr.number - subgraph_ptr.number - 1;
            let head_ancestor_opt = ctx.chain_store.ancestor_block(head_ptr, offset)
                .unwrap()
                .and_then(move |block| {
                    Some(EthereumBlockWithCalls {
                        ethereum_block: block,
                        calls: None,
                    })
                });
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
                    if head_ancestor.ethereum_block.block.parent_hash == subgraph_ptr.hash {
                        // The subgraph ptr is an ancestor of the head block.
                        // We cannot use an RPC call here to find the first interesting block
                        // due to the race conditions previously mentioned,
                        // so instead we will advance the subgraph ptr by one block.
                        // Note that head_ancestor is a child of subgraph_ptr.
                        Box::new(future::ok(ReconciliationStep::ProcessDescendantBlocks {
                            from: subgraph_ptr,
                            log_filter_opt: log_filter.clone(),
                            call_filter_opt: call_filter.clone(),
                            block_filter_opt: block_filter.clone(),
                            descendant_blocks: Box::new(stream::once(Ok(head_ancestor))),
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

    /// Perform a reconciliation step.
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
                        "block_number" => format!("{}", block.ethereum_block.block.number.unwrap()),
                        "block_hash" => format!("{}", block.ethereum_block.block.hash.unwrap())
                    );

                    // Produce pointer to parent block (using parent hash).
                    let parent_ptr = EthereumBlockPointer::to_parent(&block.ethereum_block);

                    // Revert entity changes from this block, and update subgraph ptr.
                    future::result(
                        ctx.subgraph_store
                            .revert_block_operations(
                                ctx.subgraph_id.clone(),
                                subgraph_ptr,
                                parent_ptr,
                            )
                            .map_err(Error::from)
                            .map(|()| {
                                // At this point, the loop repeats, and we try to move the subgraph ptr another
                                // step in the right direction.
                                ReconciliationStepOutcome::MoreSteps
                            }),
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
                    ))
                    .map(|()| ReconciliationStepOutcome::MoreSteps)
                    .map_err(|e| format_err!("Failed to skip blocks: {}", e)),
                )
            }
            ReconciliationStep::ProcessDescendantBlocks {
                from,
                log_filter_opt,
                call_filter_opt,
                block_filter_opt,
                descendant_blocks,
            } => {
                let mut subgraph_ptr = from;
                let log_filter_opt = log_filter_opt.clone();
                let call_filter_opt = call_filter_opt.clone();
                let block_filter_opt = block_filter_opt.clone();

                // Advance the subgraph ptr to each of the specified descendants and yield each
                // block with relevant events.
                Box::new(future::ok(ReconciliationStepOutcome::YieldBlocks(
                    Box::new(descendant_blocks
                             .and_then(move |descendant_block| {
                                 // First, check if there are blocks between subgraph_ptr and
                                 // descendant_block.
                                 let descendant_parent_ptr = EthereumBlockPointer::to_parent(&descendant_block.ethereum_block);
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
                                     ctx.subgraph_store.set_block_ptr_with_no_changes(
                                         ctx.subgraph_id.clone(),
                                         subgraph_ptr,
                                         descendant_parent_ptr,
                                     )?;
                                 }

                                 // Update our copy of the subgraph ptr to reflect the
                                 // value it will have after descendant_block is
                                 // processed.
                                 subgraph_ptr = (&descendant_block.ethereum_block).into();

                                 Ok(descendant_block)
                             })
                             .and_then(move |descendant_block| {
                                 let triggers = match parse_triggers(
                                     log_filter_opt.clone(),
                                     call_filter_opt.clone(),
                                     block_filter_opt.clone(),
                                     &descendant_block,
                                 ) {
                                     Ok(triggers) => triggers,
                                     Err(err) => return future::err(err)
                                 };
                                 future::ok(EthereumBlockWithTriggers {
                                     ethereum_block: descendant_block.ethereum_block,
                                     triggers: triggers,
                                 })
                             })) as Box<Stream<Item = _, Error = _> + Send>,
                )))
            }
        }
    }

    /// Set subgraph deployment entity synced flag if and only if the subgraph block pointer is
    /// caught up to the head block pointer.
    fn update_subgraph_synced_status(&self) -> Result<(), Error> {
        let head_ptr_opt = self.chain_store.chain_head_ptr()?;
        let subgraph_ptr = self.subgraph_store.block_ptr(self.subgraph_id.clone())?;

        if head_ptr_opt != Some(subgraph_ptr) {
            // Not synced yet
            Ok(())
        } else {
            // Synced
            let mut ops = vec![];

            // Set deployment synced flag
            ops.extend(SubgraphDeploymentEntity::update_synced_operations(
                &self.subgraph_id,
                true,
            ));

            // Find versions pointing to this deployment
            let versions = self
                .subgraph_store
                .find(SubgraphVersionEntity::query().filter(EntityFilter::Equal(
                    "deployment".to_owned(),
                    self.subgraph_id.to_string().into(),
                )))?;
            let version_ids = versions
                .iter()
                .map(|entity| entity.id().unwrap())
                .collect::<Vec<_>>();
            let version_id_values = version_ids.iter().map(Value::from).collect::<Vec<_>>();
            ops.push(EntityOperation::AbortUnless {
                description: "The same subgraph version entities must point to this deployment"
                    .to_owned(),
                query: SubgraphVersionEntity::query().filter(EntityFilter::Equal(
                    "deployment".to_owned(),
                    self.subgraph_id.to_string().into(),
                )),
                entity_ids: version_ids.clone(),
            });

            // Find subgraphs with one of these versions as pending version
            let subgraphs_to_update =
                self.subgraph_store
                    .find(SubgraphEntity::query().filter(EntityFilter::In(
                        "pendingVersion".to_owned(),
                        version_id_values.clone(),
                    )))?;
            let subgraph_ids_to_update = subgraphs_to_update
                .iter()
                .map(|entity| entity.id().unwrap())
                .collect();
            ops.push(EntityOperation::AbortUnless {
                description: "The same subgraph entities must have these versions pending"
                    .to_owned(),
                query: SubgraphEntity::query().filter(EntityFilter::In(
                    "pendingVersion".to_owned(),
                    version_id_values.clone(),
                )),
                entity_ids: subgraph_ids_to_update,
            });

            // The current versions of these subgraphs will no longer be current now that this
            // deployment is synced (they will be replaced by the pending version)
            let current_version_ids = subgraphs_to_update
                .iter()
                .filter_map(|subgraph| match subgraph.get("currentVersion") {
                    Some(Value::String(id)) => Some(id.to_owned()),
                    Some(Value::Null) => None,
                    Some(_) => panic!("subgraph entity has invalid value type in currentVersion"),
                    None => None,
                })
                .collect::<Vec<_>>();
            let current_versions = self.subgraph_store.find(
                SubgraphVersionEntity::query()
                    .filter(EntityFilter::new_in("id", current_version_ids.clone())),
            )?;

            // These versions becoming non-current might mean that some assignments are no longer
            // needed. Get a list of deployment IDs that are affected by marking these versions as
            // non-current.
            let subgraph_hashes_affected = current_versions
                .iter()
                .map(|version| {
                    SubgraphDeploymentId::new(
                        version
                            .get("deployment")
                            .unwrap()
                            .to_owned()
                            .as_string()
                            .unwrap(),
                    )
                    .unwrap()
                })
                .collect::<HashSet<_>>();

            // Read version summaries for these subgraph hashes
            let (versions_before, read_summary_ops) = self
                .subgraph_store
                .read_subgraph_version_summaries(subgraph_hashes_affected.into_iter().collect())?;
            ops.extend(read_summary_ops);

            // Simulate demoting existing current versions to non-current
            let versions_after = versions_before
                .clone()
                .into_iter()
                .map(|mut version| {
                    if current_version_ids.contains(&version.id) {
                        version.current = false;
                    }
                    version
                })
                .collect::<Vec<_>>();

            // Apply changes to assignments
            ops.extend(self.subgraph_store.reconcile_assignments(
                &self.logger,
                versions_before,
                versions_after,
                None, // no new assignments will be added
            ));

            // Update subgraph entities to promote pending versions to current
            for subgraph in subgraphs_to_update {
                let mut data = Entity::new();
                data.set("id", subgraph.id().unwrap());
                data.set("pendingVersion", Value::Null);
                data.set(
                    "currentVersion",
                    subgraph.get("pendingVersion").unwrap().to_owned(),
                );
                ops.push(EntityOperation::Set {
                    key: SubgraphEntity::key(subgraph.id().unwrap()),
                    data,
                });
            }

            self.subgraph_store
                .apply_entity_operations(ops, EventSource::None)
                .map_err(|e| format_err!("Failed to set deployment synced flag: {}", e))
        }
    }

    /// Write latest block counts into subgraph entity based on current value of head and subgraph
    /// block pointers.
    fn update_subgraph_block_count(&self) -> Result<(), Error> {
        let head_ptr_opt = self.chain_store.chain_head_ptr()?;

        match head_ptr_opt {
            None => Ok(()),
            Some(head_ptr) => {
                let ops = SubgraphDeploymentEntity::update_ethereum_blocks_count_operations(
                    &self.subgraph_id,
                    head_ptr.number,
                );
                self.subgraph_store
                    .apply_entity_operations(ops, EventSource::None)
                    .map_err(|e| format_err!("Failed to set subgraph block count: {}", e))
            }
        }
    }

    /// Load Ethereum blocks in bulk, returning results as they come back as a Stream.
    fn load_blocks(
        &self,
        block_hashes: Vec<H256>,
    ) -> impl Stream<Item = EthereumBlockWithCalls, Error = Error> + Send {
        let ctx = self.clone();

        let block_batch_size: usize = env::var_os("ETHEREUM_BLOCK_BATCH_SIZE")
            .map(|s| s.to_str().unwrap().parse().unwrap())
            .unwrap_or(50);

        let block_hashes_batches = block_hashes
            .chunks(block_batch_size) // maximum batch size
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();

        // Return a stream that lazily loads batches of blocks
        stream::iter_ok::<_, Error>(block_hashes_batches)
            .map(move |block_hashes_batch| {
                debug!(
                    ctx.logger,
                    "Requesting {} block(s) in parallel...",
                    block_hashes_batch.len()
                );

                // Start loading all blocks in this batch
                let block_futures = block_hashes_batch
                    .into_iter()
                    .map(|block_hash| ctx.load_block(block_hash));

                stream::futures_ordered(block_futures)
            })
            .flatten()
    }

    fn load_block(
        &self,
        block_hash: H256,
    ) -> impl Future<Item = EthereumBlockWithCalls, Error = Error> + Send {
        let ctx = self.clone();
        let eth = self.eth_adapter.clone();
        let logger = self.logger.clone();
        let include_calls_in_block = self.include_calls_in_blocks;
        
        // Search for the block in the store first then use the ethereum adapter as a backup
        let block = future::result(ctx.chain_store.block(block_hash))
            .and_then(move |local_block_opt| -> Box<Future<Item = _, Error = _> + Send> {
                match local_block_opt {
                    Some(block) => Box::new(future::ok(block)),
                    None => {
                        let logger = ctx.logger.clone();
                        let ctx_1 = ctx.clone();
                        let ctx_2 = ctx_1.clone();
                        Box::new(ctx_1.eth_adapter
                            .block_by_hash(&logger, block_hash)
                            .and_then(move |block_opt| {
                                block_opt.ok_or_else(move || {
                                    format_err!("Ethereum node could not find block with hash {}", block_hash)
                                })
                            })
                            .and_then(move |block| {
                                ctx_1
                                    .eth_adapter
                                    .load_full_block(&logger, block)
                                    .map_err(|e| format_err!("Error loading full block: {}", e))
                            })
                            .and_then(move |block| {
                                // Cache in store for later
                                ctx_2
                                    .chain_store
                                    .upsert_blocks(stream::once(Ok(block.clone())))
                                    .map(move |()| block)
                            }))
                    }
                }
            })
            .and_then(move |block| -> Box<Future<Item = _, Error = _> + Send>{
                if !include_calls_in_block {
                    return Box::new(future::ok(EthereumBlockWithCalls {
                        ethereum_block: block,
                        calls: None,
                    }))
                }
                let calls = eth.calls_in_block(
                    &logger,
                    block.block.number.unwrap().as_u64(),
                    block.block.hash.unwrap(),
                ).and_then(move |calls| {
                    future::ok(EthereumBlockWithCalls {
                        ethereum_block: block,
                        calls: Some(calls),
                    })
                });
                Box::new(calls)
            });
        Box::new(block)
    }
}

impl<S, C, E> BlockStreamTrait for BlockStream<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
}

impl<S, C, E> Stream for BlockStream<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
    type Item = EthereumBlockWithTriggers;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Lock Mutex to perform a state transition
        let mut state_lock = self.state.lock().unwrap();

        let mut state = BlockStreamState::Transition;
        mem::swap(&mut *state_lock, &mut state);

        let poll = loop {
            match state {
                // First time being polled
                BlockStreamState::New => {
                    // Start the reconciliation process by asking for blocks
                    let next_blocks_future = self.ctx.next_blocks(
                        self.log_filter.clone(),
                        self.call_filter.clone(),
                        self.block_filter.clone(),
                    );
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
                            if self.consecutive_err_count >= 100 {
                                return Err(e);
                            }

                            warn!(
                                self.ctx.logger,
                                "Trying again after error in block stream reconcile: {}", e
                            );

                            // Try again by restarting reconciliation
                            let next_blocks_future = self.ctx.next_blocks(
                                self.log_filter.clone(),
                                self.call_filter.clone(),
                                self.block_filter.clone(),
                            );
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
                            let next_blocks_future = self.ctx.next_blocks(
                                self.log_filter.clone(),
                                self.call_filter.clone(),
                                self.block_filter.clone(),
                            );
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
                            if self.consecutive_err_count >= 100 {
                                return Err(e);
                            }

                            warn!(
                                self.ctx.logger,
                                "Trying again after error yielding blocks to block stream: {}", e
                            );

                            // Try again by restarting reconciliation
                            let next_blocks_future = self.ctx.next_blocks(
                                self.log_filter.clone(),
                                self.call_filter.clone(),
                                self.block_filter.clone(),
                            );
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
                            let next_blocks_future = self.ctx.next_blocks(
                                self.log_filter.clone(),
                                self.call_filter.clone(),
                                self.block_filter.clone(),
                            );
                            state = BlockStreamState::Reconciliation(next_blocks_future);

                            // Poll the next_blocks() future
                            continue;
                        }

                        // Chain head update stream ended
                        Ok(Async::Ready(None)) => {
                            // Should not happen
                            return Err(format_err!("chain head update stream ended unexpectedly"));
                        }

                        Ok(Async::NotReady) => {
                            // Stay idle
                            state = BlockStreamState::Idle;
                            break Async::NotReady;
                        }

                        // mpsc channel failed
                        Err(()) => {
                            // Should not happen
                            return Err(format_err!("chain head update Receiver failed"));
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

        Box::new(self.chain_head_update_sink.clone().sink_map_err(move |_| {
            debug!(logger, "Terminating chain head updates; channel closed");
        }))
    }
}

pub struct BlockStreamBuilder<S, C, E> {
    subgraph_store: Arc<S>,
    chain_store: Arc<C>,
    eth_adapter: Arc<E>,
    node_id: NodeId,
    reorg_threshold: u64,
}

impl<S, C, E> Clone for BlockStreamBuilder<S, C, E> {
    fn clone(&self) -> Self {
        BlockStreamBuilder {
            subgraph_store: self.subgraph_store.clone(),
            chain_store: self.chain_store.clone(),
            eth_adapter: self.eth_adapter.clone(),
            node_id: self.node_id.clone(),
            reorg_threshold: self.reorg_threshold,
        }
    }
}

impl<S, C, E> BlockStreamBuilder<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
    pub fn new(
        subgraph_store: Arc<S>,
        chain_store: Arc<C>,
        eth_adapter: Arc<E>,
        node_id: NodeId,
        reorg_threshold: u64,
    ) -> Self {
        BlockStreamBuilder {
            subgraph_store,
            chain_store,
            eth_adapter,
            node_id,
            reorg_threshold,
        }
    }
}

impl<S, C, E> BlockStreamBuilderTrait for BlockStreamBuilder<S, C, E>
where
    S: Store,
    C: ChainStore,
    E: EthereumAdapter,
{
    type Stream = BlockStream<S, C, E>;

    fn build(
        &self,
        logger: Logger,
        deployment_id: SubgraphDeploymentId,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
    ) -> Self::Stream {
        let logger = logger.new(o!(
            "component" => "BlockStream",
        ));
        let logger_for_stream = logger.clone();

        // Create a chain head update stream whose lifetime is tied to the
        // liftetime of the block stream; we do this to immediately terminate
        // the chain head update listener when the block stream is shut down
        let cancel_guard = CancelGuard::new();
        let chain_head_update_stream =
            self.chain_store
                .chain_head_updates()
                .cancelable(&cancel_guard, move || {
                    debug!(logger_for_stream, "Terminating chain head updates");
                });

        // Create the actual subgraph-specific block stream
        // let log_filter = create_log_filter_from_subgraph(manifest);
        // let call_filter = create_call_filter_from_subgraph(manifest);
        // let block_filter = create_block_filter_from_subgraph(manifest);
        let block_stream = BlockStream::new(
            self.subgraph_store.clone(),
            self.chain_store.clone(),
            self.eth_adapter.clone(),
            cancel_guard,
            self.node_id.clone(),
            deployment_id,
            log_filter,
            call_filter,
            block_filter,
            self.reorg_threshold,
            logger,
        );

        // Forward chain head updates from the listener to the block stream;
        // this will be canceled as soon as the block stream goes out of scope
        tokio::spawn(
            chain_head_update_stream
                .forward(block_stream.event_sink())
                .map_err(|_| ())
                .map(|_| ()),
        );

        block_stream
    }
}

fn create_log_filter_from_subgraph(manifest: &SubgraphManifest) -> Option<EthereumLogFilter> {
    let log_filter = manifest
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
        })
        .collect::<EthereumLogFilter>();
    match log_filter.contract_address_and_event_sig_pairs.len() {
        0 => return None,
        _ => return Some(log_filter),
    }
}

fn create_call_filter_from_subgraph(manifest: &SubgraphManifest) -> Option<EthereumCallFilter> {
    let call_filter = manifest
        .data_sources
        .iter()
        .flat_map(|data_source| {
            let contract_addr = data_source.source.address;
            data_source
                .mapping
                .call_handlers
                .iter()
                .map(move |call_handler| {
                    let sig = keccak256(call_handler.function.as_bytes());
                    (contract_addr, [sig[0], sig[1], sig[2], sig[3]])
                })
        })
        .collect::<EthereumCallFilter>();
    match call_filter.contract_addresses_function_signatures.len() {
        0 => return None,
        _ => return Some(call_filter),
    }
}

fn create_block_filter_from_subgraph(manifest: &SubgraphManifest) -> Option<EthereumBlockFilter> {
    let block_filter = manifest
        .data_sources
        .iter()
        .filter(|data_source| {
            data_source
                .mapping
                .block_handler
                .is_some()
        })
        .map(|data_source| {
            data_source.source.address
        })
        .collect::<EthereumBlockFilter>();
    match block_filter.contract_addresses.len() {
        0 => return None,
        _ => return Some(block_filter),
    }
}

fn parse_triggers(
    log_filter_opt: Option<EthereumLogFilter>,
    call_filter_opt: Option<EthereumCallFilter>,
    block_filter_opt: Option<EthereumBlockFilter>,
    descendant_block: &EthereumBlockWithCalls,
) -> Result<Vec<EthereumTrigger>, Error> {
    let mut triggers = Vec::new();
    triggers.append(&mut parse_log_triggers(
        log_filter_opt,
        &descendant_block.ethereum_block,
    ));
    triggers.append(&mut parse_call_triggers(
        call_filter_opt,
        descendant_block,
    ));
    triggers.append(&mut parse_block_triggers(
        block_filter_opt,
        descendant_block,
    ));
    let tx_hash_indexes = descendant_block
        .ethereum_block
        .transaction_receipts
        .iter()
        .map(|receipt| {
            (receipt.transaction_hash, receipt.transaction_index.as_u64())
        })
        .collect::<HashMap<H256, u64>>();

    // Ensure all `Call` and `Log` triggers have a transaction index
    for trigger in triggers.iter() {
        match trigger {
            EthereumTrigger::Log(log) => {
                if !tx_hash_indexes.get(&log.transaction_hash.unwrap()).is_some() {
                    return Err(format_err!("Unable to determine transaction index for Ethereum log."))
                }
            }
            EthereumTrigger::Call(call) => {
                if !tx_hash_indexes.get(&call.transaction_hash.unwrap()).is_some() {
                    return Err(format_err!("Unable to determine transaction index for Ethereum call."))
                }
            }
            EthereumTrigger::Block(_) => continue,
        }
    }

    // Sort the triggers
    triggers
        .sort_by(|a, b| {
            let a_tx_index = a.transaction_index(&tx_hash_indexes).unwrap();
            let b_tx_index = b.transaction_index(&tx_hash_indexes).unwrap();
            if a_tx_index.is_none() && b_tx_index.is_none() {
                return Ordering::Equal
            }
            if a_tx_index.is_none() {
                return Ordering::Less
            }
            if b_tx_index.is_none() {
                return Ordering::Greater
            }
            a_tx_index.unwrap().cmp(&b_tx_index.unwrap())
        });

    Ok(triggers)
}

fn parse_log_triggers(
    log_filter: Option<EthereumLogFilter>,
    block: &EthereumBlock
) -> Vec<EthereumTrigger> {
    if log_filter.is_none() {
        return vec![]
    }
    let log_filter = log_filter.unwrap();
    block
        .transaction_receipts
        .iter()
        .flat_map(move |receipt| {
            let log_filter = log_filter.clone();
            receipt
                .logs
                .iter()
                .filter(move |log| {
                    log_filter.matches(log)
                })
                .map(move |log| {
                    EthereumTrigger::Log(log.clone())
                })
        })
        .collect()
}

fn parse_call_triggers(
    call_filter: Option<EthereumCallFilter>,
    block: &EthereumBlockWithCalls,
) -> Vec<EthereumTrigger> {
    if call_filter.is_none() || block.calls.is_none() {
        return vec![]
    }
    let call_filter = call_filter.unwrap();
    let calls = &block.calls.clone().unwrap();
    calls
        .iter()
        .filter(move |call| {
            call_filter.matches(call)
        })
        .map(move |call| {
            EthereumTrigger::Call(call.clone())
        })
        .collect()
}

fn parse_block_triggers(
    block_filter: Option<EthereumBlockFilter>,
    block: &EthereumBlockWithCalls,
) -> Vec<EthereumTrigger> {
    if block_filter.is_none() {
        return vec![]
    }
    let call_filter = EthereumCallFilter::from(block_filter.unwrap());
    let calls = &block.calls.clone().unwrap();
    calls
        .iter()
        .filter(move |call| {
            call_filter.matches(call)
        })
        .map(move |call| {
            EthereumTrigger::Block(call.clone())
        })
        .collect()
}
