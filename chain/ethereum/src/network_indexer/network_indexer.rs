use futures::future::{loop_fn, Loop};
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::try_ready;
use state_machine_future::*;
use std::collections::VecDeque;
use std::fmt;
use std::ops::Range;
use std::str::FromStr;

use graph::prelude::*;
use web3::types::H256;

use super::block_writer::BlockWriter;
use super::*;

/// Terminology used in this component:
///
/// Head / head block:
///   The most recent block of a chain.
///
/// Local head:
///   The block that the network indexer is at locally.
///   We get this from the store.
///
/// Chain head:
///   The block that the network is at.
///   We get this from the Ethereum node(s).
///
/// Common ancestor (during a reorg):
///   The most recent block that two versions of a chain (e.g. the locally
///   indexed version and the latest version that the network recognizes)
///   have in common.
///
///   When handling a reorg, this is the block after which the new version
///   has diverged. All blocks up to and including the common ancestor
///   remain untouched during the reorg. The blocks after the common ancestor
///   are reverted and the blocks from the new version are added after the
///   common ancestor.
///
///   The common ancestor is identified by traversing new blocks from a reorg
///   back to the most recent block that we already have indexed locally.
///
/// Old blocks (during a reorg):
///   Blocks after the common ancestor that are indexed locally but are
///   being removed as part of a reorg. We collect these from the store by
///   traversing from the current local head back to the common ancestor.
///
/// New blocks (during a reorg):
///   Blocks between the common ancestor and the block that triggered the
///   reorg. After reverting the old blocks, these are the blocks that need
///   to be fetched from the network and added after the common ancestor.
///
///   We collect these from the network by traversing from the block that
///   triggered the reorg back to the common ancestor.

/**
 * Helper types.
 */

type LocalHeadFuture = Box<dyn Future<Item = Option<EthereumBlockPointer>, Error = Error> + Send>;
type ChainHeadFuture = Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send>;
type BlockFuture = Box<dyn Future<Item = Option<BlockWithUncles>, Error = Error> + Send>;
type BlockStream = Box<dyn Stream<Item = Option<BlockWithUncles>, Error = Error> + Send>;
type NewBlocksFuture = Box<dyn Future<Item = VecDeque<BlockWithUncles>, Error = Error> + Send>;
type CollectBlocksToRevertFuture =
    Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>;
type RevertBlocksFuture = Box<dyn Future<Item = EthereumBlockPointer, Error = Error> + Send>;
type AddBlockFuture = Box<dyn Future<Item = EthereumBlockPointer, Error = Error> + Send>;
type SendEventFuture = Box<dyn Future<Item = (), Error = Error> + Send>;

/**
 * Helpers to create futures and streams.
 */

fn load_local_head(subgraph_id: SubgraphDeploymentId, store: Arc<dyn Store>) -> LocalHeadFuture {
    Box::new(future::result(store.clone().block_ptr(subgraph_id.clone())))
}

fn poll_chain_head(logger: Logger, adapter: Arc<dyn EthereumAdapter>) -> ChainHeadFuture {
    Box::new(adapter.latest_block(&logger).from_err())
}

fn fetch_block_and_uncles_by_number(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    block_number: u64,
) -> BlockFuture {
    let logger_for_full_block = logger.clone();
    let adapter_for_full_block = adapter.clone();

    let logger_for_uncles = logger.clone();
    let adapter_for_uncles = adapter.clone();

    Box::new(
        adapter
            .block_by_number(&logger, block_number)
            .from_err()
            .and_then(move |block| match block {
                None => Box::new(future::ok(None))
                    as Box<dyn Future<Item = Option<EthereumBlock>, Error = _> + Send>,
                Some(block) => Box::new(
                    adapter_for_full_block
                        .load_full_block(&logger_for_full_block, block)
                        .map(|block| Some(block))
                        .from_err(),
                ),
            })
            .and_then(move |block| match block {
                None => Box::new(future::ok(None))
                    as Box<dyn Future<Item = Option<BlockWithUncles>, Error = _> + Send>,
                Some(block) => Box::new(
                    adapter_for_uncles
                        .uncles(&logger_for_uncles, &block.block)
                        .and_then(move |uncles| future::ok(BlockWithUncles { block, uncles }))
                        .map(|block| Some(block)),
                ),
            }),
    )
}

fn fetch_block_and_uncles(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    block_hash: H256,
) -> BlockFuture {
    let logger_for_full_block = logger.clone();
    let adapter_for_full_block = adapter.clone();

    let logger_for_uncles = logger.clone();
    let adapter_for_uncles = adapter.clone();

    Box::new(
        adapter
            .block_by_hash(&logger, block_hash)
            .from_err()
            .and_then(move |block| match block {
                None => Box::new(future::ok(None))
                    as Box<dyn Future<Item = Option<EthereumBlock>, Error = _> + Send>,
                Some(block) => Box::new(
                    adapter_for_full_block
                        .load_full_block(&logger_for_full_block, block)
                        .map(|block| Some(block))
                        .from_err(),
                ),
            })
            .and_then(move |block| match block {
                None => Box::new(future::ok(None))
                    as Box<dyn Future<Item = Option<BlockWithUncles>, Error = _> + Send>,
                Some(block) => Box::new(
                    adapter_for_uncles
                        .uncles(&logger_for_uncles, &block.block)
                        .and_then(move |uncles| future::ok(BlockWithUncles { block, uncles }))
                        .map(|block| Some(block)),
                ),
            }),
    )
}

fn fetch_blocks(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    block_numbers: Range<u64>,
) -> BlockStream {
    Box::new(
        futures::stream::iter_ok::<_, Error>(block_numbers)
            .map(move |block_number| {
                fetch_block_and_uncles_by_number(logger.clone(), adapter.clone(), block_number)
            })
            .buffered(100),
    )
}

fn fetch_new_blocks(
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    adapter: Arc<dyn EthereumAdapter>,
    store: Arc<dyn Store>,
    head: BlockWithUncles,
) -> NewBlocksFuture {
    // Start at `head` and go back block by block until we find a block that we
    // already have in the store. That block is the common ancestor. Collect all
    // blocks as we go. Then, return all blocks including the common ancestor and
    // head.
    Box::new(
        loop_fn(vec![head], move |mut blocks| {
            let store = store.clone();

            // Get the last block from the list
            let (block_entity_key, number, hash, parent_hash) = {
                let block = blocks.last().unwrap();
                (
                    block.to_entity_key(subgraph_id.clone()),
                    block.inner().number.clone().unwrap(),
                    block.inner().hash.clone().unwrap(),
                    block.inner().parent_hash.clone(),
                )
            };

            trace!(
                logger,
                "Fetch block on new chain";
                "block" => format!("#{} ({:x})", number, hash),
            );

            // Look it up from the store
            match store.get(block_entity_key) {
                Ok(None) => {
                    // We don't have the block yet, continue with its parent
                    Box::new(
                        fetch_block_and_uncles(
                            logger.clone(),
                            adapter.clone(),
                            parent_hash.clone(),
                        )
                        .and_then(move |parent| match parent {
                            None => future::err(format_err!(
                                "failed to fetch parent block {:x}",
                                parent_hash
                            )),

                            Some(parent) => {
                                blocks.push(parent);
                                future::ok(Loop::Continue(blocks))
                            }
                        }),
                    )
                }

                Ok(Some(_)) => {
                    trace!(
                        logger,
                        "Found common ancestor";
                        "block" => format!("#{} ({:x})", number, hash),
                    );

                    // We have the block already, so this is the block after which
                    // the chain was forked
                    Box::new(future::ok(Loop::Break(blocks)))
                        as Box<dyn Future<Item = Loop<_, _>, Error = Error> + Send>
                }

                // Looking up the block failed, propoagate the error so we can
                // retry handling the reorg
                Err(e) => Box::new(future::err(e.into()))
                    as Box<dyn Future<Item = Loop<_, _>, Error = Error> + Send>,
            }
        })
        // Reverse the blocks so that the common ancestor comes first.
        .map(|mut blocks: Vec<BlockWithUncles>| {
            blocks.reverse();
            VecDeque::from(blocks)
        }),
    )
}

fn write_block(block_writer: Arc<BlockWriter>, block: BlockWithUncles) -> AddBlockFuture {
    let block_ptr = block.inner().into();
    Box::new(block_writer.write(block).map(move |_| block_ptr))
}

fn collect_blocks_to_revert(
    logger: Logger,
    subgraph_id: SubgraphDeploymentId,
    store: Arc<dyn Store>,
    head: EthereumBlockPointer,
    common_ancestor: EthereumBlockPointer,
) -> CollectBlocksToRevertFuture {
    Box::new(loop_fn(vec![head], move |mut blocks| {
        let logger = logger.clone();
        let store = store.clone();

        // Get the last block from the list
        let block_ptr = blocks.last().unwrap().clone();
        let block_ptr_for_missing_parent = block_ptr.clone();
        let block_ptr_for_invalid_parent = block_ptr.clone();

        trace!(
            logger,
            "Collect old block";
            "common_ancestor" => format_block_pointer(&common_ancestor),
            "block" => format_block_pointer(&block_ptr),
        );

        // If we've reached the common ancestor, terminate the loop and return
        // the blocks we have collected up to here
        if block_ptr == common_ancestor {
            trace!(logger, "Reached common ancestor");

            return Box::new(future::ok(Loop::Break(blocks)))
                as Box<dyn Future<Item = _, Error = _> + Send>;
        }

        // Look this block up from the store
        Box::new(
            future::result(
                store
                    .get(block_ptr.to_entity_key(subgraph_id.clone()))
                    .map_err(|e| e.into())
                    .and_then(|entity| {
                        entity.ok_or_else(|| {
                            format_err!(
                                "block {} is missing in store",
                                format_block_pointer(&block_ptr)
                            )
                        })
                    }),
            )
            // Get the parent hash from the block
            .and_then(move |block| {
                future::result(
                    block
                        .get("parent")
                        .ok_or_else(move || {
                            format_err!(
                                "block {} has no parent",
                                format_block_pointer(&block_ptr_for_missing_parent),
                            )
                        })
                        .and_then(|value| {
                            let s = value
                                .clone()
                                .as_string()
                                .expect("the `parent` field of `Block` is a reference/string");

                            H256::from_str(s.as_str()).map_err(|e| {
                                format_err!(
                                    "block {} has an invalid parent `{}`: {}",
                                    format_block_pointer(&block_ptr_for_invalid_parent),
                                    s,
                                    e,
                                )
                            })
                        }),
                )
            })
            .and_then(move |parent_hash: H256| {
                // Create a block pointer for the parent
                let parent_ptr = EthereumBlockPointer {
                    number: block_ptr.number - 1,
                    hash: parent_hash,
                };

                // Add the parent block pointer for the next iteration
                blocks.push(parent_ptr);
                future::ok(Loop::Continue(blocks))
            }),
        )
    }))
}

fn revert_blocks(
    subgraph_id: SubgraphDeploymentId,
    logger: Logger,
    store: Arc<dyn Store>,
    event_sink: Sender<NetworkIndexerEvent>,
    blocks: Vec<EthereumBlockPointer>,
) -> RevertBlocksFuture {
    // The common ancestor is the last block (we're looking at the blocks
    // starting with the most recent old block, because we're reverting in
    // reverse order).
    let common_ancestor = blocks
        .last()
        .expect("no blocks to revert, what kind of 'reorg' is this?")
        .clone();

    let logger_for_complete = logger.clone();

    Box::new(
        // Iterate over pairs of blocks in the order they need to be reverted:
        // (local_head, local_head-1), ..., (local_head-n, common_ancestor).
        stream::iter_ok(
            blocks[0..]
                .to_owned()
                .into_iter()
                .zip(blocks[1..].to_owned().into_iter()),
        )
        .for_each(move |(from, to)| {
            let event_sink = event_sink.clone();
            let logger = logger.clone();

            let logger_for_revert_err = logger.clone();
            let from_for_revert_err = from.clone();
            let to_for_revert_err = to.clone();

            let logger_for_send_err = logger.clone();
            let from_for_send_err = from.clone();
            let to_for_send_err = to.clone();

            debug!(
                logger,
                "Revert old block";
                "to" => format_block_pointer(&to),
                "from" => format_block_pointer(&from),
            );

            future::result(store.revert_block_operations(
                subgraph_id.clone(),
                from.clone(),
                to.clone(),
            ))
            .map_err(move |e| {
                debug!(
                    logger_for_revert_err,
                    "Reverting block failed";
                    "error" => format!("{}", e),
                    "to" => format_block_pointer(&to_for_revert_err),
                    "from" => format_block_pointer(&from_for_revert_err),
                );

                // Instead of an error we return the last block that we managed
                // to revert to; this will become the new local head
                from_for_revert_err
            })
            .and_then(move |_| {
                send_event(
                    event_sink.clone(),
                    NetworkIndexerEvent::Revert {
                        from: from.clone(),
                        to: to.clone(),
                    },
                )
                .map_err(move |e| {
                    debug!(
                        logger_for_send_err,
                        "Sending revert event failed";
                        "error" => format!("{}", e),
                        "to" => format_block_pointer(&to_for_send_err),
                        "from" => format_block_pointer(&from_for_send_err),
                    );

                    // Instead of an error we return the last block that we managed
                    // to revert to; this will become the new local head
                    from_for_send_err
                })
            })
        })
        .then(move |result| match result {
            Ok(_) => {
                debug!(
                    logger_for_complete,
                    "Revert old blocks complete; process next blocks"
                );
                future::ok(common_ancestor)
            }
            Err(block) => {
                debug!(
                    logger_for_complete,
                    "Revert old blocks failed; \
                     setting local head to the last block we managed to revert to"
                );
                future::ok(block)
            }
        }),
    )
}

fn send_event(
    event_sink: Sender<NetworkIndexerEvent>,
    event: NetworkIndexerEvent,
) -> SendEventFuture {
    Box::new(
        event_sink
            .send(event)
            .map(|_| ())
            .map_err(|e| format_err!("failed to emit events: {}", e)),
    )
}

/**
 * Network tracer implementation.
 */

/// Context for the network tracer.
pub struct Context {
    subgraph_id: SubgraphDeploymentId,
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    store: Arc<dyn Store>,
    event_sink: Sender<NetworkIndexerEvent>,
    block_writer: Arc<BlockWriter>,
}

/// Events emitted by the network tracer.
#[derive(Debug, PartialEq, Clone)]
pub enum NetworkIndexerEvent {
    Revert {
        from: EthereumBlockPointer,
        to: EthereumBlockPointer,
    },
    AddBlock(EthereumBlockPointer),
}

impl fmt::Display for NetworkIndexerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkIndexerEvent::Revert { from, to } => write!(
                f,
                "Revert from {} to {}",
                format_block_pointer(&from),
                format_block_pointer(&to),
            ),
            NetworkIndexerEvent::AddBlock(block) => {
                write!(f, "Add block {}", format_block_pointer(&block))
            }
        }
    }
}

/// State machine that handles block fetching and block reorganizations.
#[derive(StateMachineFuture)]
#[state_machine_future(context = "Context")]
enum StateMachine {
    /// The indexer start in an empty state and immediately moves on
    /// to loading the local head block from the store.
    #[state_machine_future(start, transitions(LoadLocalHead))]
    Start,

    /// This state waits until the local head block has been loaded from the
    /// store. It then moves on to polling the chain head block.
    #[state_machine_future(transitions(PollChainHead, Failed))]
    LoadLocalHead { local_head: LocalHeadFuture },

    /// This state waits until the chain head block has been polled
    /// successfully.
    ///
    /// Based on the (local head, chain head) pair, the indexer then moves
    /// on to fetching and processing a range of blocks starting at
    /// local head + 1 up, leading up to the chain head. This is done
    /// in chunks of e.g. 100 blocks at a time for two reasons:
    ///
    /// 1. To limit the amount of blocks we keep in memory.
    /// 2. To be able to re-evaluate the chain head and check for reorgs
    ///    frequently.
    #[state_machine_future(transitions(ProcessBlocks, PollChainHead, Failed))]
    PollChainHead {
        local_head: Option<EthereumBlockPointer>,
        chain_head: ChainHeadFuture,
    },

    /// This state takes the next block from the stream. If the stream is
    /// exhausted, it transitions back to polling the chain head block
    /// and deciding on the next chunk of blocks to fetch. If there is still
    /// a block to read from the stream, it's passed on to vetting for
    /// validation and reorg checking.
    #[state_machine_future(transitions(VetBlock, PollChainHead, Failed))]
    ProcessBlocks {
        local_head: Option<EthereumBlockPointer>,
        chain_head: LightEthereumBlock,
        next_blocks: BlockStream,
    },

    /// This state vets incoming blocks with regards to two aspects:
    ///
    /// 1. Does the block have a number and hash? This is a requirement for
    ///    indexing to continue. If not, the indexer re-evaluates the chain
    ///    head and starts over.
    ///
    /// 2. Is the block the successor of the local head block? If yes, move
    ///    on to indexing this block. If not, we have a reorg.
    ///
    /// Notes on the reorg handling:
    ///
    ///   By checking parent/child succession, we ensure that there are no gaps
    ///   in the indexed data (class mathematical induction). So if the local
    ///   head is `x` and a block `f` comes in that is not a successor/child, it
    ///   must be on a different version/fork of the chain.
    ///
    ///   E.g.:
    ///
    ///      a---b---c---x
    ///          \
    ///           +--d---e---f
    ///
    ///   In that case we need to do the following:
    ///
    ///   1. Find the common ancestor of `x` and `f`, which is the block after
    ///      which the two versions diverged (in the above example: `b`).
    ///
    ///   2. Collect old blocks betweeen the common ancestor and (including)
    ///      the local head that need to be reverted (in the above example:
    ///      `c`, `x`).
    ///
    ///   3. Fetch new blocks between the common ancestor and (including) `f`
    ///      that are to be inserted instead of the old blocks in order to
    ///      make the incoming block (`f`) the local head (in the above
    ///      example: `d`, `e`, `f`).
    #[state_machine_future(transitions(FetchNewBlocks, AddBlock, PollChainHead, Failed))]
    VetBlock {
        local_head: Option<EthereumBlockPointer>,
        chain_head: LightEthereumBlock,
        next_blocks: BlockStream,
        block: BlockWithUncles,
    },

    /// This state waits until all new blocks from the incoming block back to
    /// the common ancestor are available. Identifying the common ancestor is
    /// part of this process.
    ///
    /// If successful, the indexer moves on to collecting old blocks and
    /// reverting the indexed data to the common ancestor. If fetching the new
    /// blocks fails, it discards any new information and re-evaluates the chain
    /// head.
    ///
    /// The new blocks that were fetched are prepending to the incoming blocks
    /// stream, so that after reverting blocks the indexer can proceed with these
    /// as if no reorg happened. It'll still want to vet these blocks so it wouldn't
    /// be wise to just index the blocks without further checks.
    ///
    /// Note: This state also carries over the incoming block stream to not lose
    /// its blocks. This is because even if there was a reorg, the blocks following
    /// the current block that made us detect it will likely be valid successors.
    /// So once the reorg has been handled, the indexer should be able to
    /// continue with the remaining blocks on the stream.
    ///
    /// Only when going back to re-evaluating the chain head, the incoming
    /// blocks stream is thrown away in the hope that of receiving a better
    /// chain head with different blocks leading up to it.
    #[state_machine_future(transitions(RevertToCommonAncestor, PollChainHead, Failed))]
    FetchNewBlocks {
        local_head: Option<EthereumBlockPointer>,
        chain_head: LightEthereumBlock,
        next_blocks: BlockStream,
        new_blocks: NewBlocksFuture,
    },

    /// This state collects and reverts old blocks in the store. If successful,
    /// the indexer moves on to processing the blocks regularly (at this point,
    /// the incoming blocks stream includes new blocks for the reorg, the
    /// block that triggered the reorg and any blocks that were already in the
    /// stream following the block that triggered the reorg).
    ///
    /// After reverting, the local head is updated to the common ancestor.
    ///
    /// If reverting fails at any block, the local head is updated to the
    /// last block that we managed to revert to. Following that, the indexer
    /// re-evaluates the chain head and starts over.
    ///
    /// Note: failing to revert an old block locally may be something that
    /// the indexer cannot recover from, so it may run into a loop at this
    /// point.
    #[state_machine_future(transitions(ProcessBlocks, PollChainHead, Failed))]
    RevertToCommonAncestor {
        local_head: Option<EthereumBlockPointer>,
        chain_head: LightEthereumBlock,
        next_blocks: BlockStream,
        new_local_head: RevertBlocksFuture,
    },

    /// This state waits until a block has been written and an event for it
    /// has been sent out. After that, the indexer continues processing the
    /// next block. If anything goes wrong at this point, it's back to
    /// re-evaluating the chain head and fetching (potentially) different
    /// blocks for indexing.
    #[state_machine_future(transitions(ProcessBlocks, PollChainHead, Failed))]
    AddBlock {
        chain_head: LightEthereumBlock,
        next_blocks: BlockStream,
        old_local_head: Option<EthereumBlockPointer>,
        new_local_head: AddBlockFuture,
    },

    /// This is unused, the indexing never ends.
    #[state_machine_future(ready)]
    Ready(()),

    /// State for fatal errors that cause the indexing to terminate. This should
    /// almost never happen. If it does, it should cause the entire node to crash
    /// and restart.
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollStateMachine for StateMachine {
    fn poll_start<'a, 'c>(
        _state: &'a mut RentToOwn<'a, Start>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterStart, Error> {
        // Abort if the output stream has been closed. Depending on how the
        // network indexer is wired up, this could mean that the system shutting
        // down.
        try_ready!(context.event_sink.poll_ready());

        info!(context.logger, "Start indexing network data");

        // Start by loading the local head from the store. This is the most
        // recent block we managed to index until now.
        transition!(LoadLocalHead {
            local_head: load_local_head(context.subgraph_id.clone(), context.store.clone())
        })
    }

    fn poll_load_local_head<'a, 'c>(
        state: &'a mut RentToOwn<'a, LoadLocalHead>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterLoadLocalHead, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        info!(context.logger, "Load local head block");

        // Wait until we have the local head block; fail if we can't get it from
        // the store because that means the indexed data is broken.
        let local_head = try_ready!(state.local_head.poll());

        // Move on to poll the chain head.
        transition!(PollChainHead {
            local_head,
            chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
        })
    }

    fn poll_poll_chain_head<'a, 'c>(
        state: &'a mut RentToOwn<'a, PollChainHead>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterPollChainHead, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        match state.chain_head.poll() {
            // Wait until we have the chain head block.
            Ok(Async::NotReady) => Ok(Async::NotReady),

            // We have a (new?) chain head, decide what to do.
            Ok(Async::Ready(chain_head)) => {
                // Validate the chain head.
                if chain_head.number.is_none() || chain_head.hash.is_none() {
                    // This is fairly irregular, so log a warning.
                    warn!(
                        context.logger,
                        "Chain head block number or hash missing; try again";
                        "block" => format_light_block(&chain_head),
                    );

                    // Chain head was invalid, try getting a better one.
                    transition!(PollChainHead {
                        local_head: state.local_head,
                        chain_head: poll_chain_head(
                            context.logger.clone(),
                            context.adapter.clone()
                        ),
                    })
                }

                let state = state.take();

                debug!(
                    context.logger,
                    "Identify next blocks to index";
                    "chain_head" => format_light_block(&chain_head),
                    "local_head" => state.local_head.map_or(
                        String::from("none"), |ptr| format_block_pointer(&ptr)
                    ),
                );

                // If we're already at the chain head, keep polling it.
                if Some((&chain_head).into()) == state.local_head {
                    debug!(
                        context.logger,
                        "Already at chain head; poll chain head again";
                        "chain_head" => format_light_block(&chain_head),
                        "local_head" => state.local_head.map_or(
                            String::from("none"), |ptr| format_block_pointer(&ptr)
                        ),
                    );

                    // Chain head was invalid, try getting a better one.
                    transition!(PollChainHead {
                        local_head: state.local_head,
                        chain_head: poll_chain_head(
                            context.logger.clone(),
                            context.adapter.clone()
                        ),
                    });
                }

                // Calculate the number of blocks remaining before we are in sync with the
                // network; fetch no more than 1000 blocks at a time.
                let chain_head_number = chain_head.number.unwrap().as_u64();
                let next_block_number = state.local_head.map_or(0u64, |ptr| ptr.number + 1);
                let remaining_blocks = chain_head_number + 1 - next_block_number;
                let block_range_size = remaining_blocks.min(1000);
                let block_numbers = next_block_number..(next_block_number + block_range_size);

                // Ensure we're not trying to fetch beyond the current chain head (note: the
                // block numbers range end is _exclusive_, hence it must not be greater than
                // chain head + 1)
                assert!(
                    block_numbers.end <= chain_head_number + 1,
                    "overfetching beyond the chain head; \
                     this is a bug in the block range calculation"
                );

                info!(
                    context.logger,
                    "Process {} of {} remaining blocks",
                    block_range_size, remaining_blocks;
                    "chain_head" => format_light_block(&chain_head),
                    "local_head" => state.local_head.map_or(
                        String::from("none"), |ptr| format_block_pointer(&ptr)
                    ),
                    "range" => format!("#{}..#{}", block_numbers.start, block_numbers.end-1),
                );

                // Processing the blocks in this range.
                transition!(ProcessBlocks {
                    local_head: state.local_head,
                    chain_head,
                    next_blocks: fetch_blocks(
                        context.logger.clone(),
                        context.adapter.clone(),
                        block_numbers
                    )
                })
            }

            Err(e) => {
                trace!(
                    context.logger,
                    "Failed to poll chain head; try again";
                    "error" => format!("{}", e),
                );

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.local_head,
                    chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
                })
            }
        }
    }

    fn poll_process_blocks<'a, 'c>(
        state: &'a mut RentToOwn<'a, ProcessBlocks>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterProcessBlocks, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        // Try to read the next block.
        match state.next_blocks.poll() {
            // No block ready yet, try again later.
            Ok(Async::NotReady) => Ok(Async::NotReady),

            // The stream is exhausted, update chain head and fetch the next
            // range of blocks for processing.
            Ok(Async::Ready(None)) => {
                debug!(context.logger, "Check if there are more blocks");

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.local_head,
                    chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
                })
            }

            // The block could not be fetched but there was no clear error either;
            // try starting over with a fresh chain head.
            Ok(Async::Ready(Some(None))) => {
                trace!(
                    context.logger,
                    "Failed to fetch block; re-evaluate chain head and try again"
                );

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.local_head,
                    chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
                })
            }

            // There is a block ready to be processed; check whether it is valid
            // and whether it requires a reorg before adding it.
            Ok(Async::Ready(Some(Some(block)))) => {
                let state = state.take();

                transition!(VetBlock {
                    local_head: state.local_head,
                    chain_head: state.chain_head,
                    next_blocks: state.next_blocks,
                    block,
                })
            }

            // Fetching blocks failed; we have no choice but to start over again
            // with a fresh chain head.
            Err(e) => {
                trace!(
                    context.logger,
                    "Failed to fetch block; re-evaluate chain head and try again";
                    "error" => format!("{}", e),
                );

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.local_head,
                    chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
                })
            }
        }
    }

    fn poll_vet_block<'a, 'c>(
        state: &'a mut RentToOwn<'a, VetBlock>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterVetBlock, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        let state = state.take();
        let block = state.block;

        // Validate the block.
        if block.inner().number.is_none() || block.inner().hash.is_none() {
            // This is fairly irregular, so log a warning.
            warn!(
                context.logger,
                "Block number or hash missing; trying again";
                "block" => format_block(&block),
            );

            // The block is invalid, throw away the entire stream and
            // start with re-checking the chain head block again.
            transition!(PollChainHead {
                local_head: state.local_head,
                chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
            })
        }

        // If we encounter a block that has a smaller number than our
        // local head block, then we throw away the block stream and
        // try to start over with a fresh chain head block.
        //
        // The assumption here is that maybe the network provider being
        // used is temporarily serving from an outdated node.
        let block_number = block.inner().number.unwrap().as_u64();
        let local_head_number = state.local_head.map_or(0u64, |ptr| ptr.number);
        if block_number < local_head_number {
            // This is pretty irregular, so make log a warning.
            warn!(
                context.logger,
                "Received older block than the local head; \
                 re-evaluate chain head and try again";
                "local_head" => state.local_head.map_or(
                    String::from("none"), |ptr| format_block_pointer(&ptr)
                ),
                "block" => format_block(&block),
            );

            transition!(PollChainHead {
                local_head: state.local_head,
                chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
            })
        }

        // Check whether we have a reorg (parent of the new block != our local head).
        if block.inner().parent_ptr() != state.local_head {
            info!(
                context.logger,
                "Block requires a reorg";
                "local_head" => state.local_head.map_or(
                    String::from("none"), |ptr| format_block_pointer(&ptr)
                ),
                "parent" => block.inner().parent_ptr().map_or(
                    String::from("none"), |ptr| format_block_pointer(&ptr)
                ),
                "block" => format_block(&block),
            );

            // We are dealing with a reorg; fetch all new blocks from the incoming
            // block back to the common ancestor.
            transition!(FetchNewBlocks {
                local_head: state.local_head,
                chain_head: state.chain_head,
                next_blocks: state.next_blocks,
                new_blocks: fetch_new_blocks(
                    context.logger.clone(),
                    context.subgraph_id.clone(),
                    context.adapter.clone(),
                    context.store.clone(),
                    block
                ),
            })
        } else {
            let event_sink = context.event_sink.clone();

            // The block is a regular successor to the local head.
            // Add the block and move on.
            transition!(AddBlock {
                // Remember the old local head in case we need to roll back.
                old_local_head: state.local_head,

                // Carry over the current chain head and the incoming blocks stream.
                chain_head: state.chain_head,
                next_blocks: state.next_blocks,

                // Index the block.
                new_local_head: Box::new(
                    // Write block to the store.
                    write_block(context.block_writer.clone(), block)
                        // Send an `AddBlock` event for it.
                        .and_then(move |block_ptr| {
                            send_event(event_sink, NetworkIndexerEvent::AddBlock(block_ptr.clone()))
                                .and_then(move |_| {
                                    // Return the new block so we can update the local head.
                                    future::ok(block_ptr)
                                })
                        })
                )
            })
        }
    }

    fn poll_fetch_new_blocks<'a, 'c>(
        state: &'a mut RentToOwn<'a, FetchNewBlocks>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterFetchNewBlocks, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        match state.new_blocks.poll() {
            // Don't have the new blocks yet, try again later.
            Ok(Async::NotReady) => Ok(Async::NotReady),

            // Have the new blocks, now revert to the common ancestor, prepend
            // the new blocks to the incoming blocks stream and move towards
            // the block that triggered the reorg.
            Ok(Async::Ready(mut new_blocks)) => {
                // The first block is the common ancestor.
                let common_ancestor = new_blocks.pop_front().expect(
                    "reorgs without a common ancestor are invalid \
                     as they are entirely different chains",
                );

                let state = state.take();

                let common_ancestor_ptr = common_ancestor.inner().into();
                let local_head_ptr = state
                    .local_head
                    .expect("cannot have a reorg if there is no local head block yet")
                    .into();

                let subgraph_id_for_revert = context.subgraph_id.clone();
                let logger_for_revert = context.logger.clone();
                let store_for_revert = context.store.clone();
                let event_sink_for_revert = context.event_sink.clone();

                transition!(RevertToCommonAncestor {
                    local_head: state.local_head,
                    chain_head: state.chain_head,

                    // Make the blocks from the forked branch the next ones to process
                    // before any other incoming blocks
                    next_blocks: Box::new(
                        stream::iter_ok(new_blocks.into_iter().map(|block| Some(block)))
                            .chain(state.next_blocks)
                    ),

                    // Identify the sequence of block pointers we need to
                    // revert, going back from the local head to the common
                    // ancestor; then revert all of those by reverting them in
                    // the store and emitting revert events.
                    //
                    // If reverting fails, we update the local head to the most
                    // recent block that we managed to revert to.
                    new_local_head: Box::new(
                        collect_blocks_to_revert(
                            context.logger.clone(),
                            context.subgraph_id.clone(),
                            context.store.clone(),
                            local_head_ptr,
                            common_ancestor_ptr,
                        )
                        .and_then(move |block_ptrs| {
                            revert_blocks(
                                subgraph_id_for_revert,
                                logger_for_revert,
                                store_for_revert,
                                event_sink_for_revert,
                                block_ptrs,
                            )
                        })
                    )
                })
            }

            // Fetching the new blocks failed.
            Err(e) => {
                trace!(
                    context.logger,
                    "Fetching new blocks failed; \
                     re-evaluate chain head and try again";
                    "error" => format!("{}", e)
                );

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.local_head,
                    chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
                })
            }
        }
    }

    fn poll_revert_to_common_ancestor<'a, 'c>(
        state: &'a mut RentToOwn<'a, RevertToCommonAncestor>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRevertToCommonAncestor, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        match state.new_local_head.poll() {
            // Reverting has not finished yet, try again later.
            Ok(Async::NotReady) => Ok(Async::NotReady),

            // The revert finished and the common ancestor should become our new
            // local head. Continue processing the blocks that we pulled in for
            // the reorg.
            Ok(Async::Ready(block_ptr)) => {
                let state = state.take();

                transition!(ProcessBlocks {
                    // Set the local head to the block we have reverted to
                    local_head: Some(block_ptr),
                    chain_head: state.chain_head,
                    next_blocks: state.next_blocks,
                })
            }

            // There was an error reverting; re-evaluate the chain head
            // and try again.
            Err(e) => {
                warn!(
                    context.logger,
                    "Failed to handle reorg, re-evaluate chain head and try again";
                    "error" => format!("{}", e),
                );

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.local_head,
                    chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
                })
            }
        }
    }

    fn poll_add_block<'a, 'c>(
        state: &'a mut RentToOwn<'a, AddBlock>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterAddBlock, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        match state.new_local_head.poll() {
            // Adding the block is not complete yet, try again later.
            Ok(Async::NotReady) => return Ok(Async::NotReady),

            // We have the new local block, update it and continue processing blocks.
            Ok(Async::Ready(block_ptr)) => {
                let state = state.take();

                transition!(ProcessBlocks {
                    local_head: Some(block_ptr),
                    chain_head: state.chain_head,
                    next_blocks: state.next_blocks,
                })
            }

            // Something went wrong, back to re-evaluating the chain head it is!
            Err(e) => {
                trace!(
                    context.logger,
                    "Failed to add block, re-evaluate chain head and try again";
                    "error" => format!("{}", e),
                );

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.old_local_head,
                    chain_head: poll_chain_head(context.logger.clone(), context.adapter.clone()),
                })
            }
        }
    }
}

pub struct NetworkIndexer {
    output: Option<Receiver<NetworkIndexerEvent>>,
}

impl NetworkIndexer {
    pub fn new<S>(
        subgraph_id: SubgraphDeploymentId,
        logger: &Logger,
        adapter: Arc<dyn EthereumAdapter>,
        store: Arc<S>,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self
    where
        S: Store + ChainStore,
    {
        let logger = logger.new(o!("component" => "NetworkIndexer"));
        let logger_for_err = logger.clone();

        let stopwatch = StopwatchMetrics::new(
            logger.clone(),
            subgraph_id.clone(),
            metrics_registry.clone(),
        );

        let block_writer = Arc::new(BlockWriter::new(
            subgraph_id.clone(),
            &logger,
            store.clone(),
            stopwatch,
            metrics_registry.clone(),
        ));

        // Create a channel for emitting events
        let (event_sink, output) = channel(100);

        // Create state machine that emits block and revert events for the network
        let state_machine = StateMachine::start(Context {
            subgraph_id,
            logger,
            adapter,
            store,
            event_sink,
            block_writer,
        });

        // Launch state machine
        tokio::spawn(state_machine.map_err(move |e| {
            error!(logger_for_err, "Network indexer failed: {}", e);
        }));

        Self {
            output: Some(output),
        }
    }
}

impl EventProducer<NetworkIndexerEvent> for NetworkIndexer {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = NetworkIndexerEvent, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<dyn Stream<Item = NetworkIndexerEvent, Error = ()> + Send>)
    }
}
