use chrono::Utc;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::try_ready;
use state_machine_future::*;
use std::fmt;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use graph::prelude::*;

use super::block_writer::BlockWriter;
use super::metrics::NetworkIndexerMetrics;
use super::subgraph;
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

/**
 * Helper types.
 */

type EnsureSubgraphFuture = Box<dyn Future<Item = (), Error = Error> + Send>;
type LocalHeadFuture = Box<dyn Future<Item = Option<EthereumBlockPointer>, Error = Error> + Send>;
type ChainHeadFuture = Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send>;
type OmmersFuture = Box<dyn Future<Item = Vec<Ommer>, Error = Error> + Send>;
type BlockPointerFuture = Box<dyn Future<Item = EthereumBlockPointer, Error = Error> + Send>;
type BlockFuture = Box<dyn Future<Item = Option<BlockWithOmmers>, Error = Error> + Send>;
type BlockStream = Box<dyn Stream<Item = BlockWithOmmers, Error = Error> + Send>;
type RevertLocalHeadFuture = Box<dyn Future<Item = EthereumBlockPointer, Error = Error> + Send>;
type AddBlockFuture = Box<dyn Future<Item = EthereumBlockPointer, Error = Error> + Send>;
type SendEventFuture = Box<dyn Future<Item = (), Error = Error> + Send>;

/**
 * Helpers to create futures and streams.
 */

macro_rules! track_future {
    ($metrics: expr, $metric: ident, $metric_problems: ident, $expr: expr) => {{
        let metrics_for_measure = $metrics.clone();
        let metrics_for_err = $metrics.clone();
        let start_time = Instant::now();
        $expr
            .inspect(move |_| {
                let duration = start_time.elapsed();
                metrics_for_measure.$metric.update_duration(duration);
            })
            .map_err(move |e| {
                metrics_for_err.$metric_problems.inc();
                e
            })
    }};
}

fn ensure_subgraph(
    logger: Logger,
    store: Arc<dyn NetworkStore>,
    subgraph_name: SubgraphName,
    subgraph_id: SubgraphDeploymentId,
    start_block: Option<EthereumBlockPointer>,
    network_name: String,
) -> EnsureSubgraphFuture {
    Box::new(subgraph::ensure_subgraph_exists(
        subgraph_name,
        subgraph_id,
        logger,
        store,
        start_block,
        network_name,
    ))
}

fn load_local_head(context: &Context) -> LocalHeadFuture {
    Box::new(track_future!(
        context.metrics,
        load_local_head,
        load_local_head_problems,
        future::result(context.store.clone().block_ptr(context.subgraph_id.clone()))
    ))
}

fn poll_chain_head(context: &Context) -> ChainHeadFuture {
    let section = context.metrics.stopwatch.start_section("chain_head");

    Box::new(
        track_future!(
            context.metrics,
            poll_chain_head,
            poll_chain_head_problems,
            context
                .adapter
                .clone()
                .latest_block(&context.logger)
                .from_err()
        )
        .inspect(move |_| section.end()),
    )
}

fn fetch_ommers(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    metrics: Arc<NetworkIndexerMetrics>,
    block: &EthereumBlock,
) -> OmmersFuture {
    let block_ptr: EthereumBlockPointer = block.into();
    let ommer_hashes = block.block.uncles.clone();

    Box::new(track_future!(
        metrics,
        fetch_ommers,
        fetch_ommers_problems,
        adapter
            .uncles(&logger, &block.block)
            .and_then(move |ommers| {
                let (found, missing): (Vec<(usize, Option<_>)>, Vec<(usize, Option<_>)>) = ommers
                    .into_iter()
                    .enumerate()
                    .partition(|(_, ommer)| ommer.is_some());
                if missing.len() > 0 {
                    let missing_hashes = missing
                        .into_iter()
                        .map(|(index, _)| ommer_hashes.get(index).unwrap())
                        .map(|hash| format!("{:x}", hash))
                        .collect::<Vec<_>>();

                    // Fail if we couldn't fetch all ommers
                    future::err(format_err!(
                        "Ommers of block {} missing: {}",
                        block_ptr,
                        missing_hashes.join(", ")
                    ))
                } else {
                    future::ok(
                        found
                            .into_iter()
                            .map(|(_, ommer)| Ommer(ommer.unwrap()))
                            .collect(),
                    )
                }
            })
    ))
}

fn fetch_block_and_ommers_by_number(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    metrics: Arc<NetworkIndexerMetrics>,
    block_number: u64,
) -> BlockFuture {
    let logger_for_err = logger.clone();

    let logger_for_full_block = logger.clone();
    let adapter_for_full_block = adapter.clone();

    let logger_for_ommers = logger.clone();
    let adapter_for_ommers = adapter.clone();

    let metrics_for_full_block = metrics.clone();
    let metrics_for_ommers = metrics.clone();

    let section = metrics.stopwatch.start_section("fetch_blocks");

    Box::new(
        track_future!(
            metrics,
            fetch_block_by_number,
            fetch_block_by_number_problems,
            adapter
                .clone()
                .block_by_number(&logger, block_number)
                .from_err()
        )
        .and_then(move |block| match block {
            None => {
                debug!(
                    logger_for_err,
                    "Block not found on chain";
                    "block" => format!("#{}", block_number),
                );

                Box::new(future::ok(None)) as Box<dyn Future<Item = _, Error = _> + Send>
            }

            Some(block) => Box::new(
                track_future!(
                    metrics_for_full_block,
                    fetch_full_block,
                    fetch_full_block_problems,
                    adapter_for_full_block
                        .load_full_block(&logger_for_full_block, block)
                        .from_err()
                )
                .and_then(move |block| {
                    fetch_ommers(
                        logger_for_ommers.clone(),
                        adapter_for_ommers,
                        metrics_for_ommers,
                        &block,
                    )
                    .then(move |result| {
                        future::ok(match result {
                            Ok(ommers) => Some(BlockWithOmmers { block, ommers }),
                            Err(e) => {
                                debug!(
                                    logger_for_ommers,
                                    "Failed to fetch ommers for block";
                                    "error" => format!("{}", e),
                                    "block" => format!("{}", EthereumBlockPointer::from(block)),
                                );

                                None
                            }
                        })
                    })
                }),
            ),
        })
        .then(move |result| {
            section.end();
            result
        }),
    )
}

fn fetch_blocks(context: &Context, block_numbers: Range<u64>) -> BlockStream {
    let logger = context.logger.clone();
    let adapter = context.adapter.clone();
    let metrics = context.metrics.clone();

    Box::new(
        futures::stream::iter_ok::<_, Error>(block_numbers)
            .map(move |block_number| {
                fetch_block_and_ommers_by_number(
                    logger.clone(),
                    adapter.clone(),
                    metrics.clone(),
                    block_number,
                )
            })
            .buffered(100)
            // Terminate the stream at the first block that couldn't be found on chain.
            .take_while(|block| future::ok(block.is_some()))
            // Pull blocks out of the options now that we know they are `Some`.
            .map(|block| block.unwrap()),
    )
}

fn write_block(block_writer: Arc<BlockWriter>, block: BlockWithOmmers) -> AddBlockFuture {
    Box::new(block_writer.write(block))
}

fn load_parent_block_from_store(
    context: &Context,
    block_ptr: EthereumBlockPointer,
) -> BlockPointerFuture {
    let block_ptr_for_missing_parent = block_ptr.clone();
    let block_ptr_for_invalid_parent = block_ptr.clone();

    Box::new(
        // Load the block itself from the store
        future::result(
            context
                .store
                .clone()
                .get(block_ptr.to_entity_key(context.subgraph_id.clone()))
                .map_err(|e| e.into())
                .and_then(|entity| {
                    entity.ok_or_else(|| format_err!("block {} is missing in store", block_ptr))
                }),
        )
        // Get the parent hash from the block
        .and_then(move |block| {
            future::result(
                block
                    .get("parent")
                    .ok_or_else(move || {
                        format_err!("block {} has no parent", block_ptr_for_missing_parent,)
                    })
                    .and_then(|value| {
                        let s = value
                            .clone()
                            .as_string()
                            .expect("the `parent` field of `Block` is a reference/string");
                        H256::from_str(s.as_str()).map_err(|e| {
                            format_err!(
                                "block {} has an invalid parent `{}`: {}",
                                block_ptr_for_invalid_parent,
                                s,
                                e,
                            )
                        })
                    }),
            )
        })
        .map(move |parent_hash: H256| {
            // Create a block pointer for the parent
            EthereumBlockPointer {
                number: block_ptr.number - 1,
                hash: parent_hash,
            }
        }),
    )
}

fn revert_local_head(context: &Context, local_head: EthereumBlockPointer) -> RevertLocalHeadFuture {
    debug!(
        context.logger,
        "Revert local head block";
        "block" => format!("{}", local_head),
    );

    let store = context.store.clone();
    let event_sink = context.event_sink.clone();
    let subgraph_id = context.subgraph_id.clone();

    let logger_for_complete = context.logger.clone();

    let logger_for_revert_err = context.logger.clone();
    let local_head_for_revert_err = local_head.clone();

    let logger_for_send_err = context.logger.clone();

    Box::new(track_future!(
        context.metrics,
        revert_local_head,
        revert_local_head_problems,
        load_parent_block_from_store(context, local_head.clone())
            .and_then(move |parent_block| {
                future::result(
                    store
                        .clone()
                        .revert_block_operations(
                            subgraph_id.clone(),
                            local_head.clone(),
                            parent_block.clone(),
                        )
                        .map_err(|e| e.into())
                        .map(|_| (local_head, parent_block)),
                )
            })
            .map_err(move |e| {
                debug!(
                    logger_for_revert_err,
                    "Failed to revert local head block";
                    "error" => format!("{}", e),
                    "block" => format!("{}", local_head_for_revert_err),
                );

                // Instead of an error we return the old local head to stay on that.
                local_head_for_revert_err
            })
            .and_then(move |(from, to)| {
                let to_for_send_err = to.clone();
                send_event(
                    event_sink,
                    NetworkIndexerEvent::Revert {
                        from: from.clone(),
                        to: to.clone(),
                    },
                )
                .map(move |_| to)
                .map_err(move |e| {
                    debug!(
                        logger_for_send_err,
                        "Failed to send revert event";
                        "error" => format!("{}", e),
                        "to" => format!("{}", to_for_send_err),
                        "from" => format!("{}", from),
                    );

                    // Instead of an error we return the new local head; we've
                    // already reverted it in the store, so we _have_ to switch.
                    to_for_send_err
                })
            })
            .inspect(move |_| {
                debug!(logger_for_complete, "Reverted local head block");
            })
            .then(|result| {
                match result {
                    Ok(block) => future::ok(block),
                    Err(block) => future::ok(block),
                }
            })
    ))
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
 * Helpers for metrics
 */
fn update_chain_and_local_head_metrics(
    context: &Context,
    chain_head: &LightEthereumBlock,
    local_head: Option<EthereumBlockPointer>,
) {
    context
        .metrics
        .chain_head
        .set(chain_head.number.unwrap().as_u64() as f64);
    context
        .metrics
        .local_head
        .set(local_head.map_or(0u64, |ptr| ptr.number) as f64);
}

/**
 * Network tracer implementation.
 */

/// Context for the network tracer.
pub struct Context {
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    store: Arc<dyn NetworkStore>,
    metrics: Arc<NetworkIndexerMetrics>,
    block_writer: Arc<BlockWriter>,
    event_sink: Sender<NetworkIndexerEvent>,
    subgraph_name: SubgraphName,
    subgraph_id: SubgraphDeploymentId,
    start_block: Option<EthereumBlockPointer>,
    network_name: String,
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
            NetworkIndexerEvent::Revert { from, to } => {
                write!(f, "Revert from {} to {}", &from, &to,)
            }
            NetworkIndexerEvent::AddBlock(block) => write!(f, "Add block {}", block),
        }
    }
}

/// State machine that handles block fetching and block reorganizations.
#[derive(StateMachineFuture)]
#[state_machine_future(context = "Context")]
enum StateMachine {
    /// The indexer start in an empty state and immediately moves on to
    /// ensuring that the network subgraph exists.
    #[state_machine_future(start, transitions(EnsureSubgraph))]
    Start,

    /// This state ensures that the network subgraph that stores the
    /// indexed data exists, and creates it if necessary.
    #[state_machine_future(transitions(LoadLocalHead))]
    EnsureSubgraph {
        ensure_subgraph: EnsureSubgraphFuture,
    },

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
        prev_chain_head: Option<EthereumBlockPointer>,
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
    ///   ```ignore
    ///   a---b---c---x
    ///       \
    ///        +--d---e---f
    ///   ```
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
    ///
    ///   We don't actually do all of the above explicitly. What we do instead
    ///   is that when we encounter a block that requires a reorg, we revert the
    ///   local head, moving back by one block in the indexed data. We then poll
    ///   the chain head again, fetch up to 100 blocks, vet the next block
    ///   again, and see if we have reverted back to the common ancestor yet. If
    ///   we have, we process the next block. If we haven't, we repeat reverting
    ///   the local head. Ultimately, this will take us back to the common
    ///   ancestor, and at that point we can move forward again.
    #[state_machine_future(transitions(RevertLocalHead, AddBlock, PollChainHead, Failed))]
    VetBlock {
        local_head: Option<EthereumBlockPointer>,
        chain_head: LightEthereumBlock,
        next_blocks: BlockStream,
        block: BlockWithOmmers,
    },

    /// This state reverts the local head, moving the local indexed data
    /// back by one block.
    ///
    /// After reverting, the local head is updated to the previous local
    /// head.
    ///
    /// If reverting fails, the local head remains unchanged. If reverting
    /// succeeds but sending out the revert event for the block fails, the
    /// local head is still moved back to its parent. At this point the
    /// block has been reverted in the store, so it's too late.
    /// Note: This means that if an event does not arrive at one of the
    /// consumers of the indexer events, these consumers will have to
    /// reconcile what they know with what is in the store as they go.
    ///
    /// After reverting (or failing to revert), the indexer polls the chain head
    /// again to decide what blocks to fetch and process next.
    #[state_machine_future(transitions(PollChainHead, Failed))]
    RevertLocalHead {
        chain_head: LightEthereumBlock,
        local_head: Option<EthereumBlockPointer>,
        new_local_head: RevertLocalHeadFuture,
    },

    /// This state waits until a block has been written and an event for it
    /// has been sent out. After that, the indexer continues processing the
    /// next block. If anything goes wrong at this point, it's back to
    /// re-evaluating the chain head and fetching (potentially) different
    /// blocks for indexing.
    #[state_machine_future(transitions(ProcessBlocks, LoadLocalHead, Failed))]
    AddBlock {
        chain_head: LightEthereumBlock,
        next_blocks: BlockStream,
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

        info!(context.logger, "Ensure that the network subgraph exists");

        transition!(EnsureSubgraph {
            ensure_subgraph: ensure_subgraph(
                context.logger.clone(),
                context.store.clone(),
                context.subgraph_name.clone(),
                context.subgraph_id.clone(),
                context.start_block.clone(),
                context.network_name.clone(),
            )
        })
    }

    fn poll_ensure_subgraph<'a, 'c>(
        state: &'a mut RentToOwn<'a, EnsureSubgraph>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterEnsureSubgraph, Error> {
        // Abort if the output stream has been closed. Depending on how the
        // network indexer is wired up, this could mean that the system shutting
        // down.
        try_ready!(context.event_sink.poll_ready());

        // Ensure the subgraph exists; if creating it fails, fail the indexer
        try_ready!(state.ensure_subgraph.poll());

        info!(context.logger, "Start indexing network data");

        // Start by loading the local head from the store. This is the most
        // recent block we managed to index until now.
        transition!(LoadLocalHead {
            local_head: load_local_head(context)
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
            prev_chain_head: None,
            chain_head: poll_chain_head(context),
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
                        "block" => chain_head.format(),
                    );

                    // Chain head was invalid, try getting a better one.
                    transition!(PollChainHead {
                        local_head: state.local_head,
                        prev_chain_head: state.prev_chain_head,
                        chain_head: poll_chain_head(context,),
                    })
                }

                let state = state.take();

                if Some((&chain_head).into()) != state.prev_chain_head {
                    context
                        .metrics
                        .last_new_chain_head_time
                        .set(Utc::now().timestamp() as f64)
                }

                update_chain_and_local_head_metrics(context, &chain_head, state.local_head);

                debug!(
                    context.logger,
                    "Identify next blocks to index";
                    "chain_head" => chain_head.format(),
                    "local_head" => state.local_head.map_or(
                        String::from("none"), |ptr| format!("{}", ptr)
                    ),
                );

                // If we're already at the chain head, keep polling it.
                if Some((&chain_head).into()) == state.local_head {
                    debug!(
                        context.logger,
                        "Already at chain head; poll chain head again";
                        "chain_head" => chain_head.format(),
                        "local_head" => state.local_head.map_or(
                            String::from("none"), |ptr| format!("{}", ptr)
                        ),
                    );

                    // Chain head wasn't new, try getting a new one.
                    transition!(PollChainHead {
                        local_head: state.local_head,
                        prev_chain_head: Some(chain_head.into()),
                        chain_head: poll_chain_head(context),
                    });
                }

                // Ignore the chain head if its number is below the current local head;
                // it would mean we're switching to a shorter chain, which makes no sense
                if chain_head.number.unwrap().as_u64()
                    < state.local_head.map_or(0u64, |ptr| ptr.number)
                {
                    debug!(
                        context.logger,
                        "Chain head is for a shorter chain; poll chain head again";
                        "local_head" => state.local_head.map_or(
                            String::from("none"), |ptr| format!("{}", ptr)
                        ),
                        "chain_head" => chain_head.format(),
                    );

                    transition!(PollChainHead {
                        local_head: state.local_head,
                        prev_chain_head: state.prev_chain_head,
                        chain_head: poll_chain_head(context),
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
                    "chain_head" => format!("{}", chain_head.format()),
                    "local_head" => state.local_head.map_or(
                        String::from("none"), |ptr| format!("{}", ptr)
                    ),
                    "range" => format!("[#{}..#{}]", block_numbers.start, block_numbers.end-1),
                );

                // Processing the blocks in this range.
                transition!(ProcessBlocks {
                    local_head: state.local_head,
                    chain_head,
                    next_blocks: fetch_blocks(context, block_numbers)
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
                    prev_chain_head: state.prev_chain_head,
                    chain_head: poll_chain_head(context),
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
                    prev_chain_head: Some(state.chain_head.into()),
                    chain_head: poll_chain_head(context),
                })
            }

            // There is a block ready to be processed; check whether it is valid
            // and whether it requires a reorg before adding it.
            Ok(Async::Ready(Some(block))) => {
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
                    "Failed to fetch blocks; re-evaluate chain head and try again";
                    "error" => format!("{}", e),
                );

                let state = state.take();

                transition!(PollChainHead {
                    local_head: state.local_head,
                    prev_chain_head: Some(state.chain_head.into()),
                    chain_head: poll_chain_head(context),
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
                "block" => format!("{}", block),
            );

            // The block is invalid, throw away the entire stream and
            // start with re-checking the chain head block again.
            transition!(PollChainHead {
                local_head: state.local_head,
                prev_chain_head: Some(state.chain_head.into()),
                chain_head: poll_chain_head(context),
            })
        }

        // The `PollChainHead` state already guards against indexing shorter
        // chains; this check here is just to catch bugs in the subsequent
        // block fetching.
        let block_number = block.inner().number.unwrap().as_u64();
        match state.local_head {
            None => {
                assert!(
                    block_number == context.start_block.map_or(0u64, |ptr| ptr.number),
                    "first block must match the start block of the network indexer",
                );
            }
            Some(local_head_ptr) => {
                assert!(
                    block_number > local_head_ptr.number,
                    "block with a smaller number than the local head block; \
                     this is a bug in the indexer"
                );
            }
        }

        // Check whether we have a reorg (parent of the new block != our local head).
        if block.inner().parent_ptr() != state.local_head {
            info!(
                context.logger,
                "Block requires a reorg";
                "local_head" => state.local_head.map_or(
                    String::from("none"), |ptr| format!("{}", ptr)
                ),
                "parent" => block.inner().parent_ptr().map_or(
                    String::from("none"), |ptr| format!("{}", ptr)
                ),
                "block" => format!("{}", block),
            );

            let local_head = state
                .local_head
                .expect("cannot have a reorg without a local head block");

            // Update reorg stats
            context.metrics.reorg_count.inc();

            // We are dealing with a reorg; revert the current local head; if this
            // is a reorg of depth 1, this will take the local head back to the common
            // ancestor and we can move forward on the new version of the chain again;
            // if it is a deeper reorg, then we'll be going to revert the local head
            // repeatedly until we're back at the common ancestor.
            transition!(RevertLocalHead {
                local_head: state.local_head,
                chain_head: state.chain_head,
                new_local_head: revert_local_head(context, local_head),
            })
        } else {
            let event_sink = context.event_sink.clone();
            let metrics_for_written_block = context.metrics.clone();

            let section = context.metrics.stopwatch.start_section("transact_block");

            // The block is a regular successor to the local head.
            // Add the block and move on.
            transition!(AddBlock {
                // Carry over the current chain head and the incoming blocks stream.
                chain_head: state.chain_head,
                next_blocks: state.next_blocks,

                // Index the block.
                new_local_head: Box::new(
                    // Write block to the store.
                    track_future!(
                        context.metrics,
                        write_block,
                        write_block_problems,
                        write_block(context.block_writer.clone(), block)
                    )
                    .inspect(move |_| {
                        section.end();

                        metrics_for_written_block
                            .last_written_block_time
                            .set(Utc::now().timestamp() as f64)
                    })
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

    fn poll_revert_local_head<'a, 'c>(
        state: &'a mut RentToOwn<'a, RevertLocalHead>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterRevertLocalHead, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        match state.new_local_head.poll() {
            // Reverting has not finished yet, try again later.
            Ok(Async::NotReady) => Ok(Async::NotReady),

            // The revert finished and the block before the one that got reverted
            // should be become the local head. Poll the chain head again after this
            // to see if there are more blocks to revert or whether we can move forward
            // again.
            Ok(Async::Ready(block_ptr)) => {
                let state = state.take();

                update_chain_and_local_head_metrics(context, &state.chain_head, Some(block_ptr));

                transition!(PollChainHead {
                    local_head: Some(block_ptr),
                    prev_chain_head: Some(state.chain_head.into()),
                    chain_head: poll_chain_head(context),
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
                    prev_chain_head: Some(state.chain_head.into()),
                    chain_head: poll_chain_head(context)
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

                update_chain_and_local_head_metrics(context, &state.chain_head, Some(block_ptr));

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

                transition!(LoadLocalHead {
                    local_head: load_local_head(context)
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
        logger: &Logger,
        adapter: Arc<dyn EthereumAdapter>,
        store: Arc<S>,
        metrics_registry: Arc<dyn MetricsRegistry>,
        subgraph_name: String,
        start_block: Option<EthereumBlockPointer>,
        network_name: String,
    ) -> Self
    where
        S: Store + ChainStore,
    {
        // Create a subgraph name and ID
        let id_str = format!(
            "{}_v{}",
            subgraph_name.replace("/", "_"),
            NETWORK_INDEXER_VERSION
        );
        let subgraph_id = SubgraphDeploymentId::new(id_str).expect("valid network subgraph ID");
        let subgraph_name = SubgraphName::new(subgraph_name).expect("valid network subgraph name");

        let logger = logger.new(o!(
            "component" => "NetworkIndexer",
            "subgraph_name" => subgraph_name.to_string(),
            "subgraph_id" => subgraph_id.to_string(),
        ));

        let logger_for_err = logger.clone();

        let stopwatch = StopwatchMetrics::new(
            logger.clone(),
            subgraph_id.clone(),
            metrics_registry.clone(),
        );

        let metrics = Arc::new(NetworkIndexerMetrics::new(
            &subgraph_id,
            stopwatch.clone(),
            metrics_registry.clone(),
        ));

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
            logger,
            adapter,
            store,
            metrics,
            block_writer,
            event_sink,
            subgraph_name,
            subgraph_id,
            start_block,
            network_name,
        });

        // Launch state machine.
        // Blocking due to store interactions. Won't be blocking after #905.
        graph::spawn_blocking(
            state_machine
                .map_err(move |e| {
                    error!(logger_for_err, "Network indexer failed: {}", e);
                })
                .compat(),
        );

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
