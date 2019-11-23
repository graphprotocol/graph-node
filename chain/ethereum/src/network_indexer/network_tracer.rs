use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::try_ready;
use state_machine_future::*;
use std::fmt;
use std::ops::Range;

use graph::prelude::*;

use super::common::*;

/**
 * Helper types.
 */

type LocalHeadFuture = Box<dyn Future<Item = Option<EthereumBlockPointer>, Error = Error> + Send>;
type RemoteHeadFuture = Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send>;
type BlockFuture = Box<dyn Future<Item = Option<BlockWithUncles>, Error = Error> + Send>;
type BlockStream = Box<dyn Stream<Item = Option<BlockWithUncles>, Error = Error> + Send>;
type EmitEventsFuture = Box<dyn Future<Item = (), Error = Error> + Send>;
type ForkPathStream = Box<dyn Stream<Item = LightEthereumBlock>, Error = Error> + Send>;

/**
 * Helpers to create futures and streams.
 */

fn fetch_remote_head(logger: Logger, adapter: Arc<dyn EthereumAdapter>) -> RemoteHeadFuture {
    Box::new(adapter.latest_block(&logger).from_err())
}

fn fetch_block_and_uncles(
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

fn fetch_blocks(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    block_numbers: Range<u64>,
) -> BlockStream {
    Box::new(
        futures::stream::iter_ok::<_, Error>(block_numbers)
            .map(move |block_number| {
                fetch_block_and_uncles(logger.clone(), adapter.clone(), block_number)
            })
            .buffered(100),
    )
}

fn send_events(
    event_sink: Sender<NetworkTracerEvent>,
    events: Vec<NetworkTracerEvent>,
) -> EmitEventsFuture {
    Box::new(
        event_sink
            .send_all(futures::stream::iter_ok(events))
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
    event_sink: Sender<NetworkTracerEvent>,
}

/// Events emitted by the network tracer.
#[derive(Debug, PartialEq)]
pub enum NetworkTracerEvent {
    RevertTo { block: BlockWithUncles },
    AddBlocks { blocks: Vec<BlockWithUncles> },
}

impl fmt::Display for NetworkTracerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkTracerEvent::RevertTo { block } => write!(
                f,
                "Revert to: {} [{:x}]",
                block.inner().number.unwrap(),
                block.inner().hash.unwrap()
            ),
            NetworkTracerEvent::AddBlocks { blocks } => {
                if blocks.len() == 1 {
                    write!(
                        f,
                        "Add blocks: {} [{:x}]",
                        blocks[0].inner().number.unwrap(),
                        blocks[0].inner().hash.unwrap()
                    )
                } else {
                    write!(
                        f,
                        "Add blocks: {} [{:x}], ...,  {} [{}] ({:x} blocks)",
                        blocks[0].inner().number.unwrap(),
                        blocks[0].inner().hash.unwrap(),
                        blocks[blocks.len() - 1].inner().number.unwrap(),
                        blocks[blocks.len() - 1].inner().hash.unwrap(),
                        blocks.len(),
                    )
                }
            }
        }
    }
}

/// State machine that handles block fetching and block reorganizations.
#[derive(StateMachineFuture)]
#[state_machine_future(context = "Context")]
enum StateMachine {
    /// We start with an empty state, which immediately moves on
    /// to identifying the local head block of the network subgraph.
    #[state_machine_future(start, transitions(IdentifyLocalHead))]
    Start,

    /// This state waits until we have the local head block (we get
    /// its pointer from the store), then moves on to identifying
    /// the remote head block (the latest block on the network).
    #[state_machine_future(transitions(IdentifyRemoteHead, Failed))]
    IdentifyLocalHead { local_head: LocalHeadFuture },

    /// This state waits until the remote head block is available.
    /// Once we have this (local head, remote head) pair, we can fetch
    /// the blocks (local head)+1, (local head)+2, ..., (remote head).
    /// We do this in smaller chunks however, for two reasons:
    ///
    /// 1. To limit the amount of blocks we keep in memory.
    /// 2. To be able to check for reorgs frequently.
    ///
    /// From this state, we move on by deciding on a range of blocks
    /// and creating a stream to pull these in with some parallelization.
    /// The next state (`ReadBlocks`) will then read this stream block
    /// by block.
    #[state_machine_future(transitions(ReadBlocks, IdentifyRemoteHead, Failed))]
    IdentifyRemoteHead {
        local_head: Option<EthereumBlockPointer>,
        remote_head: RemoteHeadFuture,
    },

    /// This state takes the first block from the stream. If the stream is
    /// exhausted, it transitions back to re-checking the remote head block
    /// and deciding on the next chunk of blocks to fetch. If there is still
    /// a block to read from the stream, it's passed on to the `CheckBlock`
    /// state for reorg checking.
    #[state_machine_future(transitions(CheckBlock, IdentifyRemoteHead, Failed))]
    ReadBlocks {
        local_head: Option<EthereumBlockPointer>,
        remote_head: LightEthereumBlock,
        incoming_blocks: BlockStream,
    },

    /// This state checks whether the incoming block is the successor
    /// of the local head block. If it is, it is emitted via the `EmitEvents`
    /// state. If it is not a successor then we are dealing with a block
    /// reorg, i.e., a block that is on a fork of the chain.
    ///
    /// Note that by checking parent/child succession, this state ensures
    /// that there are no gaps. So if we are on block `x` and a block `f`
    /// comes in that is not a child, it must be on a fork of the chain,
    /// e.g.:
    ///
    ///    a---b---c---x
    ///        \
    ///         +--d---e---f
    ///
    /// In that case we need to do the following:
    ///
    /// 1. Find the fork base block (in the above example: `b`)
    /// 2. Fetch all blocks on the path between and including that
    ///    fork base block and the incoming block (in the example:
    ///    `b`, `d`, `e` and `f`)
    ///
    /// Once we have all necessary blocks (in the example: `b`, `d`, `e`
    /// and `f`), there are two actions we need to perform:
    ///
    /// a. Revert the network data to the fork base block (`b`)
    /// b. Add all blocks after the fork base block, including the
    ///    incoming block, to the indexed network data (`d`, `e` and `f`)
    ///
    /// Steps 1 and 2 are performed by identifying the incoming
    /// block as a reorg and transitioning to the `FindForkBase`
    /// state. Once that has completed the above steps, it will
    /// emit events for a) and b).
    #[state_machine_future(transitions(FindForkBase, EmitEvents, IdentifyRemoteHead, Failed))]
    CheckBlock {
        local_head: Option<EthereumBlockPointer>,
        remote_head: LightEthereumBlock,
        incoming_blocks: BlockStream,
        block: BlockWithUncles,
    },

    /// Given a block identify as being on a fork of the chain,
    /// this state tries to identify the fork base block and collect
    /// all blocks on the path from the incoming block to the fork
    /// base.
    ///
    /// If successful, it moves on to emitting events for
    /// the revert and for adding the new blocks via the `EmitEvents`
    /// state. If not successful, the state machine resets to the
    /// `IdentifyRemoteHead` block to try again.
    ///
    /// Note: This state carries over the incoming block stream to
    /// not lose the blocks in it. This is because even if there was
    /// a reorg, the blocks following the current block that made us
    /// detect it will likely be valid successors. So once the reorg
    /// information has been collected and RevertTo/AddBlocks events
    /// have been emitted for it, we should be able to continue with
    /// processing the remaining blocks on the stream.
    ///
    /// Only when we reset back to `IdentifyRemoteHead` do we throw
    /// away the stream in the hope that we'll get a better remote
    /// head with different blocks leading up to it.
    #[state_machine_future(transitions(EmitEvents, IdentifyRemoteHead, Failed))]
    FindForkBase {
        local_head: Option<EthereumBlockPointer>,
        remote_head: LightEthereumBlock,
        forked_block: BlockWithUncles,
        incoming_blocks: BlockStream,
        fork_path: ForkPathStream,
    },

    /// Waits until all events have been emitted/sent, then transition
    /// back to processing blocks from the open stream of incoming blocks.
    #[state_machine_future(transitions(ReadBlocks, Failed))]
    EmitEvents {
        local_head: Option<EthereumBlockPointer>,
        remote_head: LightEthereumBlock,
        incoming_blocks: BlockStream,
        emitting: EmitEventsFuture,
    },

    /// This is unused, the indexing never ends.
    #[state_machine_future(ready)]
    Ready(()),

    /// State for fatal errors that cause the indexing to terminate.
    /// This should almost never happen. If it does, it should cause
    /// the entire node to crash / restart.
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollStateMachine for StateMachine {
    fn poll_start<'a, 'c>(
        _state: &'a mut RentToOwn<'a, Start>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterStart, Error> {
        debug!(context.logger, "Start");

        // Start by pulling the local head from the store. This is the most
        // recent block we managed to index until now.
        transition!(IdentifyLocalHead {
            local_head: Box::new(future::result(
                context.store.clone().block_ptr(context.subgraph_id.clone())
            ))
        })
    }

    fn poll_identify_local_head<'a, 'c>(
        state: &'a mut RentToOwn<'a, IdentifyLocalHead>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterIdentifyLocalHead, Error> {
        debug!(context.logger, "Identify local head");

        // Once we have the local head, move on and identify the latest block
        // that's on chain.
        let local_head = try_ready!(state.local_head.poll());
        transition!(IdentifyRemoteHead {
            local_head,
            remote_head: fetch_remote_head(context.logger.clone(), context.adapter.clone()),
        })
    }

    fn poll_identify_remote_head<'a, 'c>(
        state: &'a mut RentToOwn<'a, IdentifyRemoteHead>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterIdentifyRemoteHead, Error> {
        debug!(context.logger, "Identify remote head");

        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        // Wait until we have the remote head.
        let remote_head = try_ready!(state.remote_head.poll());

        // Validate the remote head.
        if remote_head.number.is_none() || remote_head.hash.is_none() {
            warn!(
                context.logger,
                "Remote head block number or hash missing; trying again";
                "block_number" => format!("{:?}", remote_head.number),
                "block_hash" => format!("{:?}", remote_head.hash),
            );

            transition!(IdentifyRemoteHead {
                local_head: state.local_head,
                remote_head: fetch_remote_head(context.logger.clone(), context.adapter.clone()),
            })
        }

        let state = state.take();

        // Pull number out of the local and remote head; we can safely do that now.
        let remote_head_number = remote_head.number.unwrap().as_u64();
        let next_block_number = state.local_head.map_or(0u64, |ptr| ptr.number + 1);

        // Calculate the number of blocks remaining before we are in sync with the
        // remote; fetch no more than 1000 blocks at a time
        let remaining_blocks = remote_head_number + 1 - next_block_number;
        let block_range_size = remaining_blocks.min(1000);
        let block_numbers = next_block_number..(next_block_number + block_range_size);

        debug!(
            context.logger,
            "Fetch {} of {} remaining blocks ({:?}]",
            block_range_size, remaining_blocks, block_numbers;
            "local_head" => format!(
                "({}, {})",
                state.local_head.map_or("none".into(), |ptr| format!("{}", ptr.number)),
                state.local_head.map_or("none".into(), |ptr| format!("{:?}", ptr.hash))
            ),
            "remote_head" => format!(
                "({}, {:?})",
                remote_head.number.unwrap(),
                remote_head.hash.unwrap()
            ),
        );

        // Fetch this block range
        transition!(ReadBlocks {
            local_head: state.local_head,
            remote_head,
            incoming_blocks: fetch_blocks(
                context.logger.clone(),
                context.adapter.clone(),
                block_numbers
            )
        })
    }

    fn poll_read_blocks<'a, 'c>(
        state: &'a mut RentToOwn<'a, ReadBlocks>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterReadBlocks, Error> {
        debug!(context.logger, "Read blocks");

        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        // Wait until there is either an incoming block ready to be processed or
        // the stream is exhausted.
        let item = try_ready!(state.incoming_blocks.poll());

        match item {
            // The stream is exhausted, update the remote head and fetch the
            // next range of blocks for processing.
            None => {
                let state = state.take();

                transition!(IdentifyRemoteHead {
                    local_head: state.local_head,
                    remote_head: fetch_remote_head(context.logger.clone(), context.adapter.clone()),
                })
            }
            // The next block could not be fetched, try starting over
            // from a fresh remote head
            Some(None) => {
                let state = state.take();

                transition!(IdentifyRemoteHead {
                    local_head: state.local_head,
                    remote_head: fetch_remote_head(context.logger.clone(), context.adapter.clone()),
                })
            }

            // There is an incoming block ready to be processed;
            // check it before emitting it to the rest of the system
            Some(Some(block)) => {
                let state = state.take();

                transition!(CheckBlock {
                    local_head: state.local_head,
                    remote_head: state.remote_head,
                    incoming_blocks: state.incoming_blocks,
                    block,
                })
            }
        }
    }

    fn poll_check_block<'a, 'c>(
        state: &'a mut RentToOwn<'a, CheckBlock>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterCheckBlock, Error> {
        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        let state = state.take();
        let block = state.block;

        debug!(
            context.logger,
            "Check block";
            "block_number" => format!("{:?}", block.inner().number),
            "block_hash" => format!("{:?}", block.inner().hash),
        );

        // Validate the block.
        if block.inner().number.is_none() || block.inner().hash.is_none() {
            warn!(
                context.logger,
                "Block number or hash missing; trying again";
                "block_number" => format!("{:?}", block.inner().number),
                "block_hash" => format!("{:?}", block.inner().hash),
            );

            // The block is invalid, throw away the entire stream and
            // start with re-checking the remote head block again.
            transition!(IdentifyRemoteHead {
                local_head: state.local_head,
                remote_head: fetch_remote_head(context.logger.clone(), context.adapter.clone()),
            })
        }

        // If we encounter a block that has a smaller number than our
        // local head block, then we throw away the block stream and
        // try to start over with a fresh remote head block.
        let block_number = block.inner().number.unwrap().as_u64();
        let local_head_number = state.local_head.map_or(0u64, |ptr| ptr.number);
        if block_number < local_head_number {
            transition!(IdentifyRemoteHead {
                local_head: state.local_head,
                remote_head: fetch_remote_head(context.logger.clone(), context.adapter.clone()),
            })
        }

        // Check whether we have a reorg (parent of the block != our local head).
        if block.inner().parent_ptr() != state.local_head {
            transition!(FindForkBase {
                local_head: state.local_head,
                remote_head: state.remote_head,
                incoming_blocks: state.incoming_blocks,
                forked_block: block,
                fork_path: trace_fork_path()
            })
        } else {
            transition!(EmitEvents {
                // Advance the local head to the new block
                local_head: Some(block.inner().into()),

                // Carry over the current remote head and the
                // incoming blocks stream.
                remote_head: state.remote_head,
                incoming_blocks: state.incoming_blocks,

                // Send events on the way
                emitting: send_events(
                    context.event_sink.clone(),
                    vec![NetworkTracerEvent::AddBlocks {
                        blocks: vec![block]
                    }]
                ),
            })
        }
    }

    fn poll_find_fork_base<'a, 'c>(
        _state: &'a mut RentToOwn<'a, FindForkBase>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterFindForkBase, Error> {
        debug!(context.logger, "Find fork base for reorg");

        unimplemented!();
    }

    fn poll_emit_events<'a, 'c>(
        state: &'a mut RentToOwn<'a, EmitEvents>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterEmitEvents, Error> {
        debug!(context.logger, "Emit events");

        // Abort if the output stream has been closed.
        try_ready!(context.event_sink.poll_ready());

        // Remain in this state until all events have been emitted
        try_ready!(state.emitting.poll());

        let state = state.take();

        // We may have more incoming blocks to process, continue with that.
        transition!(ReadBlocks {
            local_head: state.local_head,
            remote_head: state.remote_head,
            incoming_blocks: state.incoming_blocks,
        })
    }
}

pub struct NetworkTracer {
    output: Option<Receiver<NetworkTracerEvent>>,
}

impl NetworkTracer {
    pub fn new(
        subgraph_id: SubgraphDeploymentId,
        logger: &Logger,
        adapter: Arc<dyn EthereumAdapter>,
        store: Arc<dyn Store>,
        _metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let logger = logger.new(o!("component" => "NetworkTracer"));
        let logger_for_err = logger.clone();

        // Create a channel for emitting events
        let (event_sink, output) = channel(100);

        // Create state machine that emits block and revert events for the network
        let state_machine = StateMachine::start(Context {
            subgraph_id,
            logger,
            adapter,
            store,
            event_sink,
        });

        // Launch state machine
        tokio::spawn(state_machine.map_err(move |e| {
            error!(logger_for_err, "Network tracer failed: {}", e);
        }));

        Self {
            output: Some(output),
        }
    }
}

impl EventProducer<NetworkTracerEvent> for NetworkTracer {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = NetworkTracerEvent, Error = ()> + Send>> {
        self.output
            .take()
            .map(|s| Box::new(s) as Box<dyn Stream<Item = NetworkTracerEvent, Error = ()> + Send>)
    }
}
