use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::try_ready;
use state_machine_future::*;
use std::fmt;
use std::ops::Range;

use graph::prelude::*;

use super::common::*;

/**
 * Helpers to create futures and streams.
 */

fn fetch_remote_head(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
) -> Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send> {
    Box::new(adapter.latest_block(&logger).from_err().and_then(|block| {
        match (block.number, block.hash) {
            (Some(_), Some(_)) => future::ok(block),
            (number, hash) => future::err(format_err!(
                "remote head block number or hash missing: number = {:?}, hash = {:?}",
                number,
                hash
            )),
        }
    }))
}

fn fetch_block_and_uncles(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    block_number: u64,
) -> impl Future<Item = BlockWithUncles, Error = Error> {
    let logger_for_full_block = logger.clone();
    let adapter_for_full_block = adapter.clone();

    let logger_for_uncles = logger.clone();
    let adapter_for_uncles = adapter.clone();

    adapter
        .block_by_number(&logger, block_number)
        .from_err()
        .and_then(move |block| {
            let block = block.expect("received no block for number");

            adapter_for_full_block
                .load_full_block(&logger_for_full_block, block)
                .from_err()
        })
        .and_then(move |block| {
            adapter_for_uncles
                .uncles(&logger_for_uncles, &block.block)
                .and_then(move |uncles| future::ok(BlockWithUncles { block, uncles }))
        })
}

fn fetch_blocks(
    logger: Logger,
    adapter: Arc<dyn EthereumAdapter>,
    block_numbers: Range<u64>,
) -> Box<dyn Stream<Item = BlockWithUncles, Error = Error> + Send> {
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
) -> Box<impl Future<Item = (), Error = Error> + Send> {
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
#[derive(Debug)]
pub enum NetworkTracerEvent {
    RevertTo { block: BlockWithUncles },
    AddBlocks { blocks: Vec<BlockWithUncles> },
}

impl fmt::Display for NetworkTracerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkTracerEvent::RevertTo { block } => write!(
                f,
                "(revert-to, {} [{}])",
                block.inner().number.unwrap(),
                block.inner().hash.unwrap()
            ),
            NetworkTracerEvent::AddBlocks { blocks } => write!(
                f,
                "(add-blocks, {} block(s) from {} [{}] to {} [{}])",
                blocks.len(),
                blocks[0].inner().number.unwrap(),
                blocks[0].inner().hash.unwrap(),
                blocks[blocks.len() - 1].inner().number.unwrap(),
                blocks[blocks.len() - 1].inner().hash.unwrap(),
            ),
        }
    }
}

#[derive(StateMachineFuture)]
#[state_machine_future(context = "Context")]
enum StateMachine {
    #[state_machine_future(start, transitions(IdentifyLocalHead))]
    Start,

    #[state_machine_future(transitions(IdentifyRemoteHead, Failed))]
    IdentifyLocalHead {
        local_head: Box<dyn Future<Item = Option<EthereumBlockPointer>, Error = Error> + Send>,
    },

    #[state_machine_future(transitions(FetchBlocks, Failed))]
    IdentifyRemoteHead {
        local_head: Option<EthereumBlockPointer>,
        remote_head: Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send>,
    },

    #[state_machine_future(transitions(CheckBlock, IdentifyRemoteHead, Failed))]
    FetchBlocks {
        local_head: Option<EthereumBlockPointer>,
        remote_head: LightEthereumBlock,
        fetched_blocks: Box<dyn Stream<Item = BlockWithUncles, Error = Error> + Send>,
    },

    #[state_machine_future(transitions(FindBranchBase, EmitEvents, Failed))]
    CheckBlock {
        local_head: Option<EthereumBlockPointer>,
        remote_head: LightEthereumBlock,
        fetched_blocks: Box<dyn Stream<Item = BlockWithUncles, Error = Error> + Send>,
        block: BlockWithUncles,
    },

    #[state_machine_future(transitions(FetchMissingBlocks, Failed))]
    FindBranchBase {
        local_block: BlockWithUncles,
        forked_block: BlockWithUncles,
    },

    #[state_machine_future(transitions(EmitEvents, Failed))]
    FetchMissingBlocks {
        block_before: BlockWithUncles,
        block_after: BlockWithUncles,
    },

    #[state_machine_future(transitions(FetchBlocks, Failed))]
    EmitEvents {
        local_head: Option<EthereumBlockPointer>,
        remote_head: LightEthereumBlock,
        fetched_blocks: Box<dyn Stream<Item = BlockWithUncles, Error = Error> + Send>,
        sending: Box<dyn Future<Item = (), Error = Error> + Send>,
    },

    #[state_machine_future(ready)]
    Ready(()),

    #[state_machine_future(error)]
    Failed(Error),
}

impl PollStateMachine for StateMachine {
    fn poll_start<'a, 'c>(
        _state: &'a mut RentToOwn<'a, Start>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterStart, Error> {
        debug!(context.logger, "Start");

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

        let remote_head = try_ready!(state.remote_head.poll());

        // Pull number out of the remote head
        let remote_head_number = remote_head.number.unwrap().as_u64();

        let state = state.take();

        // Calculate the number of blocks remaining before we are in sync with the
        // remote; fetch no more than 1000 blocks at a time
        let remaining_blocks = remote_head_number - state.local_head.map_or(0u64, |ptr| ptr.number);
        let block_range_size = remaining_blocks.min(1000);
        let block_numbers = state.local_head.map_or(0u64, |ptr| ptr.number + 1)
            ..(state.local_head.map_or(0u64, |ptr| ptr.number + 1) + block_range_size);

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

        transition!(FetchBlocks {
            local_head: state.local_head,
            remote_head,
            fetched_blocks: fetch_blocks(
                context.logger.clone(),
                context.adapter.clone(),
                block_numbers
            )
        })
    }

    fn poll_fetch_blocks<'a, 'c>(
        state: &'a mut RentToOwn<'a, FetchBlocks>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterFetchBlocks, Error> {
        let item = try_ready!(state.fetched_blocks.poll());

        match item {
            None => {
                let state = state.take();

                transition!(IdentifyRemoteHead {
                    local_head: state.local_head,
                    remote_head: fetch_remote_head(context.logger.clone(), context.adapter.clone()),
                })
            }
            Some(block) => {
                let state = state.take();

                transition!(CheckBlock {
                    local_head: state.local_head,
                    remote_head: state.remote_head,
                    fetched_blocks: state.fetched_blocks,
                    block,
                })
            }
        }
    }

    fn poll_check_block<'a, 'c>(
        state: &'a mut RentToOwn<'a, CheckBlock>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterCheckBlock, Error> {
        let state = state.take();

        match (state.block.inner().number, state.block.inner().hash) {
            (Some(_), Some(_)) => {
                // We have a valid block
                debug!(
                    context.logger,
                    "Check block";
                    "block_number" => format!("{}", state.block.inner().number.unwrap()),
                    "block_hash" => format!("{:x}", state.block.inner().hash.unwrap())
                );

                // Check whether we have a reorg (parent of the block != our local head)
                let parent = state.block.inner().parent_ptr();
                if parent != state.local_head {
                    panic!("Reorgs are not implemented yet");
                } else {
                    transition!(EmitEvents {
                        // Advance the local head to the new block
                        local_head: Some(state.block.inner().into()),

                        remote_head: state.remote_head,
                        fetched_blocks: state.fetched_blocks,

                        // Send events on the way
                        sending: send_events(
                            context.event_sink.clone(),
                            vec![NetworkTracerEvent::AddBlocks {
                                blocks: vec![state.block]
                            }]
                        ),
                    })
                }
            }
            (number, hash) => transition!(Failed(format_err!(
                "block number or hash missing: number = {:?}, hash = {:?}",
                number,
                hash
            ))),
        }
    }

    fn poll_find_branch_base<'a, 'c>(
        _state: &'a mut RentToOwn<'a, FindBranchBase>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterFindBranchBase, Error> {
        debug!(context.logger, "Find branch base for reorg");

        unimplemented!();
    }

    fn poll_fetch_missing_blocks<'a, 'c>(
        _state: &'a mut RentToOwn<'a, FetchMissingBlocks>,
        context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterFetchMissingBlocks, Error> {
        debug!(context.logger, "Fetch missing blocks for reorg");

        unimplemented!();
    }

    fn poll_emit_events<'a, 'c>(
        state: &'a mut RentToOwn<'a, EmitEvents>,
        _context: &'c mut RentToOwn<'c, Context>,
    ) -> Poll<AfterEmitEvents, Error> {
        // Remain in this state until all events have been sent
        try_ready!(state.sending.poll());

        let state = state.take();

        transition!(FetchBlocks {
            local_head: state.local_head,
            remote_head: state.remote_head,
            fetched_blocks: state.fetched_blocks,
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
        let context = Context {
            subgraph_id,
            logger,
            adapter,
            store,
            event_sink,
        };
        tokio::spawn(StateMachine::start(context).map_err(move |e| {
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
