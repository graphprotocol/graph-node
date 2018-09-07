use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::sync::Mutex;
use web3::types::Block;
use web3::types::Transaction;

use graph::components::ethereum::EthereumEventFilter;
use graph::components::store::HeadBlockUpdateEvent;
use graph::components::subgraph::RuntimeHostEvent;
use graph::components::subgraph::SubgraphProviderEvent;
use graph::prelude::*;

// TODO choose a good number
const REORG_THRESHOLD: u64 = 300;

pub struct RuntimeManager {
    logger: Logger,
    input: Sender<SubgraphProviderEvent>,
}

impl RuntimeManager where {
    /// Creates a new runtime manager.
    pub fn new<S, E, T>(
        logger: &Logger,
        store: Arc<Mutex<S>>,
        eth_adapter: Arc<Mutex<E>>,
        host_builder: T,
    ) -> Self
    where
        S: Store + 'static,
        E: EthereumAdapter,
        T: RuntimeHostBuilder,
    {
        let logger = logger.new(o!("component" => "RuntimeManager"));

        // Create channel for receiving subgraph provider events.
        let (subgraph_sender, subgraph_receiver) = channel(100);

        // Handle incoming events from the subgraph provider.
        Self::handle_subgraph_events(
            logger.clone(),
            store,
            eth_adapter,
            host_builder,
            subgraph_receiver,
        );

        RuntimeManager {
            logger,
            input: subgraph_sender,
        }
    }

    /// Handle incoming events from subgraph providers.
    fn handle_subgraph_events<S, E, T>(
        logger: Logger,
        store: Arc<Mutex<S>>,
        eth_adapter: Arc<Mutex<E>>,
        mut host_builder: T,
        receiver: Receiver<SubgraphProviderEvent>,
    ) where
        S: Store + 'static,
        E: EthereumAdapter,
        T: RuntimeHostBuilder,
    {
        // Handles each incoming event from the subgraph.
        fn handle_runtime_event<S: Store + 'static>(store: Arc<Mutex<S>>, event: RuntimeHostEvent) {
            match event {
                RuntimeHostEvent::EntitySet(store_key, entity, block) => {
                    let store = store.lock().unwrap();
                    // TODO this code is incorrect. One TX should be used for entire block.
                    let mut tx = store
                        .begin_transaction(SubgraphId(store_key.subgraph.clone()), block)
                        .unwrap();
                    tx.set(store_key, entity)
                        .expect("Failed to set entity in the store");
                    tx.commit().unwrap();
                }
                RuntimeHostEvent::EntityRemoved(store_key, block) => {
                    let store = store.lock().unwrap();
                    // TODO this code is incorrect. One TX should be used for entire block.
                    let mut tx = store
                        .begin_transaction(SubgraphId(store_key.subgraph.clone()), block)
                        .unwrap();
                    tx.delete(store_key)
                        .expect("Failed to delete entity from the store");
                    tx.commit().unwrap();
                }
            }
        }

        enum EventType {
            Subgraph(SubgraphProviderEvent),
            HeadBlock(HeadBlockUpdateEvent),
        }
        let head_logger = logger.clone();
        let head_block_updates = store
            .lock()
            .unwrap()
            .head_block_updates()
            .map(EventType::HeadBlock)
            .map_err(move |e| {
                error!(head_logger, "head block update stream error: {}", e);
            });
        let subgraph_events = receiver.map(EventType::Subgraph);

        let mut runtime_hosts_by_subgraph = HashMap::new();
        tokio::spawn(
            head_block_updates
                .select(subgraph_events)
                .for_each(move |event| {
                    match event {
                        EventType::Subgraph(SubgraphProviderEvent::SubgraphAdded(manifest)) => {
                            info!(logger, "Host mapping runtimes for subgraph";
                          "location" => &manifest.location);

                            // Add entry to store for subgraph
                            store
                                .lock()
                                .unwrap()
                                .add_subgraph_if_missing(SubgraphId(manifest.id.clone()))
                                .unwrap();

                            // Create a new runtime host for each data source in the subgraph manifest
                            let mut new_hosts = manifest
                                .data_sources
                                .iter()
                                .map(|d| host_builder.build(manifest.clone(), d.clone()))
                                .collect::<Vec<_>>();

                            // Forward events from the runtime host to the store; this
                            // Tokio task will terminate when the corresponding subgraph
                            // is removed and the host and its event sender are dropped
                            for mut new_host in new_hosts.iter_mut() {
                                let store = store.clone();
                                tokio::spawn(new_host.take_event_stream().unwrap().for_each(
                                    move |event| {
                                        handle_runtime_event(store.clone(), event);
                                        Ok(())
                                    },
                                ));
                            }

                            // Add the new hosts to the list of managed runtime hosts
                            runtime_hosts_by_subgraph.insert(manifest.id, new_hosts);
                        }
                        EventType::Subgraph(SubgraphProviderEvent::SubgraphRemoved(id)) => {
                            // Destroy all runtime hosts for this subgraph; this will
                            // also terminate the host's event stream
                            runtime_hosts_by_subgraph.remove(&id);
                        }
                        EventType::HeadBlock(_) => {
                            for (subgraph_id, runtime_hosts) in runtime_hosts_by_subgraph.iter_mut()
                            {
                                handle_head_block_update(
                                    logger.clone(),
                                    store.clone(),
                                    eth_adapter.clone(),
                                    SubgraphId(subgraph_id.to_owned()),
                                    runtime_hosts,
                                ).unwrap();
                            }
                        }
                    }

                    Ok(())
                }),
        );
    }
}

impl EventConsumer<SubgraphProviderEvent> for RuntimeManager {
    /// Get the wrapped event sink.
    fn event_sink(&self) -> Box<Sink<SinkItem = SubgraphProviderEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}

fn handle_head_block_update<S, E, H>(
    logger: Logger,
    store: Arc<Mutex<S>>,
    eth_adapter: Arc<Mutex<E>>,
    subgraph_id: SubgraphId,
    runtime_hosts: &mut [H],
) -> Result<(), Error>
where
    S: Store + 'static,
    E: EthereumAdapter,
    H: RuntimeHost,
{
    // TODO handle VersionConflicts
    // TODO remove .wait()s, maybe?

    info!(
        logger,
        "Handling head block update for subgraph {}", subgraph_id.0
    );

    // Create an event filter that will match any event relevant to this subgraph
    let event_filter = runtime_hosts
        .iter()
        .map(|host| host.event_filter())
        .sum::<EthereumEventFilter>();

    loop {
        // Get pointers from database for comparison
        let head_ptr = store
            .lock()
            .unwrap()
            .head_block_ptr()?
            .expect("should not receive head block update before head block pointer is set");
        let subgraph_ptr = store.lock().unwrap().block_ptr(subgraph_id.clone())?;

        debug!(logger, "head_ptr = {:?}", head_ptr);
        debug!(logger, "subgraph_ptr = {:?}", subgraph_ptr);

        // Only continue if the subgraph block ptr is behind the head block ptr.
        // subgraph_ptr > head_ptr shouldn't happen, but if it does, it's safest to just stop.
        if subgraph_ptr.number >= head_ptr.number {
            break Ok(());
        }

        // Subgraph ptr is behind head ptr.
        // Each loop iteration, we'll move the subgraph ptr one step in the right direction.
        // First question: which direction should the ptr be moved?
        enum Step {
            ToParent,                               // backwards one block
            ToDescendants(Vec<Block<Transaction>>), // forwards, processing one or more blocks
        }
        let step = {
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
                let is_on_main_chain = eth_adapter
                    .lock()
                    .unwrap()
                    .is_on_main_chain(subgraph_ptr)
                    .wait()?;
                if is_on_main_chain {
                    // The subgraph ptr points to a block on the main chain.
                    // This means that the last block we processed does not need to be reverted.
                    // Therefore, our direction of travel will be forward, towards the chain head.

                    // As an optimization, instead of advancing one block, we will use an Ethereum
                    // RPC call to find the first few blocks between the subgraph ptr and the reorg
                    // threshold that has event(s) we are interested in.
                    // Note that we use block numbers here.
                    // This is an artifact of Ethereum RPC limitations.
                    // It is only safe to use block numbers because we are beyond the reorg
                    // threshold.

                    // Start with first block after subgraph ptr
                    let from = subgraph_ptr.number + 1;

                    // End just prior to reorg threshold.
                    // It isn't safe to go any farther due to race conditions.
                    let to = head_ptr.number - REORG_THRESHOLD;

                    debug!(logger, "Finding next blocks with relevant events...");
                    let descendant_ptrs = eth_adapter
                        .lock()
                        .unwrap()
                        .find_first_blocks_with_events(from, to, event_filter.clone())
                        .wait()?;
                    debug!(logger, "Done finding next blocks.");

                    if descendant_ptrs.is_empty() {
                        // No matching events in range.
                        // Therefore, we can update the subgraph ptr without any changes to the
                        // entity data.

                        // We need to look up what block hash corresponds to the block number
                        // `to`.
                        // Again, this is only safe from race conditions due to being beyond
                        // the reorg threshold.
                        let new_ptr = eth_adapter
                            .lock()
                            .unwrap()
                            .block_by_number(to)
                            .wait()?
                            .into();

                        store.lock().unwrap().set_block_ptr_with_no_changes(
                            subgraph_id.clone(),
                            subgraph_ptr,
                            new_ptr,
                        )?;

                        // There were no events to process in this case, so we have already
                        // completed the subgraph ptr step.
                        // Continue outer loop.
                        continue;
                    } else {
                        // The next few interesting blocks are at descendant_ptrs.
                        // In particular, descendant_ptrs is a list of all blocks between
                        // subgraph_ptr and descendant_ptrs.last() that contain relevant events.
                        // This will allow us to advance the subgraph_ptr to descendant_ptrs.last()
                        // while being confident that we did not miss any relevant events.

                        // Load the blocks
                        debug!(
                            logger,
                            "Found {} block(s) with events. Loading blocks...",
                            descendant_ptrs.len()
                        );
                        let descendant_blocks = stream::futures_ordered(
                            descendant_ptrs.into_iter().map(|descendant_ptr| {
                                let eth_adapter = eth_adapter.clone();
                                let store = store.clone();

                                // Try locally first. Otherwise, get block from Ethereum node.
                                let block_result = store.lock().unwrap().block(descendant_ptr.hash);
                                future::result(block_result).and_then(
                                    move |block_from_store| -> Box<Future<Item = _, Error = _>> {
                                        if let Some(block) = block_from_store {
                                            Box::new(future::ok(block))
                                        } else {
                                            eth_adapter
                                                .lock()
                                                .unwrap()
                                                .block_by_hash(descendant_ptr.hash)
                                        }
                                    },
                                )
                            }),
                        ).collect()
                            .wait()?;

                        // Proceed to those blocks
                        Step::ToDescendants(descendant_blocks)
                    }
                } else {
                    // The subgraph ptr points to a block that was uncled.
                    // We need to revert this block.
                    Step::ToParent
                }
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
                let ancestor_block_opt = store.lock().unwrap().ancestor_block(head_ptr, offset)?;
                match ancestor_block_opt {
                    None => {
                        // Block is missing in the block store.
                        // This generally won't happen often, but can happen if the head ptr has
                        // been updated since we retrieved the head ptr, and the block store has
                        // been garbage collected.
                        // It's easiest to start over at this point.
                        continue;
                    }
                    Some(ancestor_block) => {
                        // We stopped one block short, so we'll compare the parent hash to the
                        // subgraph ptr.
                        if ancestor_block.parent_hash == subgraph_ptr.hash {
                            // The subgraph ptr is an ancestor of the head block.
                            // We cannot use an RPC call here to find the first interesting block
                            // due to the race conditions previously mentioned,
                            // so instead we will advance the subgraph ptr by one block.
                            // Note that ancestor_block is a child of subgraph_ptr.
                            Step::ToDescendants(vec![ancestor_block.into()])
                        } else {
                            // The subgraph ptr is not on the main chain.
                            // We will need to step back (possibly repeatedly) one block at a time
                            // until we are back on the main chain.
                            Step::ToParent
                        }
                    }
                }
            }
        };

        // We now know where to take the subgraph ptr.
        match step {
            Step::ToParent => {
                // We would like to move to the parent of the current block.
                // This means we need to revert this block.

                // First, we need the block data.
                let block = {
                    // Try locally first. Otherwise, get block from Ethereum node.
                    let block_from_store = store.lock().unwrap().block(subgraph_ptr.hash)?;
                    if let Some(block) = block_from_store {
                        Ok(block)
                    } else {
                        eth_adapter
                            .lock()
                            .unwrap()
                            .block_by_hash(subgraph_ptr.hash)
                            .wait()
                    }
                }?;

                // Revert entity changes from this block, and update subgraph ptr.
                store
                    .lock()
                    .unwrap()
                    .revert_block(subgraph_id.clone(), block)?;

                // At this point, the loop repeats, and we try to move the subgraph ptr another
                // step in the right direction.
            }
            Step::ToDescendants(descendant_blocks) => {
                let descendant_block_count = descendant_blocks.len();
                debug!(
                    logger,
                    "Advancing subgraph ptr to process {} block(s)...", descendant_block_count
                );

                // Advance the subgraph ptr to each of the specified descendants.
                let mut subgraph_ptr = subgraph_ptr;
                for descendant_block in descendant_blocks.into_iter() {
                    // First, check if there are blocks between subgraph_ptr and descendant_block.
                    let descendant_parent_ptr = EthereumBlockPointer::to_parent(&descendant_block);
                    if subgraph_ptr != descendant_parent_ptr {
                        // descendant_block is not a direct child.
                        // Therefore, there are blocks that are irrelevant to this subgraph that we can skip.

                        // Update subgraph_ptr in store to skip the irrelevant blocks.
                        store.lock().unwrap().set_block_ptr_with_no_changes(
                            subgraph_id.clone(),
                            subgraph_ptr,
                            descendant_parent_ptr,
                        )?;
                    }

                    // subgraph ptr is now the direct parent of descendant_block
                    subgraph_ptr = descendant_parent_ptr;
                    let descendant_ptr = EthereumBlockPointer::from(descendant_block.clone());

                    // TODO future enhancement: load a recent history of blocks before running mappings

                    // Next, we will determine what relevant events are contained in this block.
                    let events = eth_adapter
                        .lock()
                        .unwrap()
                        .get_events_in_block(descendant_block, event_filter.clone())
                        .collect()
                        .wait()?;

                    debug!(
                        logger,
                        "Processing block #{}. {} event(s) are relevant to this subgraph.",
                        descendant_ptr.number,
                        events.len()
                    );

                    // Then, we will distribute each event to each of the runtime hosts.
                    // The execution order is important to ensure entity data is produced
                    // deterministically.
                    // TODO runtime host order should be deterministic
                    // TODO use a single StoreTransaction, use commit instead of set_block_ptr
                    events.iter().for_each(|event| {
                        runtime_hosts
                            .iter_mut()
                            .for_each(|host| host.process_event(event.clone()).wait().unwrap())
                    });
                    store.lock().unwrap().set_block_ptr_with_no_changes(
                        subgraph_id.clone(),
                        subgraph_ptr,
                        descendant_ptr,
                    )?;
                    subgraph_ptr = descendant_ptr;

                    debug!(logger, "Done processing block #{}.", descendant_ptr.number);
                }

                debug!(logger, "Processed {} block(s).", descendant_block_count);

                // At this point, the loop repeats, and we try to move the subgraph ptr another
                // step in the right direction.
            }
        }
    }
}
