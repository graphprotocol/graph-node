use crate::subgraph::inputs::IndexingInputs;
use graph::blockchain::block_stream::{BlockStream, BufferedBlockStream};
use graph::blockchain::Blockchain;
use graph::prelude::{CheapClone, Error, SubgraphInstanceMetrics};
use std::sync::Arc;

const BUFFERED_BLOCK_STREAM_SIZE: usize = 100;
const BUFFERED_FIREHOSE_STREAM_SIZE: usize = 1;

pub async fn new_block_stream<C: Blockchain>(
    inputs: &IndexingInputs<C>,
    filter: &C::TriggerFilter,
    metrics: &SubgraphInstanceMetrics,
) -> Result<Box<dyn BlockStream<C>>, Error> {
    let is_firehose = inputs.chain.chain_client().is_firehose();

    let buffer_size = match is_firehose {
        true => BUFFERED_FIREHOSE_STREAM_SIZE,
        false => BUFFERED_BLOCK_STREAM_SIZE,
    };

    let block_stream = inputs
        .chain
        .new_block_stream(
            inputs.deployment.clone(),
            inputs.store.cheap_clone(),
            inputs.start_blocks.clone(),
            Arc::new(filter.clone()),
            inputs.unified_api_version.clone(),
        )
        .await;
    if is_firehose && block_stream.is_err() {
        metrics.firehose_connection_errors.inc();
    }

    Ok(BufferedBlockStream::spawn_from_stream(
        block_stream?,
        buffer_size,
    ))
}
