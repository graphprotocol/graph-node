use std::sync::Arc;

use anyhow::Error;

use crate::blockchain::Block as BlockchainBlock;
use crate::firehose;

pub fn decode_firehose_block<M>(
    block_response: &firehose::Response,
) -> Result<Arc<dyn BlockchainBlock>, Error>
where
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    let any_block = block_response
        .block
        .as_ref()
        .expect("block payload information should always be present");

    Ok(Arc::new(M::decode(any_block.value.as_ref())?))
}
