use std::sync::Arc;

use super::bstream;
use crate::blockchain::Block as BlockchainBlock;
use anyhow::Error;

pub fn decode_firehose_block<M>(
    block_response: &bstream::BlockResponseV2,
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
