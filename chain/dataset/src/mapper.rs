use anyhow::{anyhow, Error};
use graph::blockchain::block_stream::{
    BlockStreamError, BlockStreamEvent, BlockStreamMapper, BlockWithTriggers, FirehoseCursor,
};
use graph::blockchain::BlockTime;
use graph::prelude::BlockPtr;
use graph::prelude::{async_trait, BlockHash, BlockNumber, Logger};
use graph::slog::error;
use graph::substreams::Clock;

use crate::Chain;

// WasmBlockMapper will not perform any transformation to the block and cannot make assumptions
// about the block format. This mode just works a passthrough from the block stream to the subgraph
// mapping which will do the decoding and store actions.
pub struct WasmBlockMapper {
    pub handler: String,
}

#[async_trait]
impl BlockStreamMapper<Chain> for WasmBlockMapper {
    fn decode_block(
        &self,
        _output: Option<&[u8]>,
    ) -> Result<Option<crate::Block>, BlockStreamError> {
        unreachable!("WasmBlockMapper does not do block decoding")
    }

    async fn block_with_triggers(
        &self,
        _logger: &Logger,
        _block: crate::Block,
    ) -> Result<BlockWithTriggers<Chain>, BlockStreamError> {
        unreachable!("WasmBlockMapper does not do trigger decoding")
    }

    async fn handle_substreams_block(
        &self,
        logger: &Logger,
        clock: Clock,
        cursor: FirehoseCursor,
        block: Vec<u8>,
    ) -> Result<BlockStreamEvent<Chain>, BlockStreamError> {
        let Clock {
            id,
            number,
            timestamp,
        } = clock;

        let block_ptr = BlockPtr {
            hash: BlockHash::from(id.into_bytes()),
            number: BlockNumber::from(TryInto::<i32>::try_into(number).map_err(Error::from)?),
        };

        let block_data = block.into_boxed_slice();

        // `timestamp` is an `Option`, but it should always be set
        let timestamp = match timestamp {
            None => {
                error!(logger,
                    "Substream block is missing a timestamp";
                    "cursor" => cursor.to_string(),
                    "number" => number,
                );
                return Err(anyhow!(
                    "Substream block is missing a timestamp at cursor {cursor}, block number {number}"
                )).map_err(BlockStreamError::from);
            }
            Some(ts) => BlockTime::since_epoch(ts.seconds, ts.nanos as u32),
        };

        Ok(BlockStreamEvent::ProcessWasmBlock(
            block_ptr,
            timestamp,
            block_data,
            self.handler.clone(),
            cursor,
        ))
    }
}
