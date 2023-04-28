use crate::{Block, Chain, EntityChanges, TriggerData};
use graph::blockchain::block_stream::{
    BlockStreamEvent, BlockWithTriggers, FirehoseCursor, SubstreamsError, SubstreamsMapper,
};
use graph::prelude::{async_trait, BlockHash, BlockNumber, BlockPtr, Logger};
use graph::substreams::Clock;
use graph::substreams_rpc::response::Message as SubstreamsMessage;
use prost::Message;

pub struct Mapper {}

#[async_trait]
impl SubstreamsMapper<Chain> for Mapper {
    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        message: Option<SubstreamsMessage>,
    ) -> Result<Option<BlockStreamEvent<Chain>>, SubstreamsError> {
        match message {
            Some(SubstreamsMessage::BlockUndoSignal(undo)) => {
                let valid_block = match undo.last_valid_block {
                    Some(clock) => clock,
                    None => return Err(SubstreamsError::InvalidUndoError),
                };
                let valid_ptr = BlockPtr {
                    hash: valid_block.id.trim_start_matches("0x").try_into()?,
                    number: valid_block.number as i32,
                };
                return Ok(Some(BlockStreamEvent::Revert(
                    valid_ptr,
                    FirehoseCursor::from(undo.last_valid_cursor.clone()),
                )));
            }

            Some(SubstreamsMessage::BlockScopedData(block_scoped_data)) => {
                let module_output = match &block_scoped_data.output {
                    Some(out) => out,
                    None => return Ok(None),
                };

                let clock = match block_scoped_data.clock {
                    Some(clock) => clock,
                    None => return Err(SubstreamsError::MissingClockError),
                };

                let cursor = &block_scoped_data.cursor;

                let Clock {
                    id: hash,
                    number,
                    timestamp: _,
                } = clock;

                let hash: BlockHash = hash.as_str().try_into()?;
                let number: BlockNumber = number as BlockNumber;

                let changes: EntityChanges = match module_output.map_output.as_ref() {
                    Some(msg) => Message::decode(msg.value.as_slice())
                        .map_err(SubstreamsError::DecodingError)?,
                    None => EntityChanges {
                        entity_changes: [].to_vec(),
                    },
                };

                // Even though the trigger processor for substreams doesn't care about TriggerData
                // there are a bunch of places in the runner that check if trigger data
                // empty and skip processing if so. This will prolly breakdown
                // close to head so we will need to improve things.

                // TODO(filipe): Fix once either trigger data can be empty
                // or we move the changes into trigger data.
                Ok(Some(BlockStreamEvent::ProcessBlock(
                    BlockWithTriggers::new(
                        Block {
                            hash,
                            number,
                            changes,
                        },
                        vec![TriggerData {}],
                        logger,
                    ),
                    FirehoseCursor::from(cursor.clone()),
                )))
            }

            // ignoring Progress messages and SessionInit
            // We are only interested in Data and Undo signals
            _ => Ok(None),
        }
    }
}
