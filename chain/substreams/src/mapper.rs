use crate::{Block, Chain, TriggerData};
use graph::blockchain::block_stream::SubstreamsError::{
    MultipleModuleOutputError, UnexpectedStoreDeltaOutput,
};
use graph::blockchain::block_stream::{
    BlockStreamEvent, BlockWithTriggers, FirehoseCursor, SubstreamsError, SubstreamsMapper,
};
use graph::prelude::{async_trait, BlockNumber, BlockPtr, Logger};
use graph::substreams::module_output::Data;
use graph::substreams::{BlockScopedData, ForkStep};
use prost::Message;

pub struct Mapper {}

#[async_trait]
impl SubstreamsMapper<Chain> for Mapper {
    async fn to_block_stream_event(
        &self,
        _logger: &Logger,
        block_scoped_data: &BlockScopedData,
    ) -> Result<Option<BlockStreamEvent<Chain>>, SubstreamsError> {
        let step = ForkStep::from_i32(block_scoped_data.step).unwrap_or_else(|| {
            panic!(
                "unknown step i32 value {}, maybe you forgot update & re-regenerate the protobuf definitions?",
                block_scoped_data.step
            )
        });

        if block_scoped_data.outputs.len() == 0 {
            return Ok(None);
        }

        if block_scoped_data.outputs.len() > 1 {
            return Err(MultipleModuleOutputError());
        }

        //todo: handle step
        let module_output = &block_scoped_data.outputs[0];
        let cursor = &block_scoped_data.cursor;

        match module_output.data.as_ref().unwrap() {
            Data::MapOutput(msg) => {
                let changes: Block = Message::decode(msg.value.as_slice()).unwrap();

                use ForkStep::*;
                match step {
                    StepIrreversible | StepNew => Ok(Some(BlockStreamEvent::ProcessBlock(
                        // Even though the trigger processor for substreams doesn't care about TriggerData
                        // there are a bunch of places in the runner that check if trigger data
                        // empty and skip processing if so. This will prolly breakdown
                        // close to head so we will need to improve things.

                        // TODO(filipe): Fix once either trigger data can be empty
                        // or we move the changes into trigger data.
                        BlockWithTriggers::new(changes, vec![TriggerData {}]),
                        FirehoseCursor::from(cursor.clone()),
                    ))),
                    StepUndo => {
                        let parent_ptr = BlockPtr {
                            hash: changes.prev_block_id.clone().into(),
                            number: changes.prev_block_number as BlockNumber,
                        };

                        Ok(Some(BlockStreamEvent::Revert(
                            parent_ptr,
                            FirehoseCursor::from(cursor.clone()),
                        )))
                    }
                    StepUnknown => {
                        panic!("unknown step should not happen in the Firehose response")
                    }
                }
            }
            Data::StoreDeltas(_) => Err(UnexpectedStoreDeltaOutput()),
        }
    }
}
