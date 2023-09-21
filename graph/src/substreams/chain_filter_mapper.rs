// use std::collections::HashMap;
// use std::marker::PhantomData;
// use std::str::FromStr;
// use std::sync::Arc;

// use crate::blockchain::block_stream::{
//     BlockStreamEvent, BlockWithTriggers, FirehoseCursor, SubstreamsError, SubstreamsMapper,
// };
// use crate::blockchain::{BlockHash, BlockPtr, Blockchain};
// use crate::components::store::BlockNumber;
// use crate::schema::InputSchema;
// use crate::substreams_rpc::response::Message as SubstreamsMessage;
// use anyhow::Error;
// use async_trait::async_trait;
// use prost::Message;
// use slog::{o, Logger};

// use super::Clock;

// // Mapper will transform the proto content coming from substreams in the graph-out format
// // into the internal Block representation. If schema is passed then additional transformation
// // into from the substreams block representation is performed into the Entity model used by
// // the store. If schema is None then only the original block is passed. This None should only
// // be used for block ingestion where entity content is empty and gets discarded.
// pub struct Mapper<C: Blockchain> {
//     pub schema: Option<Arc<InputSchema>>,
//     chain: PhantomData<C>,
// }

// #[async_trait]
// impl<C: Blockchain> SubstreamsMapper<C> for Mapper<C> {
//     fn decode(
//         &self,
//         _output: Option<prost_types::Any>,
//     ) -> Result<Option<BlockWithTriggers<C>>, Error> {
//         unimplemented!()
//     }

//     async fn to_block_stream_event(
//         &self,
//         logger: &mut Logger,
//         message: Option<SubstreamsMessage>,
//     ) -> Result<Option<BlockStreamEvent<C>>, SubstreamsError> {
//         match message {
//             Some(SubstreamsMessage::Session(session_init)) => {
//                 *logger = logger.new(o!("trace_id" => session_init.trace_id));
//                 return Ok(None);
//             }
//             Some(SubstreamsMessage::BlockUndoSignal(undo)) => {
//                 let valid_block = match undo.last_valid_block {
//                     Some(clock) => clock,
//                     None => return Err(SubstreamsError::InvalidUndoError),
//                 };
//                 let valid_ptr = BlockPtr {
//                     hash: valid_block.id.trim_start_matches("0x").try_into()?,
//                     number: valid_block.number as i32,
//                 };
//                 return Ok(Some(BlockStreamEvent::Revert(
//                     valid_ptr,
//                     FirehoseCursor::from(undo.last_valid_cursor.clone()),
//                 )));
//             }

//             Some(SubstreamsMessage::BlockScopedData(block_scoped_data)) => {
//                 let module_output = match &block_scoped_data.output {
//                     Some(out) => out,
//                     None => return Ok(None),
//                 };

//                 let clock = match block_scoped_data.clock {
//                     Some(clock) => clock,
//                     None => return Err(SubstreamsError::MissingClockError),
//                 };

//                 let cursor = &block_scoped_data.cursor;

//                 let Clock {
//                     id: hash,
//                     number,
//                     timestamp: _,
//                 } = clock;

//                 let hash: BlockHash = hash.as_str().try_into()?;
//                 let number: BlockNumber = number as BlockNumber;

//                 // // let changes: EntityChanges = match module_output.map_output.as_ref() {
//                 // //     Some(msg) => Message::decode(msg.value.as_slice())
//                 // //         .map_err(SubstreamsError::DecodingError)?,
//                 // //     None => EntityChanges {
//                 // //         entity_changes: [].to_vec(),
//                 // //     },
//                 // // };

//                 // let parsed_changes = match self.schema.as_ref() {
//                 //     Some(schema) => parse_changes(&changes, schema)?,
//                 //     None => vec![],
//                 // };
//                 // let mut triggers = vec![];
//                 // if changes.entity_changes.len() >= 1 {
//                 //     triggers.push(TriggerData {});
//                 // }

//                 // Even though the trigger processor for substreams doesn't care about TriggerData
//                 // there are a bunch of places in the runner that check if trigger data
//                 // empty and skip processing if so. This will prolly breakdown
//                 // close to head so we will need to improve things.

//                 // TODO(filipe): Fix once either trigger data can be empty
//                 // or we move the changes into trigger data.
//                 // Ok(Some(BlockStreamEvent::ProcessBlock(
//                 //     BlockWithTriggers::new(
//                 //         Block {
//                 //             hash,
//                 //             number,
//                 //             changes,
//                 //             parsed_changes,
//                 //         },
//                 //         triggers,
//                 //         logger,
//                 //     ),
//                 //     FirehoseCursor::from(cursor.clone()),
//                 // )))
//                 todo!()
//             }

//             // ignoring Progress messages and SessionInit
//             // We are only interested in Data and Undo signals
//             _ => Ok(None),
//         }
//     }
// }
