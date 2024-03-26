use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use slog::Logger;

use crate::anyhow::Result;
use crate::blockchain::block_stream::FirehoseCursor;
use crate::blockchain::{BlockHash, BlockPtr, BlockTime};
use crate::cheap_clone::CheapClone;
use crate::components::metrics::stopwatch::StopwatchMetrics;
use crate::components::store::write::EntityModification;
use crate::components::store::{
    DeploymentLocator, SegmentDetails, SubgraphSegment, SubgraphSegmentId, SubgraphStore,
    WritableStore,
};
use crate::data::store::scalar::Bytes;
use crate::data::store::Id;
use crate::data::subgraph::LATEST_VERSION;
use crate::data::value::Word;
use crate::indexer::{BlockSender, EncodedTriggers, State};
use crate::prelude::Value;
use crate::schema::{EntityKey, InputSchema};
use crate::{components::store::BlockNumber, indexer::IndexerStore};

const SCHEMA: &str = "
type Trigger @entity(immutable: true) {
  id: ID!
  number: Int!
  hash: Bytes!
  data: Bytes!
}";

pub struct PostgresIndexerDB {
    store: Arc<dyn SubgraphStore>,
    global_writable: Arc<dyn WritableStore>,
    deployment: DeploymentLocator,
    logger: Logger,
    metrics: StopwatchMetrics,
    schema: InputSchema,
}

impl PostgresIndexerDB {
    pub async fn new(
        store: Arc<dyn SubgraphStore>,
        deployment: DeploymentLocator,
        logger: Logger,
        metrics: StopwatchMetrics,
    ) -> Self {
        let schema = InputSchema::parse(LATEST_VERSION, SCHEMA, deployment.hash.clone()).unwrap();
        let global_writable = store
            .cheap_clone()
            .writable(
                logger.cheap_clone(),
                deployment.id,
                SubgraphSegment::AllBlocks,
                Arc::new(vec![]),
            )
            .await
            .unwrap();

        Self {
            store,
            deployment,
            logger,
            metrics,
            schema,
            global_writable,
        }
    }
}

#[async_trait]
impl IndexerStore for PostgresIndexerDB {
    async fn get_segments(&self) -> Result<Vec<SubgraphSegment>> {
        self.global_writable
            .get_segments(self.deployment.id)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn set_segments(
        &self,
        segments: Vec<(BlockNumber, BlockNumber)>,
    ) -> Result<Vec<SubgraphSegment>> {
        let segments = segments
            .into_iter()
            .map(|(start, end)| SegmentDetails {
                // This will be auto generated
                id: SubgraphSegmentId(0),
                deployment: self.deployment.id,
                start_block: start,
                stop_block: end,
                current_block: None,
            })
            .collect();

        self.global_writable
            .create_segments(self.deployment.id, segments)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn get_last_stable_block(&self) -> Result<Option<BlockNumber>> {
        Ok(self.global_writable.block_ptr().map(|b| b.block_number()))
    }

    async fn stream_from(&self, bn: BlockNumber, bs: BlockSender) -> Result<()> {
        let entity_type = self.schema.entity_type("Trigger").unwrap();
        let mut next_start_block = bn;
        loop {
            let segments = self
                .global_writable
                .get_segments(self.deployment.id)
                .await?
                .into_iter()
                .map(|s| {
                    assert!(matches!(s, SubgraphSegment::Range(_)));
                    s.details().unwrap().clone()
                })
                .collect();
            let range = next_segment_block_range(&segments, Some(next_start_block), 500)
                .unwrap()
                .unwrap();
            println!("## range: {:?}", range);

            let last_block = range.end;
            // when there are no blocks to consume start=end so this should be a noop.
            let keys: BTreeSet<EntityKey> = range
                .into_iter()
                .map(|block_number| {
                    entity_type.key(Id::String(Word::from(block_number.to_string())))
                })
                // .map(|block_number| entity_type.key(Id::Int8(block_number as i64)))
                .collect();

            if keys.is_empty() {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            // println!("## keys: {:?}", keys);
            let blocks = self.global_writable.get_many(keys.clone())?;
            // println!("## blocks: {:?}", blocks);

            for key in keys {
                let block = match blocks.get(&key) {
                    Some(block) => block,
                    None => continue,
                };
                let ptr = match block.get("hash").unwrap() {
                    Value::Bytes(bs) => {
                        let hash = BlockHash(bs.as_slice().to_vec().into_boxed_slice());

                        BlockPtr { hash, number: bn }
                    }
                    _ => unreachable!(),
                };
                let trigger = match block.get("data").unwrap() {
                    Value::Bytes(bs) => EncodedTriggers(bs.as_slice().to_vec().into_boxed_slice()),
                    _ => unreachable!(),
                };

                bs.send((ptr, trigger)).await?;
            }
            next_start_block = last_block;
        }
    }
    async fn get(&self, _bn: BlockNumber, _s: SubgraphSegment) -> Result<Option<EncodedTriggers>> {
        unimplemented!()
    }
    async fn set(
        &self,
        bn: BlockPtr,
        s: &SubgraphSegment,
        state: &State,
        triggers: EncodedTriggers,
    ) -> Result<()> {
        let writable = self
            .store
            .cheap_clone()
            .writable(
                self.logger.cheap_clone(),
                self.deployment.id,
                s.clone(),
                Arc::new(vec![]),
            )
            .await?;
        let data: HashMap<Word, Value> = HashMap::from_iter(vec![
            (Word::from("id"), Value::Int8(bn.number as i64)),
            (
                Word::from("hash"),
                Value::Bytes(Bytes::from(bn.hash.0.as_ref())),
            ),
            (Word::from("number"), Value::Int(bn.number)),
            (
                Word::from("data"),
                Value::Bytes(Bytes::from(triggers.0.as_ref())),
            ),
        ]);

        let entity_type = self.schema.entity_type("Trigger").unwrap();
        let entity = self.schema.make_entity(data).unwrap();

        let entity = EntityModification::Insert {
            key: entity_type.key(Id::String(bn.number.to_string().into())),
            data: Arc::new(entity),
            block: bn.number,
            end: None,
        };

        writable
            .transact_block_operations(
                bn,
                BlockTime::NONE,
                FirehoseCursor::None,
                vec![entity],
                &self.metrics,
                vec![],
                vec![],
                vec![],
                false,
                false,
            )
            .await?;

        Ok(())
    }

    async fn get_state(&self, _bn: BlockNumber) -> Result<State> {
        unimplemented!()
    }
    async fn set_last_stable_block(&self, segment: SubgraphSegment, bn: BlockNumber) -> Result<()> {
        let details = match segment {
            SubgraphSegment::AllBlocks => unreachable!(),
            SubgraphSegment::Range(details) => details,
        };
        let stop_block = details.stop_block;
        assert_eq!(bn, stop_block - 1);

        self.global_writable
            .mark_subgraph_segment_complete(details)
            .await
            .map_err(anyhow::Error::from)
    }
}

/// Gets the next range of blocks that is ready to stream
/// Returns the range that includes the start block if one is provided.
/// If start block is provided it will return the range [start_block,`current_block`[  
/// wthin the relevant segment.
/// If start block is None, the same range of the lowest segment is returned.
/// When the `current_block` of the segment is lower or eql the provided start_block then (start_block, start_block)
/// is returned indicating there are no new blocks for processing.
fn next_segment_block_range(
    segments: &Vec<SegmentDetails>,
    start_block: Option<BlockNumber>,
    range_size: i32,
) -> anyhow::Result<Option<Range<BlockNumber>>> {
    // Start block will be included in the range
    fn take_n_blocks(
        segments: &Vec<SegmentDetails>,
        start_block: BlockNumber,
        n: i32,
    ) -> Option<Range<BlockNumber>> {
        let mut stop_block = start_block;
        let min_start_block = segments
            .iter()
            .map(|s| s.start_block)
            .min()
            .unwrap_or_default();
        let start_block = start_block.max(min_start_block);

        for segment in segments {
            let is_complete = segment.is_complete();
            let starts_after_this_segment = start_block >= segment.stop_block;

            match (is_complete, starts_after_this_segment) {
                (true, true) => continue,
                // [start_block, stop_block] + next segment
                (true, false) => {
                    stop_block = segment.stop_block;

                    let size = stop_block - start_block;
                    if size >= n {
                        return Some(start_block..start_block + n);
                    }

                    continue;
                }
                (false, true) => return None,
                // last segment we can process
                (false, false) => {
                    stop_block = match segment.current_block {
                        // at this point either this is the first segment and stop_block == start_block
                        // or a previous segment has been included.
                        Some(curr) if curr > stop_block => curr,
                        _ => stop_block,
                    };

                    if start_block == stop_block {
                        return None;
                    }

                    break;
                }
            }
        }

        let size = stop_block - start_block;
        if size >= n {
            return Some(start_block..start_block + n);
        }

        Some(start_block..stop_block)
    }

    if segments.is_empty() {
        return Ok(None);
    }

    let start_block = match start_block {
        Some(sb) => sb,
        None => {
            return Ok(take_n_blocks(
                &segments,
                segments.first().unwrap().start_block,
                range_size,
            ))
        }
    };

    if segments.last().unwrap().stop_block <= start_block {
        return Ok(Some(start_block..i32::MAX));
    }

    Ok(take_n_blocks(&segments, start_block, range_size))
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use crate::components::store::{BlockNumber, DeploymentId, SegmentDetails};

    #[ignore]
    #[test]
    fn next_segment_block_range() {
        struct Case<'a> {
            name: &'a str,
            segments: Vec<(BlockNumber, BlockNumber, Option<BlockNumber>)>,
            start_block: Option<BlockNumber>,
            range_size: i32,
            result: Option<Range<BlockNumber>>,
        }

        let cases = vec![
            Case {
                name: "no segments",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "none start block",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "available blocks in segments shorter than range",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "no more blocks in segments",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
            Case {
                name: "segments no completed",
                segments: vec![],
                start_block: todo!(),
                range_size: todo!(),
                result: todo!(),
            },
        ];

        for case in cases.into_iter() {
            let Case {
                name,
                segments,
                start_block,
                range_size,
                result,
            } = case;
            let segments: Vec<SegmentDetails> = segments
                .into_iter()
                .map(|(start, stop, current)| SegmentDetails {
                    id: crate::components::store::SubgraphSegmentId(0),
                    deployment: DeploymentId(0),
                    start_block: start,
                    stop_block: stop,
                    current_block: current,
                })
                .collect();

            let range =
                super::next_segment_block_range(&segments, start_block, range_size).unwrap();
            assert_eq!(
                range, result,
                "case failed: {}. Expected {:?} and got {:?}",
                name, result, range
            );
        }
    }
}
