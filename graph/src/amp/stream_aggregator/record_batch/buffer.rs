use std::collections::{btree_map::Entry, BTreeMap};

use alloy::primitives::{BlockHash, BlockNumber};
use anyhow::{bail, Result};
use arrow::array::RecordBatch;

use super::{Aggregator, RecordBatchGroup, RecordBatchGroups, StreamRecordBatch};

/// Buffers record batches from multiple streams in memory and creates
/// groups of record batches by block number and hash pairs.
pub(in super::super) struct Buffer {
    aggregators: Vec<Aggregator>,
    num_streams: usize,
    buffer_size: usize,
}

impl Buffer {
    /// Creates a new buffer that can handle exactly `num_streams` number of streams.
    ///
    /// Creates a new associated `Aggregator` for each stream.
    /// The `buffer_size` specifies how many record batches for each stream can be buffered at most.
    pub(in super::super) fn new(num_streams: usize, buffer_size: usize) -> Self {
        let aggregators = (0..num_streams).map(|_| Aggregator::new()).collect();

        Self {
            aggregators,
            num_streams,
            buffer_size,
        }
    }

    /// Extends the aggregator for `stream_index` with data from a new `record_batch`.
    ///
    /// # Errors
    ///
    /// Errors if the aggregator cannot be extended.
    ///
    /// The returned error is deterministic.
    ///
    /// # Panics
    ///
    /// Panics if the `stream_index` is greater than the initialized number of streams.
    pub(in super::super) fn extend(
        &mut self,
        stream_index: usize,
        record_batch: RecordBatch,
    ) -> Result<()> {
        assert!(stream_index < self.num_streams);
        self.aggregators[stream_index].extend(record_batch)
    }

    /// Removes and returns all completed groups from this buffer.
    ///
    /// # Errors
    ///
    /// Errors if aggregators fail to return completed groups.
    ///
    /// The returned error is deterministic.
    ///
    /// # Panics
    ///
    /// Panics if aggregators return inconsistent responses.
    pub(in super::super) fn completed_groups(&mut self) -> Result<Option<RecordBatchGroups>> {
        let Some(max_completed_block_ptr) = self.max_completed_block_ptr()? else {
            return Ok(None);
        };

        let mut ordered_completed_groups = BTreeMap::new();

        for (stream_index, agg) in self.aggregators.iter_mut().enumerate() {
            let Some(completed_groups) = agg.completed_groups(&max_completed_block_ptr)? else {
                continue;
            };

            for (block_ptr, record_batch) in completed_groups {
                match ordered_completed_groups.entry(block_ptr) {
                    Entry::Vacant(entry) => {
                        entry.insert(RecordBatchGroup {
                            record_batches: vec![StreamRecordBatch {
                                stream_index,
                                record_batch,
                            }],
                        });
                    }
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().record_batches.push(StreamRecordBatch {
                            stream_index,
                            record_batch,
                        });
                    }
                }
            }
        }

        assert!(!ordered_completed_groups.is_empty());
        Ok(Some(ordered_completed_groups))
    }

    /// Marks the aggregator for the `stream_index` as finalized.
    ///
    /// A finalized aggregator cannot be extended.
    ///
    /// # Panics
    ///
    /// Panics if the `stream_index` is greater than the initialized number of streams.
    pub(in super::super) fn finalize(&mut self, stream_index: usize) {
        assert!(stream_index < self.num_streams);
        self.aggregators[stream_index].finalize();
    }

    /// Returns `true` if the aggregator for `stream_index` is finalized.
    ///
    /// # Panics
    ///
    /// Panics if the `stream_index` is greater than the initialized number of streams.
    pub(in super::super) fn is_finalized(&self, stream_index: usize) -> bool {
        assert!(stream_index < self.num_streams);
        self.aggregators[stream_index].is_finalized()
    }

    /// Returns `true` if all aggregators are finalized.
    pub(in super::super) fn all_finalized(&self) -> bool {
        self.aggregators.iter().all(|agg| agg.is_finalized())
    }

    /// Returns `true` if the aggregator for `stream_index` can be extended.
    ///
    /// # Panics
    ///
    /// Panics if the `stream_index` is greater than the initialized number of streams.
    pub(in super::super) fn has_capacity(&self, stream_index: usize) -> bool {
        assert!(stream_index < self.num_streams);
        self.aggregators[stream_index].len() < self.buffer_size
    }

    /// Returns `true` if the stream `stream_index` is not allowed to make progress and
    /// its aggregator does not contain any completed groups.
    ///
    /// # Panics
    ///
    /// Panics if the `stream_index` is greater than the initialized number of streams.
    pub(in super::super) fn is_blocked(&self, stream_index: usize) -> bool {
        !self.has_capacity(stream_index)
            && !self.is_finalized(stream_index)
            && !self.aggregators[stream_index].has_completed_groups()
    }

    /// Returns the number of record batches stream `stream_index` has buffered.
    ///
    /// # Panics
    ///
    /// Panics if the `stream_index` is greater than the initialized number of streams.
    pub(in super::super) fn size(&self, stream_index: usize) -> usize {
        assert!(stream_index < self.num_streams);
        self.aggregators[stream_index].len()
    }

    /// Returns the block number and hash pair for the most recent completed group across all streams.
    ///
    /// Finds the highest block number that all streams have completed. This ensures
    /// slower streams can still produce valid completed groups without skipping any groups.
    /// The function returns the minimum of all maximum completed blocks to maintain consistency.
    ///
    /// # Errors
    ///
    /// Returns an error if multiple streams return the same block number but different hashes.
    ///
    /// The returned error is deterministic.
    ///
    /// # Note
    ///
    /// Potential reorgs are not handled at this level and are treated as data corruption.
    fn max_completed_block_ptr(&self) -> Result<Option<(BlockNumber, BlockHash)>> {
        let mut max_completed_block_ptrs: BTreeMap<&BlockNumber, &BlockHash> = BTreeMap::new();

        for (stream_index, agg) in self.aggregators.iter().enumerate() {
            let Some((max_completed_block_number, max_completed_block_hash)) =
                agg.max_completed_block_ptr()
            else {
                if !agg.is_finalized() {
                    return Ok(None);
                }

                continue;
            };

            match max_completed_block_ptrs.entry(max_completed_block_number) {
                Entry::Vacant(entry) => {
                    entry.insert(max_completed_block_hash);
                }
                Entry::Occupied(entry) => {
                    if *entry.get() != max_completed_block_hash {
                        bail!("aggregated data is corrupted: stream {} produced block hash '0x{}' for block {}, but a previous stream set the block hash to '0x{}'",
                            stream_index,
                            hex::encode(max_completed_block_hash),
                            max_completed_block_number,
                            hex::encode(entry.get()),
                        );
                    }
                }
            };
        }

        Ok(max_completed_block_ptrs
            .into_iter()
            .next()
            .map(|(block_number, block_hash)| (*block_number, *block_hash)))
    }
}
