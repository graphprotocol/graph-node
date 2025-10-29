use std::{
    collections::{btree_map::Entry, BTreeMap, HashSet},
    sync::{Arc, Weak},
};

use alloy::primitives::{BlockHash, BlockNumber};
use anyhow::{bail, Context, Result};
use arrow::array::RecordBatch;

use super::{Decoder, GroupData};
use crate::cheap_clone::CheapClone;

/// Groups record batches by block number and hash pairs.
///
/// This aggregator collects and organizes record batches based on their
/// associated block identifiers.
pub(super) struct Aggregator {
    buffer: BTreeMap<(BlockNumber, BlockHash), GroupData>,
    buffered_record_batches: Vec<Weak<RecordBatch>>,
    is_finalized: bool,
}

impl Aggregator {
    /// Creates a new empty aggregator.
    pub(super) fn new() -> Self {
        Self {
            buffer: BTreeMap::new(),
            buffered_record_batches: Vec::new(),
            is_finalized: false,
        }
    }

    /// Extends this aggregator with data from a new `record_batch`.
    ///
    /// Processes each row in the `record_batch` and groups them by block number
    /// and hash. Each unique block is stored in the internal buffer with references
    /// to all rows that belong to that block.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `record_batch` does not contain block numbers or hashes
    /// - `record_batch` contains invalid block numbers or hashes
    /// - `record_batch` data is not ordered
    /// - `record_batch` data is not consistent
    ///
    /// The returned error is deterministic.
    ///
    /// # Panics
    ///
    /// Panics if this aggregator has already been finalized.
    pub(super) fn extend(&mut self, record_batch: RecordBatch) -> Result<()> {
        assert!(!self.is_finalized);

        let record_batch = Arc::new(record_batch);
        let decoder = Decoder::new(&record_batch)?;

        self.buffered_record_batches
            .push(Arc::downgrade(&record_batch));

        let num_rows = record_batch.num_rows();
        let mut record_batch_buffered: HashSet<(BlockNumber, BlockHash)> =
            HashSet::with_capacity(num_rows);

        for row_index in 0..num_rows {
            let err_cx = || format!("invalid group data at row {row_index}");
            let block_number = decoder.block_number(row_index).with_context(err_cx)?;
            let block_hash = decoder.block_hash(row_index).with_context(err_cx)?;
            let block_ptr = (block_number, block_hash);

            self.ensure_incremental_update(&block_ptr)
                .with_context(err_cx)?;

            match self.buffer.entry(block_ptr) {
                Entry::Vacant(entry) => {
                    entry.insert(GroupData::new(record_batch.cheap_clone(), row_index));
                    record_batch_buffered.insert(block_ptr);
                }
                Entry::Occupied(mut entry) => {
                    let group_data = entry.get_mut();

                    if !record_batch_buffered.contains(&block_ptr) {
                        group_data.add(record_batch.cheap_clone(), row_index);
                        record_batch_buffered.insert(block_ptr);
                    } else {
                        group_data.add_row_index(row_index);
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns the block number and hash pair for the most recent completed group.
    ///
    /// A group is considered complete when:
    /// - There is a group with a higher block number in the internal buffer
    /// - This aggregator is finalized
    ///
    /// Any group in this aggregator with a lower block number than the one returned by
    /// this method is also considered complete.
    pub(super) fn max_completed_block_ptr(&self) -> Option<&(BlockNumber, BlockHash)> {
        let mut iter = self.buffer.keys().rev();

        if self.is_finalized {
            return iter.next();
        }

        iter.skip(1).next()
    }

    /// Returns `true` if this aggregator contains completed groups.
    ///
    /// A group is considered complete when:
    /// - There is a group with a higher block number in the internal buffer
    /// - This aggregator is finalized
    pub(super) fn has_completed_groups(&self) -> bool {
        (self.is_finalized && !self.buffer.is_empty()) || self.buffer.len() > 1
    }

    /// Removes and returns completed groups from this aggregator up to `max_block_ptr`.
    ///
    /// # Errors
    ///
    /// Returns an error if groups cannot be converted into record batches.
    ///
    /// The returned error is deterministic.
    ///
    /// # Panics
    ///
    /// Panics if `max_block_ptr` is greater than the most recent completed block in this aggregator.
    pub(super) fn completed_groups(
        &mut self,
        max_block_ptr: &(BlockNumber, BlockHash),
    ) -> Result<Option<BTreeMap<(BlockNumber, BlockHash), RecordBatch>>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let Some(max_completed_block_ptr) = self.max_completed_block_ptr() else {
            return Ok(None);
        };

        assert!(max_block_ptr <= max_completed_block_ptr);
        let incomplete_groups = self.buffer.split_off(max_block_ptr);
        let mut completed_groups = std::mem::replace(&mut self.buffer, incomplete_groups);

        if let Some((block_ptr, _)) = self.buffer.first_key_value() {
            if block_ptr == max_block_ptr {
                let (block_ptr, group_data) = self.buffer.pop_first().unwrap();
                completed_groups.insert(block_ptr, group_data);
            }
        }

        if completed_groups.is_empty() {
            return Ok(None);
        }

        let completed_groups = completed_groups
            .into_iter()
            .map(|(block_ptr, group_data)| Ok((block_ptr, group_data.into_record_batch()?)))
            .collect::<Result<BTreeMap<_, _>>>()?;

        self.buffered_record_batches
            .retain(|weak_ref| weak_ref.strong_count() > 0);

        Ok(Some(completed_groups))
    }

    /// Marks this aggregator as finalized.
    ///
    /// A finalized aggregator cannot be extended.
    pub(super) fn finalize(&mut self) {
        self.is_finalized = true;
    }

    /// Returns `true` if this aggregator is finalized.
    pub(super) fn is_finalized(&self) -> bool {
        self.is_finalized
    }

    /// Returns the number of record batches that this aggregator holds strong references to.
    pub(super) fn len(&self) -> usize {
        self.buffered_record_batches
            .iter()
            .filter(|weak_ref| weak_ref.strong_count() > 0)
            .count()
    }

    /// Ensures that block updates arrive in sequential order.
    ///
    /// Validates that the provided block number and hash represent a valid
    /// incremental update relative to the last block in the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The block number is less than the maximum stored block number
    /// - The block number equals the maximum but has a different hash
    ///
    /// The returned error is deterministic.
    ///
    /// # Note
    ///
    /// Potential reorgs are not handled at this level and are
    /// treated as data corruption.
    fn ensure_incremental_update(
        &self,
        (block_number, block_hash): &(BlockNumber, BlockHash),
    ) -> Result<()> {
        let Some(((max_block_number, max_block_hash), _)) = self.buffer.last_key_value() else {
            return Ok(());
        };

        if block_number < max_block_number {
            bail!("received block number {block_number} after {max_block_number}");
        }

        if block_number == max_block_number && block_hash != max_block_hash {
            bail!(
                "received block hash '0x{}' after '0x{}' for block number {block_number}",
                hex::encode(&block_hash),
                hex::encode(&max_block_hash)
            );
        }

        Ok(())
    }
}
