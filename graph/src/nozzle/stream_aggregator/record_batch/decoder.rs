use alloy::primitives::{BlockHash, BlockNumber};
use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;

use crate::nozzle::codec::{
    self,
    utils::{auto_block_hash_decoder, auto_block_number_decoder},
};

/// Decodes the data required for stream aggregation.
pub(super) struct Decoder<'a> {
    /// Block numbers serve as group keys for related record batches.
    block_number: Box<dyn codec::Decoder<Option<BlockNumber>> + 'a>,

    /// Block hashes ensure data consistency across tables and datasets.
    block_hash: Box<dyn codec::Decoder<Option<BlockHash>> + 'a>,
}

impl<'a> Decoder<'a> {
    /// Constructs a new decoder for `record_batch`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `record_batch` does not contain valid block number or hash columns
    ///
    /// The returned error is deterministic.
    pub(super) fn new(record_batch: &'a RecordBatch) -> Result<Self> {
        Ok(Self {
            block_number: auto_block_number_decoder(record_batch)?,
            block_hash: auto_block_hash_decoder(record_batch)?,
        })
    }

    /// Returns the block number at `row_index`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The block number at `row_index` is null
    ///
    /// The returned error is deterministic.
    pub(super) fn block_number(&self, row_index: usize) -> Result<BlockNumber> {
        self.block_number
            .decode(row_index)?
            .ok_or_else(|| anyhow!("block number is empty"))
    }

    /// Returns the block hash at `row_index`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The block hash at `row_index` is null or invalid
    ///
    /// The returned error is deterministic.
    pub(super) fn block_hash(&self, row_index: usize) -> Result<BlockHash> {
        self.block_hash
            .decode(row_index)?
            .ok_or_else(|| anyhow!("block hash is empty"))
    }
}
