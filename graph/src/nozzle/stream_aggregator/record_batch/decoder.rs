use alloy::primitives::{BlockHash, BlockNumber};
use anyhow::{bail, Context, Result};
use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch, UInt64Array};

use crate::nozzle::common::column_aliases;

/// Decodes the data required for stream aggregation.
pub(super) struct Decoder<'a> {
    /// Block numbers serve as group keys for related record batches.
    block_number_column: &'a UInt64Array,

    /// Block hashes ensure data consistency across tables and datasets.
    block_hash_column: &'a FixedSizeBinaryArray,
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
            block_number_column: block_number_column(record_batch)?,
            block_hash_column: block_hash_column(record_batch)?,
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
        if self.block_number_column.is_null(row_index) {
            bail!("block number is null");
        }

        Ok(self.block_number_column.value(row_index))
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
        if self.block_hash_column.is_null(row_index) {
            bail!("block hash is null");
        }

        BlockHash::try_from(self.block_hash_column.value(row_index))
            .context("block hash is invalid")
    }
}

fn block_number_column<'a>(record_batch: &'a RecordBatch) -> Result<&'a UInt64Array> {
    for &column_name in column_aliases::BLOCK_NUMBER {
        let Some(column) = record_batch.column_by_name(column_name) else {
            continue;
        };

        return column
            .as_any()
            .downcast_ref()
            .context("failed to downcast block number column");
    }

    bail!("failed to find block number column");
}

fn block_hash_column<'a>(record_batch: &'a RecordBatch) -> Result<&'a FixedSizeBinaryArray> {
    for &column_name in column_aliases::BLOCK_HASH {
        let Some(column) = record_batch.column_by_name(column_name) else {
            continue;
        };

        return column
            .as_any()
            .downcast_ref()
            .context("failed to downcast block hash column");
    }

    bail!("failed to find block hash column");
}
