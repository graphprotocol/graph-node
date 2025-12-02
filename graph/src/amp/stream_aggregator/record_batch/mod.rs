//! This module handles grouping record batches from multiple streams.
//!
//! # Safety
//!
//! The implementation occasionally uses `assert` and `unwrap` to ensure consistency
//! between related types and methods.
//!
//! This is safe because the functionality is internal and not exposed to other modules.
//!
//! A panic indicates a critical error in the grouping algorithm.

mod aggregator;
mod buffer;
mod decoder;
mod group_data;

use std::collections::BTreeMap;

use alloy::primitives::{BlockHash, BlockNumber};
use arrow::array::RecordBatch;

use self::{aggregator::Aggregator, decoder::Decoder, group_data::GroupData};

pub(super) use buffer::Buffer;

/// Maps block number and hash pairs to record batches.
pub type RecordBatchGroups = BTreeMap<(BlockNumber, BlockHash), RecordBatchGroup>;

/// Contains record batches associated with a specific block number and hash pair.
pub struct RecordBatchGroup {
    pub record_batches: Vec<StreamRecordBatch>,
}

/// Contains a record batch and the index of its source stream.
pub struct StreamRecordBatch {
    pub stream_index: usize,
    pub record_batch: RecordBatch,
}
