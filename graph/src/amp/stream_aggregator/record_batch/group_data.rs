use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{RecordBatch, UInt64Array},
    compute::{concat_batches, take_record_batch},
};

/// Contains references to all record batches and rows of a group.
pub(super) struct GroupData {
    parts: Vec<Part>,
}

struct Part {
    record_batch: Arc<RecordBatch>,
    row_indices: Vec<u64>,
}

impl GroupData {
    /// Creates a new group with an initial `record_batch` and `row_index`.
    pub(super) fn new(record_batch: Arc<RecordBatch>, row_index: usize) -> Self {
        Self {
            parts: vec![Part {
                record_batch,
                row_indices: vec![row_index as u64],
            }],
        }
    }

    /// Adds a new `record_batch` and `row_index` to this group.
    pub(super) fn add(&mut self, record_batch: Arc<RecordBatch>, row_index: usize) {
        self.parts.push(Part {
            record_batch,
            row_indices: vec![row_index as u64],
        })
    }

    /// Adds a `row_index` to the most recent record batch in this group.
    ///
    /// # Panics
    ///
    /// Panics if this group is empty.
    pub(super) fn add_row_index(&mut self, row_index: usize) {
        assert!(!self.parts.is_empty());

        self.parts
            .last_mut()
            .unwrap()
            .row_indices
            .push(row_index as u64);
    }

    /// Converts this group into a single record batch.
    ///
    /// Merges all group rows from all record batches together.
    ///
    /// # Errors
    ///
    /// Returns an error if the record batches in this group have incompatible types.
    ///
    /// The returned error is deterministic.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - This group is empty
    /// - This group contains invalid row indices
    pub(super) fn into_record_batch(self) -> Result<RecordBatch> {
        assert!(!self.parts.is_empty());

        let schema = self.parts[0].record_batch.schema();
        let mut partial_record_batches = Vec::with_capacity(self.parts.len());

        for part in self.parts {
            let Part {
                record_batch,
                row_indices,
            } = part;

            let row_indices = UInt64Array::from(row_indices);
            let partial_record_batch = take_record_batch(&record_batch, &row_indices).unwrap();

            partial_record_batches.push(partial_record_batch);
        }

        concat_batches(&schema, &partial_record_batches).context("failed to merge record batches")
    }
}
