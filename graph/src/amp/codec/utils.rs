use alloy::primitives::{BlockHash, BlockNumber};
use anyhow::{bail, Context, Result};
use arrow::array::{
    Array, FixedSizeBinaryArray, RecordBatch, TimestampNanosecondArray, UInt64Array,
};
use chrono::{DateTime, Utc};

use super::{ArrayDecoder, Decoder};
use crate::amp::common::column_aliases;

pub fn auto_block_number_decoder<'a>(
    record_batch: &'a RecordBatch,
) -> Result<Box<dyn Decoder<Option<BlockNumber>> + 'a>> {
    let column_index = column_index(record_batch, column_aliases::BLOCK_NUMBER)
        .context("failed to find block numbers column")?;

    block_number_decoder(record_batch, column_index)
}

pub fn block_number_decoder<'a>(
    record_batch: &'a RecordBatch,
    column_index: usize,
) -> Result<Box<dyn Decoder<Option<BlockNumber>> + 'a>> {
    column_decoder::<UInt64Array, BlockNumber>(record_batch, column_index)
}

pub fn auto_block_hash_decoder<'a>(
    record_batch: &'a RecordBatch,
) -> Result<Box<dyn Decoder<Option<BlockHash>> + 'a>> {
    let column_index = column_index(record_batch, column_aliases::BLOCK_HASH)
        .context("failed to find block hashes column")?;

    block_hash_decoder(record_batch, column_index)
}

pub fn block_hash_decoder<'a>(
    record_batch: &'a RecordBatch,
    column_index: usize,
) -> Result<Box<dyn Decoder<Option<BlockHash>> + 'a>> {
    column_decoder::<FixedSizeBinaryArray, BlockHash>(record_batch, column_index)
}

pub fn auto_block_timestamp_decoder<'a>(
    record_batch: &'a RecordBatch,
) -> Result<Box<dyn Decoder<Option<DateTime<Utc>>> + 'a>> {
    let column_index = column_index(record_batch, column_aliases::BLOCK_TIMESTAMP)
        .context("failed to find block timestamps column")?;

    block_timestamp_decoder(record_batch, column_index)
}

pub fn block_timestamp_decoder<'a>(
    record_batch: &'a RecordBatch,
    column_index: usize,
) -> Result<Box<dyn Decoder<Option<DateTime<Utc>>> + 'a>> {
    column_decoder::<TimestampNanosecondArray, DateTime<Utc>>(record_batch, column_index)
}

pub fn column_index(
    record_batch: &RecordBatch,
    column_names: impl IntoIterator<Item = impl AsRef<str>>,
) -> Option<usize> {
    let schema_ref = record_batch.schema_ref();

    for column_name in column_names {
        if let Some((column_index, _)) = schema_ref.column_with_name(column_name.as_ref()) {
            return Some(column_index);
        }
    }

    return None;
}

pub fn column_decoder<'a, T: 'static, U>(
    record_batch: &'a RecordBatch,
    column_index: usize,
) -> Result<Box<dyn Decoder<Option<U>> + 'a>>
where
    T: Array,
    ArrayDecoder<'a, T>: Decoder<Option<U>>,
{
    if column_index >= record_batch.num_columns() {
        bail!("column {column_index} does not exist");
    }

    let array = record_batch.column(column_index);
    let decoder = ArrayDecoder::<T>::new(array)?;

    Ok(Box::new(decoder))
}
