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
) -> Result<(&'static str, Box<dyn Decoder<Option<BlockNumber>> + 'a>)> {
    let (&column_name, column_index) = find_column(record_batch, column_aliases::BLOCK_NUMBER)
        .with_context(|| {
            format!(
                "failed to find block numbers column; expected one of: {}",
                column_aliases::BLOCK_NUMBER.join(", ")
            )
        })?;

    block_number_decoder(record_batch, column_index)
        .map(|decoder| (column_name, decoder))
        .with_context(|| format!("column '{column_name}' is not valid"))
}

pub fn block_number_decoder<'a>(
    record_batch: &'a RecordBatch,
    column_index: usize,
) -> Result<Box<dyn Decoder<Option<BlockNumber>> + 'a>> {
    column_decoder::<UInt64Array, BlockNumber>(record_batch, column_index, false)
}

pub fn auto_block_hash_decoder<'a>(
    record_batch: &'a RecordBatch,
) -> Result<(&'static str, Box<dyn Decoder<Option<BlockHash>> + 'a>)> {
    let (&column_name, column_index) = find_column(record_batch, column_aliases::BLOCK_HASH)
        .with_context(|| {
            format!(
                "failed to find block hashes column; expected one of: {}",
                column_aliases::BLOCK_HASH.join(", ")
            )
        })?;

    block_hash_decoder(record_batch, column_index)
        .map(|decoder| (column_name, decoder))
        .with_context(|| format!("column '{column_name}' is not valid"))
}

pub fn block_hash_decoder<'a>(
    record_batch: &'a RecordBatch,
    column_index: usize,
) -> Result<Box<dyn Decoder<Option<BlockHash>> + 'a>> {
    column_decoder::<FixedSizeBinaryArray, BlockHash>(record_batch, column_index, false)
}

pub fn auto_block_timestamp_decoder<'a>(
    record_batch: &'a RecordBatch,
) -> Result<(&'static str, Box<dyn Decoder<Option<DateTime<Utc>>> + 'a>)> {
    let (&column_name, column_index) = find_column(record_batch, column_aliases::BLOCK_TIMESTAMP)
        .with_context(|| {
        format!(
            "failed to find block timestamps column; expected one of: {}",
            column_aliases::BLOCK_TIMESTAMP.join(", ")
        )
    })?;

    block_timestamp_decoder(record_batch, column_index)
        .map(|decoder| (column_name, decoder))
        .with_context(|| format!("column '{column_name}' is not valid"))
}

pub fn block_timestamp_decoder<'a>(
    record_batch: &'a RecordBatch,
    column_index: usize,
) -> Result<Box<dyn Decoder<Option<DateTime<Utc>>> + 'a>> {
    column_decoder::<TimestampNanosecondArray, DateTime<Utc>>(record_batch, column_index, false)
}

pub fn find_column<T>(
    record_batch: &RecordBatch,
    column_names: impl IntoIterator<Item = T>,
) -> Option<(T, usize)>
where
    T: AsRef<str>,
{
    let schema_ref = record_batch.schema_ref();

    for column_name in column_names {
        if let Some((column_index, _)) = schema_ref.column_with_name(column_name.as_ref()) {
            return Some((column_name, column_index));
        }
    }

    return None;
}

pub fn column_decoder<'a, T: 'static, U>(
    record_batch: &'a RecordBatch,
    column_index: usize,
    nullable: bool,
) -> Result<Box<dyn Decoder<Option<U>> + 'a>>
where
    T: Array,
    ArrayDecoder<'a, T>: Decoder<Option<U>>,
{
    if column_index >= record_batch.num_columns() {
        bail!("column does not exist");
    }

    let array = record_batch.column(column_index);

    if !nullable && array.is_nullable() {
        bail!("column must not have nullable values");
    }

    let decoder = ArrayDecoder::<T>::new(array)?;

    Ok(Box::new(decoder))
}
