use std::{fmt::Display, sync::LazyLock};

use alloy::primitives::B256;
use anyhow::{anyhow, Result};
use arrow::{
    array::{
        timezone::Tz, Array, ArrayAccessor, BinaryArray, BinaryViewArray, BooleanArray,
        Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
        LargeStringArray, PrimitiveArray, StringArray, StringViewArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::ArrowTemporalType,
};
use chrono::{DateTime, Utc};

use super::decoder::Decoder;
use crate::data::store::scalar::{BigDecimal, BigInt};

/// Decodes Arrow arrays into Subgraph types.
pub(super) struct ArrayDecoder<'a, T: 'static>(&'a T);

impl<'a, T> ArrayDecoder<'a, T>
where
    T: Array + 'static,
{
    /// Creates a new Arrow array decoder.
    ///
    /// # Errors
    ///
    /// Returns an error if the `array` cannot be downcasted to type `T`.
    ///
    /// The returned error is deterministic.
    pub(super) fn new(array: &'a dyn Array) -> Result<Self> {
        Ok(Self(downcast_ref(array)?))
    }
}

macro_rules! check_value {
    ($self:ident, $row_index:ident) => {
        if $row_index >= $self.0.len() {
            return Ok(None);
        }

        if $self.0.is_null($row_index) {
            return Ok(None);
        }
    };
}

impl<'a, T> ArrayDecoder<'a, T>
where
    &'a T: ArrayAccessor,
{
    fn value<V>(
        &'a self,
        row_index: usize,
        mapping: impl FnOnce(<&'a T as ArrayAccessor>::Item) -> Result<V>,
    ) -> Result<Option<V>> {
        check_value!(self, row_index);
        mapping(self.0.value(row_index)).map(Some)
    }
}

impl Decoder<Option<bool>> for ArrayDecoder<'_, BooleanArray> {
    fn decode(&self, row_index: usize) -> Result<Option<bool>> {
        self.value(row_index, Ok)
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, Int8Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Int8Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Int8Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_signed_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, Int16Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Int16Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Int16Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_signed_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, Int32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Int32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Int32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_signed_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, Int64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Int64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Int64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_signed_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, UInt8Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, UInt8Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, UInt8Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_unsigned_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, UInt16Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, UInt16Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, UInt16Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_unsigned_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, UInt32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, UInt32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, UInt32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_unsigned_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, UInt64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, UInt64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<u64>> for ArrayDecoder<'_, UInt64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<u64>> {
        self.value(row_index, Ok)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, UInt64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_unsigned_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, Float16Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |x| Ok(f64::from(x).into()))
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, Float32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |x| Ok(f64::from(x).into()))
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, Float64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |x| Ok(x.into()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, Decimal128Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Decimal128Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Decimal128Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_signed_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, Decimal128Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |x| {
            let scale = self.0.scale() as i64;
            let big_int = decode_signed_big_int(x.to_le_bytes())?;

            Ok(BigDecimal::new(big_int, -scale))
        })
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, Decimal256Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        self.value(row_index, |x| decode_i32(x.as_i128()))
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Decimal256Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        self.value(row_index, |x| decode_i64(x.as_i128()))
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Decimal256Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |x| decode_signed_big_int(x.to_le_bytes()))
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, Decimal256Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |x| {
            let scale = self.0.scale() as i64;
            let big_int = decode_signed_big_int(x.to_le_bytes())?;

            Ok(BigDecimal::new(big_int, -scale))
        })
    }
}

impl Decoder<Option<String>> for ArrayDecoder<'_, StringArray> {
    fn decode(&self, row_index: usize) -> Result<Option<String>> {
        self.value(row_index, |x| Ok(x.to_string()))
    }
}

impl Decoder<Option<String>> for ArrayDecoder<'_, StringViewArray> {
    fn decode(&self, row_index: usize) -> Result<Option<String>> {
        self.value(row_index, |x| Ok(x.to_string()))
    }
}

impl Decoder<Option<String>> for ArrayDecoder<'_, LargeStringArray> {
    fn decode(&self, row_index: usize) -> Result<Option<String>> {
        self.value(row_index, |x| Ok(x.to_string()))
    }
}

impl Decoder<Option<Box<[u8]>>> for ArrayDecoder<'_, BinaryArray> {
    fn decode(&self, row_index: usize) -> Result<Option<Box<[u8]>>> {
        self.value(row_index, |x| Ok(x.into()))
    }
}

impl Decoder<Option<Box<[u8]>>> for ArrayDecoder<'_, BinaryViewArray> {
    fn decode(&self, row_index: usize) -> Result<Option<Box<[u8]>>> {
        self.value(row_index, |x| Ok(x.into()))
    }
}

impl Decoder<Option<Box<[u8]>>> for ArrayDecoder<'_, FixedSizeBinaryArray> {
    fn decode(&self, row_index: usize) -> Result<Option<Box<[u8]>>> {
        self.value(row_index, |x| Ok(x.into()))
    }
}

impl Decoder<Option<B256>> for ArrayDecoder<'_, FixedSizeBinaryArray> {
    fn decode(&self, row_index: usize) -> Result<Option<B256>> {
        self.value(row_index, |x| {
            B256::try_from(x)
                .map_err(|_| anyhow!("failed to convert '{}' to 'B256'", hex::encode(x)))
        })
    }
}

impl Decoder<Option<Box<[u8]>>> for ArrayDecoder<'_, LargeBinaryArray> {
    fn decode(&self, row_index: usize) -> Result<Option<Box<[u8]>>> {
        self.value(row_index, |x| Ok(x.into()))
    }
}

impl Decoder<Option<DateTime<Utc>>> for ArrayDecoder<'_, TimestampSecondArray> {
    fn decode(&self, row_index: usize) -> Result<Option<DateTime<Utc>>> {
        check_value!(self, row_index);
        decode_timestamp(self.0, row_index).map(Some)
    }
}

impl Decoder<Option<DateTime<Utc>>> for ArrayDecoder<'_, TimestampMillisecondArray> {
    fn decode(&self, row_index: usize) -> Result<Option<DateTime<Utc>>> {
        check_value!(self, row_index);
        decode_timestamp(self.0, row_index).map(Some)
    }
}

impl Decoder<Option<DateTime<Utc>>> for ArrayDecoder<'_, TimestampMicrosecondArray> {
    fn decode(&self, row_index: usize) -> Result<Option<DateTime<Utc>>> {
        check_value!(self, row_index);
        decode_timestamp(self.0, row_index).map(Some)
    }
}

impl Decoder<Option<DateTime<Utc>>> for ArrayDecoder<'_, TimestampNanosecondArray> {
    fn decode(&self, row_index: usize) -> Result<Option<DateTime<Utc>>> {
        check_value!(self, row_index);
        decode_timestamp(self.0, row_index).map(Some)
    }
}

fn downcast_ref<'a, T>(array: &'a dyn Array) -> Result<&'a T>
where
    T: Array + 'static,
{
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| anyhow!("failed to downcast array"))
}

fn decode_i32<T>(n: T) -> Result<i32>
where
    T: TryInto<i32> + Copy + Display,
{
    n.try_into()
        .map_err(|_| anyhow!("failed to convert '{n}' to 'i32'"))
}

fn decode_i64<T>(n: T) -> Result<i64>
where
    T: TryInto<i64> + Copy + Display,
{
    n.try_into()
        .map_err(|_| anyhow!("failed to convert '{n}' to 'i64'"))
}

fn decode_signed_big_int(le_bytes: impl AsRef<[u8]>) -> Result<BigInt> {
    let le_bytes = le_bytes.as_ref();

    BigInt::from_signed_bytes_le(le_bytes)
        .map_err(|_| anyhow!("failed to convert '{}' to 'BigInt'", hex::encode(le_bytes)))
}

fn decode_unsigned_big_int(le_bytes: impl AsRef<[u8]>) -> Result<BigInt> {
    let le_bytes = le_bytes.as_ref();

    BigInt::from_unsigned_bytes_le(le_bytes)
        .map_err(|_| anyhow!("failed to convert '{}' to 'BigInt'", hex::encode(le_bytes)))
}

fn decode_timestamp<T>(array: &PrimitiveArray<T>, row_index: usize) -> Result<DateTime<Utc>>
where
    T: ArrowTemporalType,
    i64: From<T::Native>,
{
    static UTC: LazyLock<Tz> = LazyLock::new(|| "+00:00".parse().unwrap());

    let Some(timestamp) = array.value_as_datetime_with_tz(row_index, *UTC) else {
        return Err(anyhow!("failed to decode timestamp; unknown timezone"));
    };

    Ok(timestamp.to_utc())
}
