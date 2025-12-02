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

/// Decodes Arrow arrays into subgraph types.
pub struct ArrayDecoder<'a, T: 'static>(&'a T);

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
    pub fn new(array: &'a dyn Array) -> Result<Self> {
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
        self.value(row_index, |value| Ok(value.to_f32().into()))
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, Float32Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |value| Ok(value.into()))
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, Float64Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |value| Ok(value.into()))
    }
}

impl Decoder<Option<i32>> for ArrayDecoder<'_, Decimal128Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i32>> {
        if self.0.scale() != 0 {
            return Err(anyhow!("cannot decode `i32` from a decimal value"));
        }

        self.value(row_index, decode_i32)
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Decimal128Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        if self.0.scale() != 0 {
            return Err(anyhow!("cannot decode `i64` from a decimal value"));
        }

        self.value(row_index, decode_i64)
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Decimal128Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        if self.0.scale() != 0 {
            return Err(anyhow!("cannot decode `BigInt` from a decimal value"));
        }

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
        if self.0.scale() != 0 {
            return Err(anyhow!("cannot decode `i32` from a decimal value"));
        }

        self.value(row_index, |value| {
            let value = value
                .to_i128()
                .ok_or_else(|| anyhow!("cannot decode `i32` from a larger `i256` value"))?;

            decode_i32(value)
        })
    }
}

impl Decoder<Option<i64>> for ArrayDecoder<'_, Decimal256Array> {
    fn decode(&self, row_index: usize) -> Result<Option<i64>> {
        if self.0.scale() != 0 {
            return Err(anyhow!("cannot decode `i64` from a decimal value"));
        }

        self.value(row_index, |value| {
            let value = value
                .to_i128()
                .ok_or_else(|| anyhow!("cannot decode `i64` from a larger `i256` value"))?;

            decode_i64(value)
        })
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, Decimal256Array> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        if self.0.scale() != 0 {
            return Err(anyhow!("cannot decode `BigInt` from a decimal value"));
        }

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

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, StringArray> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |value| {
            value
                .parse()
                .map_err(|_| anyhow!("failed to parse `BigInt` from a non-numeric string value"))
        })
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, StringArray> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |value| {
            value.parse().map_err(|_| {
                anyhow!("failed to parse `BigDecimal` from a non-numeric string value")
            })
        })
    }
}

impl Decoder<Option<String>> for ArrayDecoder<'_, StringViewArray> {
    fn decode(&self, row_index: usize) -> Result<Option<String>> {
        self.value(row_index, |x| Ok(x.to_string()))
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, StringViewArray> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |value| {
            value
                .parse()
                .map_err(|_| anyhow!("failed to parse `BigInt` from a non-numeric string value"))
        })
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, StringViewArray> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |value| {
            value.parse().map_err(|_| {
                anyhow!("failed to parse `BigDecimal` from a non-numeric string value")
            })
        })
    }
}

impl Decoder<Option<String>> for ArrayDecoder<'_, LargeStringArray> {
    fn decode(&self, row_index: usize) -> Result<Option<String>> {
        self.value(row_index, |x| Ok(x.to_string()))
    }
}

impl Decoder<Option<BigInt>> for ArrayDecoder<'_, LargeStringArray> {
    fn decode(&self, row_index: usize) -> Result<Option<BigInt>> {
        self.value(row_index, |value| {
            value
                .parse()
                .map_err(|_| anyhow!("failed to parse `BigInt` from a non-numeric string value"))
        })
    }
}

impl Decoder<Option<BigDecimal>> for ArrayDecoder<'_, LargeStringArray> {
    fn decode(&self, row_index: usize) -> Result<Option<BigDecimal>> {
        self.value(row_index, |value| {
            value.parse().map_err(|_| {
                anyhow!("failed to parse `BigDecimal` from a non-numeric string value")
            })
        })
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::i256;
    use chrono::TimeZone;
    use half::f16;

    use super::super::test_fixtures::*;
    use super::*;

    mod boolean_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, BooleanArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("boolean").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_values() {
            let decoder = decoder::<bool>();

            assert_eq!(decoder.decode(0).unwrap(), Some(true));
            assert_eq!(decoder.decode(1).unwrap(), Some(false));
            assert_eq!(decoder.decode(2).unwrap(), Some(true));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<bool>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<UInt32Array>::new(RECORD_BATCH.column_by_name("boolean").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod int8_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Int8Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Int8Array>::new(RECORD_BATCH.column_by_name("int8").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
            assert_eq!(decoder.decode(2).unwrap(), Some(i8::MAX as i32));
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
            assert_eq!(decoder.decode(2).unwrap(), Some(i8::MAX as i64));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(i8::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("int8").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod int16_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Int16Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Int16Array>::new(RECORD_BATCH.column_by_name("int16").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
            assert_eq!(decoder.decode(2).unwrap(), Some(i16::MAX as i32));
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
            assert_eq!(decoder.decode(2).unwrap(), Some(i16::MAX as i64));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(i16::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("int16").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod int32_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Int32Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Int32Array>::new(RECORD_BATCH.column_by_name("int32").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
            assert_eq!(decoder.decode(2).unwrap(), Some(i32::MAX));
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
            assert_eq!(decoder.decode(2).unwrap(), Some(i32::MAX as i64));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(i32::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("int32").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod int64_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Int64Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Int64Array>::new(RECORD_BATCH.column_by_name("int64").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
        }

        #[test]
        fn fail_to_decode_i32_values_from_larger_values() {
            let decoder = decoder::<i32>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
            assert_eq!(decoder.decode(2).unwrap(), Some(i64::MAX));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(i64::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("int64").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod uint8_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, UInt8Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<UInt8Array>::new(RECORD_BATCH.column_by_name("uint8").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
            assert_eq!(decoder.decode(2).unwrap(), Some(u8::MAX as i32));
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
            assert_eq!(decoder.decode(2).unwrap(), Some(u8::MAX as i64));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(u8::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("uint8").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod uint16_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, UInt16Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<UInt16Array>::new(RECORD_BATCH.column_by_name("uint16").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
            assert_eq!(decoder.decode(2).unwrap(), Some(u16::MAX as i32));
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
            assert_eq!(decoder.decode(2).unwrap(), Some(u16::MAX as i64));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(u16::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("uint16").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod uint32_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, UInt32Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<UInt32Array>::new(RECORD_BATCH.column_by_name("uint32").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
        }

        #[test]
        fn fail_to_decode_i32_values_from_larger_values() {
            let decoder = decoder::<i32>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
            assert_eq!(decoder.decode(2).unwrap(), Some(u32::MAX as i64));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(u32::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("uint32").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod uint64_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, UInt64Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<UInt64Array>::new(RECORD_BATCH.column_by_name("uint64").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
        }

        #[test]
        fn fail_to_decode_i32_values_from_larger_values() {
            let decoder = decoder::<i32>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
        }

        #[test]
        fn fail_to_decode_i64_values_from_larger_values() {
            let decoder = decoder::<i64>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(u64::MAX)));
        }

        #[test]
        fn decode_valid_u64_values() {
            let decoder = decoder::<u64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10u64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20u64));
            assert_eq!(decoder.decode(2).unwrap(), Some(u64::MAX));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("uint64").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod float16_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Float16Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Float16Array>::new(RECORD_BATCH.column_by_name("float16").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigDecimal::from(10.0)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigDecimal::from(20.0)));
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(BigDecimal::from(f16::MAX.to_f32()))
            );
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<BigDecimal>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("float16").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod float32_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Float32Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Float32Array>::new(RECORD_BATCH.column_by_name("float32").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigDecimal::from(10.0)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigDecimal::from(20.0)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigDecimal::from(f32::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<BigDecimal>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("float32").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod float64_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Float64Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Float64Array>::new(RECORD_BATCH.column_by_name("float64").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigDecimal::from(10.0)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigDecimal::from(20.0)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigDecimal::from(f64::MAX)));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<BigDecimal>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("float64").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod decimal128_decoder_without_scale {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Decimal128Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Decimal128Array>::new(
                    RECORD_BATCH.column_by_name("decimal128").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
        }

        #[test]
        fn fail_to_decode_i32_values_from_larger_values() {
            let decoder = decoder::<i32>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
        }

        #[test]
        fn fail_to_decode_i64_values_from_larger_values() {
            let decoder = decoder::<i64>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(i128::MAX)));
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigDecimal::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigDecimal::from(20)));
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(BigDecimal::from(i128::MAX))
            );
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("decimal128").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod decimal128_decoder_with_scale {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Decimal128Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Decimal128Array>::new(
                    RECORD_BATCH
                        .column_by_name("decimal128_with_scale")
                        .unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn fail_to_decode_i32_values() {
            let decoder = decoder::<i32>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn fail_to_decode_i64_values() {
            let decoder = decoder::<i64>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn fail_to_decode_big_int_values() {
            let decoder = decoder::<BigInt>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(
                decoder.decode(0).unwrap(),
                Some(BigDecimal::new(10.into(), -10))
            );
            assert_eq!(
                decoder.decode(1).unwrap(),
                Some(BigDecimal::new(20.into(), -10))
            );
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(BigDecimal::new(i128::MAX.into(), -10))
            );
        }
    }

    mod decimal256_decoder_without_scale {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Decimal256Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Decimal256Array>::new(
                    RECORD_BATCH.column_by_name("decimal256").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_i32_values() {
            let decoder = decoder::<i32>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i32));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i32));
        }

        #[test]
        fn fail_to_decode_i32_values_from_larger_values() {
            let decoder = decoder::<i32>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_i64_values() {
            let decoder = decoder::<i64>();

            assert_eq!(decoder.decode(0).unwrap(), Some(10i64));
            assert_eq!(decoder.decode(1).unwrap(), Some(20i64));
        }

        #[test]
        fn fail_to_decode_i64_values_from_larger_values() {
            let decoder = decoder::<i64>();

            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigInt::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigInt::from(20)));
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(BigInt::from_signed_bytes_be(&i256::MAX.to_be_bytes()).unwrap())
            );
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(0).unwrap(), Some(BigDecimal::from(10)));
            assert_eq!(decoder.decode(1).unwrap(), Some(BigDecimal::from(20)));
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(BigDecimal::new(
                    BigInt::from_signed_bytes_be(&i256::MAX.to_be_bytes()).unwrap(),
                    0
                ))
            );
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<i32>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("decimal256").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod decimal256_decoder_with_scale {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, Decimal256Array>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<Decimal256Array>::new(
                    RECORD_BATCH
                        .column_by_name("decimal256_with_scale")
                        .unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn fail_to_decode_i32_values() {
            let decoder = decoder::<i32>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn fail_to_decode_i64_values() {
            let decoder = decoder::<i64>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn fail_to_decode_big_int_values() {
            let decoder = decoder::<BigInt>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(
                decoder.decode(0).unwrap(),
                Some(BigDecimal::new(10.into(), -10))
            );
            assert_eq!(
                decoder.decode(1).unwrap(),
                Some(BigDecimal::new(20.into(), -10))
            );
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(BigDecimal::new(
                    BigInt::from_signed_bytes_be(&i256::MAX.to_be_bytes()).unwrap(),
                    -10
                ))
            );
        }
    }

    mod utf8_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, StringArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<StringArray>::new(RECORD_BATCH.column_by_name("utf8").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_string_values() {
            let decoder = decoder::<String>();

            assert_eq!(decoder.decode(0).unwrap(), Some("aa".to_string()));
            assert_eq!(decoder.decode(1).unwrap(), Some("bb".to_string()));
            assert_eq!(decoder.decode(2).unwrap(), Some("30".to_string()));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(30)));
        }

        #[test]
        fn fail_to_decode_big_int_values_from_non_numeric_values() {
            let decoder = decoder::<BigInt>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(2).unwrap(), Some(BigDecimal::from(30)));
        }

        #[test]
        fn fail_to_decode_big_decimal_values_from_non_numeric_values() {
            let decoder = decoder::<BigDecimal>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<String>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("utf8").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod utf8_view_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, StringViewArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<StringViewArray>::new(
                    RECORD_BATCH.column_by_name("utf8_view").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_string_values() {
            let decoder = decoder::<String>();

            assert_eq!(decoder.decode(0).unwrap(), Some("aa".to_string()));
            assert_eq!(decoder.decode(1).unwrap(), Some("bb".to_string()));
            assert_eq!(decoder.decode(2).unwrap(), Some("30".to_string()));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(30)));
        }

        #[test]
        fn fail_to_decode_big_int_values_from_non_numeric_values() {
            let decoder = decoder::<BigInt>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(2).unwrap(), Some(BigDecimal::from(30)));
        }

        #[test]
        fn fail_to_decode_big_decimal_values_from_non_numeric_values() {
            let decoder = decoder::<BigDecimal>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<String>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("utf8_view").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod large_utf8_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, LargeStringArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<LargeStringArray>::new(
                    RECORD_BATCH.column_by_name("large_utf8").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_string_values() {
            let decoder = decoder::<String>();

            assert_eq!(decoder.decode(0).unwrap(), Some("aa".to_string()));
            assert_eq!(decoder.decode(1).unwrap(), Some("bb".to_string()));
            assert_eq!(decoder.decode(2).unwrap(), Some("30".to_string()));
        }

        #[test]
        fn decode_valid_big_int_values() {
            let decoder = decoder::<BigInt>();

            assert_eq!(decoder.decode(2).unwrap(), Some(BigInt::from(30)));
        }

        #[test]
        fn fail_to_decode_big_int_values_from_non_numeric_values() {
            let decoder = decoder::<BigInt>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
        }

        #[test]
        fn decode_valid_big_decimal_values() {
            let decoder = decoder::<BigDecimal>();

            assert_eq!(decoder.decode(2).unwrap(), Some(BigDecimal::from(30)));
        }

        #[test]
        fn fail_to_decode_big_decimal_values_from_non_numeric_values() {
            let decoder = decoder::<BigDecimal>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<String>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("large_utf8").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod binary_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, BinaryArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<BinaryArray>::new(RECORD_BATCH.column_by_name("binary").unwrap())
                    .unwrap(),
            )
        }

        #[test]
        fn decode_valid_binary_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert_eq!(decoder.decode(0).unwrap(), Some((b"aa".as_slice()).into()));
            assert_eq!(decoder.decode(1).unwrap(), Some((b"bb".as_slice()).into()));
            assert_eq!(decoder.decode(2).unwrap(), Some((b"cc".as_slice()).into()));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("binary").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod binary_view_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, BinaryViewArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<BinaryViewArray>::new(
                    RECORD_BATCH.column_by_name("binary_view").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_binary_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert_eq!(decoder.decode(0).unwrap(), Some((b"aa".as_slice()).into()));
            assert_eq!(decoder.decode(1).unwrap(), Some((b"bb".as_slice()).into()));
            assert_eq!(decoder.decode(2).unwrap(), Some((b"cc".as_slice()).into()));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("binary_view").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod fixed_size_binary_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, FixedSizeBinaryArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<FixedSizeBinaryArray>::new(
                    RECORD_BATCH.column_by_name("fixed_size_binary").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_binary_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert_eq!(decoder.decode(0).unwrap(), Some((b"aa".as_slice()).into()));
            assert_eq!(decoder.decode(1).unwrap(), Some((b"bb".as_slice()).into()));
            assert_eq!(decoder.decode(2).unwrap(), Some((b"cc".as_slice()).into()));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_decode_b256_values_from_invalid_binary_size() {
            let decoder = decoder::<B256>();

            decoder.decode(0).unwrap_err();
            decoder.decode(1).unwrap_err();
            decoder.decode(2).unwrap_err();
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(
                RECORD_BATCH.column_by_name("fixed_size_binary").unwrap(),
            )
            .map(|_| ())
            .unwrap_err();
        }
    }

    mod fixed_size_binary_32_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, FixedSizeBinaryArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<FixedSizeBinaryArray>::new(
                    RECORD_BATCH.column_by_name("fixed_size_binary_32").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_b256_values() {
            let decoder = decoder::<B256>();

            assert_eq!(decoder.decode(0).unwrap(), Some(B256::from([10u8; 32])));
            assert_eq!(decoder.decode(1).unwrap(), Some(B256::from([20u8; 32])));
            assert_eq!(decoder.decode(2).unwrap(), Some(B256::from([30u8; 32])));
        }
    }

    mod large_binary_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, LargeBinaryArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<LargeBinaryArray>::new(
                    RECORD_BATCH.column_by_name("large_binary").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_binary_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert_eq!(decoder.decode(0).unwrap(), Some((b"aa".as_slice()).into()));
            assert_eq!(decoder.decode(1).unwrap(), Some((b"bb".as_slice()).into()));
            assert_eq!(decoder.decode(2).unwrap(), Some((b"cc".as_slice()).into()));
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<Box<[u8]>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(RECORD_BATCH.column_by_name("large_binary").unwrap())
                .map(|_| ())
                .unwrap_err();
        }
    }

    mod timestamp_second_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, TimestampSecondArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<TimestampSecondArray>::new(
                    RECORD_BATCH.column_by_name("timestamp_second").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert_eq!(
                decoder.decode(0).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap())
            );
            assert_eq!(
                decoder.decode(1).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 10, 10, 10, 10, 10).unwrap())
            );
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 12, 31, 23, 59, 59).unwrap())
            );
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(
                RECORD_BATCH.column_by_name("timestamp_second").unwrap(),
            )
            .map(|_| ())
            .unwrap_err();
        }
    }

    mod timestamp_millisecond_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, TimestampMillisecondArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<TimestampMillisecondArray>::new(
                    RECORD_BATCH
                        .column_by_name("timestamp_millisecond")
                        .unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert_eq!(
                decoder.decode(0).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap())
            );
            assert_eq!(
                decoder.decode(1).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 10, 10, 10, 10, 10).unwrap())
            );
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 12, 31, 23, 59, 59).unwrap())
            );
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(
                RECORD_BATCH
                    .column_by_name("timestamp_millisecond")
                    .unwrap(),
            )
            .map(|_| ())
            .unwrap_err();
        }
    }

    mod timestamp_microsecond_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, TimestampMicrosecondArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<TimestampMicrosecondArray>::new(
                    RECORD_BATCH
                        .column_by_name("timestamp_microsecond")
                        .unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert_eq!(
                decoder.decode(0).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap())
            );
            assert_eq!(
                decoder.decode(1).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 10, 10, 10, 10, 10).unwrap())
            );
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 12, 31, 23, 59, 59).unwrap())
            );
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(
                RECORD_BATCH
                    .column_by_name("timestamp_microsecond")
                    .unwrap(),
            )
            .map(|_| ())
            .unwrap_err();
        }
    }

    mod timestamp_nanosecond_decoder {
        use super::*;

        fn decoder<T>() -> Box<dyn Decoder<Option<T>>>
        where
            ArrayDecoder<'static, TimestampNanosecondArray>: Decoder<Option<T>>,
        {
            Box::new(
                ArrayDecoder::<TimestampNanosecondArray>::new(
                    RECORD_BATCH.column_by_name("timestamp_nanosecond").unwrap(),
                )
                .unwrap(),
            )
        }

        #[test]
        fn decode_valid_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert_eq!(
                decoder.decode(0).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap())
            );
            assert_eq!(
                decoder.decode(1).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 10, 10, 10, 10, 10).unwrap())
            );
            assert_eq!(
                decoder.decode(2).unwrap(),
                Some(Utc.with_ymd_and_hms(2020, 12, 31, 23, 59, 59).unwrap())
            );
        }

        #[test]
        fn handle_missing_values() {
            let decoder = decoder::<DateTime<Utc>>();

            assert!(decoder.decode(3).unwrap().is_none());
            assert!(decoder.decode(4).unwrap().is_none());
        }

        #[test]
        fn fail_to_create_decoder_with_invalid_type() {
            ArrayDecoder::<BooleanArray>::new(
                RECORD_BATCH.column_by_name("timestamp_nanosecond").unwrap(),
            )
            .map(|_| ())
            .unwrap_err();
        }
    }
}
