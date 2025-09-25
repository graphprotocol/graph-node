use anyhow::{anyhow, Context, Result};
use arrow::{
    array::{
        Array, BinaryArray, BinaryViewArray, BooleanArray, Decimal128Array, Decimal256Array,
        FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeListArray,
        LargeListViewArray, LargeStringArray, ListArray, ListViewArray, StringArray,
        StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{DataType, TimeUnit},
};
use chrono::{DateTime, Utc};

use super::{ArrayDecoder, Decoder, ListDecoder, MappingDecoder};
use crate::data::store::{
    scalar::{BigDecimal, BigInt, Bytes, Timestamp},
    Value, ValueType,
};

/// Returns a decoder that converts an Arrow array into Subgraph store values.
///
/// # Errors
///
/// Returns an error if the Subgraph store type is not compatible with the Arrow array type.
///
/// The returned error is deterministic.
pub(super) fn value_decoder<'a>(
    value_type: ValueType,
    is_list: bool,
    array: &'a dyn Array,
) -> Result<Box<dyn Decoder<Value> + 'a>> {
    let decoder = if is_list {
        list_value_decoder(value_type, array)
    } else {
        single_value_decoder(value_type, array)
    };

    decoder.with_context(|| {
        format!(
            "failed to decode '{}' from '{}'",
            value_type.to_str(),
            array.data_type(),
        )
    })
}

fn list_value_decoder<'a>(
    value_type: ValueType,
    array: &'a dyn Array,
) -> Result<Box<dyn Decoder<Value> + 'a>> {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let decoder = single_value_decoder(value_type, list.values())?;
            let list_decoder = ListDecoder::new(decoder, list.value_offsets().into());

            Ok(mapping_decoder(list_decoder, Value::List))
        }
        DataType::ListView(_) => {
            let list = array.as_any().downcast_ref::<ListViewArray>().unwrap();
            let decoder = single_value_decoder(value_type, list.values())?;
            let list_decoder = ListDecoder::new(decoder, list.value_offsets().into());

            Ok(mapping_decoder(list_decoder, Value::List))
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let decoder = single_value_decoder(value_type, list.values())?;
            let list_decoder = ListDecoder::new(decoder, list.value_length().into());

            Ok(mapping_decoder(list_decoder, Value::List))
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let decoder = single_value_decoder(value_type, list.values())?;
            let list_decoder = ListDecoder::new(decoder, list.value_offsets().into());

            Ok(mapping_decoder(list_decoder, Value::List))
        }
        DataType::LargeListView(_) => {
            let list = array.as_any().downcast_ref::<LargeListViewArray>().unwrap();
            let decoder = single_value_decoder(value_type, list.values())?;
            let list_decoder = ListDecoder::new(decoder, list.value_offsets().into());

            Ok(mapping_decoder(list_decoder, Value::List))
        }
        data_type => Err(anyhow!("'{data_type}' is not a supported list type")),
    }
}

fn single_value_decoder<'a>(
    value_type: ValueType,
    array: &'a dyn Array,
) -> Result<Box<dyn Decoder<Value> + 'a>> {
    let incompatible_types_err = || Err(anyhow!("incompatible types"));

    let decoder = match (value_type, array.data_type()) {
        (ValueType::Boolean, DataType::Boolean) => {
            let array_decoder = ArrayDecoder::<BooleanArray>::new(array)?;
            mapping_decoder(array_decoder, Value::Bool)
        }
        (ValueType::Boolean, _) => return incompatible_types_err(),

        (ValueType::Int, data_type) if is_integer(data_type) => {
            let integer_decoder = integer_decoder::<Option<i32>>(array)?;
            mapping_decoder(integer_decoder, Value::Int)
        }
        (ValueType::Int, _) => return incompatible_types_err(),

        (ValueType::Int8, data_type) if is_integer(data_type) => {
            let integer_decoder = integer_decoder::<Option<i64>>(array)?;
            mapping_decoder(integer_decoder, Value::Int8)
        }
        (ValueType::Int8, _) => return incompatible_types_err(),

        (ValueType::BigInt, data_type) if is_integer(data_type) => {
            let integer_decoder = integer_decoder::<Option<BigInt>>(array)?;
            mapping_decoder(integer_decoder, Value::BigInt)
        }
        (ValueType::BigInt, _) => return incompatible_types_err(),

        (ValueType::BigDecimal, data_type) if is_decimal(data_type) => {
            let decimal_decoder = decimal_decoder::<Option<BigDecimal>>(array)?;
            mapping_decoder(decimal_decoder, Value::BigDecimal)
        }
        (ValueType::BigDecimal, _) => return incompatible_types_err(),

        (ValueType::Bytes, data_type) if is_binary(data_type) => {
            let binary_decoder = binary_decoder::<Option<Box<[u8]>>>(array)?;
            mapping_decoder(binary_decoder, |x| Bytes::from(&*x).into())
        }
        (ValueType::Bytes, _) => return incompatible_types_err(),

        (ValueType::String, data_type) if is_string(data_type) => {
            let string_decoder = string_decoder::<Option<String>>(array)?;
            mapping_decoder(string_decoder, Value::String)
        }
        (ValueType::String, data_type) if is_integer(data_type) => {
            let integer_decoder = integer_decoder::<Option<BigInt>>(array)?;
            mapping_decoder(integer_decoder, |x| x.to_string().into())
        }
        (ValueType::String, data_type) if is_binary(data_type) => {
            let binary_decoder = binary_decoder::<Option<Box<[u8]>>>(array)?;
            mapping_decoder(binary_decoder, |x| format!("0x{}", hex::encode(x)).into())
        }
        (ValueType::String, _) => return incompatible_types_err(),

        (ValueType::Timestamp, data_type) if is_timestamp(data_type) => {
            let timestamp_decoder = timestamp_decoder::<Option<DateTime<Utc>>>(array)?;
            mapping_decoder(timestamp_decoder, |x| Timestamp(x).into())
        }
        (ValueType::Timestamp, _) => return incompatible_types_err(),
    };

    Ok(decoder)
}

fn mapping_decoder<'a, T, U: 'static>(
    array_decoder: T,
    mapping: fn(U) -> Value,
) -> Box<dyn Decoder<Value> + 'a>
where
    T: Decoder<Option<U>> + 'a,
{
    Box::new(MappingDecoder::new(
        array_decoder,
        move |value: Option<U>| match value {
            Some(value) => mapping(value),
            None => Value::Null,
        },
    ))
}

fn is_integer(data_type: &DataType) -> bool {
    use DataType::*;

    matches! {
        data_type,
        Int8 | Int16 | Int32 | Int64 |
        UInt8 | UInt16 | UInt32 | UInt64 |
        Decimal128(_, 0) | Decimal256(_, 0)
    }
}

fn integer_decoder<'a, T>(array: &'a dyn Array) -> Result<Box<dyn Decoder<T> + 'a>>
where
    T: 'static,
    ArrayDecoder<'a, Int8Array>: Decoder<T>,
    ArrayDecoder<'a, Int16Array>: Decoder<T>,
    ArrayDecoder<'a, Int32Array>: Decoder<T>,
    ArrayDecoder<'a, Int64Array>: Decoder<T>,
    ArrayDecoder<'a, UInt8Array>: Decoder<T>,
    ArrayDecoder<'a, UInt16Array>: Decoder<T>,
    ArrayDecoder<'a, UInt32Array>: Decoder<T>,
    ArrayDecoder<'a, UInt64Array>: Decoder<T>,
    ArrayDecoder<'a, Decimal128Array>: Decoder<T>,
    ArrayDecoder<'a, Decimal256Array>: Decoder<T>,
{
    use DataType::*;

    let array_decoder: Box<dyn Decoder<T>> = match array.data_type() {
        Int8 => Box::new(ArrayDecoder::<Int8Array>::new(array)?),
        Int16 => Box::new(ArrayDecoder::<Int16Array>::new(array)?),
        Int32 => Box::new(ArrayDecoder::<Int32Array>::new(array)?),
        Int64 => Box::new(ArrayDecoder::<Int64Array>::new(array)?),
        UInt8 => Box::new(ArrayDecoder::<UInt8Array>::new(array)?),
        UInt16 => Box::new(ArrayDecoder::<UInt16Array>::new(array)?),
        UInt32 => Box::new(ArrayDecoder::<UInt32Array>::new(array)?),
        UInt64 => Box::new(ArrayDecoder::<UInt64Array>::new(array)?),
        Decimal128(_, 0) => Box::new(ArrayDecoder::<Decimal128Array>::new(array)?),
        Decimal256(_, 0) => Box::new(ArrayDecoder::<Decimal256Array>::new(array)?),
        data_type => return Err(anyhow!("'{data_type}' is not a supported integer type")),
    };

    Ok(array_decoder)
}

fn is_decimal(data_type: &DataType) -> bool {
    use DataType::*;

    matches! {
        data_type,
        Float16 | Float32 | Float64 |
        Decimal128(_, _) | Decimal256(_, _)
    }
}

fn decimal_decoder<'a, T>(array: &'a dyn Array) -> Result<Box<dyn Decoder<T> + 'a>>
where
    T: 'static,
    ArrayDecoder<'a, Float16Array>: Decoder<T>,
    ArrayDecoder<'a, Float32Array>: Decoder<T>,
    ArrayDecoder<'a, Float64Array>: Decoder<T>,
    ArrayDecoder<'a, Decimal128Array>: Decoder<T>,
    ArrayDecoder<'a, Decimal256Array>: Decoder<T>,
{
    use DataType::*;

    let array_decoder: Box<dyn Decoder<T>> = match array.data_type() {
        Float16 => Box::new(ArrayDecoder::<Float16Array>::new(array)?),
        Float32 => Box::new(ArrayDecoder::<Float32Array>::new(array)?),
        Float64 => Box::new(ArrayDecoder::<Float64Array>::new(array)?),
        Decimal128(_, _) => Box::new(ArrayDecoder::<Decimal128Array>::new(array)?),
        Decimal256(_, _) => Box::new(ArrayDecoder::<Decimal256Array>::new(array)?),
        data_type => return Err(anyhow!("'{data_type}' is not a supported decimal type")),
    };

    Ok(array_decoder)
}

fn is_binary(data_type: &DataType) -> bool {
    use DataType::*;

    matches! {
        data_type,
        Binary | BinaryView | FixedSizeBinary(_) | LargeBinary
    }
}

fn binary_decoder<'a, T>(array: &'a dyn Array) -> Result<Box<dyn Decoder<T> + 'a>>
where
    T: 'static,
    ArrayDecoder<'a, BinaryArray>: Decoder<T>,
    ArrayDecoder<'a, BinaryViewArray>: Decoder<T>,
    ArrayDecoder<'a, FixedSizeBinaryArray>: Decoder<T>,
    ArrayDecoder<'a, LargeBinaryArray>: Decoder<T>,
{
    use DataType::*;

    let array_decoder: Box<dyn Decoder<T>> = match array.data_type() {
        Binary => Box::new(ArrayDecoder::<BinaryArray>::new(array)?),
        BinaryView => Box::new(ArrayDecoder::<BinaryViewArray>::new(array)?),
        FixedSizeBinary(_) => Box::new(ArrayDecoder::<FixedSizeBinaryArray>::new(array)?),
        LargeBinary => Box::new(ArrayDecoder::<LargeBinaryArray>::new(array)?),
        data_type => return Err(anyhow!("'{data_type}' is not a supported binary type")),
    };

    Ok(array_decoder)
}

fn is_string(data_type: &DataType) -> bool {
    use DataType::*;

    matches! {
        data_type,
        Utf8 | Utf8View | LargeUtf8
    }
}

fn string_decoder<'a, T>(array: &'a dyn Array) -> Result<Box<dyn Decoder<T> + 'a>>
where
    T: 'static,
    ArrayDecoder<'a, StringArray>: Decoder<T>,
    ArrayDecoder<'a, StringViewArray>: Decoder<T>,
    ArrayDecoder<'a, LargeStringArray>: Decoder<T>,
{
    use DataType::*;

    let array_decoder: Box<dyn Decoder<T>> = match array.data_type() {
        Utf8 => Box::new(ArrayDecoder::<StringArray>::new(array)?),
        Utf8View => Box::new(ArrayDecoder::<StringViewArray>::new(array)?),
        LargeUtf8 => Box::new(ArrayDecoder::<LargeStringArray>::new(array)?),
        data_type => return Err(anyhow!("'{data_type}' is not a supported string type")),
    };

    Ok(array_decoder)
}

fn is_timestamp(data_type: &DataType) -> bool {
    use DataType::*;

    matches! {
        data_type,
        Timestamp(TimeUnit::Second, _) |
        Timestamp(TimeUnit::Millisecond, _) |
        Timestamp(TimeUnit::Microsecond, _) |
        Timestamp(TimeUnit::Nanosecond, _)
    }
}

fn timestamp_decoder<'a, T>(array: &'a dyn Array) -> Result<Box<dyn Decoder<T> + 'a>>
where
    T: 'static,
    ArrayDecoder<'a, TimestampSecondArray>: Decoder<T>,
    ArrayDecoder<'a, TimestampMillisecondArray>: Decoder<T>,
    ArrayDecoder<'a, TimestampMicrosecondArray>: Decoder<T>,
    ArrayDecoder<'a, TimestampNanosecondArray>: Decoder<T>,
{
    use DataType::*;

    let array_decoder: Box<dyn Decoder<T>> = match array.data_type() {
        Timestamp(TimeUnit::Second, _) => {
            Box::new(ArrayDecoder::<TimestampSecondArray>::new(array)?) //
        }
        Timestamp(TimeUnit::Millisecond, _) => {
            Box::new(ArrayDecoder::<TimestampMillisecondArray>::new(array)?) //
        }
        Timestamp(TimeUnit::Microsecond, _) => {
            Box::new(ArrayDecoder::<TimestampMicrosecondArray>::new(array)?) //
        }
        Timestamp(TimeUnit::Nanosecond, _) => {
            Box::new(ArrayDecoder::<TimestampNanosecondArray>::new(array)?) //
        }
        data_type => return Err(anyhow!("'{data_type}' is not a supported timestamp type")),
    };

    Ok(array_decoder)
}
