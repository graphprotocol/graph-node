use std::sync::{Arc, LazyLock};

use arrow::{
    array::{
        BinaryArray, BinaryViewArray, BooleanArray, BooleanBuilder, Decimal128Builder,
        Decimal256Builder, FixedSizeBinaryArray, FixedSizeListBuilder, Float16Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
        LargeListBuilder, LargeListViewBuilder, LargeStringArray, ListBuilder, ListViewBuilder,
        RecordBatch, StringArray, StringViewArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{
        i256, DataType, Field, Schema, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
    },
};
use chrono::{TimeZone, Utc};
use half::f16;

pub static RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let record_batches = [
        &BOOLEAN_RECORD_BATCH,
        &INT_RECORD_BATCH,
        &UINT_RECORD_BATCH,
        &DECIMAL_RECORD_BATCH,
        &FLOAT_RECORD_BATCH,
        &STRING_RECORD_BATCH,
        &BINARY_RECORD_BATCH,
        &TIMESTAMP_RECORD_BATCH,
    ];

    let schemas = record_batches
        .iter()
        .map(|record_batch| (*record_batch.schema()).clone());

    let columns = record_batches
        .into_iter()
        .flat_map(|record_batch| record_batch.columns())
        .cloned()
        .collect::<Vec<_>>();

    RecordBatch::try_new(Schema::try_merge(schemas).unwrap().into(), columns).unwrap()
});

pub static BOOLEAN_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new("boolean", DataType::Boolean, true),
        Field::new(
            "boolean_list",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
        Field::new(
            "boolean_list_view",
            DataType::ListView(Arc::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
        Field::new(
            "boolean_fixed_size_list",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Boolean, true)), 3),
            true,
        ),
        Field::new(
            "boolean_large_list",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
        Field::new(
            "boolean_large_list_view",
            DataType::LargeListView(Arc::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
    ]);

    let builder = || {
        let mut builder = BooleanBuilder::new();
        builder.append_value(true);
        builder.append_value(false);
        builder.append_value(true);
        builder
    };

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new({
                let mut list_builder = ListBuilder::new(builder());
                list_builder.append(true);
                list_builder.append(false);
                list_builder.append(false);
                list_builder.finish()
            }),
            Arc::new({
                let mut list_builder = ListViewBuilder::new(builder());
                list_builder.append(true);
                list_builder.append(false);
                list_builder.append(false);
                list_builder.finish()
            }),
            Arc::new({
                let mut list_builder = FixedSizeListBuilder::new(builder(), 3);
                list_builder.append(true);
                list_builder.values().append_null();
                list_builder.values().append_null();
                list_builder.values().append_null();
                list_builder.append(false);
                list_builder.values().append_null();
                list_builder.values().append_null();
                list_builder.values().append_null();
                list_builder.append(false);
                list_builder.finish()
            }),
            Arc::new({
                let mut list_builder = LargeListBuilder::new(builder());
                list_builder.append(true);
                list_builder.append(false);
                list_builder.append(false);
                list_builder.finish()
            }),
            Arc::new({
                let mut list_builder = LargeListViewBuilder::new(builder());
                list_builder.append(true);
                list_builder.append(false);
                list_builder.append(false);
                list_builder.finish()
            }),
        ],
    )
    .unwrap()
});

pub static INT_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new("int8", DataType::Int8, true),
        Field::new("int16", DataType::Int16, true),
        Field::new("int32", DataType::Int32, true),
        Field::new("int64", DataType::Int64, true),
    ]);

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int8Array::from(vec![10, 20, i8::MAX])),
            Arc::new(Int16Array::from(vec![10, 20, i16::MAX])),
            Arc::new(Int32Array::from(vec![10, 20, i32::MAX])),
            Arc::new(Int64Array::from(vec![10, 20, i64::MAX])),
        ],
    )
    .unwrap()
});

pub static UINT_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new("uint8", DataType::UInt8, true),
        Field::new("uint16", DataType::UInt16, true),
        Field::new("uint32", DataType::UInt32, true),
        Field::new("uint64", DataType::UInt64, true),
    ]);

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(UInt8Array::from(vec![10, 20, u8::MAX])),
            Arc::new(UInt16Array::from(vec![10, 20, u16::MAX])),
            Arc::new(UInt32Array::from(vec![10, 20, u32::MAX])),
            Arc::new(UInt64Array::from(vec![10, 20, u64::MAX])),
        ],
    )
    .unwrap()
});

pub static DECIMAL_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new(
            "decimal128",
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0),
            true,
        ),
        Field::new(
            "decimal128_with_scale",
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, 10),
            true,
        ),
        Field::new(
            "decimal256",
            DataType::Decimal256(DECIMAL256_MAX_PRECISION, 0),
            true,
        ),
        Field::new(
            "decimal256_with_scale",
            DataType::Decimal256(DECIMAL256_MAX_PRECISION, 10),
            true,
        ),
    ]);

    let decimal_128_array = |scale: i8| {
        let mut builder = Decimal128Builder::new()
            .with_precision_and_scale(DECIMAL128_MAX_PRECISION, scale)
            .unwrap();

        builder.append_value(10);
        builder.append_value(20);
        builder.append_value(i128::MAX);
        builder.finish()
    };

    let decimal_256_array = |scale: i8| {
        let mut builder = Decimal256Builder::new()
            .with_precision_and_scale(DECIMAL256_MAX_PRECISION, scale)
            .unwrap();

        builder.append_value(10.into());
        builder.append_value(20.into());
        builder.append_value(i256::MAX);
        builder.finish()
    };

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(decimal_128_array(0)),
            Arc::new(decimal_128_array(10)),
            Arc::new(decimal_256_array(0)),
            Arc::new(decimal_256_array(10)),
        ],
    )
    .unwrap()
});

pub static FLOAT_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new("float16", DataType::Float16, true),
        Field::new("float32", DataType::Float32, true),
        Field::new("float64", DataType::Float64, true),
    ]);

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Float16Array::from(vec![
                f16::from_f32(10.0),
                f16::from_f32(20.0),
                f16::MAX,
            ])),
            Arc::new(Float32Array::from(vec![10.0, 20.0, f32::MAX])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, f64::MAX])),
        ],
    )
    .unwrap()
});

pub static STRING_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new("utf8", DataType::Utf8, true),
        Field::new("utf8_view", DataType::Utf8View, true),
        Field::new("large_utf8", DataType::LargeUtf8, true),
    ]);

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(StringArray::from(vec!["aa", "bb", "30"])),
            Arc::new(StringViewArray::from(vec!["aa", "bb", "30"])),
            Arc::new(LargeStringArray::from(vec!["aa", "bb", "30"])),
        ],
    )
    .unwrap()
});

pub static BINARY_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new("binary", DataType::Binary, true),
        Field::new("binary_view", DataType::BinaryView, true),
        Field::new("fixed_size_binary", DataType::FixedSizeBinary(2), true),
        Field::new("fixed_size_binary_32", DataType::FixedSizeBinary(32), true),
        Field::new("large_binary", DataType::LargeBinary, true),
    ]);

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(BinaryArray::from(vec![b"aa".as_ref(), b"bb", b"cc"])),
            Arc::new(BinaryViewArray::from(vec![b"aa".as_ref(), b"bb", b"cc"])),
            Arc::new(FixedSizeBinaryArray::from(vec![b"aa", b"bb", b"cc"])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                &[10; 32], &[20; 32], &[30; 32],
            ])),
            Arc::new(LargeBinaryArray::from(vec![b"aa".as_ref(), b"bb", b"cc"])),
        ],
    )
    .unwrap()
});

pub static TIMESTAMP_RECORD_BATCH: LazyLock<RecordBatch> = LazyLock::new(|| {
    let schema = Schema::new(vec![
        Field::new(
            "timestamp_second",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "timestamp_millisecond",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "timestamp_microsecond",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_nanosecond",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]);

    let date_time_one = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let date_time_two = Utc.with_ymd_and_hms(2020, 10, 10, 10, 10, 10).unwrap();
    let date_time_three = Utc.with_ymd_and_hms(2020, 12, 31, 23, 59, 59).unwrap();

    RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(TimestampSecondArray::from(vec![
                date_time_one.timestamp(),
                date_time_two.timestamp(),
                date_time_three.timestamp(),
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                date_time_one.timestamp_millis(),
                date_time_two.timestamp_millis(),
                date_time_three.timestamp_millis(),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                date_time_one.timestamp_micros(),
                date_time_two.timestamp_micros(),
                date_time_three.timestamp_micros(),
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                date_time_one.timestamp_nanos_opt().unwrap(),
                date_time_two.timestamp_nanos_opt().unwrap(),
                date_time_three.timestamp_nanos_opt().unwrap(),
            ])),
        ],
    )
    .unwrap()
});

#[test]
fn record_batch_is_valid() {
    let _schema = BOOLEAN_RECORD_BATCH.schema();
    let _schema = INT_RECORD_BATCH.schema();
    let _schema = UINT_RECORD_BATCH.schema();
    let _schema = DECIMAL_RECORD_BATCH.schema();
    let _schema = FLOAT_RECORD_BATCH.schema();
    let _schema = STRING_RECORD_BATCH.schema();
    let _schema = BINARY_RECORD_BATCH.schema();
    let _schema = TIMESTAMP_RECORD_BATCH.schema();

    let _schema = RECORD_BATCH.schema();
}
