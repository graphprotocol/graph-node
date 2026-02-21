use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Schema};

use graph::components::store::StoreError;

use crate::relational::value::{OidRow, OidValue};

/// Convert a batch of `DynamicRow<OidValue>` rows into an Arrow `RecordBatch`.
///
/// The `schema` defines the expected column names, types, and nullability.
/// Each row must have exactly `schema.fields().len()` values, positionally
/// matching the schema fields.
///
/// The dump query selects `lower(block_range)` and `upper(block_range)` as
/// separate Int32 columns, so the converter never sees `OidValue::Int4Range`.
pub fn rows_to_record_batch(schema: &Schema, rows: &[OidRow]) -> Result<RecordBatch, StoreError> {
    let num_fields = schema.fields().len();
    let num_rows = rows.len();

    // Create builders for each column
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|field| ColumnBuilder::new(field.data_type(), num_rows))
        .collect::<Result<_, _>>()?;

    // Append each row's values to the builders
    for (row_idx, row) in rows.iter().enumerate() {
        if row.len() != num_fields {
            return Err(StoreError::InternalError(format!(
                "row {row_idx} has {} values but schema has {num_fields} fields",
                row.len()
            )));
        }
        for (col_idx, value) in row.into_iter().enumerate() {
            builders[col_idx].append(value, row_idx)?;
        }
    }

    // Finish builders into arrays
    let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| StoreError::InternalError(format!("failed to build RecordBatch: {e}")))
}

/// A type-erased column builder that wraps the appropriate Arrow array builder.
enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Binary(BinaryBuilder),
    Utf8(StringBuilder),
    TimestampMicro(TimestampMicrosecondBuilder),
    BooleanList(ListBuilder<BooleanBuilder>),
    Int32List(ListBuilder<Int32Builder>),
    Int64List(ListBuilder<Int64Builder>),
    BinaryList(ListBuilder<BinaryBuilder>),
    Utf8List(ListBuilder<StringBuilder>),
    TimestampMicroList(ListBuilder<TimestampMicrosecondBuilder>),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, capacity: usize) -> Result<Self, StoreError> {
        match data_type {
            DataType::Boolean => Ok(Self::Boolean(BooleanBuilder::with_capacity(capacity))),
            DataType::Int32 => Ok(Self::Int32(Int32Builder::with_capacity(capacity))),
            DataType::Int64 => Ok(Self::Int64(Int64Builder::with_capacity(capacity))),
            DataType::Binary => Ok(Self::Binary(BinaryBuilder::with_capacity(capacity, 0))),
            DataType::Utf8 => Ok(Self::Utf8(StringBuilder::with_capacity(capacity, 0))),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => Ok(
                Self::TimestampMicro(TimestampMicrosecondBuilder::with_capacity(capacity)),
            ),
            DataType::List(inner) => match inner.data_type() {
                DataType::Boolean => Ok(Self::BooleanList(ListBuilder::with_capacity(
                    BooleanBuilder::new(),
                    capacity,
                ))),
                DataType::Int32 => Ok(Self::Int32List(ListBuilder::with_capacity(
                    Int32Builder::new(),
                    capacity,
                ))),
                DataType::Int64 => Ok(Self::Int64List(ListBuilder::with_capacity(
                    Int64Builder::new(),
                    capacity,
                ))),
                DataType::Binary => Ok(Self::BinaryList(ListBuilder::with_capacity(
                    BinaryBuilder::new(),
                    capacity,
                ))),
                DataType::Utf8 => Ok(Self::Utf8List(ListBuilder::with_capacity(
                    StringBuilder::new(),
                    capacity,
                ))),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
                    Ok(Self::TimestampMicroList(ListBuilder::with_capacity(
                        TimestampMicrosecondBuilder::new(),
                        capacity,
                    )))
                }
                other => Err(StoreError::InternalError(format!(
                    "unsupported list element type: {other:?}"
                ))),
            },
            other => Err(StoreError::InternalError(format!(
                "unsupported Arrow data type for column builder: {other:?}"
            ))),
        }
    }

    fn append(&mut self, value: &OidValue, row_idx: usize) -> Result<(), StoreError> {
        match (self, value) {
            // Null for any builder type
            (Self::Boolean(b), OidValue::Null) => b.append_null(),
            (Self::Int32(b), OidValue::Null) => b.append_null(),
            (Self::Int64(b), OidValue::Null) => b.append_null(),
            (Self::Binary(b), OidValue::Null) => b.append_null(),
            (Self::Utf8(b), OidValue::Null) => b.append_null(),
            (Self::TimestampMicro(b), OidValue::Null) => b.append_null(),
            (Self::BooleanList(b), OidValue::Null) => b.append_null(),
            (Self::Int32List(b), OidValue::Null) => b.append_null(),
            (Self::Int64List(b), OidValue::Null) => b.append_null(),
            (Self::BinaryList(b), OidValue::Null) => b.append_null(),
            (Self::Utf8List(b), OidValue::Null) => b.append_null(),
            (Self::TimestampMicroList(b), OidValue::Null) => b.append_null(),

            // Scalar values
            (Self::Boolean(b), OidValue::Bool(v)) => b.append_value(*v),
            (Self::Int32(b), OidValue::Int(v)) => b.append_value(*v),
            (Self::Int64(b), OidValue::Int8(v)) => b.append_value(*v),
            (Self::Int64(b), OidValue::Int(v)) => b.append_value(*v as i64),
            (Self::Binary(b), OidValue::Bytes(v)) => b.append_value(v.as_ref()),
            (Self::Utf8(b), OidValue::String(v)) => b.append_value(v),
            (Self::Utf8(b), OidValue::BigDecimal(v)) => b.append_value(v.to_string()),
            (Self::TimestampMicro(b), OidValue::Timestamp(v)) => {
                b.append_value(v.as_microseconds_since_epoch())
            }

            // Array values
            (Self::BooleanList(b), OidValue::BoolArray(vals)) => {
                for v in vals {
                    b.values().append_value(*v);
                }
                b.append(true);
            }
            (Self::Int32List(b), OidValue::Ints(vals)) => {
                for v in vals {
                    b.values().append_value(*v);
                }
                b.append(true);
            }
            (Self::Int64List(b), OidValue::Int8Array(vals)) => {
                for v in vals {
                    b.values().append_value(*v);
                }
                b.append(true);
            }
            (Self::BinaryList(b), OidValue::BytesArray(vals)) => {
                for v in vals {
                    b.values().append_value(v.as_ref());
                }
                b.append(true);
            }
            (Self::Utf8List(b), OidValue::StringArray(vals)) => {
                for v in vals {
                    b.values().append_value(v);
                }
                b.append(true);
            }
            (Self::Utf8List(b), OidValue::BigDecimalArray(vals)) => {
                for v in vals {
                    b.values().append_value(v.to_string());
                }
                b.append(true);
            }
            (Self::TimestampMicroList(b), OidValue::TimestampArray(vals)) => {
                for v in vals {
                    b.values().append_value(v.as_microseconds_since_epoch());
                }
                b.append(true);
            }

            (builder, value) => {
                return Err(StoreError::InternalError(format!(
                    "row {row_idx}: type mismatch - cannot append {value:?} to {builder_type}",
                    builder_type = builder.type_name()
                )));
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::TimestampMicro(mut b) => Arc::new(b.finish()),
            Self::BooleanList(mut b) => Arc::new(b.finish()),
            Self::Int32List(mut b) => Arc::new(b.finish()),
            Self::Int64List(mut b) => Arc::new(b.finish()),
            Self::BinaryList(mut b) => Arc::new(b.finish()),
            Self::Utf8List(mut b) => Arc::new(b.finish()),
            Self::TimestampMicroList(mut b) => Arc::new(b.finish()),
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            Self::Boolean(_) => "Boolean",
            Self::Int32(_) => "Int32",
            Self::Int64(_) => "Int64",
            Self::Binary(_) => "Binary",
            Self::Utf8(_) => "Utf8",
            Self::TimestampMicro(_) => "TimestampMicrosecond",
            Self::BooleanList(_) => "List<Boolean>",
            Self::Int32List(_) => "List<Int32>",
            Self::Int64List(_) => "List<Int64>",
            Self::BinaryList(_) => "List<Binary>",
            Self::Utf8List(_) => "List<Utf8>",
            Self::TimestampMicroList(_) => "List<TimestampMicrosecond>",
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BinaryArray, BooleanArray, Int32Array, Int64Array, ListArray, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use graph::data::store::scalar::{BigDecimal, Bytes, Timestamp};
    use std::str::FromStr;
    use std::sync::Arc;

    use super::*;

    fn make_row(values: Vec<OidValue>) -> OidRow {
        values.into_iter().collect()
    }

    #[test]
    fn scalar_columns() {
        let schema = Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new("block$", DataType::Int32, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("data", DataType::Binary, false),
            Field::new("amount", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]);

        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::Int(100),
                OidValue::String("token-1".to_string()),
                OidValue::Bool(true),
                OidValue::Bytes(Bytes::from(vec![0xab, 0xcd])),
                OidValue::BigDecimal(BigDecimal::from_str("12345678901234567890").unwrap()),
                OidValue::Timestamp(
                    Timestamp::from_microseconds_since_epoch(1_000_000_000_000).unwrap(),
                ),
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::Int(101),
                OidValue::String("token-2".to_string()),
                OidValue::Bool(false),
                OidValue::Bytes(Bytes::from(vec![0xff])),
                OidValue::BigDecimal(BigDecimal::from_str("999").unwrap()),
                OidValue::Timestamp(
                    Timestamp::from_microseconds_since_epoch(2_000_000_000_000).unwrap(),
                ),
            ]),
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);

        // vid
        let vid = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(vid.value(0), 1);
        assert_eq!(vid.value(1), 2);

        // block$
        let block = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(block.value(0), 100);
        assert_eq!(block.value(1), 101);

        // id
        let id = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id.value(0), "token-1");
        assert_eq!(id.value(1), "token-2");

        // flag
        let flag = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flag.value(0));
        assert!(!flag.value(1));

        // data
        let data = batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(data.value(0), &[0xab, 0xcd]);
        assert_eq!(data.value(1), &[0xff]);

        // amount (BigDecimal → Utf8)
        let amount = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(amount.value(0), "12345678901234567890");
        assert_eq!(amount.value(1), "999");

        // ts
        let ts = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_000_000_000_000);
        assert_eq!(ts.value(1), 2_000_000_000_000);
    }

    #[test]
    fn nullable_columns() {
        let schema = Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int32, true),
        ]);

        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::String("hello".to_string()),
                OidValue::Int(42),
            ]),
            make_row(vec![OidValue::Int8(2), OidValue::Null, OidValue::Null]),
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!name.is_null(0));
        assert_eq!(name.value(0), "hello");
        assert!(name.is_null(1));

        let count = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(count.value(0), 42);
        assert!(count.is_null(1));
    }

    #[test]
    fn list_columns() {
        let schema = Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]);

        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::StringArray(vec!["foo".to_string(), "bar".to_string()]),
                OidValue::Ints(vec![10, 20, 30]),
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::StringArray(vec![]),
                OidValue::Null,
            ]),
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // tags
        let tags = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let tags_0 = tags.value(0);
        let tags_0_str = tags_0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(tags_0_str.len(), 2);
        assert_eq!(tags_0_str.value(0), "foo");
        assert_eq!(tags_0_str.value(1), "bar");

        let tags_1 = tags.value(1);
        let tags_1_str = tags_1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(tags_1_str.len(), 0);

        // scores
        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let scores_0 = scores.value(0);
        let scores_0_int = scores_0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(scores_0_int.len(), 3);
        assert_eq!(scores_0_int.value(0), 10);
        assert_eq!(scores_0_int.value(2), 30);
        assert!(scores.is_null(1));
    }

    #[test]
    fn empty_batch() {
        let schema = Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let batch = rows_to_record_batch(&schema, &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn column_count_mismatch_errors() {
        let schema = Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let rows = vec![make_row(vec![OidValue::Int8(1)])];
        let err = rows_to_record_batch(&schema, &rows).unwrap_err();
        assert!(err.to_string().contains("1 values but schema has 2 fields"));
    }

    #[test]
    fn type_mismatch_errors() {
        let schema = Schema::new(vec![Field::new("vid", DataType::Int64, false)]);
        let rows = vec![make_row(vec![OidValue::String("wrong".to_string())])];
        let err = rows_to_record_batch(&schema, &rows).unwrap_err();
        assert!(err.to_string().contains("type mismatch"));
    }
}
