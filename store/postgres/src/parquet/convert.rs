use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Int32Array,
    Int32Builder, Int64Array, Int64Builder, ListArray, ListBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Schema};

use graph::components::store::StoreError;
use graph::data::store::scalar;
use graph::data::store::Value;
use graph::data::value::Word;
use graph::prelude::BlockNumber;

use crate::relational::value::{OidRow, OidValue};
use crate::relational::{ColumnType, Table};

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

/// A row extracted from a Parquet file, ready for insertion into an entity table.
#[allow(dead_code)]
pub struct RestoreRow {
    pub vid: i64,
    pub block: BlockNumber,
    /// `None` for immutable entities, `Some` for mutable (block_range_end).
    pub block_range_end: Option<Option<BlockNumber>>,
    pub causality_region: Option<i32>,
    pub values: Vec<(Word, Value)>,
}

/// Convert an Arrow `RecordBatch` back into `RestoreRow`s using the entity
/// table's schema to guide type interpretation.
///
/// The batch must follow the column layout produced by `schema::arrow_schema`:
/// `vid`, block tracking columns, optional `causality_region`, then data columns.
#[allow(dead_code)]
pub fn record_batch_to_restore_rows(
    batch: &RecordBatch,
    table: &Table,
) -> Result<Vec<RestoreRow>, StoreError> {
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    // System column indices follow the layout in schema::arrow_schema
    let vid_array = downcast_i64(batch.column(0), "vid")?;

    let mut col_idx = 1;

    // Block tracking
    let block_array = downcast_i32(batch.column(col_idx), "block")?;
    col_idx += 1;

    let block_end_array = if !table.immutable {
        let arr = downcast_i32(batch.column(col_idx), "block_range_end")?;
        col_idx += 1;
        Some(arr)
    } else {
        None
    };

    // Causality region
    let cr_array = if table.has_causality_region {
        let arr = downcast_i32(batch.column(col_idx), "causality_region")?;
        col_idx += 1;
        Some(arr)
    } else {
        None
    };

    // Data columns: iterate table columns, skip fulltext
    let data_columns: Vec<_> = table.columns.iter().filter(|c| !c.is_fulltext()).collect();

    // Pre-resolve Arrow column references for data columns
    let arrow_data_cols: Vec<_> = data_columns
        .iter()
        .enumerate()
        .map(|(i, _)| batch.column(col_idx + i))
        .collect();

    for row in 0..num_rows {
        let vid = vid_array.value(row);
        let block = block_array.value(row);

        let block_range_end = block_end_array.as_ref().map(|arr| {
            if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row))
            }
        });

        let causality_region = cr_array.as_ref().map(|arr| arr.value(row));

        let mut values = Vec::with_capacity(data_columns.len());
        for (i, col) in data_columns.iter().enumerate() {
            let arrow_col = arrow_data_cols[i].as_ref();
            let value = if arrow_col.is_null(row) {
                Value::Null
            } else if col.is_list() {
                extract_list_value(arrow_col, row, &col.column_type)?
            } else {
                extract_scalar_value(arrow_col, row, &col.column_type)?
            };
            values.push((Word::from(col.name.as_str()), value));
        }

        rows.push(RestoreRow {
            vid,
            block,
            block_range_end,
            causality_region,
            values,
        });
    }

    Ok(rows)
}

/// A row extracted from a `data_sources$` Parquet file.
#[allow(dead_code)]
pub struct DataSourceRestoreRow {
    pub vid: i64,
    pub block_range_start: BlockNumber,
    pub block_range_end: Option<BlockNumber>,
    pub causality_region: i32,
    pub manifest_idx: i32,
    pub parent: Option<i32>,
    pub id: Option<Vec<u8>>,
    pub param: Option<Vec<u8>>,
    pub context: Option<String>,
    pub done_at: Option<i32>,
}

/// Convert an Arrow `RecordBatch` from a `data_sources$` Parquet file
/// into `DataSourceRestoreRow`s.
///
/// The batch must follow the fixed column layout from
/// `schema::data_sources_arrow_schema`.
#[allow(dead_code)]
pub fn record_batch_to_data_source_rows(
    batch: &RecordBatch,
) -> Result<Vec<DataSourceRestoreRow>, StoreError> {
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    let vid = downcast_i64(batch.column(0), "vid")?;
    let block_start = downcast_i32(batch.column(1), "block_range_start")?;
    let block_end = downcast_i32(batch.column(2), "block_range_end")?;
    let cr = downcast_i32(batch.column(3), "causality_region")?;
    let manifest_idx = downcast_i32(batch.column(4), "manifest_idx")?;
    let parent = downcast_i32(batch.column(5), "parent")?;
    let id_arr = downcast_binary(batch.column(6), "id")?;
    let param_arr = downcast_binary(batch.column(7), "param")?;
    let context_arr = downcast_utf8(batch.column(8), "context")?;
    let done_at = downcast_i32(batch.column(9), "done_at")?;

    for row in 0..num_rows {
        rows.push(DataSourceRestoreRow {
            vid: vid.value(row),
            block_range_start: block_start.value(row),
            block_range_end: if block_end.is_null(row) {
                None
            } else {
                Some(block_end.value(row))
            },
            causality_region: cr.value(row),
            manifest_idx: manifest_idx.value(row),
            parent: if parent.is_null(row) {
                None
            } else {
                Some(parent.value(row))
            },
            id: if id_arr.is_null(row) {
                None
            } else {
                Some(id_arr.value(row).to_vec())
            },
            param: if param_arr.is_null(row) {
                None
            } else {
                Some(param_arr.value(row).to_vec())
            },
            context: if context_arr.is_null(row) {
                None
            } else {
                Some(context_arr.value(row).to_string())
            },
            done_at: if done_at.is_null(row) {
                None
            } else {
                Some(done_at.value(row))
            },
        });
    }

    Ok(rows)
}

// -- Downcasting helpers --

fn downcast_i64<'a>(array: &'a ArrayRef, name: &str) -> Result<&'a Int64Array, StoreError> {
    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        StoreError::InternalError(format!("expected Int64Array for column '{name}'"))
    })
}

fn downcast_i32<'a>(array: &'a ArrayRef, name: &str) -> Result<&'a Int32Array, StoreError> {
    array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
        StoreError::InternalError(format!("expected Int32Array for column '{name}'"))
    })
}

fn downcast_binary<'a>(array: &'a ArrayRef, name: &str) -> Result<&'a BinaryArray, StoreError> {
    array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        StoreError::InternalError(format!("expected BinaryArray for column '{name}'"))
    })
}

fn downcast_utf8<'a>(array: &'a ArrayRef, name: &str) -> Result<&'a StringArray, StoreError> {
    array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        StoreError::InternalError(format!("expected StringArray for column '{name}'"))
    })
}

/// Extract a scalar `Value` from an Arrow array at the given row.
///
/// The `column_type` determines how to interpret the Arrow data (e.g.
/// a Utf8 array might be String, BigInt, or BigDecimal).
fn extract_scalar_value(
    array: &dyn Array,
    row: usize,
    column_type: &ColumnType,
) -> Result<Value, StoreError> {
    match column_type {
        ColumnType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| err("BooleanArray", "Boolean"))?;
            Ok(Value::Bool(arr.value(row)))
        }
        ColumnType::Int => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| err("Int32Array", "Int"))?;
            Ok(Value::Int(arr.value(row)))
        }
        ColumnType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| err("Int64Array", "Int8"))?;
            Ok(Value::Int8(arr.value(row)))
        }
        ColumnType::Bytes => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| err("BinaryArray", "Bytes"))?;
            Ok(Value::Bytes(scalar::Bytes::from(arr.value(row))))
        }
        ColumnType::BigInt => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| err("StringArray", "BigInt"))?;
            let s = arr.value(row);
            let big = scalar::BigInt::from_str(s)
                .map_err(|e| StoreError::InternalError(format!("invalid BigInt '{s}': {e}")))?;
            Ok(Value::BigInt(big))
        }
        ColumnType::BigDecimal => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| err("StringArray", "BigDecimal"))?;
            let s = arr.value(row);
            let bd = scalar::BigDecimal::from_str(s)
                .map_err(|e| StoreError::InternalError(format!("invalid BigDecimal '{s}': {e}")))?;
            Ok(Value::BigDecimal(bd))
        }
        ColumnType::Timestamp => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| err("TimestampMicrosecondArray", "Timestamp"))?;
            let micros = arr.value(row);
            let ts = scalar::Timestamp::from_microseconds_since_epoch(micros).map_err(|e| {
                StoreError::InternalError(format!("invalid Timestamp {micros}: {e}"))
            })?;
            Ok(Value::Timestamp(ts))
        }
        ColumnType::String | ColumnType::Enum(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| err("StringArray", "String/Enum"))?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        ColumnType::TSVector(_) => Err(StoreError::InternalError(
            "TSVector columns should not appear in parquet data".into(),
        )),
    }
}

/// Extract a list `Value` from a `ListArray` at the given row.
fn extract_list_value(
    array: &dyn Array,
    row: usize,
    column_type: &ColumnType,
) -> Result<Value, StoreError> {
    let list_arr = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| StoreError::InternalError("expected ListArray for list column".into()))?;

    let inner = list_arr.value(row);
    let len = inner.len();
    let mut items = Vec::with_capacity(len);

    for i in 0..len {
        if inner.is_null(i) {
            items.push(Value::Null);
        } else {
            items.push(extract_scalar_value(inner.as_ref(), i, column_type)?);
        }
    }

    Ok(Value::List(items))
}

fn err(expected: &str, column_type: &str) -> StoreError {
    StoreError::InternalError(format!("expected {expected} for {column_type} column"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::array::{
        Array, BinaryArray, BooleanArray, Int32Array, Int64Array, ListArray, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use graph::data::store::scalar::{BigDecimal, Bytes, Timestamp};
    use graph::prelude::DeploymentHash;
    use graph::schema::InputSchema;
    use std::str::FromStr;
    use std::sync::Arc;

    use super::*;
    use crate::layout_for_tests::{make_dummy_site, Catalog, Layout, Namespace};
    use crate::parquet::schema::arrow_schema;

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

    // -- Restore direction tests --

    fn test_layout(gql: &str) -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = InputSchema::parse_latest(gql, subgraph.clone()).expect("Test schema invalid");
        let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let catalog =
            Catalog::for_tests(site.clone(), BTreeSet::new()).expect("Can not create catalog");
        Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
    }

    fn test_layout_with_causality(gql: &str, entity_name: &str) -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = InputSchema::parse_latest(gql, subgraph.clone()).expect("Test schema invalid");
        let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let entity_type = schema.entity_type(entity_name).unwrap();
        let ents = BTreeSet::from_iter(vec![entity_type]);
        let catalog = Catalog::for_tests(site.clone(), ents).expect("Can not create catalog");
        Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
    }

    #[test]
    fn restore_immutable_entity() {
        let layout = test_layout(
            "type Token @entity(immutable: true) { id: ID!, name: String!, decimals: Int! }",
        );
        let table = layout.table("token").unwrap();
        let schema = arrow_schema(table);

        // Build a RecordBatch via the dump direction
        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::Int(100),
                OidValue::String("token-1".into()),
                OidValue::String("Token One".into()),
                OidValue::Int(18),
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::Int(101),
                OidValue::String("token-2".into()),
                OidValue::String("Token Two".into()),
                OidValue::Int(6),
            ]),
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        let restore_rows = record_batch_to_restore_rows(&batch, table).unwrap();

        assert_eq!(restore_rows.len(), 2);

        // Row 0
        assert_eq!(restore_rows[0].vid, 1);
        assert_eq!(restore_rows[0].block, 100);
        assert!(restore_rows[0].block_range_end.is_none()); // immutable
        assert!(restore_rows[0].causality_region.is_none());

        assert_eq!(restore_rows[0].values.len(), 3); // id, name, decimals
        assert_eq!(restore_rows[0].values[0].0.as_str(), "id");
        assert_eq!(restore_rows[0].values[0].1, Value::String("token-1".into()));
        assert_eq!(restore_rows[0].values[1].0.as_str(), "name");
        assert_eq!(
            restore_rows[0].values[1].1,
            Value::String("Token One".into())
        );
        assert_eq!(restore_rows[0].values[2].0.as_str(), "decimals");
        assert_eq!(restore_rows[0].values[2].1, Value::Int(18));

        // Row 1
        assert_eq!(restore_rows[1].vid, 2);
        assert_eq!(restore_rows[1].block, 101);
        assert_eq!(restore_rows[1].values[2].1, Value::Int(6));
    }

    #[test]
    fn restore_mutable_entity_with_causality() {
        let layout = test_layout_with_causality(
            "type Transfer @entity { id: ID!, amount: BigInt! }",
            "Transfer",
        );
        let table = layout.table("transfer").unwrap();
        let schema = arrow_schema(table);

        let rows = vec![
            make_row(vec![
                OidValue::Int8(10),
                OidValue::Int(500),
                OidValue::Int(600),
                OidValue::Int(0),
                OidValue::String("tx-1".into()),
                OidValue::String("12345678901234567890".into()),
            ]),
            make_row(vec![
                OidValue::Int8(11),
                OidValue::Int(550),
                OidValue::Null, // unbounded (current)
                OidValue::Int(1),
                OidValue::String("tx-2".into()),
                OidValue::String("99999".into()),
            ]),
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        let restore_rows = record_batch_to_restore_rows(&batch, table).unwrap();

        assert_eq!(restore_rows.len(), 2);

        // Row 0: closed block range
        assert_eq!(restore_rows[0].vid, 10);
        assert_eq!(restore_rows[0].block, 500);
        assert_eq!(restore_rows[0].block_range_end, Some(Some(600)));
        assert_eq!(restore_rows[0].causality_region, Some(0));
        assert_eq!(restore_rows[0].values[0].0.as_str(), "id");
        assert_eq!(restore_rows[0].values[0].1, Value::String("tx-1".into()));
        assert_eq!(restore_rows[0].values[1].0.as_str(), "amount");
        assert_eq!(
            restore_rows[0].values[1].1,
            Value::BigInt(scalar::BigInt::from_str("12345678901234567890").unwrap())
        );

        // Row 1: open block range (current)
        assert_eq!(restore_rows[1].vid, 11);
        assert_eq!(restore_rows[1].block, 550);
        assert_eq!(restore_rows[1].block_range_end, Some(None)); // unbounded
        assert_eq!(restore_rows[1].causality_region, Some(1));
    }

    #[test]
    fn restore_nullable_and_list_columns() {
        let layout = test_layout(
            "type Pool @entity(immutable: true) { id: ID!, tags: [String!]!, description: String }",
        );
        let table = layout.table("pool").unwrap();
        let schema = arrow_schema(table);

        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::Int(100),
                OidValue::String("pool-1".into()),
                OidValue::StringArray(vec!["defi".into(), "amm".into()]),
                OidValue::String("A pool".into()),
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::Int(101),
                OidValue::String("pool-2".into()),
                OidValue::StringArray(vec![]),
                OidValue::Null,
            ]),
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        let restore_rows = record_batch_to_restore_rows(&batch, table).unwrap();

        assert_eq!(restore_rows.len(), 2);

        // Row 0: tags is a list of strings
        let tags_val = &restore_rows[0].values[1].1;
        match tags_val {
            Value::List(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Value::String("defi".into()));
                assert_eq!(items[1], Value::String("amm".into()));
            }
            other => panic!("expected List, got {:?}", other),
        }
        assert_eq!(restore_rows[0].values[2].1, Value::String("A pool".into()));

        // Row 1: empty list and null description
        let tags_val = &restore_rows[1].values[1].1;
        match tags_val {
            Value::List(items) => assert_eq!(items.len(), 0),
            other => panic!("expected List, got {:?}", other),
        }
        assert_eq!(restore_rows[1].values[2].1, Value::Null);
    }

    #[test]
    fn restore_all_scalar_types() {
        let layout = test_layout(
            "type Everything @entity(immutable: true) { \
                id: ID!, \
                flag: Boolean!, \
                small: Int!, \
                big: Int8!, \
                amount: BigInt!, \
                price: BigDecimal!, \
                data: Bytes!, \
                ts: Timestamp!, \
                label: String! \
            }",
        );
        let table = layout.table("everything").unwrap();
        let schema = arrow_schema(table);

        let rows = vec![make_row(vec![
            OidValue::Int8(1),
            OidValue::Int(100),
            OidValue::String("thing-1".into()),
            OidValue::Bool(true),
            OidValue::Int(42),
            OidValue::Int8(9_999_999),
            OidValue::String("12345678901234567890".into()),
            OidValue::BigDecimal(BigDecimal::from_str("3.14159").unwrap()),
            OidValue::Bytes(Bytes::from(vec![0xab, 0xcd])),
            OidValue::Timestamp(Timestamp::from_microseconds_since_epoch(1_000_000).unwrap()),
            OidValue::String("hello".into()),
        ])];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        let restore_rows = record_batch_to_restore_rows(&batch, table).unwrap();

        assert_eq!(restore_rows.len(), 1);
        let vals = &restore_rows[0].values;

        // id
        assert_eq!(vals[0].1, Value::String("thing-1".into()));
        // flag
        assert_eq!(vals[1].1, Value::Bool(true));
        // small
        assert_eq!(vals[2].1, Value::Int(42));
        // big
        assert_eq!(vals[3].1, Value::Int8(9_999_999));
        // amount (BigInt)
        assert_eq!(
            vals[4].1,
            Value::BigInt(scalar::BigInt::from_str("12345678901234567890").unwrap())
        );
        // price (BigDecimal)
        assert_eq!(
            vals[5].1,
            Value::BigDecimal(BigDecimal::from_str("3.14159").unwrap())
        );
        // data (Bytes)
        assert_eq!(vals[6].1, Value::Bytes(Bytes::from(vec![0xab, 0xcd])));
        // ts (Timestamp)
        assert_eq!(
            vals[7].1,
            Value::Timestamp(Timestamp::from_microseconds_since_epoch(1_000_000).unwrap())
        );
        // label
        assert_eq!(vals[8].1, Value::String("hello".into()));
    }

    #[test]
    fn restore_data_source_rows() {
        use crate::parquet::schema::data_sources_arrow_schema;

        let schema = data_sources_arrow_schema();
        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::Int(100),
                OidValue::Int(200),
                OidValue::Int(1),
                OidValue::Int(0),
                OidValue::Null,
                OidValue::Null,
                OidValue::Bytes(Bytes::from(vec![0x01, 0x02])),
                OidValue::String(r#"{"key":"value"}"#.into()),
                OidValue::Null,
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::Int(150),
                OidValue::Null, // unbounded
                OidValue::Int(2),
                OidValue::Int(1),
                OidValue::Int(5),
                OidValue::Bytes(Bytes::from(vec![0xaa])),
                OidValue::Null,
                OidValue::Null,
                OidValue::Int(300),
            ]),
        ];

        let batch = rows_to_record_batch(&schema, &rows).unwrap();
        let ds_rows = record_batch_to_data_source_rows(&batch).unwrap();

        assert_eq!(ds_rows.len(), 2);

        // Row 0
        assert_eq!(ds_rows[0].vid, 1);
        assert_eq!(ds_rows[0].block_range_start, 100);
        assert_eq!(ds_rows[0].block_range_end, Some(200));
        assert_eq!(ds_rows[0].causality_region, 1);
        assert_eq!(ds_rows[0].manifest_idx, 0);
        assert!(ds_rows[0].parent.is_none());
        assert!(ds_rows[0].id.is_none());
        assert_eq!(ds_rows[0].param.as_deref(), Some(&[0x01, 0x02][..]));
        assert_eq!(ds_rows[0].context.as_deref(), Some(r#"{"key":"value"}"#));
        assert!(ds_rows[0].done_at.is_none());

        // Row 1
        assert_eq!(ds_rows[1].vid, 2);
        assert_eq!(ds_rows[1].block_range_start, 150);
        assert!(ds_rows[1].block_range_end.is_none());
        assert_eq!(ds_rows[1].causality_region, 2);
        assert_eq!(ds_rows[1].manifest_idx, 1);
        assert_eq!(ds_rows[1].parent, Some(5));
        assert_eq!(ds_rows[1].id.as_deref(), Some(&[0xaa][..]));
        assert!(ds_rows[1].param.is_none());
        assert!(ds_rows[1].context.is_none());
        assert_eq!(ds_rows[1].done_at, Some(300));
    }

    #[test]
    fn restore_empty_batch() {
        let layout = test_layout("type Token @entity(immutable: true) { id: ID!, name: String! }");
        let table = layout.table("token").unwrap();
        let schema = arrow_schema(table);

        let batch = rows_to_record_batch(&schema, &[]).unwrap();
        let restore_rows = record_batch_to_restore_rows(&batch, table).unwrap();
        assert!(restore_rows.is_empty());
    }
}
