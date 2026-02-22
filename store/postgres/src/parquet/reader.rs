use std::fs;
use std::path::Path;

use arrow::array::RecordBatch;
use graph::components::store::StoreError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Read all record batches from a Parquet file.
///
/// Opens the file, reads all row groups, and returns them as a vector
/// of `RecordBatch`es. The batches retain the schema embedded in the
/// Parquet file.
#[allow(dead_code)]
pub fn read_batches(path: &Path) -> Result<Vec<RecordBatch>, StoreError> {
    let file = fs::File::open(path).map_err(|e| {
        StoreError::InternalError(format!(
            "failed to open parquet file {}: {e}",
            path.display()
        ))
    })?;

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| {
            StoreError::InternalError(format!(
                "failed to create parquet reader for {}: {e}",
                path.display()
            ))
        })?
        .build()
        .map_err(|e| {
            StoreError::InternalError(format!(
                "failed to build parquet reader for {}: {e}",
                path.display()
            ))
        })?;

    reader
        .into_iter()
        .map(|batch| {
            batch.map_err(|e| {
                StoreError::InternalError(format!(
                    "failed to read record batch from {}: {e}",
                    path.display()
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, BinaryArray, BooleanArray, Int32Array, Int64Array, ListArray, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use graph::data::store::scalar::{BigDecimal, Bytes, Timestamp};
    use std::str::FromStr;

    use super::*;
    use crate::parquet::convert::rows_to_record_batch;
    use crate::parquet::writer::ParquetChunkWriter;
    use crate::relational::value::{OidRow, OidValue};

    fn make_row(values: Vec<OidValue>) -> OidRow {
        values.into_iter().collect()
    }

    struct TempFile(std::path::PathBuf);

    impl TempFile {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "graph_node_reader_test_{}_{name}.parquet",
                std::process::id()
            ));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn write_and_read(name: &str, schema: &Schema, rows: &[OidRow]) -> Vec<RecordBatch> {
        let tmp = TempFile::new(name);
        let batch = rows_to_record_batch(schema, rows).unwrap();

        let min_vid = if rows.is_empty() { 0 } else { 1 };
        let max_vid = rows.len() as i64;

        let mut writer = ParquetChunkWriter::new(
            tmp.path().to_path_buf(),
            "test/chunk_000000.parquet".into(),
            schema,
        )
        .unwrap();
        writer.write_batch(&batch, min_vid, max_vid).unwrap();
        writer.finish().unwrap();

        read_batches(tmp.path()).unwrap()
    }

    #[test]
    fn roundtrip_scalar_columns() {
        let schema = Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new("block$", DataType::Int32, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, true),
            Field::new("data", DataType::Binary, true),
            Field::new("amount", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);

        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::Int(100),
                OidValue::String("token-1".into()),
                OidValue::Bool(true),
                OidValue::Bytes(Bytes::from(vec![0xab, 0xcd])),
                OidValue::BigDecimal(BigDecimal::from_str("12345678901234567890").unwrap()),
                OidValue::Timestamp(Timestamp::from_microseconds_since_epoch(1_000_000).unwrap()),
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::Int(101),
                OidValue::String("token-2".into()),
                OidValue::Null,
                OidValue::Null,
                OidValue::BigDecimal(BigDecimal::from_str("999").unwrap()),
                OidValue::Null,
            ]),
        ];

        let batches = write_and_read("scalar", &schema, &rows);
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);
        assert_eq!(batch.schema().fields().len(), schema.fields().len());

        // Verify schema matches
        for (got, expected) in batch.schema().fields().iter().zip(schema.fields().iter()) {
            assert_eq!(got.name(), expected.name());
            assert_eq!(got.data_type(), expected.data_type());
        }

        // Spot-check values
        let vid = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(vid.value(0), 1);
        assert_eq!(vid.value(1), 2);

        let id = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id.value(0), "token-1");
        assert_eq!(id.value(1), "token-2");

        let flag = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flag.value(0));
        assert!(flag.is_null(1));

        let data = batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(data.value(0), &[0xab, 0xcd]);
        assert!(data.is_null(1));

        let ts = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_000_000);
        assert!(ts.is_null(1));
    }

    #[test]
    fn roundtrip_list_columns() {
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
                OidValue::StringArray(vec!["foo".into(), "bar".into()]),
                OidValue::Ints(vec![10, 20]),
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::StringArray(vec![]),
                OidValue::Null,
            ]),
        ];

        let batches = write_and_read("list", &schema, &rows);
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

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

        let scores = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!scores.is_null(0));
        assert!(scores.is_null(1));
    }

    #[test]
    fn roundtrip_mutable_block_range() {
        let schema = Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new("block_range_start", DataType::Int32, false),
            Field::new("block_range_end", DataType::Int32, true),
            Field::new("id", DataType::Utf8, false),
        ]);

        let rows = vec![
            make_row(vec![
                OidValue::Int8(1),
                OidValue::Int(100),
                OidValue::Int(200),
                OidValue::String("a".into()),
            ]),
            make_row(vec![
                OidValue::Int8(2),
                OidValue::Int(150),
                OidValue::Null, // unbounded (current)
                OidValue::String("b".into()),
            ]),
        ];

        let batches = write_and_read("mutable", &schema, &rows);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let start = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(start.value(0), 100);
        assert_eq!(start.value(1), 150);

        let end = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(end.value(0), 200);
        assert!(end.is_null(1));
    }

    #[test]
    fn nonexistent_file_returns_error() {
        let result = read_batches(Path::new("/tmp/nonexistent_graph_node_test.parquet"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("failed to open parquet file"));
    }
}
