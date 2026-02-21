use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use graph::components::store::StoreError;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use serde::{Deserialize, Serialize};

/// Per-chunk metadata recorded in `metadata.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub file: String,
    pub min_vid: i64,
    pub max_vid: i64,
    pub row_count: usize,
}

/// Writes `RecordBatch`es into a single Parquet file using ZSTD compression.
///
/// Tracks row count and vid range across all written batches. Call
/// `finish()` to flush and close the file, returning a `ChunkInfo`
/// summary.
pub struct ParquetChunkWriter {
    writer: ArrowWriter<fs::File>,
    /// Relative path from the dump directory (e.g. `"Token/chunk_000000.parquet"`).
    relative_path: String,
    row_count: usize,
    min_vid: i64,
    max_vid: i64,
}

impl ParquetChunkWriter {
    /// Create a new writer for a Parquet chunk file.
    ///
    /// `path` is the absolute file path. `relative_path` is stored in the
    /// resulting `ChunkInfo` (e.g. `"Token/chunk_000000.parquet"`).
    pub fn new(path: PathBuf, relative_path: String, schema: &Schema) -> Result<Self, StoreError> {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();

        let file = fs::File::create(&path).map_err(|e| {
            StoreError::InternalError(format!(
                "failed to create parquet file {}: {e}",
                path.display()
            ))
        })?;

        let writer =
            ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).map_err(|e| {
                StoreError::InternalError(format!(
                    "failed to create ArrowWriter for {}: {e}",
                    path.display()
                ))
            })?;

        Ok(Self {
            writer,
            relative_path,
            row_count: 0,
            min_vid: i64::MAX,
            max_vid: i64::MIN,
        })
    }

    /// Write a `RecordBatch` and update tracking stats.
    ///
    /// `batch_min_vid` and `batch_max_vid` are the vid bounds of this
    /// batch (typically the first and last vid values).
    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        batch_min_vid: i64,
        batch_max_vid: i64,
    ) -> Result<(), StoreError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.writer
            .write(batch)
            .map_err(|e| StoreError::InternalError(format!("failed to write RecordBatch: {e}")))?;
        self.row_count += batch.num_rows();
        self.min_vid = self.min_vid.min(batch_min_vid);
        self.max_vid = self.max_vid.max(batch_max_vid);
        Ok(())
    }

    /// Flush and close the Parquet file, returning chunk metadata.
    pub fn finish(self) -> Result<ChunkInfo, StoreError> {
        self.writer.close().map_err(|e| {
            StoreError::InternalError(format!("failed to close parquet writer: {e}"))
        })?;
        Ok(ChunkInfo {
            file: self.relative_path,
            min_vid: self.min_vid,
            max_vid: self.max_vid,
            row_count: self.row_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BinaryArray, BooleanArray, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    use super::*;

    /// Create a unique temp file path for a test. The caller is responsible
    /// for cleanup via the returned `TempFile` guard.
    struct TempFile(PathBuf);

    impl TempFile {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "graph_node_test_{}_{name}.parquet",
                std::process::id()
            ));
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("vid", DataType::Int64, false),
            Field::new("block$", DataType::Int32, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, true),
            Field::new("data", DataType::Binary, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ])
    }

    fn test_batch(schema: &Schema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![100, 101, 102])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"\xab\xcd"),
                    None,
                    Some(b"\xff"),
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_000_000),
                    Some(2_000_000),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    /// Read all record batches from a parquet file.
    fn read_parquet(path: &std::path::Path) -> Vec<RecordBatch> {
        let file = std::fs::File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        reader.map(|r| r.unwrap()).collect()
    }

    #[test]
    fn write_and_read_back() {
        let schema = test_schema();
        let batch = test_batch(&schema);
        let tmp = TempFile::new("write_read");

        let mut writer = ParquetChunkWriter::new(
            tmp.path().to_path_buf(),
            "Test/chunk_000000.parquet".into(),
            &schema,
        )
        .unwrap();
        writer.write_batch(&batch, 1, 3).unwrap();
        let chunk_info = writer.finish().unwrap();

        assert_eq!(chunk_info.file, "Test/chunk_000000.parquet");
        assert_eq!(chunk_info.min_vid, 1);
        assert_eq!(chunk_info.max_vid, 3);
        assert_eq!(chunk_info.row_count, 3);

        // Read back and verify
        let batches = read_parquet(tmp.path());
        assert_eq!(batches.len(), 1);
        let read_batch = &batches[0];

        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 6);

        let vid = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(vid.values(), &[1, 2, 3]);

        let block = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(block.values(), &[100, 101, 102]);

        let id = read_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id.value(0), "a");
        assert_eq!(id.value(2), "c");

        let flag = read_batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flag.value(0));
        assert!(flag.is_null(1));
        assert!(!flag.value(2));

        let data = read_batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(data.value(0), b"\xab\xcd");
        assert!(data.is_null(1));

        let ts = read_batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_000_000);
        assert!(ts.is_null(2));
    }

    #[test]
    fn multiple_batches_accumulate_stats() {
        let schema = test_schema();

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(BinaryArray::from_vec(vec![b"a", b"b"])),
                Arc::new(TimestampMicrosecondArray::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![30])),
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["z"])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BinaryArray::from_vec(vec![b"c"])),
                Arc::new(TimestampMicrosecondArray::from(vec![300])),
            ],
        )
        .unwrap();

        let tmp = TempFile::new("multi_batch");

        let mut writer = ParquetChunkWriter::new(
            tmp.path().to_path_buf(),
            "Foo/chunk_000000.parquet".into(),
            &schema,
        )
        .unwrap();
        writer.write_batch(&batch1, 10, 20).unwrap();
        writer.write_batch(&batch2, 30, 30).unwrap();
        let chunk_info = writer.finish().unwrap();

        assert_eq!(chunk_info.min_vid, 10);
        assert_eq!(chunk_info.max_vid, 30);
        assert_eq!(chunk_info.row_count, 3);

        // Verify all 3 rows readable
        let batches = read_parquet(tmp.path());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn empty_batch_is_noop() {
        let schema = test_schema();
        let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
        let tmp = TempFile::new("empty_batch");

        let mut writer = ParquetChunkWriter::new(
            tmp.path().to_path_buf(),
            "X/chunk_000000.parquet".into(),
            &schema,
        )
        .unwrap();
        writer.write_batch(&empty, 0, 0).unwrap();
        let chunk_info = writer.finish().unwrap();

        assert_eq!(chunk_info.row_count, 0);
        // min_vid/max_vid stay at initial sentinel values when nothing was written
        assert_eq!(chunk_info.min_vid, i64::MAX);
        assert_eq!(chunk_info.max_vid, i64::MIN);
    }

    #[test]
    fn chunk_info_serialization() {
        let info = ChunkInfo {
            file: "Token/chunk_000000.parquet".into(),
            min_vid: 0,
            max_vid: 50000,
            row_count: 50000,
        };
        let json = serde_json::to_string_pretty(&info).unwrap();
        assert!(json.contains("Token/chunk_000000.parquet"));
        assert!(json.contains("50000"));

        let deserialized: ChunkInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.file, info.file);
        assert_eq!(deserialized.min_vid, info.min_vid);
        assert_eq!(deserialized.max_vid, info.max_vid);
        assert_eq!(deserialized.row_count, info.row_count);
    }
}
