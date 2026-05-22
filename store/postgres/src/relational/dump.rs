use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, fs};

use diesel::QueryDsl;
use diesel::dsl::sql;
use diesel::sql_types::{
    Array, BigInt, Binary, Bool, Integer, Nullable, Numeric, Text, Timestamptz, Untyped,
};
use diesel_async::RunQueryDsl;
use diesel_dynamic_schema::DynamicSelectClause;

use graph::components::store::DumpReporter;
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth, SubgraphManifestEntity};
use graph::prelude::{DeploymentHash, StoreError, SubgraphDeploymentEntity};
use serde::{Deserialize, Serialize};

use crate::AsyncPgConnection;
use crate::catalog;
use crate::detail::deployment_entity;
use crate::parquet::convert::rows_to_record_batch;
use crate::parquet::schema::{arrow_schema, clamp_arrow_schema, data_sources_arrow_schema};
use crate::parquet::writer::{ChunkInfo, ParquetChunkWriter};
use crate::relational::dsl;
use crate::relational::index::{IndexCreator, IndexList};
use crate::relational::value::OidRow;
use crate::relational::{ColumnType, SqlName, Table as RelTable};
use crate::vid_batcher::{VidBatcher, VidRange};

use super::Layout;

#[derive(Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub spec_version: String,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub features: Vec<String>,
    pub entities_with_causality_region: Vec<String>,
    pub history_blocks: i32,
}

impl Manifest {
    fn new(data: SubgraphManifestEntity) -> (Self, String, Option<String>) {
        let SubgraphManifestEntity {
            spec_version,
            description,
            repository,
            features,
            schema,
            raw_yaml,
            entities_with_causality_region,
            history_blocks,
        } = data;
        let this = Self {
            spec_version,
            description,
            repository,
            features,
            entities_with_causality_region: entities_with_causality_region
                .into_iter()
                .map(|v| v.to_string())
                .collect(),
            history_blocks,
        };
        (this, schema, raw_yaml)
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct BlockPtr {
    pub number: i32,
    pub hash: String,
}

impl BlockPtr {
    fn new(block_ptr: graph::prelude::BlockPtr) -> Self {
        let graph::prelude::BlockPtr { number, hash } = block_ptr;
        let hash = hash.hash_hex();
        Self { number, hash }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Error {
    pub message: String,
    pub block_ptr: Option<BlockPtr>,
    pub handler: Option<String>,
    pub deterministic: bool,
}

impl Error {
    fn new(error: SubgraphError) -> Self {
        let SubgraphError {
            subgraph_id: _,
            message,
            block_ptr,
            handler,
            deterministic,
        } = error;
        let block_ptr = block_ptr.map(BlockPtr::new);
        Self {
            message,
            block_ptr,
            handler,
            deterministic,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Health {
    pub failed: bool,
    pub health: String,
    pub fatal_error: Option<Error>,
    pub non_fatal_errors: Vec<Error>,
}

impl Health {
    fn new(
        failed: bool,
        health: SubgraphHealth,
        fatal_error: Option<SubgraphError>,
        non_fatal_errors: Vec<SubgraphError>,
    ) -> Self {
        let health = health.as_str().to_string();
        let fatal_error = fatal_error.map(Error::new);
        let non_fatal_errors = non_fatal_errors.into_iter().map(Error::new).collect();
        Self {
            failed,
            health,
            fatal_error,
            non_fatal_errors,
        }
    }
}

/// Per-table metadata recorded in `metadata.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableInfo {
    pub immutable: bool,
    pub has_causality_region: bool,
    pub chunks: Vec<ChunkInfo>,
    /// Clamp files for mutable tables: record rows whose `block_range`
    /// upper bound was set since a previous dump. Empty for immutable
    /// tables and for full (non-incremental) dumps.
    #[serde(default)]
    pub clamps: Vec<ChunkInfo>,
    pub max_vid: i64,
}

/// Top-level metadata for a subgraph dump (written as `metadata.json`).
///
/// The `tables` field is initially empty and populated during the dump
/// as entity tables are written to Parquet files.
#[derive(Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub version: u32,
    pub deployment: DeploymentHash,
    pub network: String,
    pub manifest: Manifest,
    pub earliest_block_number: i32,
    pub start_block: Option<BlockPtr>,
    pub head_block: Option<BlockPtr>,
    pub entity_count: usize,
    pub graft_base: Option<DeploymentHash>,
    pub graft_block: Option<BlockPtr>,
    pub debug_fork: Option<DeploymentHash>,
    pub health: Health,
    pub indexes: HashMap<String, Vec<String>>,
    pub tables: BTreeMap<String, TableInfo>,
}

impl Metadata {
    /// Read and validate a dump's `metadata.json`.
    pub fn from_file(path: &Path) -> Result<Self, StoreError> {
        let content = fs::read_to_string(path).map_err(|e| {
            StoreError::InternalError(format!("failed to read {}: {e}", path.display()))
        })?;
        let metadata: Self = serde_json::from_str(&content).map_err(|e| {
            StoreError::InternalError(format!("failed to parse {}: {e}", path.display()))
        })?;
        if metadata.version != 1 {
            return Err(StoreError::InternalError(format!(
                "unsupported dump version {} (expected 1)",
                metadata.version
            )));
        }
        Ok(metadata)
    }

    fn new(
        deployment: DeploymentHash,
        network: String,
        entity_count: usize,
        data: SubgraphDeploymentEntity,
        index_list: IndexList,
    ) -> (Self, String, Option<String>) {
        let indexes = Self::indexes(index_list);
        let SubgraphDeploymentEntity {
            manifest,
            failed,
            health,
            synced_at: _,
            fatal_error,
            non_fatal_errors,
            earliest_block_number,
            start_block,
            latest_block,
            graft_base,
            graft_block,
            debug_fork,
            reorg_count: _,
            current_reorg_depth: _,
            max_reorg_depth: _,
        } = data;
        let (manifest, schema, yaml) = Manifest::new(manifest);
        let health = Health::new(failed, health, fatal_error, non_fatal_errors);
        let start_block = start_block.map(BlockPtr::new);
        let head_block = latest_block.map(BlockPtr::new);
        let graft_block = graft_block.map(BlockPtr::new);
        let this = Self {
            version: 1,
            deployment,
            network,
            manifest,
            earliest_block_number,
            start_block,
            head_block,
            entity_count,
            graft_base,
            graft_block,
            debug_fork,
            health,
            indexes,
            tables: BTreeMap::new(),
        };
        (this, schema, yaml)
    }

    fn indexes(index_list: IndexList) -> HashMap<String, Vec<String>> {
        let mut res = HashMap::new();
        let creat = IndexCreator::new(false, true, false);
        for (name, indexes) in index_list.indexes {
            let mut indexes2 = Vec::new();
            for index in indexes {
                let Ok(index) = index.with_nsp("sgd".to_string()) else {
                    continue;
                };
                let Ok(index) = creat.to_sql(&index) else {
                    continue;
                };
                indexes2.push(index);
            }
            res.insert(name, indexes2);
        }
        res
    }
}

fn chunk_filename(index: usize) -> String {
    format!("chunk_{:06}.parquet", index)
}

fn clamp_filename(index: usize) -> String {
    format!("clamp_{:06}.parquet", index)
}

/// Build a `DynamicSelectClause` for dumping an entity table. The
/// selected columns match the Arrow schema from `parquet::schema::arrow_schema`:
/// `vid`, block columns (split for mutable), `causality_region`, then data columns.
fn dump_select<'a>(
    table: &'a dsl::Table<'a>,
    meta: &'a RelTable,
) -> diesel::query_builder::BoxedSelectStatement<
    'a,
    Untyped,
    diesel::query_builder::FromClause<dsl::Table<'a>>,
    diesel::pg::Pg,
> {
    type SelectClause<'b> = DynamicSelectClause<'b, diesel::pg::Pg, dsl::Table<'b>>;

    fn add_data_field<'b>(
        select: &mut SelectClause<'b>,
        table: &'b dsl::Table<'b>,
        column: &'b crate::relational::Column,
    ) {
        fn add_typed<'c, ST: diesel::sql_types::SingleValue + Send>(
            select: &mut SelectClause<'c>,
            table: &'c dsl::Table<'c>,
            name: &str,
            is_list: bool,
            is_nullable: bool,
        ) {
            match (is_list, is_nullable) {
                (true, true) => {
                    select.add_field(table.column(name).unwrap().bind::<Nullable<Array<ST>>>())
                }
                (true, false) => select.add_field(table.column(name).unwrap().bind::<Array<ST>>()),
                (false, true) => {
                    select.add_field(table.column(name).unwrap().bind::<Nullable<ST>>())
                }
                (false, false) => select.add_field(table.column(name).unwrap().bind::<ST>()),
            }
        }

        let name = column.name.as_str();
        let is_list = column.is_list();
        let is_nullable = column.is_nullable();

        match &column.column_type {
            ColumnType::Boolean => add_typed::<Bool>(select, table, name, is_list, is_nullable),
            ColumnType::BigDecimal | ColumnType::BigInt => {
                add_typed::<Numeric>(select, table, name, is_list, is_nullable)
            }
            ColumnType::Bytes => add_typed::<Binary>(select, table, name, is_list, is_nullable),
            ColumnType::Int => add_typed::<Integer>(select, table, name, is_list, is_nullable),
            ColumnType::Int8 => add_typed::<BigInt>(select, table, name, is_list, is_nullable),
            ColumnType::Timestamp => {
                add_typed::<Timestamptz>(select, table, name, is_list, is_nullable)
            }
            ColumnType::String => add_typed::<Text>(select, table, name, is_list, is_nullable),
            ColumnType::Enum(_) => {
                // Cast enum to text for dump
                let alias = table.alias.as_str();
                let cast = if is_list { "text[]" } else { "text" };
                let expr = format!("{alias}.\"{name}\"::{cast}");
                match (is_list, is_nullable) {
                    (true, true) => select.add_field(sql::<Nullable<Array<Text>>>(&expr)),
                    (true, false) => select.add_field(sql::<Array<Text>>(&expr)),
                    (false, true) => select.add_field(sql::<Nullable<Text>>(&expr)),
                    (false, false) => select.add_field(sql::<Text>(&expr)),
                }
                let _ = alias;
            }
            ColumnType::TSVector(_) => { /* skip fulltext columns */ }
        }
    }

    let mut selection = DynamicSelectClause::new();

    // vid
    selection.add_field(table.column("vid").unwrap().bind::<BigInt>());

    // Block columns
    if meta.immutable {
        selection.add_field(table.column("block$").unwrap().bind::<Integer>());
    } else {
        selection.add_field(sql::<Integer>("lower(c.block_range)"));
        selection.add_field(sql::<Nullable<Integer>>("upper(c.block_range)"));
    }

    // Causality region
    if meta.has_causality_region {
        selection.add_field(table.column("causality_region").unwrap().bind::<Integer>());
    }

    // Data columns
    for col in &meta.columns {
        if col.is_fulltext() {
            continue;
        }
        add_data_field(&mut selection, table, col);
    }

    diesel::QueryDsl::select(*table, selection).into_boxed()
}

/// Query the vid range for a table. Returns `(min_vid, max_vid)`.
async fn vid_range(conn: &mut AsyncPgConnection, table: &RelTable) -> Result<VidRange, StoreError> {
    let query = format!(
        "select coalesce(min(vid), 0)::int8 as min_vid, coalesce(max(vid), -1)::int8 as max_vid from {}",
        table.qualified_name
    );
    diesel::sql_query(&query)
        .get_result::<VidRange>(conn)
        .await
        .map_err(StoreError::from)
}

/// Row type for clamp queries.
#[derive(diesel::QueryableByName)]
struct ClampRow {
    #[diesel(sql_type = BigInt)]
    vid: i64,
    #[diesel(sql_type = Integer)]
    block_range_end: i32,
}

/// Convert `ClampRow`s into an Arrow `RecordBatch`.
fn clamp_rows_to_record_batch(
    schema: &arrow::datatypes::Schema,
    rows: &[ClampRow],
) -> Result<arrow::array::RecordBatch, StoreError> {
    use arrow::array::{Int32Builder, Int64Builder, RecordBatch};
    use std::sync::Arc;

    let n = rows.len();
    let mut vid_b = Int64Builder::with_capacity(n);
    let mut bre_b = Int32Builder::with_capacity(n);

    for row in rows {
        vid_b.append_value(row.vid);
        bre_b.append_value(row.block_range_end);
    }

    let arrays: Vec<arrow::array::ArrayRef> =
        vec![Arc::new(vid_b.finish()), Arc::new(bre_b.finish())];

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| StoreError::InternalError(format!("failed to build clamp batch: {e}")))
}

const CLAMP_BATCH_SIZE: i64 = 50_000;

/// Query rows whose `block_range` was closed since the previous dump
/// and write them to a clamp Parquet file.
async fn dump_clamp(
    conn: &mut AsyncPgConnection,
    table: &RelTable,
    dir: &Path,
    prev_max_vid: i64,
    prev_head_block_number: i32,
    clamp_index: usize,
    reporter: &mut dyn DumpReporter,
) -> Result<Option<ChunkInfo>, StoreError> {
    let table_dir_name = table.object.as_str();
    let qualified = &table.qualified_name;
    let schema = clamp_arrow_schema();

    // Count rows to dump
    let count_query = format!(
        "select count(*)::int8 as cnt from {} \
         where vid <= $1 \
           and upper(block_range) is not null \
           and upper(block_range) > $2",
        qualified,
    );
    let count: i64 = diesel::sql_query(&count_query)
        .bind::<BigInt, _>(prev_max_vid)
        .bind::<Integer, _>(prev_head_block_number)
        .get_result::<CountRow>(conn)
        .await
        .map_err(StoreError::from)?
        .cnt;

    if count == 0 {
        return Ok(None);
    }

    reporter.start_clamps(table_dir_name, count as usize);

    let table_dir = dir.join(table_dir_name);
    fs::create_dir_all(&table_dir)
        .map_err(|e| StoreError::InternalError(format!("failed to create dir: {e}")))?;

    let clamp_file = clamp_filename(clamp_index);
    let relative_path = format!("{}/{}", table_dir_name, clamp_file);
    let abs_path = table_dir.join(&clamp_file);
    let mut writer = ParquetChunkWriter::new(abs_path, relative_path, &schema)?;

    let mut total_rows = 0usize;
    let mut start_vid: i64 = 0;
    loop {
        let query = format!(
            "select vid::int8, upper(block_range)::int4 as block_range_end \
               from {} \
              where vid <= $1 \
                and vid > $2 \
                and upper(block_range) is not null \
                and upper(block_range) > $3 \
              order by vid \
              limit $4",
            qualified,
        );
        let rows: Vec<ClampRow> = diesel::sql_query(&query)
            .bind::<BigInt, _>(prev_max_vid)
            .bind::<BigInt, _>(start_vid)
            .bind::<Integer, _>(prev_head_block_number)
            .bind::<BigInt, _>(CLAMP_BATCH_SIZE)
            .load(conn)
            .await
            .map_err(StoreError::from)?;

        if rows.is_empty() {
            break;
        }

        let batch_max_vid = rows[rows.len() - 1].vid;
        let batch_min_vid = rows[0].vid;
        let batch = clamp_rows_to_record_batch(&schema, &rows)?;
        total_rows += rows.len();
        writer.write_batch(&batch, batch_min_vid, batch_max_vid)?;
        start_vid = batch_max_vid;

        if (rows.len() as i64) < CLAMP_BATCH_SIZE {
            break;
        }
    }

    let chunk_info = writer.finish()?;
    reporter.finish_clamps(table_dir_name, total_rows);

    if chunk_info.row_count > 0 {
        Ok(Some(chunk_info))
    } else {
        Ok(None)
    }
}

/// Dump clamps for the `data_sources$` table.
async fn dump_data_sources_clamp(
    conn: &mut AsyncPgConnection,
    namespace: &str,
    dir: &Path,
    prev_max_vid: i64,
    prev_head_block_number: i32,
    clamp_index: usize,
    reporter: &mut dyn DumpReporter,
) -> Result<Option<ChunkInfo>, StoreError> {
    let qualified = format!("\"{}\".\"{}\"", namespace, DATA_SOURCES_TABLE);
    let schema = clamp_arrow_schema();

    let count_query = format!(
        "select count(*)::int8 as cnt from {} \
         where vid <= $1 \
           and upper(block_range) is not null \
           and upper(block_range) > $2",
        qualified,
    );
    let count: i64 = diesel::sql_query(&count_query)
        .bind::<BigInt, _>(prev_max_vid)
        .bind::<Integer, _>(prev_head_block_number)
        .get_result::<CountRow>(conn)
        .await
        .map_err(StoreError::from)?
        .cnt;

    if count == 0 {
        return Ok(None);
    }

    reporter.start_clamps(DATA_SOURCES_TABLE, count as usize);

    let ds_dir = dir.join(DATA_SOURCES_TABLE);
    fs::create_dir_all(&ds_dir)
        .map_err(|e| StoreError::InternalError(format!("failed to create dir: {e}")))?;

    let clamp_file = clamp_filename(clamp_index);
    let relative_path = format!("{}/{}", DATA_SOURCES_TABLE, clamp_file);
    let abs_path = ds_dir.join(&clamp_file);
    let mut writer = ParquetChunkWriter::new(abs_path, relative_path, &schema)?;

    let mut total_rows = 0usize;
    let mut start_vid: i64 = 0;
    loop {
        let query = format!(
            "select vid::int8, upper(block_range)::int4 as block_range_end \
               from {} \
              where vid <= $1 \
                and vid > $2 \
                and upper(block_range) is not null \
                and upper(block_range) > $3 \
              order by vid \
              limit $4",
            qualified,
        );
        let rows: Vec<ClampRow> = diesel::sql_query(&query)
            .bind::<BigInt, _>(prev_max_vid)
            .bind::<BigInt, _>(start_vid)
            .bind::<Integer, _>(prev_head_block_number)
            .bind::<BigInt, _>(CLAMP_BATCH_SIZE)
            .load(conn)
            .await
            .map_err(StoreError::from)?;

        if rows.is_empty() {
            break;
        }

        let batch_max_vid = rows[rows.len() - 1].vid;
        let batch_min_vid = rows[0].vid;
        let batch = clamp_rows_to_record_batch(&schema, &rows)?;
        total_rows += rows.len();
        writer.write_batch(&batch, batch_min_vid, batch_max_vid)?;
        start_vid = batch_max_vid;

        if (rows.len() as i64) < CLAMP_BATCH_SIZE {
            break;
        }
    }

    let chunk_info = writer.finish()?;
    reporter.finish_clamps(DATA_SOURCES_TABLE, total_rows);

    if chunk_info.row_count > 0 {
        Ok(Some(chunk_info))
    } else {
        Ok(None)
    }
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    cnt: i64,
}

/// Dump a single entity table to a Parquet file.
///
/// When `prev` is `Some`, performs an incremental dump: only new rows
/// (vid > prev.max_vid) are written, and clamps are generated for
/// mutable tables.
async fn dump_entity_table(
    conn: &mut AsyncPgConnection,
    table: &RelTable,
    dir: &Path,
    prev: Option<&TableInfo>,
    prev_head_block_number: Option<i32>,
    reporter: &mut dyn DumpReporter,
) -> Result<TableInfo, StoreError> {
    let arrow_schema = arrow_schema(table);
    let table_dir_name = table.object.as_str();
    let table_dir = dir.join(table_dir_name);
    fs::create_dir_all(&table_dir)
        .map_err(|e| StoreError::InternalError(format!("failed to create dir: {e}")))?;

    let range = vid_range(conn, table).await?;

    // For incremental dumps, adjust the start of the vid range
    let (effective_range, chunk_index, mut chunks, mut clamps) = match prev {
        Some(prev_info) => {
            let new_min = prev_info.max_vid + 1;
            let effective = VidRange {
                min: new_min,
                max: range.max,
            };
            (
                effective,
                prev_info.chunks.len(),
                prev_info.chunks.clone(),
                prev_info.clamps.clone(),
            )
        }
        None => (range, 0, vec![], vec![]),
    };

    let rows_approx = if effective_range.is_empty() {
        0
    } else {
        (effective_range.max - effective_range.min + 1) as usize
    };
    reporter.start_table(table_dir_name, rows_approx);

    if effective_range.is_empty() {
        // No new rows, but for incremental mutable tables we still
        // need to check for clamps
        if let (Some(prev_info), Some(prev_head)) = (prev, prev_head_block_number)
            && !table.immutable
        {
            let clamp_index = prev_info.clamps.len();
            if let Some(clamp_info) = dump_clamp(
                conn,
                table,
                dir,
                prev_info.max_vid,
                prev_head,
                clamp_index,
                reporter,
            )
            .await?
            {
                clamps.push(clamp_info);
            }
        }

        let max_vid = prev.map_or(-1, |p| p.max_vid);
        reporter.finish_table(table_dir_name, 0);
        return Ok(TableInfo {
            immutable: table.immutable,
            has_causality_region: table.has_causality_region,
            chunks,
            clamps,
            max_vid,
        });
    }

    let chunk_file = chunk_filename(chunk_index);
    let relative_path = format!("{}/{}", table_dir_name, chunk_file);
    let abs_path = table_dir.join(&chunk_file);
    let mut writer = ParquetChunkWriter::new(abs_path, relative_path, &arrow_schema)?;

    let dsl_table = dsl::Table::new(table);
    let mut batcher = VidBatcher::load(conn, &table.nsp, table, effective_range).await?;
    let mut total_rows = 0usize;

    while !batcher.finished() {
        let (_, rows_opt) = batcher
            .step(async |start, end| {
                let query = dump_select(&dsl_table, table)
                    .filter(sql::<Bool>("vid >= ").bind::<BigInt, _>(start))
                    .filter(sql::<Bool>("vid <= ").bind::<BigInt, _>(end))
                    .order(sql::<Untyped>("vid"));

                let rows: Vec<OidRow> = query.load(conn).await?;
                if rows.is_empty() {
                    return Ok(0usize);
                }

                let count = rows.len();
                let batch = rows_to_record_batch(&arrow_schema, &rows)?;
                let batch_min_vid = start;
                let batch_max_vid = end;
                writer.write_batch(&batch, batch_min_vid, batch_max_vid)?;
                Ok(count)
            })
            .await?;
        if let Some(rows) = rows_opt {
            total_rows += rows;
            reporter.batch_dumped(table_dir_name, rows);
        }
    }

    let chunk_info = writer.finish()?;

    let max_vid = if chunk_info.row_count > 0 {
        chunk_info.max_vid
    } else {
        prev.map_or(-1, |p| p.max_vid)
    };

    if chunk_info.row_count > 0 {
        chunks.push(chunk_info);
    }

    // For incremental mutable tables, dump clamps
    if let (Some(prev_info), Some(prev_head)) = (prev, prev_head_block_number)
        && !table.immutable
    {
        let clamp_index = prev_info.clamps.len();
        if let Some(clamp_info) = dump_clamp(
            conn,
            table,
            dir,
            prev_info.max_vid,
            prev_head,
            clamp_index,
            reporter,
        )
        .await?
        {
            clamps.push(clamp_info);
        }
    }

    reporter.finish_table(table_dir_name, total_rows);

    Ok(TableInfo {
        immutable: table.immutable,
        has_causality_region: table.has_causality_region,
        chunks,
        clamps,
        max_vid,
    })
}

/// Row type for the `data_sources$` table dump query.
#[derive(diesel::QueryableByName)]
struct DataSourceRow {
    #[diesel(sql_type = BigInt)]
    vid: i64,
    #[diesel(sql_type = Integer)]
    block_range_start: i32,
    #[diesel(sql_type = Nullable<Integer>)]
    block_range_end: Option<i32>,
    #[diesel(sql_type = Integer)]
    causality_region: i32,
    #[diesel(sql_type = Integer)]
    manifest_idx: i32,
    #[diesel(sql_type = Nullable<Integer>)]
    parent: Option<i32>,
    #[diesel(sql_type = Nullable<Binary>)]
    id: Option<Vec<u8>>,
    #[diesel(sql_type = Nullable<Binary>)]
    param: Option<Vec<u8>>,
    #[diesel(sql_type = Nullable<Text>)]
    context: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    done_at: Option<i32>,
}

/// Convert `DataSourceRow`s to an Arrow `RecordBatch` using the fixed
/// `data_sources$` schema.
fn data_source_rows_to_record_batch(
    schema: &arrow::datatypes::Schema,
    rows: &[DataSourceRow],
) -> Result<arrow::array::RecordBatch, StoreError> {
    use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder};
    use std::sync::Arc;

    let n = rows.len();
    let mut vid_b = Int64Builder::with_capacity(n);
    let mut brs_b = Int32Builder::with_capacity(n);
    let mut bre_b = Int32Builder::with_capacity(n);
    let mut cr_b = Int32Builder::with_capacity(n);
    let mut mi_b = Int32Builder::with_capacity(n);
    let mut parent_b = Int32Builder::with_capacity(n);
    let mut id_b = BinaryBuilder::with_capacity(n, 0);
    let mut param_b = BinaryBuilder::with_capacity(n, 0);
    let mut ctx_b = StringBuilder::with_capacity(n, 0);
    let mut da_b = Int32Builder::with_capacity(n);

    for row in rows {
        vid_b.append_value(row.vid);
        brs_b.append_value(row.block_range_start);
        match row.block_range_end {
            Some(v) => bre_b.append_value(v),
            None => bre_b.append_null(),
        }
        cr_b.append_value(row.causality_region);
        mi_b.append_value(row.manifest_idx);
        match row.parent {
            Some(v) => parent_b.append_value(v),
            None => parent_b.append_null(),
        }
        match &row.id {
            Some(v) => id_b.append_value(v),
            None => id_b.append_null(),
        }
        match &row.param {
            Some(v) => param_b.append_value(v),
            None => param_b.append_null(),
        }
        match &row.context {
            Some(v) => ctx_b.append_value(v),
            None => ctx_b.append_null(),
        }
        match row.done_at {
            Some(v) => da_b.append_value(v),
            None => da_b.append_null(),
        }
    }

    let arrays: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(vid_b.finish()),
        Arc::new(brs_b.finish()),
        Arc::new(bre_b.finish()),
        Arc::new(cr_b.finish()),
        Arc::new(mi_b.finish()),
        Arc::new(parent_b.finish()),
        Arc::new(id_b.finish()),
        Arc::new(param_b.finish()),
        Arc::new(ctx_b.finish()),
        Arc::new(da_b.finish()),
    ];

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| StoreError::InternalError(format!("failed to build data_sources batch: {e}")))
}

const DATA_SOURCES_TABLE: &str = "data_sources$";
const DATA_SOURCES_BATCH_SIZE: i64 = 10_000;

/// Dump the `data_sources$` table to a Parquet file, if it exists.
async fn dump_data_sources(
    conn: &mut AsyncPgConnection,
    namespace: &str,
    dir: &Path,
    prev: Option<&TableInfo>,
    prev_head_block_number: Option<i32>,
    reporter: &mut dyn DumpReporter,
) -> Result<Option<TableInfo>, StoreError> {
    let table_name = SqlName::verbatim(DATA_SOURCES_TABLE.to_string());
    if !catalog::table_exists(conn, namespace, &table_name).await? {
        return Ok(None);
    }

    reporter.start_data_sources();

    let qualified = format!("\"{}\".\"{}\"", namespace, DATA_SOURCES_TABLE);

    // Get vid range
    let range_query = format!(
        "select coalesce(min(vid), 0)::int8 as min_vid, coalesce(max(vid), -1)::int8 as max_vid from {}",
        qualified,
    );
    let range: VidRange = diesel::sql_query(&range_query)
        .get_result(conn)
        .await
        .map_err(StoreError::from)?;

    // For incremental dumps, adjust start
    let (effective_min, chunk_index, mut chunks, mut clamps) = match prev {
        Some(prev_info) => (
            prev_info.max_vid + 1,
            prev_info.chunks.len(),
            prev_info.chunks.clone(),
            prev_info.clamps.clone(),
        ),
        None => (range.min, 0, vec![], vec![]),
    };

    let effective_empty = range.is_empty() || effective_min > range.max;

    if effective_empty {
        // No new rows, but still check for clamps on incremental
        if let (Some(prev_info), Some(prev_head)) = (prev, prev_head_block_number) {
            let clamp_index = prev_info.clamps.len();
            if let Some(clamp_info) = dump_data_sources_clamp(
                conn,
                namespace,
                dir,
                prev_info.max_vid,
                prev_head,
                clamp_index,
                reporter,
            )
            .await?
            {
                clamps.push(clamp_info);
            }
        }

        let max_vid = prev.map_or(-1, |p| p.max_vid);
        reporter.finish_data_sources(0);
        return Ok(Some(TableInfo {
            immutable: false,
            has_causality_region: true,
            chunks,
            clamps,
            max_vid,
        }));
    }

    let schema = data_sources_arrow_schema();
    let ds_dir = dir.join(DATA_SOURCES_TABLE);
    fs::create_dir_all(&ds_dir)
        .map_err(|e| StoreError::InternalError(format!("failed to create dir: {e}")))?;

    let chunk_file = chunk_filename(chunk_index);
    let relative_path = format!("{}/{}", DATA_SOURCES_TABLE, chunk_file);
    let abs_path = ds_dir.join(&chunk_file);
    let mut writer = ParquetChunkWriter::new(abs_path, relative_path, &schema)?;

    let mut total_rows = 0usize;
    let mut start = effective_min;
    while start <= range.max {
        let end = (start + DATA_SOURCES_BATCH_SIZE - 1).min(range.max);
        let query = format!(
            "select vid::int8, \
                    lower(block_range) as block_range_start, \
                    upper(block_range) as block_range_end, \
                    causality_region, \
                    manifest_idx, \
                    parent, \
                    id, \
                    param, \
                    context::text, \
                    done_at \
               from {} \
              where vid >= $1 and vid <= $2 \
              order by vid",
            qualified,
        );
        let rows: Vec<DataSourceRow> = diesel::sql_query(&query)
            .bind::<BigInt, _>(start)
            .bind::<BigInt, _>(end)
            .load(conn)
            .await
            .map_err(StoreError::from)?;

        if !rows.is_empty() {
            total_rows += rows.len();
            let batch = data_source_rows_to_record_batch(&schema, &rows)?;
            writer.write_batch(&batch, start, end)?;
        }

        start = end + 1;
    }

    let chunk_info = writer.finish()?;

    let max_vid = if chunk_info.row_count > 0 {
        chunk_info.max_vid
    } else {
        prev.map_or(-1, |p| p.max_vid)
    };

    if chunk_info.row_count > 0 {
        chunks.push(chunk_info);
    }

    // Dump clamps for incremental
    if let (Some(prev_info), Some(prev_head)) = (prev, prev_head_block_number) {
        let clamp_index = prev_info.clamps.len();
        if let Some(clamp_info) = dump_data_sources_clamp(
            conn,
            namespace,
            dir,
            prev_info.max_vid,
            prev_head,
            clamp_index,
            reporter,
        )
        .await?
        {
            clamps.push(clamp_info);
        }
    }

    reporter.finish_data_sources(total_rows);

    Ok(Some(TableInfo {
        immutable: false,
        has_causality_region: true,
        chunks,
        clamps,
        max_vid,
    }))
}

impl Layout {
    pub(crate) async fn dump(
        &self,
        conn: &mut AsyncPgConnection,
        index_list: IndexList,
        dir: PathBuf,
        network: &str,
        entity_count: usize,
        reporter: &mut dyn DumpReporter,
    ) -> Result<(), StoreError> {
        fn write_file(name: PathBuf, contents: &str) -> Result<(), StoreError> {
            let mut file = fs::File::create(name).map_err(|e| StoreError::Unknown(e.into()))?;
            file.write_all(contents.as_bytes())
                .map_err(|e| StoreError::Unknown(e.into()))?;
            Ok(())
        }

        // Check for a previous dump (incremental mode)
        let metadata_path = dir.join("metadata.json");
        let prev = if metadata_path.exists() {
            let prev = Metadata::from_file(&metadata_path)?;
            if prev.deployment != self.site.deployment {
                return Err(StoreError::InternalError(format!(
                    "incremental dump refused: previous dump is for deployment '{}' \
                     but current deployment is '{}'",
                    prev.deployment, self.site.deployment
                )));
            }
            Some(prev)
        } else {
            None
        };

        let prev_head_block_number = prev
            .as_ref()
            .and_then(|p| p.head_block.as_ref())
            .map(|b| b.number);

        let deployment = deployment_entity(conn, &self.site, &self.input_schema).await?;
        let (mut metadata, schema, yaml) = Metadata::new(
            self.site.deployment.clone(),
            network.to_string(),
            entity_count,
            deployment,
            index_list,
        );

        // Guard: refuse incremental if current head ≤ previous head
        // (could indicate a reorg)
        if let (Some(prev_head), Some(curr_head)) = (
            prev_head_block_number,
            metadata.head_block.as_ref().map(|b| b.number),
        ) && curr_head <= prev_head
        {
            return Err(StoreError::InternalError(format!(
                "incremental dump refused: current head block ({}) <= previous head block ({}); \
                     possible reorg — delete the dump directory and re-dump",
                curr_head, prev_head
            )));
        }

        write_file(dir.join("schema.graphql"), &schema)?;

        if let Some(yaml) = yaml {
            write_file(dir.join("subgraph.yaml"), &yaml)?;
        }

        // Dump entity tables sorted by name for determinism
        let mut tables: Vec<_> = self.tables.values().collect();
        tables.sort_by_key(|t| t.name.as_str().to_string());

        reporter.start(self.site.deployment.as_str(), tables.len());

        for table in &tables {
            let table_name = table.object.as_str();
            let prev_table = prev.as_ref().and_then(|p| p.tables.get(table_name));
            let table_info = dump_entity_table(
                conn,
                table,
                &dir,
                prev_table,
                prev_head_block_number,
                reporter,
            )
            .await?;
            metadata.tables.insert(table_name.to_string(), table_info);
        }

        // Carry forward tables from previous dump that are no longer in
        // the current layout (schema evolution — rare but possible)
        if let Some(ref prev_meta) = prev {
            for (name, info) in &prev_meta.tables {
                if name == DATA_SOURCES_TABLE {
                    continue;
                }
                if !metadata.tables.contains_key(name) {
                    metadata.tables.insert(name.clone(), info.clone());
                }
            }
        }

        // Dump data_sources$ if it exists
        let namespace = self.site.namespace.as_str();
        let prev_ds = prev.as_ref().and_then(|p| p.tables.get(DATA_SOURCES_TABLE));
        if let Some(ds_info) = dump_data_sources(
            conn,
            namespace,
            &dir,
            prev_ds,
            prev_head_block_number,
            reporter,
        )
        .await?
        {
            metadata
                .tables
                .insert(DATA_SOURCES_TABLE.to_string(), ds_info);
        } else if let Some(prev_ds_info) = prev_ds {
            // Carry forward from previous dump
            metadata
                .tables
                .insert(DATA_SOURCES_TABLE.to_string(), prev_ds_info.clone());
        }

        // Write metadata.json atomically via tmp file + rename
        let tmp_path = dir.join("metadata.json.tmp");
        write_file(tmp_path.clone(), &serde_json::to_string_pretty(&metadata)?)?;
        fs::rename(&tmp_path, dir.join("metadata.json")).map_err(|e| {
            StoreError::InternalError(format!("failed to rename metadata.json.tmp: {e}"))
        })?;

        reporter.finish();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_info_without_clamps_field() {
        // Old dumps don't have a `clamps` field. Verify deserialization
        // works and produces an empty vec.
        let json = r#"{
            "immutable": false,
            "has_causality_region": true,
            "chunks": [],
            "max_vid": 42
        }"#;
        let info: TableInfo = serde_json::from_str(json).unwrap();
        assert!(info.clamps.is_empty());
        assert_eq!(info.max_vid, 42);
        assert!(!info.immutable);
    }

    #[test]
    fn table_info_with_clamps_field() {
        let json = r#"{
            "immutable": false,
            "has_causality_region": false,
            "chunks": [],
            "clamps": [{"file": "Foo/clamp_000000.parquet", "min_vid": 1, "max_vid": 10, "row_count": 5}],
            "max_vid": 100
        }"#;
        let info: TableInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.clamps.len(), 1);
        assert_eq!(info.clamps[0].file, "Foo/clamp_000000.parquet");
        assert_eq!(info.clamps[0].row_count, 5);
    }

    #[test]
    fn chunk_and_clamp_filenames() {
        assert_eq!(chunk_filename(0), "chunk_000000.parquet");
        assert_eq!(chunk_filename(1), "chunk_000001.parquet");
        assert_eq!(chunk_filename(999), "chunk_000999.parquet");

        assert_eq!(clamp_filename(0), "clamp_000000.parquet");
        assert_eq!(clamp_filename(1), "clamp_000001.parquet");
    }

    #[test]
    fn clamp_record_batch_construction() {
        use crate::parquet::schema::clamp_arrow_schema;

        let schema = clamp_arrow_schema();
        let rows = vec![
            ClampRow {
                vid: 10,
                block_range_end: 500,
            },
            ClampRow {
                vid: 20,
                block_range_end: 600,
            },
            ClampRow {
                vid: 30,
                block_range_end: 700,
            },
        ];
        let batch = clamp_rows_to_record_batch(&schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        let vid = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(vid.value(0), 10);
        assert_eq!(vid.value(1), 20);
        assert_eq!(vid.value(2), 30);

        let bre = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(bre.value(0), 500);
        assert_eq!(bre.value(1), 600);
        assert_eq!(bre.value(2), 700);
    }
}
