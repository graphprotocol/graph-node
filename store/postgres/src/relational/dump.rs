use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, fs};

use diesel::dsl::sql;
use diesel::sql_types::{
    Array, BigInt, Binary, Bool, Integer, Nullable, Numeric, Text, Timestamptz, Untyped,
};
use diesel::QueryDsl;
use diesel_async::RunQueryDsl;
use diesel_dynamic_schema::DynamicSelectClause;

use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth, SubgraphManifestEntity};
use graph::prelude::{DeploymentHash, StoreError, SubgraphDeploymentEntity};
use serde::{Deserialize, Serialize};

use crate::catalog;
use crate::detail::deployment_entity;
use crate::parquet::convert::rows_to_record_batch;
use crate::parquet::schema::{arrow_schema, data_sources_arrow_schema};
use crate::parquet::writer::{ChunkInfo, ParquetChunkWriter};
use crate::relational::dsl;
use crate::relational::index::IndexList;
use crate::relational::value::OidRow;
use crate::relational::{ColumnType, SqlName, Table as RelTable};
use crate::vid_batcher::{VidBatcher, VidRange};
use crate::AsyncPgConnection;

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
        for (name, indexes) in index_list.indexes {
            let mut indexes2 = Vec::new();
            for index in indexes {
                let Ok(index) = index.with_nsp("sgd".to_string()) else {
                    continue;
                };
                let Ok(index) = index.to_sql(true, true) else {
                    continue;
                };
                indexes2.push(index);
            }
            res.insert(name, indexes2);
        }
        res
    }
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
                let alias = table.meta.name.as_str();
                let cast = if is_list { "text[]" } else { "text" };
                let expr = format!("c.\"{}\"::{}", name, cast);
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

/// Dump a single entity table to a Parquet file.
async fn dump_entity_table(
    conn: &mut AsyncPgConnection,
    table: &RelTable,
    dir: &Path,
) -> Result<TableInfo, StoreError> {
    let arrow_schema = arrow_schema(table);
    let table_dir_name = table.object.as_str();
    let table_dir = dir.join(table_dir_name);
    fs::create_dir_all(&table_dir)
        .map_err(|e| StoreError::InternalError(format!("failed to create dir: {e}")))?;

    let range = vid_range(conn, table).await?;

    if range.is_empty() {
        return Ok(TableInfo {
            immutable: table.immutable,
            has_causality_region: table.has_causality_region,
            chunks: vec![],
            max_vid: -1,
        });
    }

    let relative_path = format!("{}/chunk_000000.parquet", table_dir_name);
    let abs_path = table_dir.join("chunk_000000.parquet");
    let mut writer = ParquetChunkWriter::new(abs_path, relative_path, &arrow_schema)?;

    let dsl_table = dsl::Table::new(table);
    let mut batcher = VidBatcher::load(conn, &table.nsp, table, range).await?;

    while !batcher.finished() {
        batcher
            .step(async |start, end| {
                let query = dump_select(&dsl_table, table)
                    .filter(sql::<Bool>("vid >= ").bind::<BigInt, _>(start))
                    .filter(sql::<Bool>("vid <= ").bind::<BigInt, _>(end))
                    .order(sql::<Untyped>("vid"));

                let rows: Vec<OidRow> = query.load(conn).await?;
                if rows.is_empty() {
                    return Ok(());
                }

                let batch = rows_to_record_batch(&arrow_schema, &rows)?;
                // Extract vid bounds from first/last row
                let batch_min_vid = start;
                let batch_max_vid = end;
                writer.write_batch(&batch, batch_min_vid, batch_max_vid)?;
                Ok(())
            })
            .await?;
    }

    let chunk_info = writer.finish()?;

    let max_vid = if chunk_info.row_count > 0 {
        chunk_info.max_vid
    } else {
        -1
    };

    let chunks = if chunk_info.row_count > 0 {
        vec![chunk_info]
    } else {
        vec![]
    };

    Ok(TableInfo {
        immutable: table.immutable,
        has_causality_region: table.has_causality_region,
        chunks,
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
) -> Result<Option<TableInfo>, StoreError> {
    let table_name = SqlName::verbatim(DATA_SOURCES_TABLE.to_string());
    if !catalog::table_exists(conn, namespace, &table_name).await? {
        return Ok(None);
    }

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

    if range.is_empty() {
        return Ok(Some(TableInfo {
            immutable: false,
            has_causality_region: true,
            chunks: vec![],
            max_vid: -1,
        }));
    }

    let schema = data_sources_arrow_schema();
    let ds_dir = dir.join(DATA_SOURCES_TABLE);
    fs::create_dir_all(&ds_dir)
        .map_err(|e| StoreError::InternalError(format!("failed to create dir: {e}")))?;

    let relative_path = format!("{}/chunk_000000.parquet", DATA_SOURCES_TABLE);
    let abs_path = ds_dir.join("chunk_000000.parquet");
    let mut writer = ParquetChunkWriter::new(abs_path, relative_path, &schema)?;

    let mut start = range.min;
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
            let batch = data_source_rows_to_record_batch(&schema, &rows)?;
            writer.write_batch(&batch, start, end)?;
        }

        start = end + 1;
    }

    let chunk_info = writer.finish()?;

    let max_vid = if chunk_info.row_count > 0 {
        chunk_info.max_vid
    } else {
        -1
    };

    let chunks = if chunk_info.row_count > 0 {
        vec![chunk_info]
    } else {
        vec![]
    };

    Ok(Some(TableInfo {
        immutable: false,
        has_causality_region: true,
        chunks,
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
    ) -> Result<(), StoreError> {
        fn write_file(name: PathBuf, contents: &str) -> Result<(), StoreError> {
            let mut file = fs::File::create(name).map_err(|e| StoreError::Unknown(e.into()))?;
            file.write_all(contents.as_bytes())
                .map_err(|e| StoreError::Unknown(e.into()))?;
            Ok(())
        }

        let deployment = deployment_entity(conn, &self.site, &self.input_schema).await?;
        let (mut metadata, schema, yaml) = Metadata::new(
            self.site.deployment.clone(),
            network.to_string(),
            entity_count,
            deployment,
            index_list,
        );

        write_file(dir.join("schema.graphql"), &schema)?;

        if let Some(yaml) = yaml {
            write_file(dir.join("subgraph.yaml"), &yaml)?;
        }

        // Dump entity tables sorted by name for determinism
        let mut tables: Vec<_> = self.tables.values().collect();
        tables.sort_by_key(|t| t.name.as_str().to_string());

        for table in tables {
            let table_info = dump_entity_table(conn, table, &dir).await?;
            metadata
                .tables
                .insert(table.object.as_str().to_string(), table_info);
        }

        // Dump data_sources$ if it exists
        let namespace = self.site.namespace.as_str();
        if let Some(ds_info) = dump_data_sources(conn, namespace, &dir).await? {
            metadata
                .tables
                .insert(DATA_SOURCES_TABLE.to_string(), ds_info);
        }

        // Write metadata.json atomically via tmp file + rename
        let tmp_path = dir.join("metadata.json.tmp");
        write_file(tmp_path.clone(), &serde_json::to_string_pretty(&metadata)?)?;
        fs::rename(&tmp_path, dir.join("metadata.json")).map_err(|e| {
            StoreError::InternalError(format!("failed to rename metadata.json.tmp: {e}"))
        })?;

        Ok(())
    }
}
