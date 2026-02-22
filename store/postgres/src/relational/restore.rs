//! Restore a subgraph deployment from a dump directory.
//!
//! The dump directory must contain:
//! - `metadata.json` — deployment metadata and per-table state
//! - `schema.graphql` — raw GraphQL schema text
//! - Per-entity Parquet files in subdirectories

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use diesel::dsl::update;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::sql_types::{BigInt, Binary, Integer, Nullable, Text};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use graph::blockchain::BlockHash;
use graph::data::subgraph::schema::{DeploymentCreate, SubgraphManifestEntity};
use graph::prelude::{info, BlockPtr as GraphBlockPtr, Logger, StoreError};
use graph::schema::{EntityType, InputSchema};
use graph::semver::Version;

use crate::catalog;
use crate::deployment::create_deployment;
use crate::dynds::DataSourcesTable;
use crate::parquet::convert::{
    record_batch_to_data_source_rows, record_batch_to_restore_rows, DataSourceRestoreRow,
};
use crate::parquet::reader::read_batches;
use crate::primary::Site;
use crate::relational::dump::{Metadata, TableInfo};
use crate::relational::{Layout, Table, VID_COLUMN};
use crate::relational_queries::InsertQuery;
use crate::vid_batcher::VidRange;
use crate::AsyncPgConnection;

const DATA_SOURCES_TABLE: &str = "data_sources$";

/// Convert a dump `BlockPtr` (hex hash string) to a graph `BlockPtr`.
fn to_graph_block_ptr(bp: &super::dump::BlockPtr) -> Result<GraphBlockPtr, StoreError> {
    let hash = BlockHash::try_from(bp.hash.as_str())
        .map_err(|e| StoreError::InternalError(format!("invalid block hash '{}': {e}", bp.hash)))?;
    Ok(GraphBlockPtr {
        number: bp.number,
        hash,
    })
}

/// Query the current max(vid) for a table. Returns -1 if the table is empty.
async fn current_max_vid(
    conn: &mut AsyncPgConnection,
    qualified_name: &str,
) -> Result<i64, StoreError> {
    let query = format!(
        "select coalesce(min(vid), 0)::int8 as min_vid, \
                coalesce(max(vid), -1)::int8 as max_vid \
           from {}",
        qualified_name
    );
    let range: VidRange = diesel::sql_query(&query)
        .get_result(conn)
        .await
        .map_err(StoreError::from)?;
    Ok(range.max)
}

/// Import a single entity table from Parquet chunks.
///
/// Supports resumability: checks the current max(vid) in the DB table
/// and skips already-imported rows.
async fn import_entity_table(
    conn: &mut AsyncPgConnection,
    table: &Table,
    table_info: &TableInfo,
    dir: &Path,
    logger: &Logger,
) -> Result<usize, StoreError> {
    if table_info.chunks.is_empty() || table_info.max_vid < 0 {
        return Ok(0);
    }

    let max_vid_db = current_max_vid(conn, table.qualified_name.as_str()).await?;
    if max_vid_db >= table_info.max_vid {
        info!(
            logger,
            "Table {} already fully restored, skipping",
            table.object.as_str()
        );
        return Ok(0);
    }

    let chunk_size = InsertQuery::chunk_size(table);
    let mut total_inserted = 0usize;

    for chunk_info in &table_info.chunks {
        // Skip chunks that are fully imported
        if chunk_info.max_vid <= max_vid_db {
            continue;
        }

        let chunk_path = dir.join(&chunk_info.file);
        let batches = read_batches(&chunk_path)?;

        for batch in &batches {
            let mut rows = record_batch_to_restore_rows(batch, table)?;

            // Filter out already-imported rows (for boundary chunks on resume)
            if max_vid_db >= 0 {
                rows.retain(|row| row.vid > max_vid_db);
            }

            if rows.is_empty() {
                continue;
            }

            // Split into InsertQuery-sized chunks and execute
            for chunk in rows.chunks(chunk_size) {
                InsertQuery::for_restore(table, chunk)?
                    .execute(conn)
                    .await?;
                total_inserted += chunk.len();
            }
        }
    }

    info!(
        logger,
        "Restored {} rows into {}",
        total_inserted,
        table.object.as_str()
    );
    Ok(total_inserted)
}

/// Insert a single data_sources$ row via raw SQL.
async fn insert_data_source_row(
    conn: &mut AsyncPgConnection,
    qualified_table: &str,
    row: &DataSourceRestoreRow,
) -> Result<(), StoreError> {
    let query = format!(
        "INSERT INTO {} (vid, block_range, causality_region, manifest_idx, \
                parent, id, param, context, done_at) \
         VALUES ($1, int4range($2, $3), $4, $5, $6, $7, $8, $9::jsonb, $10)",
        qualified_table,
    );
    diesel::sql_query(&query)
        .bind::<BigInt, _>(row.vid)
        .bind::<Integer, _>(row.block_range_start)
        .bind::<Nullable<Integer>, _>(row.block_range_end)
        .bind::<Integer, _>(row.causality_region)
        .bind::<Integer, _>(row.manifest_idx)
        .bind::<Nullable<Integer>, _>(row.parent)
        .bind::<Nullable<Binary>, _>(row.id.as_deref())
        .bind::<Nullable<Binary>, _>(row.param.as_deref())
        .bind::<Nullable<Text>, _>(row.context.as_deref())
        .bind::<Nullable<Integer>, _>(row.done_at)
        .execute(conn)
        .await
        .map_err(StoreError::from)?;
    Ok(())
}

/// Import the `data_sources$` table from Parquet chunks.
async fn import_data_sources(
    conn: &mut AsyncPgConnection,
    namespace: &str,
    table_info: &TableInfo,
    dir: &Path,
    logger: &Logger,
) -> Result<usize, StoreError> {
    if table_info.chunks.is_empty() || table_info.max_vid < 0 {
        return Ok(0);
    }

    let qualified = format!("\"{}\".\"{DATA_SOURCES_TABLE}\"", namespace);
    let max_vid_db = current_max_vid(conn, &qualified).await?;
    if max_vid_db >= table_info.max_vid {
        info!(logger, "data_sources$ already fully restored, skipping");
        return Ok(0);
    }

    let mut total_inserted = 0usize;

    for chunk_info in &table_info.chunks {
        if chunk_info.max_vid <= max_vid_db {
            continue;
        }

        let chunk_path = dir.join(&chunk_info.file);
        let batches = read_batches(&chunk_path)?;

        for batch in &batches {
            let rows = record_batch_to_data_source_rows(batch)?;

            for row in &rows {
                if max_vid_db >= 0 && row.vid <= max_vid_db {
                    continue;
                }
                insert_data_source_row(conn, &qualified, row).await?;
                total_inserted += 1;
            }
        }
    }

    info!(logger, "Restored {} data_sources$ rows", total_inserted);
    Ok(total_inserted)
}

/// Restore a subgraph deployment's schema and metadata from a dump
/// directory.
///
/// This creates the deployment metadata rows (`subgraphs.head`,
/// `subgraphs.deployment`, `subgraphs.subgraph_manifest`), entity
/// tables, and optionally the `data_sources$` table.
///
/// The caller is responsible for:
/// - Reading `metadata.json` via `Metadata::from_file()`
/// - Site allocation and conflict resolution (force-drop)
/// - Obtaining the right shard connection
///
/// Entity data import and finalization are handled separately.
#[allow(dead_code)]
pub async fn create_schema(
    conn: &mut AsyncPgConnection,
    site: Arc<Site>,
    metadata: &Metadata,
    dir: &Path,
) -> Result<Layout, StoreError> {
    // 1. Read schema.graphql
    let schema_path = dir.join("schema.graphql");
    let schema_text = fs::read_to_string(&schema_path).map_err(|e| {
        StoreError::InternalError(format!("failed to read {}: {e}", schema_path.display()))
    })?;

    // 2. Read subgraph.yaml (optional)
    let yaml_path = dir.join("subgraph.yaml");
    let raw_yaml = fs::read_to_string(&yaml_path).ok();

    // 3. Parse schema
    let spec_version = Version::parse(&metadata.manifest.spec_version).map_err(|e| {
        StoreError::InternalError(format!(
            "invalid spec_version '{}': {e}",
            metadata.manifest.spec_version
        ))
    })?;
    let input_schema = InputSchema::parse(&spec_version, &schema_text, site.deployment.clone())?;

    // 4. Resolve entities_with_causality_region from names
    let entities_with_causality_region: BTreeSet<EntityType> = metadata
        .manifest
        .entities_with_causality_region
        .iter()
        .map(|name| input_schema.entity_type(name))
        .collect::<Result<_, _>>()
        .map_err(StoreError::from)?;

    // 5. Build SubgraphManifestEntity for create_deployment
    let manifest_entity = SubgraphManifestEntity {
        spec_version: metadata.manifest.spec_version.clone(),
        description: metadata.manifest.description.clone(),
        repository: metadata.manifest.repository.clone(),
        features: metadata.manifest.features.clone(),
        schema: schema_text,
        raw_yaml,
        entities_with_causality_region: entities_with_causality_region.iter().cloned().collect(),
        history_blocks: metadata.manifest.history_blocks,
    };

    let start_block = metadata
        .start_block
        .as_ref()
        .map(to_graph_block_ptr)
        .transpose()?;
    let graft_block = metadata
        .graft_block
        .as_ref()
        .map(to_graph_block_ptr)
        .transpose()?;

    let create = DeploymentCreate {
        manifest: manifest_entity,
        start_block,
        graft_base: metadata.graft_base.clone(),
        graft_block,
        debug_fork: metadata.debug_fork.clone(),
        history_blocks_override: None,
    };

    // 6. Create deployment metadata rows
    create_deployment(conn, &site, create, false, false).await?;

    // 7. Create database schema and entity tables
    let query = format!("create schema {}", &site.namespace);
    conn.batch_execute(&query).await?;

    let layout = Layout::create_relational_schema(
        conn,
        site.clone(),
        &input_schema,
        entities_with_causality_region,
        None,
    )
    .await?;

    // 8. Create data_sources$ table if present in dump
    if metadata.tables.contains_key(DATA_SOURCES_TABLE) {
        let ds_table = DataSourcesTable::new(site.namespace.clone());
        let ddl = ds_table.as_ddl();
        conn.batch_execute(&ddl).await?;
    }

    Ok(layout)
}

/// Import entity data and data_sources$ from Parquet files into the
/// database tables created by `create_schema`.
///
/// This is resumable: if interrupted, it can be called again and will
/// skip already-imported rows by checking the current max(vid) in each
/// table.
#[allow(dead_code)]
pub async fn import_data(
    conn: &mut AsyncPgConnection,
    layout: &Layout,
    metadata: &Metadata,
    dir: &Path,
    logger: &Logger,
) -> Result<(), StoreError> {
    // Import entity tables (sorted by name for determinism)
    let mut table_names: Vec<_> = metadata
        .tables
        .keys()
        .filter(|name| name.as_str() != DATA_SOURCES_TABLE)
        .collect();
    table_names.sort();

    for table_name in table_names {
        let table_info = &metadata.tables[table_name];
        let table = layout
            .tables
            .values()
            .find(|t| t.object.as_str() == table_name)
            .ok_or_else(|| {
                StoreError::InternalError(format!(
                    "table '{}' from dump not found in layout",
                    table_name,
                ))
            })?;
        import_entity_table(conn, table, table_info, dir, logger).await?;
    }

    // Import data_sources$ if present
    if let Some(ds_info) = metadata.tables.get(DATA_SOURCES_TABLE) {
        let namespace = layout.site.namespace.as_str();
        import_data_sources(conn, namespace, ds_info, dir, logger).await?;
    }

    Ok(())
}

/// Finalize a restored deployment by resetting vid sequences and setting
/// the head block pointer.
///
/// This must be called after `import_data` has completed successfully.
/// Setting the head block is the very last operation — it marks the
/// deployment as "ready".
#[allow(dead_code)]
pub async fn finalize(
    conn: &mut AsyncPgConnection,
    layout: &Layout,
    metadata: &Metadata,
    logger: &Logger,
) -> Result<(), StoreError> {
    let nsp = layout.site.namespace.as_str();

    // 1. Reset vid sequences for entity tables that use bigserial.
    //    Tables where has_vid_seq() is true use plain bigint (no sequence).
    let mut table_names: Vec<_> = metadata
        .tables
        .keys()
        .filter(|name| name.as_str() != DATA_SOURCES_TABLE)
        .collect();
    table_names.sort();

    for table_name in table_names {
        let table_info = &metadata.tables[table_name];
        if table_info.max_vid < 0 {
            continue;
        }

        let table = layout
            .tables
            .values()
            .find(|t| t.object.as_str() == table_name)
            .ok_or_else(|| {
                StoreError::InternalError(format!(
                    "table '{}' from dump not found in layout",
                    table_name,
                ))
            })?;

        if table.object.has_vid_seq() {
            continue;
        }

        let vid_seq = catalog::seq_name(&table.name, VID_COLUMN);
        let query = format!(
            "SELECT setval('\"{nsp}\".\"{vid_seq}\"', {})",
            table_info.max_vid
        );
        conn.batch_execute(&query).await.map_err(|e| {
            StoreError::InternalError(format!("reset vid seq for {table_name}: {e}"))
        })?;
    }

    // 2. Reset data_sources$ vid sequence if present
    if let Some(ds_info) = metadata.tables.get(DATA_SOURCES_TABLE) {
        if ds_info.max_vid >= 0 {
            let qualified = format!("\"{nsp}\".\"{DATA_SOURCES_TABLE}\"");
            let query = format!(
                "SELECT setval(pg_get_serial_sequence('{qualified}', 'vid'), {})",
                ds_info.max_vid
            );
            conn.batch_execute(&query).await.map_err(|e| {
                StoreError::InternalError(format!("reset data_sources$ vid seq: {e}"))
            })?;
        }
    }

    // 3. Update earliest_block_number (may differ from start_block after
    //    pruning) and set the head block pointer. Setting the head block
    //    is the very last step: it makes the deployment "ready".
    {
        use crate::deployment::deployment as d;
        use crate::deployment::head as h;

        update(d::table.filter(d::id.eq(layout.site.id)))
            .set(d::earliest_block_number.eq(metadata.earliest_block_number))
            .execute(conn)
            .await
            .map_err(StoreError::from)?;

        if let Some(head) = &metadata.head_block {
            let head_ptr = to_graph_block_ptr(head)?;
            update(h::table.filter(h::id.eq(layout.site.id)))
                .set((
                    h::block_number.eq(head_ptr.number),
                    h::block_hash.eq(head_ptr.hash_slice()),
                    h::entity_count.eq(metadata.entity_count as i64),
                ))
                .execute(conn)
                .await
                .map_err(StoreError::from)?;

            info!(
                logger,
                "Finalized restore: head block #{}, entity count {}",
                head_ptr.number,
                metadata.entity_count
            );
        } else {
            info!(logger, "Finalized restore (no head block in dump)");
        }
    }

    Ok(())
}
