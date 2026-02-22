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

use diesel_async::SimpleAsyncConnection;

use graph::blockchain::BlockHash;
use graph::data::subgraph::schema::{DeploymentCreate, SubgraphManifestEntity};
use graph::prelude::{BlockPtr as GraphBlockPtr, StoreError};
use graph::schema::{EntityType, InputSchema};
use graph::semver::Version;

use crate::deployment::create_deployment;
use crate::dynds::DataSourcesTable;
use crate::primary::Site;
use crate::relational::dump::Metadata;
use crate::relational::Layout;
use crate::AsyncPgConnection;

/// Convert a dump `BlockPtr` (hex hash string) to a graph `BlockPtr`.
fn to_graph_block_ptr(bp: &super::dump::BlockPtr) -> Result<GraphBlockPtr, StoreError> {
    let hash = BlockHash::try_from(bp.hash.as_str())
        .map_err(|e| StoreError::InternalError(format!("invalid block hash '{}': {e}", bp.hash)))?;
    Ok(GraphBlockPtr {
        number: bp.number,
        hash,
    })
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
    if metadata.tables.contains_key("data_sources$") {
        let ds_table = DataSourcesTable::new(site.namespace.clone());
        let ddl = ds_table.as_ddl();
        conn.batch_execute(&ddl).await?;
    }

    Ok(layout)
}
