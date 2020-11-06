//! Utilities for dealing with subgraph metadata
use diesel::dsl::{delete, insert_into, sql, update};
use diesel::pg::PgConnection;
use diesel::prelude::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension, QueryDsl,
    RunQueryDsl,
};
use diesel::sql_types::Text;
use graph::data::subgraph::schema::{
    generate_entity_id, SubgraphDeploymentAssignmentEntity, SubgraphManifestEntity, SUBGRAPHS_ID,
};
use graph::prelude::{
    bigdecimal::ToPrimitive, entity, format_err, web3::types::H256, BigDecimal, BlockNumber,
    DeploymentState, EntityChange, EntityChangeOperation, EthereumBlockPointer, MetadataOperation,
    NodeId, Schema, StoreError, StoreEvent, SubgraphDeploymentEntity, SubgraphDeploymentId,
    SubgraphName, SubgraphVersionSwitchingMode, TypedEntity,
};
use std::convert::TryFrom;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::block_range::UNVERSIONED_RANGE;

// Diesel tables for some of the metadata
// See also: ed42d219c6704a4aab57ce1ea66698e7
// Changes to the GraphQL schema might require changes to these tables.
// The definitions of the tables can be generated with
//    cargo run -p graph-store-postgres --example layout -- \
//      -g diesel store/postgres/src/subgraphs.graphql subgraphs
table! {
    subgraphs.subgraph (vid) {
        vid -> BigInt,
        id -> Text,
        name -> Text,
        current_version -> Nullable<Text>,
        pending_version -> Nullable<Text>,
        created_at -> Numeric,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.subgraph_version (vid) {
        vid -> BigInt,
        id -> Text,
        subgraph -> Text,
        deployment -> Text,
        created_at -> Numeric,
        block_range -> Range<Integer>,
    }
}

#[derive(DbEnum, Debug, Clone, Copy)]
pub enum SubgraphHealth {
    Failed,
    Healthy,
    Unhealthy,
}

table! {
    subgraphs.subgraph_deployment (vid) {
        vid -> BigInt,
        id -> Text,
        manifest -> Text,
        failed -> Bool,
        health -> crate::metadata::SubgraphHealthMapping,
        synced -> Bool,
        fatal_error -> Nullable<Text>,
        non_fatal_errors -> Array<Text>,
        earliest_ethereum_block_hash -> Nullable<Binary>,
        earliest_ethereum_block_number -> Nullable<Numeric>,
        latest_ethereum_block_hash -> Nullable<Binary>,
        latest_ethereum_block_number -> Nullable<Numeric>,
        entity_count -> Numeric,
        graft_base -> Nullable<Text>,
        graft_block_hash -> Nullable<Binary>,
        graft_block_number -> Nullable<Numeric>,
        reorg_count -> Integer,
        current_reorg_depth -> Integer,
        max_reorg_depth -> Integer,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.subgraph_deployment_assignment (vid) {
        vid -> BigInt,
        id -> Text,
        node_id -> Text,
        cost -> Numeric,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.subgraph_error (vid) {
        vid -> BigInt,
        id -> Text,
        subgraph_id -> Nullable<Text>,
        message -> Text,
        block_number -> Nullable<Numeric>,
        block_hash -> Nullable<Binary>,
        handler -> Nullable<Text>,
        deterministic -> Bool,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.dynamic_ethereum_contract_data_source (vid) {
        vid -> BigInt,
        id -> Text,
        kind -> Text,
        name -> Text,
        network -> Nullable<Text>,
        source -> Text,
        mapping -> Text,
        templates -> Nullable<Array<Text>>,
        ethereum_block_hash -> Binary,
        ethereum_block_number -> Numeric,
        deployment -> Text,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.subgraph_manifest (vid) {
        vid -> BigInt,
        id -> Text,
        spec_version -> Text,
        description -> Nullable<Text>,
        repository -> Nullable<Text>,
        schema -> Text,
        data_sources -> Array<Text>,
        templates -> Nullable<Array<Text>>,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.ethereum_contract_data_source (vid) {
        vid -> BigInt,
        id -> Text,
        kind -> Text,
        name -> Text,
        network -> Nullable<Text>,
        source -> Text,
        mapping -> Text,
        templates -> Nullable<Array<Text>>,
        block_range -> Range<Integer>,
    }
}

allow_tables_to_appear_in_same_query!(subgraph, subgraph_version, subgraph_deployment);

/// Look up the graft point for the given subgraph in the database and
/// return it
pub fn deployment_graft(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<Option<(SubgraphDeploymentId, EthereumBlockPointer)>, StoreError> {
    use subgraph_deployment as sd;

    if id.is_meta() {
        // There is no SubgraphDeployment for the metadata subgraph
        Ok(None)
    } else {
        match sd::table
            .select((sd::graft_base, sd::graft_block_hash, sd::graft_block_number))
            .filter(sd::id.eq(id.as_str()))
            .first::<(Option<String>, Option<Vec<u8>>, Option<BigDecimal>)>(conn)?
        {
            (None, None, None) => Ok(None),
            (Some(subgraph), Some(hash), Some(block)) => {
                let hash = H256::from_slice(hash.as_slice());
                let block = block.to_u64().expect("block numbers fit into a u64");
                let subgraph = SubgraphDeploymentId::new(subgraph.clone()).map_err(|_| {
                    StoreError::Unknown(format_err!(
                        "the base subgraph for a graft must be a valid subgraph id but is `{}`",
                        subgraph
                    ))
                })?;
                Ok(Some((subgraph, EthereumBlockPointer::from((hash, block)))))
            }
            _ => unreachable!(
                "graftBlockHash and graftBlockNumber are either both set or neither is set"
            ),
        }
    }
}

pub fn subgraph_schema(
    conn: &PgConnection,
    id: SubgraphDeploymentId,
) -> Result<Schema, StoreError> {
    // The subgraph of subgraphs schema is built-in and doesn't have a
    // SubgraphManifest in the database
    const SUBGRAPHS_SCHEMA: &str = include_str!("subgraphs.graphql");
    let res = if id.is_meta() {
        Schema::parse(SUBGRAPHS_SCHEMA, id)
    } else {
        use subgraph_manifest as sm;
        let manifest_id = SubgraphManifestEntity::id(&id);
        let s: String = sm::table
            .select(sm::schema)
            .filter(sm::id.eq(manifest_id.as_str()))
            .first(conn)?;
        Schema::parse(s.as_str(), id)
    };
    res.map_err(|e| StoreError::Unknown(e))
}

pub fn subgraph_network(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<Option<String>, StoreError> {
    use ethereum_contract_data_source as ds;
    use subgraph_manifest as sm;

    let manifest_id = SubgraphManifestEntity::id(&id);
    sm::table
        .select(sm::data_sources)
        .filter(sm::id.eq(manifest_id.as_str()))
        .first::<Vec<String>>(conn)?
        // The NetworkIndexer creates a manifest with an empty
        // array of data sources and we therefore accept 'None'
        // here
        .first()
        .map(|ds_id| {
            ds::table
                .select(ds::network)
                .filter(ds::id.eq(&ds_id))
                .first::<Option<String>>(conn)
        })
        .transpose()
        .map(|x| x.flatten())
        .map_err(|e| e.into())
}

fn block_ptr_store_event(id: &SubgraphDeploymentId) -> StoreEvent {
    let change = EntityChange {
        entity_type: SubgraphDeploymentEntity::TYPENAME.to_string(),
        entity_id: id.to_string(),
        subgraph_id: SUBGRAPHS_ID.to_owned(),
        operation: EntityChangeOperation::Set,
    };
    StoreEvent::new(vec![change])
}

pub fn forward_block_ptr(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    ptr: EthereumBlockPointer,
) -> Result<StoreEvent, StoreError> {
    use subgraph_deployment as d;

    // Work around a Diesel issue with serializing BigDecimals to numeric
    let number = format!("{}::numeric", ptr.number);

    update(d::table.filter(d::id.eq(id.as_str())))
        .set((
            d::latest_ethereum_block_number.eq(sql(&number)),
            d::latest_ethereum_block_hash.eq(ptr.hash.as_bytes()),
            d::current_reorg_depth.eq(0),
        ))
        .execute(conn)
        .map(|_| block_ptr_store_event(id))
        .map_err(|e| e.into())
}

pub fn revert_block_ptr(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    ptr: EthereumBlockPointer,
) -> Result<StoreEvent, StoreError> {
    use subgraph_deployment as d;

    // Work around a Diesel issue with serializing BigDecimals to numeric
    let number = format!("{}::numeric", ptr.number);

    update(d::table.filter(d::id.eq(id.as_str())))
        .set((
            d::latest_ethereum_block_number.eq(sql(&number)),
            d::latest_ethereum_block_hash.eq(ptr.hash.as_bytes()),
            d::reorg_count.eq(d::reorg_count + 1),
            d::current_reorg_depth.eq(d::current_reorg_depth + 1),
            d::max_reorg_depth.eq(sql("greatest(current_reorg_depth + 1, max_reorg_depth)")),
        ))
        .execute(conn)
        .map(|_| block_ptr_store_event(id))
        .map_err(|e| e.into())
}

fn convert_to_u32(number: Option<i32>, field: &str, subgraph: &str) -> Result<u32, StoreError> {
    number
        .ok_or_else(|| {
            StoreError::ConstraintViolation(format!(
                "missing {} for subgraph `{}`",
                field, subgraph
            ))
        })
        .and_then(|number| {
            u32::try_from(number).map_err(|_| {
                StoreError::ConstraintViolation(format!(
                    "invalid value {:?} for {} in subgraph {}",
                    number, field, subgraph
                ))
            })
        })
}

/// Translate `latest` into a `BlockNumber`. If `latest` is `None` or does
/// not represent an `i32`, return an error
fn latest_as_block_number(
    latest: Option<BigDecimal>,
    subgraph: &str,
) -> Result<BlockNumber, StoreError> {
    match latest {
        None => Err(StoreError::QueryExecutionError(format!(
            "Subgraph `{}` has not started syncing yet. Wait for it to ingest \
             a few blocks before querying it",
            subgraph
        ))),
        Some(latest) => latest.to_i32().ok_or_else(|| {
            StoreError::ConstraintViolation(format!(
                "Subgraph `{}` has an \
                 invalid latest_ethereum_block_number `{:?}` that can not be \
                 represented as an i32",
                subgraph, latest
            ))
        }),
    }
}

pub fn deployment_state_from_name(
    conn: &PgConnection,
    name: SubgraphName,
) -> Result<DeploymentState, StoreError> {
    use subgraph as s;
    use subgraph_deployment as d;
    use subgraph_version as v;

    let mut rows = s::table
        .left_outer_join(v::table.on(s::current_version.eq(v::id.nullable())))
        .left_outer_join(d::table.on(v::deployment.eq(d::id)))
        .filter(s::name.eq(name.as_str()))
        .select((
            s::id,
            v::id.nullable(),
            d::id.nullable(),
            d::reorg_count.nullable(),
            d::max_reorg_depth.nullable(),
            d::latest_ethereum_block_number.nullable(),
        ))
        .load::<(
            String,
            Option<String>,
            Option<String>,
            Option<i32>,
            Option<i32>,
            Option<BigDecimal>,
        )>(conn)?;
    if rows.len() == 0 {
        Err(StoreError::QueryExecutionError(format!(
            "Subgraph `{}` not found",
            name.as_str()
        )))
    } else if rows.len() > 1 {
        Err(StoreError::ConstraintViolation(format!(
            "Multiple subgraphs with the name `{}` exist",
            name.as_str()
        )))
    } else {
        let (_, vid, did, reorg_count, max_reorg_depth, latest_ethereum_block_number) =
            rows.pop().unwrap();
        match (vid, did) {
            (None, _) => Err(StoreError::QueryExecutionError(format!(
                "The subgraph `{}` has no current version. \
            The subgraph may have been created but not deployed yet. Make sure \
            to run `graph deploy` to deploy the subgraph and have it start \
            indexing.",
                name.as_str()
            ))),
            (Some(vid), None) => Err(StoreError::ConstraintViolation(format!(
                "The version `{}` of subgraph `{}` is missing a deployment",
                vid,
                name.as_str()
            ))),
            (Some(_), Some(did)) => {
                let id = SubgraphDeploymentId::new(did).map_err(|s| {
                    StoreError::ConstraintViolation(format!(
                        "Illegal deployment id `{}` for current version of `{}`",
                        s,
                        name.as_str()
                    ))
                })?;
                let reorg_count = convert_to_u32(reorg_count, "reorg_count", name.as_str())?;
                let max_reorg_depth =
                    convert_to_u32(max_reorg_depth, "max_reorg_depth", name.as_str())?;
                let latest_ethereum_block_number =
                    latest_as_block_number(latest_ethereum_block_number, name.as_str())?;
                Ok(DeploymentState {
                    id,
                    reorg_count,
                    max_reorg_depth,
                    latest_ethereum_block_number,
                })
            }
        }
    }
}

pub fn deployment_state_from_id(
    conn: &PgConnection,
    id: SubgraphDeploymentId,
) -> Result<DeploymentState, StoreError> {
    use subgraph_deployment as d;

    match d::table
        .filter(d::id.eq(id.as_str()))
        .select((
            d::id,
            d::reorg_count,
            d::max_reorg_depth,
            d::latest_ethereum_block_number,
        ))
        .first::<(String, i32, i32, Option<BigDecimal>)>(conn)
        .optional()?
    {
        None => Err(StoreError::QueryExecutionError(format!(
            "No data found for subgraph {}",
            id
        ))),
        Some((_, reorg_count, max_reorg_depth, latest_ethereum_block_number)) => {
            let reorg_count = convert_to_u32(Some(reorg_count), "reorg_count", id.as_str())?;
            let max_reorg_depth =
                convert_to_u32(Some(max_reorg_depth), "max_reorg_depth", id.as_str())?;
            let latest_ethereum_block_number =
                latest_as_block_number(latest_ethereum_block_number, id.as_str())?;

            Ok(DeploymentState {
                id,
                reorg_count,
                max_reorg_depth,
                latest_ethereum_block_number,
            })
        }
    }
}

/// Delete all assignments for deployments that are neither the current nor the
/// pending version of a subgraph and return the deployment id's
fn remove_unused_assignments(conn: &PgConnection) -> Result<Vec<EntityChange>, StoreError> {
    const QUERY: &str = "
    delete from subgraphs.subgraph_deployment_assignment a
    where not exists (select 1
                        from subgraphs.subgraph s, subgraphs.subgraph_version v
                       where v.id in (s.current_version, s.pending_version)
                         and v.deployment = a.id)
    returning a.id
    ";
    #[derive(QueryableByName)]
    struct Removed {
        #[sql_type = "Text"]
        id: String,
    }

    Ok(diesel::sql_query(QUERY)
        .load::<Removed>(conn)?
        .into_iter()
        .map(|r| {
            MetadataOperation::Remove {
                entity: SubgraphDeploymentAssignmentEntity::TYPENAME,
                id: r.id,
            }
            .into()
        })
        .filter_map(|e| e)
        .collect::<Vec<_>>())
}

/// Mark the deployment `id` as synced, and promote it to the current version
/// everywhere where it was the pending version so far, and remove any
/// assignments that are not needed any longer as a result. Return the changes
/// that were made to assignments in the process
pub fn deployment_synced(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<Vec<EntityChange>, StoreError> {
    use subgraph as s;
    use subgraph_deployment as d;
    use subgraph_version as v;

    // Subgraphs where we need to promote the version
    let pending_subgraph_versions: Vec<(String, String)> = s::table
        .inner_join(v::table.on(s::pending_version.eq(v::id.nullable())))
        .filter(v::deployment.eq(id.as_str()))
        .select((s::id, v::id))
        .for_update()
        .load(conn)?;

    // Switch the pending version to the current version
    for (subgraph, version) in pending_subgraph_versions {
        update(s::table.filter(s::id.eq(&subgraph)))
            .set((
                s::current_version.eq(&version),
                s::pending_version.eq::<Option<&str>>(None),
            ))
            .execute(conn)?;
    }

    let changes = remove_unused_assignments(conn)?;

    update(d::table.filter(d::id.eq(id.as_str())))
        .set(d::synced.eq(true))
        .execute(conn)?;

    Ok(changes)
}

/// Create a new subgraph with the given name. If one already exists, use
/// the existing one. Return the `id` of the newly created or existing
/// subgraph
pub fn create_subgraph(conn: &PgConnection, name: &SubgraphName) -> Result<String, StoreError> {
    use subgraph as s;

    let id = generate_entity_id();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let inserted = insert_into(s::table)
        .values((
            s::id.eq(&id),
            s::name.eq(name.as_str()),
            // using BigDecimal::from(created_at) produced a scale error
            s::created_at.eq(sql(&format!("{}", created_at))),
            s::block_range.eq(UNVERSIONED_RANGE),
        ))
        .on_conflict(s::name)
        .do_nothing()
        .execute(conn)?;
    if inserted == 0 {
        let existing_id = s::table
            .filter(s::name.eq(name.as_str()))
            .select(s::id)
            .first::<String>(conn)?;
        Ok(existing_id)
    } else {
        Ok(id)
    }
}

pub fn create_subgraph_version(
    conn: &PgConnection,
    name: SubgraphName,
    id: &SubgraphDeploymentId,
    node_id: NodeId,
    mode: SubgraphVersionSwitchingMode,
) -> Result<Vec<EntityChange>, StoreError> {
    use subgraph as s;
    use subgraph_deployment as d;
    use subgraph_deployment_assignment as a;
    use subgraph_version as v;
    use SubgraphVersionSwitchingMode::*;

    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Check the current state of the the subgraph. If no subgraph with the
    // name exists, create one
    let info = s::table
        .filter(s::name.eq(name.as_str()))
        .select((s::id, s::current_version))
        .first(conn)
        .optional()?;
    let (subgraph_id, current_version): (String, Option<String>) = match info {
        Some((subgraph_id, current_version)) => (subgraph_id, current_version),
        None => (create_subgraph(conn, &name)?, None),
    };

    // See if the current version of that subgraph is synced. If the subgraph
    // has no current version, we treat it the same as if it were not synced
    // The `optional` below only comes into play if data is corrupted/missing;
    // ignoring that via `optional` makes it possible to fix a missing version
    // or deployment by deploying over it.
    let current_exists_and_synced = match &current_version {
        Some(current_version) => d::table
            .inner_join(v::table.on(v::deployment.eq(d::id)))
            .filter(v::id.eq(&current_version))
            .select(d::synced)
            .first::<bool>(conn)
            .optional()?
            .unwrap_or(false),
        None => false,
    };

    // Create the actual subgraph version
    let version_id = generate_entity_id();
    insert_into(v::table)
        .values((
            v::id.eq(&version_id),
            v::subgraph.eq(&subgraph_id),
            v::deployment.eq(id.as_str()),
            // using BigDecimal::from(created_at) produced a scale error
            v::created_at.eq(sql(&format!("{}", created_at))),
            v::block_range.eq(UNVERSIONED_RANGE),
        ))
        .execute(conn)?;

    // Create a subgraph assignment if there isn't one already
    let new_assignment = a::table
        .filter(a::id.eq(id.as_str()))
        .select(a::id)
        .first::<String>(conn)
        .optional()?
        .is_none();
    if new_assignment {
        insert_into(a::table)
            .values((
                a::id.eq(id.as_str()),
                a::node_id.eq(node_id.as_str()),
                a::block_range.eq(UNVERSIONED_RANGE),
                a::cost.eq(sql("1")),
            ))
            .execute(conn)?;
    }

    // See if we should make this the current or pending version
    let subgraph_row = update(s::table.filter(s::id.eq(&subgraph_id)));
    match (mode, current_exists_and_synced) {
        (Instant, _) | (Synced, false) => {
            subgraph_row
                .set((
                    s::current_version.eq(&version_id),
                    s::pending_version.eq::<Option<&str>>(None),
                ))
                .execute(conn)?;
        }
        (Synced, true) => {
            subgraph_row
                .set(s::pending_version.eq(&version_id))
                .execute(conn)?;
        }
    }

    // Clean up any assignments we might have displaced
    let mut changes = remove_unused_assignments(conn)?;
    if new_assignment {
        let change = EntityChange::from_key(
            MetadataOperation::entity_key(
                SubgraphDeploymentAssignmentEntity::TYPENAME,
                id.to_string(),
            ),
            EntityChangeOperation::Set,
        );
        changes.push(change);
    }
    Ok(changes)
}

pub fn remove_subgraph(
    conn: &PgConnection,
    name: SubgraphName,
) -> Result<Vec<EntityChange>, StoreError> {
    use subgraph as s;
    use subgraph_version as v;

    // Get the id of the given subgraph. If no subgraph with the
    // name exists, there is nothing to do
    let subgraph: Option<String> = s::table
        .filter(s::name.eq(name.as_str()))
        .select(s::id)
        .first(conn)
        .optional()?;
    if let Some(subgraph) = subgraph {
        delete(v::table.filter(v::subgraph.eq(&subgraph))).execute(conn)?;
        delete(s::table.filter(s::id.eq(subgraph))).execute(conn)?;
        remove_unused_assignments(conn)
    } else {
        Ok(vec![])
    }
}

pub fn reassign_subgraph(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    node: &NodeId,
) -> Result<Vec<EntityChange>, StoreError> {
    use subgraph_deployment_assignment as a;

    let updates = update(a::table.filter(a::id.eq(id.as_str())))
        .set(a::node_id.eq(node.as_str()))
        .execute(conn)?;
    match updates {
        0 => Err(StoreError::DeploymentNotFound(id.to_string())),
        1 => {
            let op = MetadataOperation::Set {
                entity: SubgraphDeploymentAssignmentEntity::TYPENAME,
                id: id.to_string(),
                data: entity! { node_id: node.to_string() },
            };
            let change: Option<EntityChange> = op.into();
            let change =
                change.expect("converting a MetadataOperation::Set to an EntityChange is safe");
            Ok(vec![change])
        }
        _ => {
            // `id` is the primary key of the subgraph_deployment_assignment table,
            // and we can therefore only update no or one entry
            unreachable!()
        }
    }
}

/// Clear the `SubgraphHealth::Failed` status of a subgraph and mark it as
/// healthy or unhealthy depending on whether it also had non-fatal errors
pub fn unfail_deployment(conn: &PgConnection, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
    use diesel::dsl::count;
    use subgraph_deployment as d;
    use subgraph_error as e;
    use SubgraphHealth::*;

    let has_non_fatal_errors = e::table
        .filter(e::subgraph_id.eq(id.as_str()))
        .filter(e::deterministic)
        .select(count(e::id))
        .first::<i64>(conn)?
        > 0;

    let prev_health = if has_non_fatal_errors {
        Unhealthy
    } else {
        Healthy
    };

    // The update does nothing unless the subgraph is in state 'failed'
    update(
        d::table
            .filter(d::id.eq(id.as_str()))
            .filter(d::health.eq(Failed)),
    )
    .set((
        d::failed.eq(false),
        d::health.eq(prev_health),
        d::fatal_error.eq::<Option<String>>(None),
    ))
    .execute(conn)?;
    Ok(())
}
