//! Utilities for dealing with deployment metadata. Any connection passed
//! into these methods must be for the shard that holds the actual
//! deployment data and metadata
use diesel::pg::PgConnection;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl};
use diesel::{
    dsl::{delete, insert_into, select, sql, update},
    sql_types::Integer,
};
use graph::data::subgraph::schema::SubgraphError;
use graph::data::subgraph::{
    schema::{MetadataType, SubgraphManifestEntity},
    SubgraphFeature,
};
use graph::prelude::{
    anyhow, bigdecimal::ToPrimitive, hex, web3::types::H256, BigDecimal, BlockNumber,
    DeploymentState, EntityChange, EntityChangeOperation, EthereumBlockPointer, Schema, StoreError,
    StoreEvent, SubgraphDeploymentId,
};
use stable_hash::crypto::SetHasher;
use std::str::FromStr;
use std::{collections::BTreeSet, convert::TryFrom, ops::Bound};

use crate::block_range::BLOCK_RANGE_COLUMN;
use graph::constraint_violation;

// Diesel tables for some of the metadata
// See also: ed42d219c6704a4aab57ce1ea66698e7
// Changes to the GraphQL schema might require changes to these tables.
// The definitions of the tables can be generated with
//    cargo run -p graph-store-postgres --example layout -- \
//      -g diesel store/postgres/src/subgraphs.graphql subgraphs
#[derive(DbEnum, Debug, Clone, Copy)]
pub enum SubgraphHealth {
    Failed,
    Healthy,
    Unhealthy,
}

impl From<SubgraphHealth> for graph::data::subgraph::schema::SubgraphHealth {
    fn from(health: SubgraphHealth) -> Self {
        use graph::data::subgraph::schema::SubgraphHealth as H;
        use SubgraphHealth as Db;

        match health {
            Db::Failed => H::Failed,
            Db::Healthy => H::Healthy,
            Db::Unhealthy => H::Unhealthy,
        }
    }
}

table! {
    subgraphs.subgraph_deployment (vid) {
        vid -> BigInt,
        id -> Text,
        manifest -> Text,
        failed -> Bool,
        health -> crate::deployment::SubgraphHealthMapping,
        synced -> Bool,
        fatal_error -> Nullable<Text>,
        non_fatal_errors -> Array<Text>,
        earliest_ethereum_block_hash -> Nullable<Binary>,
        earliest_ethereum_block_number -> Nullable<Numeric>,
        latest_ethereum_block_hash -> Nullable<Binary>,
        latest_ethereum_block_number -> Nullable<Numeric>,
        last_healthy_ethereum_block_hash -> Nullable<Binary>,
        last_healthy_ethereum_block_number -> Nullable<Numeric>,
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
    subgraphs.subgraph_error (vid) {
        vid -> BigInt,
        id -> Text,
        subgraph_id -> Text,
        message -> Text,
        block_hash -> Nullable<Binary>,
        handler -> Nullable<Text>,
        deterministic -> Bool,
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
        features -> Array<Text>,
        schema -> Text,
        block_range -> Range<Integer>,
    }
}

allow_tables_to_appear_in_same_query!(subgraph_deployment, subgraph_error);

/// Look up the graft point for the given subgraph in the database and
/// return it. If `pending_only` is `true`, only return `Some(_)` if the
/// deployment has not progressed past the graft point, i.e., data has not
/// been copied for the graft
fn graft(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    pending_only: bool,
) -> Result<Option<(SubgraphDeploymentId, EthereumBlockPointer)>, StoreError> {
    use subgraph_deployment as sd;

    let graft_query = sd::table
        .select((sd::graft_base, sd::graft_block_hash, sd::graft_block_number))
        .filter(sd::id.eq(id.as_str()));
    // The name of the base subgraph, the hash, and block number
    let graft: (Option<String>, Option<Vec<u8>>, Option<BigDecimal>) = if pending_only {
        graft_query
            .filter(sd::graft_block_number.ge(sql("coalesce(latest_ethereum_block_number, 0)")))
            .first(conn)
            .optional()?
            .unwrap_or((None, None, None))
    } else {
        graft_query
            .first(conn)
            .optional()?
            .unwrap_or((None, None, None))
    };
    match graft {
        (None, None, None) => Ok(None),
        (Some(subgraph), Some(hash), Some(block)) => {
            let hash = H256::from_slice(hash.as_slice());
            let block = block.to_u64().expect("block numbers fit into a u64");
            let subgraph = SubgraphDeploymentId::new(subgraph.clone()).map_err(|_| {
                StoreError::Unknown(anyhow!(
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

/// Look up the graft point for the given subgraph in the database and
/// return it. Returns `None` if the deployment does not have
/// a graft or if the subgraph has already progress past the graft point,
/// indicating that the data copying for grafting has been performed
pub fn graft_pending(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<Option<(SubgraphDeploymentId, EthereumBlockPointer)>, StoreError> {
    graft(conn, id, true)
}

/// Look up the graft point for the given subgraph in the database and
/// return it. Returns `None` if the deployment does not have
/// a graft
pub fn graft_point(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<Option<(SubgraphDeploymentId, EthereumBlockPointer)>, StoreError> {
    graft(conn, id, false)
}

pub fn schema(conn: &PgConnection, id: SubgraphDeploymentId) -> Result<Schema, StoreError> {
    use subgraph_manifest as sm;
    let manifest_id = SubgraphManifestEntity::id(&id);
    let s: String = sm::table
        .select(sm::schema)
        .filter(sm::id.eq(manifest_id.as_str()))
        .first(conn)?;
    Schema::parse(s.as_str(), id).map_err(|e| StoreError::Unknown(e))
}

pub fn manifest_info(
    conn: &PgConnection,
    id: SubgraphDeploymentId,
) -> Result<(Schema, Option<String>, Option<String>), StoreError> {
    use subgraph_manifest as sm;
    let manifest_id = SubgraphManifestEntity::id(&id);
    let (s, description, repository): (String, Option<String>, Option<String>) = sm::table
        .select((sm::schema, sm::description, sm::repository))
        .filter(sm::id.eq(manifest_id.as_str()))
        .first(conn)?;
    Schema::parse(s.as_str(), id)
        .map_err(|e| StoreError::Unknown(e))
        .map(|schema| (schema, description, repository))
}

pub fn features(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<BTreeSet<SubgraphFeature>, StoreError> {
    use subgraph_manifest as sm;

    let manifest_id = SubgraphManifestEntity::id(&id);
    let features: Vec<String> = sm::table
        .select(sm::features)
        .filter(sm::id.eq(manifest_id.as_str()))
        .first(conn)
        .unwrap();
    features
        .iter()
        .map(|f| SubgraphFeature::from_str(f).map_err(StoreError::from))
        .collect()
}

fn block_ptr_store_event(id: &SubgraphDeploymentId) -> StoreEvent {
    let change = EntityChange {
        entity_type: MetadataType::SubgraphDeployment.into(),
        entity_id: id.to_string(),
        subgraph_id: id.to_owned(),
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

pub fn block_ptr(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<Option<EthereumBlockPointer>, StoreError> {
    use subgraph_deployment as d;

    let (number, hash) = d::table
        .filter(d::id.eq(id.as_str()))
        .select((
            d::latest_ethereum_block_number,
            d::latest_ethereum_block_hash,
        ))
        .first::<(Option<BigDecimal>, Option<Vec<u8>>)>(conn)?;

    let ptr = crate::detail::block(id.as_str(), "latest_ethereum_block", hash, number)?
        .map(|block| block.to_ptr());
    Ok(ptr)
}

fn convert_to_u32(number: Option<i32>, field: &str, subgraph: &str) -> Result<u32, StoreError> {
    number
        .ok_or_else(|| constraint_violation!("missing {} for subgraph `{}`", field, subgraph))
        .and_then(|number| {
            u32::try_from(number).map_err(|_| {
                constraint_violation!(
                    "invalid value {:?} for {} in subgraph {}",
                    number,
                    field,
                    subgraph
                )
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
            constraint_violation!(
                "Subgraph `{}` has an \
                 invalid latest_ethereum_block_number `{:?}` that can not be \
                 represented as an i32",
                subgraph,
                latest
            )
        }),
    }
}

pub fn state(conn: &PgConnection, id: SubgraphDeploymentId) -> Result<DeploymentState, StoreError> {
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

/// Mark the deployment `id` as synced
pub fn set_synced(conn: &PgConnection, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    update(
        d::table
            .filter(d::id.eq(id.as_str()))
            .filter(d::synced.eq(false)),
    )
    .set(d::synced.eq(true))
    .execute(conn)?;
    Ok(())
}

/// Returns `true` if the deployment `id` exists
pub fn exists(conn: &PgConnection, id: &str) -> Result<bool, StoreError> {
    use subgraph_deployment as d;

    let exists = d::table
        .filter(d::id.eq(id))
        .count()
        .get_result::<i64>(conn)?
        > 0;
    Ok(exists)
}

/// Returns `true` if the deployment `id` exists and is synced
pub fn exists_and_synced(conn: &PgConnection, id: &str) -> Result<bool, StoreError> {
    use subgraph_deployment as d;

    let synced = d::table
        .filter(d::id.eq(id))
        .select(d::synced)
        .first(conn)
        .optional()?
        .unwrap_or(false);
    Ok(synced)
}

// Does nothing if the error already exists. Returns the error id.
fn insert_subgraph_error(conn: &PgConnection, error: SubgraphError) -> anyhow::Result<String> {
    use subgraph_error as e;

    let error_id = hex::encode(&stable_hash::utils::stable_hash::<SetHasher, _>(&error));
    let SubgraphError {
        subgraph_id,
        message,
        handler,
        block_ptr,
        deterministic,
    } = error;

    let block_num = match block_ptr {
        None => {
            assert_eq!(deterministic, false);
            crate::block_range::BLOCK_UNVERSIONED
        }
        Some(block) => crate::block_range::block_number(&block),
    };

    insert_into(e::table)
        .values((
            e::id.eq(&error_id),
            e::subgraph_id.eq(subgraph_id.as_str()),
            e::message.eq(message),
            e::handler.eq(handler),
            e::deterministic.eq(deterministic),
            e::block_hash.eq(block_ptr.as_ref().map(|ptr| ptr.hash.as_bytes())),
            e::block_range.eq((Bound::Included(block_num), Bound::Unbounded)),
        ))
        .on_conflict_do_nothing()
        .execute(conn)?;

    Ok(error_id)
}

pub fn fail(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    error: SubgraphError,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    let error_id = insert_subgraph_error(conn, error)?;
    update(d::table.filter(d::id.eq(id.as_str())))
        .set((
            d::failed.eq(true),
            d::health.eq(SubgraphHealth::Failed),
            d::fatal_error.eq(Some(error_id)),
        ))
        .execute(conn)?;
    Ok(())
}

/// If `block` is `None`, assumes the latest block.
pub(crate) fn has_non_fatal_errors(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    block: Option<BlockNumber>,
) -> Result<bool, StoreError> {
    use subgraph_deployment as d;
    use subgraph_error as e;

    let block = match block {
        Some(block) => d::table.select(sql(&block.to_string())).into_boxed(),
        None => d::table
            .filter(d::id.eq(id.as_str()))
            .select(d::latest_ethereum_block_number)
            .into_boxed(),
    };

    select(diesel::dsl::exists(
        e::table
            .filter(e::subgraph_id.eq(id.as_str()))
            .filter(e::deterministic)
            .filter(
                sql("block_range @> ")
                    .bind(block.single_value())
                    .sql("::int"),
            ),
    ))
    .get_result(conn)
    .map_err(|e| e.into())
}

/// Clear the `SubgraphHealth::Failed` status of a subgraph and mark it as
/// healthy or unhealthy depending on whether it also had non-fatal errors
pub fn unfail(conn: &PgConnection, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
    use subgraph_deployment as d;
    use subgraph_error as e;
    use SubgraphHealth::*;

    let prev_health = if has_non_fatal_errors(conn, id, None)? {
        Unhealthy
    } else {
        Healthy
    };

    let fatal_error_id = match d::table
        .filter(d::id.eq(id.as_str()))
        .select(d::fatal_error)
        .get_result::<Option<String>>(conn)?
    {
        Some(fatal_error_id) => fatal_error_id,

        // If the subgraph is not failed then there is nothing to do.
        None => return Ok(()),
    };

    // Unfail the deployment.
    update(d::table.filter(d::id.eq(id.as_str())))
        .set((
            d::failed.eq(false),
            d::health.eq(prev_health),
            d::fatal_error.eq::<Option<String>>(None),
        ))
        .execute(conn)?;

    // Delete the fatal error.
    delete(e::table.filter(e::id.eq(fatal_error_id))).execute(conn)?;

    Ok(())
}

/// Insert the errors and check if the subgraph needs to be set as unhealthy.
pub(crate) fn insert_subgraph_errors(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    deterministic_errors: Vec<SubgraphError>,
    block: BlockNumber,
) -> Result<(), StoreError> {
    for error in deterministic_errors {
        insert_subgraph_error(conn, error)?;
    }

    check_health(conn, id, block)
}

#[cfg(debug_assertions)]
pub(crate) fn error_count(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
) -> Result<usize, StoreError> {
    use subgraph_error as e;

    Ok(e::table
        .filter(e::subgraph_id.eq(id.as_str()))
        .count()
        .get_result::<i64>(conn)? as usize)
}

/// Checks if the subgraph is healthy or unhealthy as of the given block, or the subgraph latest
/// block if `None`, based on the presence of non-fatal errors. Has no effect on failed subgraphs.
fn check_health(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    block: BlockNumber,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    let has_errors = has_non_fatal_errors(conn, id, Some(block))?;

    let (new, old) = match has_errors {
        true => (SubgraphHealth::Unhealthy, SubgraphHealth::Healthy),
        false => (SubgraphHealth::Healthy, SubgraphHealth::Unhealthy),
    };

    update(
        d::table
            .filter(d::id.eq(id.as_str()))
            .filter(d::health.eq(old)),
    )
    .set(d::health.eq(new))
    .execute(conn)
    .map(|_| ())
    .map_err(|e| e.into())
}

/// Reverts the errors and updates the subgraph health if necessary.
pub(crate) fn revert_subgraph_errors(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    reverted_block: BlockNumber,
) -> Result<(), StoreError> {
    use subgraph_error as e;

    let lower_geq = format!("lower({}) >= ", BLOCK_RANGE_COLUMN);
    delete(
        e::table
            .filter(e::subgraph_id.eq(id.as_str()))
            .filter(sql(&lower_geq).bind::<Integer, _>(reverted_block)),
    )
    .execute(conn)?;

    // The result will be the same at `reverted_block` or `reverted_block - 1` since the errors at
    // `reverted_block` were just deleted, but semantically we care about `reverted_block - 1` which
    // is the block being reverted to.
    check_health(conn, id, reverted_block - 1)
}

/// Drop the schema `namespace`. This deletes all data for the subgraph,
/// and can not be reversed. It does not remove any of the metadata
/// in the `subgraphs` schema for the deployment
pub fn drop_schema(
    conn: &diesel::pg::PgConnection,
    namespace: &crate::primary::Namespace,
) -> Result<(), StoreError> {
    use diesel::connection::SimpleConnection;

    let query = format!("drop schema if exists {} cascade", namespace);
    Ok(conn.batch_execute(&*query)?)
}
