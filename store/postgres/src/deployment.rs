//! Utilities for dealing with deployment metadata. Any connection passed
//! into these methods must be for the shard that holds the actual
//! deployment data and metadata
use diesel::{
    connection::SimpleConnection,
    dsl::{count, delete, insert_into, select, sql, update},
    sql_types::Integer,
};
use diesel::{expression::SqlLiteral, pg::PgConnection, sql_types::Numeric};
use diesel::{
    prelude::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl},
    sql_query,
    sql_types::{Nullable, Text},
};
use graph::data::subgraph::{schema::SubgraphManifestEntity, SubgraphFeature};
use graph::prelude::{
    anyhow, bigdecimal::ToPrimitive, hex, web3::types::H256, BigDecimal, BlockNumber, BlockPtr,
    DeploymentHash, DeploymentState, Schema, StoreError,
};
use graph::{data::subgraph::schema::SubgraphError, prelude::SubgraphDeploymentEntity};
use stable_hash::crypto::SetHasher;
use std::str::FromStr;
use std::{collections::BTreeSet, convert::TryFrom, ops::Bound};

use crate::connection_pool::ForeignServer;
use crate::{block_range::BLOCK_RANGE_COLUMN, primary::Site};
use graph::constraint_violation;

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
    subgraphs.subgraph_deployment (id) {
        id -> Integer,
        deployment -> Text,
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
    subgraphs.subgraph_manifest {
        id -> Integer,
        spec_version -> Text,
        description -> Nullable<Text>,
        repository -> Nullable<Text>,
        features -> Array<Text>,
        schema -> Text,
    }
}

allow_tables_to_appear_in_same_query!(subgraph_deployment, subgraph_error);

/// Look up the graft point for the given subgraph in the database and
/// return it. If `pending_only` is `true`, only return `Some(_)` if the
/// deployment has not progressed past the graft point, i.e., data has not
/// been copied for the graft
fn graft(
    conn: &PgConnection,
    id: &DeploymentHash,
    pending_only: bool,
) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
    use subgraph_deployment as sd;

    let graft_query = sd::table
        .select((sd::graft_base, sd::graft_block_hash, sd::graft_block_number))
        .filter(sd::deployment.eq(id.as_str()));
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
            let subgraph = DeploymentHash::new(subgraph.clone()).map_err(|_| {
                StoreError::Unknown(anyhow!(
                    "the base subgraph for a graft must be a valid subgraph id but is `{}`",
                    subgraph
                ))
            })?;
            Ok(Some((subgraph, BlockPtr::from((hash, block)))))
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
    id: &DeploymentHash,
) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
    graft(conn, id, true)
}

/// Look up the graft point for the given subgraph in the database and
/// return it. Returns `None` if the deployment does not have
/// a graft
pub fn graft_point(
    conn: &PgConnection,
    id: &DeploymentHash,
) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
    graft(conn, id, false)
}

pub fn schema(conn: &PgConnection, site: &Site) -> Result<Schema, StoreError> {
    use subgraph_manifest as sm;
    let s: String = sm::table
        .select(sm::schema)
        .filter(sm::id.eq(site.id))
        .first(conn)?;
    Schema::parse(s.as_str(), site.deployment.clone()).map_err(|e| StoreError::Unknown(e))
}

pub fn manifest_info(
    conn: &PgConnection,
    site: &Site,
) -> Result<(Schema, Option<String>, Option<String>), StoreError> {
    use subgraph_manifest as sm;
    let (s, description, repository): (String, Option<String>, Option<String>) = sm::table
        .select((sm::schema, sm::description, sm::repository))
        .filter(sm::id.eq(site.id))
        .first(conn)?;
    Schema::parse(s.as_str(), site.deployment.clone())
        .map_err(|e| StoreError::Unknown(e))
        .map(|schema| (schema, description, repository))
}

pub fn features(conn: &PgConnection, site: &Site) -> Result<BTreeSet<SubgraphFeature>, StoreError> {
    use subgraph_manifest as sm;

    let features: Vec<String> = sm::table
        .select(sm::features)
        .filter(sm::id.eq(site.id))
        .first(conn)
        .unwrap();
    features
        .iter()
        .map(|f| SubgraphFeature::from_str(f).map_err(StoreError::from))
        .collect()
}

pub fn forward_block_ptr(
    conn: &PgConnection,
    id: &DeploymentHash,
    ptr: BlockPtr,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    // Work around a Diesel issue with serializing BigDecimals to numeric
    let number = format!("{}::numeric", ptr.number);

    update(d::table.filter(d::deployment.eq(id.as_str())))
        .set((
            d::latest_ethereum_block_number.eq(sql(&number)),
            d::latest_ethereum_block_hash.eq(ptr.hash_slice()),
            d::current_reorg_depth.eq(0),
        ))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.into())
}

pub fn revert_block_ptr(
    conn: &PgConnection,
    id: &DeploymentHash,
    ptr: BlockPtr,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    // Work around a Diesel issue with serializing BigDecimals to numeric
    let number = format!("{}::numeric", ptr.number);

    update(d::table.filter(d::deployment.eq(id.as_str())))
        .set((
            d::latest_ethereum_block_number.eq(sql(&number)),
            d::latest_ethereum_block_hash.eq(ptr.hash_slice()),
            d::reorg_count.eq(d::reorg_count + 1),
            d::current_reorg_depth.eq(d::current_reorg_depth + 1),
            d::max_reorg_depth.eq(sql("greatest(current_reorg_depth + 1, max_reorg_depth)")),
        ))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.into())
}

pub fn block_ptr(conn: &PgConnection, id: &DeploymentHash) -> Result<Option<BlockPtr>, StoreError> {
    use subgraph_deployment as d;

    let (number, hash) = d::table
        .filter(d::deployment.eq(id.as_str()))
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

pub fn state(conn: &PgConnection, id: DeploymentHash) -> Result<DeploymentState, StoreError> {
    use subgraph_deployment as d;

    match d::table
        .filter(d::deployment.eq(id.as_str()))
        .select((
            d::deployment,
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
pub fn set_synced(conn: &PgConnection, id: &DeploymentHash) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    update(
        d::table
            .filter(d::deployment.eq(id.as_str()))
            .filter(d::synced.eq(false)),
    )
    .set(d::synced.eq(true))
    .execute(conn)?;
    Ok(())
}

/// Returns `true` if the deployment (as identified by `site.id`)
pub fn exists(conn: &PgConnection, site: &Site) -> Result<bool, StoreError> {
    use subgraph_deployment as d;

    let exists = d::table
        .filter(d::id.eq(site.id))
        .count()
        .get_result::<i64>(conn)?
        > 0;
    Ok(exists)
}

/// Returns `true` if the deployment `id` exists and is synced
pub fn exists_and_synced(conn: &PgConnection, id: &str) -> Result<bool, StoreError> {
    use subgraph_deployment as d;

    let synced = d::table
        .filter(d::deployment.eq(id))
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

    let block_num = match &block_ptr {
        None => {
            assert_eq!(deterministic, false);
            crate::block_range::BLOCK_UNVERSIONED
        }
        Some(block) => crate::block_range::block_number(block),
    };

    insert_into(e::table)
        .values((
            e::id.eq(&error_id),
            e::subgraph_id.eq(subgraph_id.as_str()),
            e::message.eq(message),
            e::handler.eq(handler),
            e::deterministic.eq(deterministic),
            e::block_hash.eq(block_ptr.as_ref().map(|ptr| ptr.hash_slice())),
            e::block_range.eq((Bound::Included(block_num), Bound::Unbounded)),
        ))
        .on_conflict_do_nothing()
        .execute(conn)?;

    Ok(error_id)
}

pub fn fail(
    conn: &PgConnection,
    id: &DeploymentHash,
    error: SubgraphError,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    let error_id = insert_subgraph_error(conn, error)?;
    update(d::table.filter(d::deployment.eq(id.as_str())))
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
    id: &DeploymentHash,
    block: Option<BlockNumber>,
) -> Result<bool, StoreError> {
    use subgraph_deployment as d;
    use subgraph_error as e;

    let block = match block {
        Some(block) => d::table.select(sql(&block.to_string())).into_boxed(),
        None => d::table
            .filter(d::deployment.eq(id.as_str()))
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
pub fn unfail(conn: &PgConnection, id: &DeploymentHash) -> Result<(), StoreError> {
    use subgraph_deployment as d;
    use subgraph_error as e;
    use SubgraphHealth::*;

    let prev_health = if has_non_fatal_errors(conn, id, None)? {
        Unhealthy
    } else {
        Healthy
    };

    let fatal_error_id = match d::table
        .filter(d::deployment.eq(id.as_str()))
        .select(d::fatal_error)
        .get_result::<Option<String>>(conn)?
    {
        Some(fatal_error_id) => fatal_error_id,

        // If the subgraph is not failed then there is nothing to do.
        None => return Ok(()),
    };

    // Unfail the deployment.
    update(d::table.filter(d::deployment.eq(id.as_str())))
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
    id: &DeploymentHash,
    deterministic_errors: Vec<SubgraphError>,
    block: BlockNumber,
) -> Result<(), StoreError> {
    for error in deterministic_errors {
        insert_subgraph_error(conn, error)?;
    }

    check_health(conn, id, block)
}

#[cfg(debug_assertions)]
pub(crate) fn error_count(conn: &PgConnection, id: &DeploymentHash) -> Result<usize, StoreError> {
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
    id: &DeploymentHash,
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
            .filter(d::deployment.eq(id.as_str()))
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
    id: &DeploymentHash,
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

/// Copy the dynamic data sources for `src` to `dst`. All data sources that
/// were created up to and including `target_block` will be copied.
pub(crate) fn copy_errors(
    conn: &PgConnection,
    src: &Site,
    dst: &Site,
    target_block: &BlockPtr,
) -> Result<usize, StoreError> {
    use subgraph_error as e;

    let src_nsp = if src.shard == dst.shard {
        "subgraphs".to_string()
    } else {
        ForeignServer::metadata_schema(&src.shard)
    };

    // Check whether there are any errors for dst which indicates we already
    // did copy
    let count = e::table
        .filter(e::subgraph_id.eq(dst.deployment.as_str()))
        .select(count(e::vid))
        .get_result::<i64>(conn)?;
    if count > 0 {
        return Ok(count as usize);
    }

    // We calculate a new id since the subgraph_error table has an exclusion
    // constraint on (id, block_range) and we need to make sure that newly
    // inserted errors do not collide with existing ones. For new subgraph
    // errors, we use a stable hash; for copied ones we just use an MD5.
    //
    // Longer term, we should get rid of the id column, since it is only
    // needed to deduplicate error messages, which would be better achieved
    // with a unique index
    let query = format!(
        "\
      insert into subgraphs.subgraph_error(id,
             subgraph_id, message, block_hash, handler, deterministic, block_range)
      select md5($2 || e.message || coalesce(e.block_hash, 'nohash') || coalesce(e.handler, 'nohandler') || e.deterministic) as id,
             $2 as subgraph_id, e.message, e.block_hash,
             e.handler, e.deterministic, e.block_range
        from {src_nsp}.subgraph_error e
       where e.subgraph_id = $1
         and lower(e.block_range) <= $3",
        src_nsp = src_nsp
    );

    Ok(sql_query(&query)
        .bind::<Text, _>(src.deployment.as_str())
        .bind::<Text, _>(dst.deployment.as_str())
        .bind::<Integer, _>(target_block.number)
        .execute(conn)?)
}

/// Drop the schema `namespace`. This deletes all data for the subgraph,
/// and can not be reversed. It does not remove any of the metadata
/// in the `subgraphs` schema for the deployment
pub fn drop_schema(
    conn: &diesel::pg::PgConnection,
    namespace: &crate::primary::Namespace,
) -> Result<(), StoreError> {
    let query = format!("drop schema if exists {} cascade", namespace);
    Ok(conn.batch_execute(&*query)?)
}

pub fn drop_metadata(conn: &PgConnection, site: &Site) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    // We don't need to delete from subgraph_manifest or subgraph_error
    // since that cascades from deleting the subgraph_deployment
    delete(d::table.filter(d::id.eq(site.id))).execute(conn)?;
    Ok(())
}

pub fn create_deployment(
    conn: &PgConnection,
    site: &Site,
    deployment: SubgraphDeploymentEntity,
    exists: bool,
    replace: bool,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;
    use subgraph_manifest as m;

    fn b(ptr: &Option<BlockPtr>) -> Option<&[u8]> {
        ptr.as_ref().map(|ptr| ptr.hash_slice())
    }

    fn n(ptr: &Option<BlockPtr>) -> SqlLiteral<Nullable<Numeric>> {
        match ptr {
            None => sql("null"),
            Some(ptr) => sql(&format!("{}::numeric", ptr.number)),
        }
    }

    let SubgraphDeploymentEntity {
        manifest:
            SubgraphManifestEntity {
                spec_version,
                description,
                repository,
                features,
                schema,
            },
        failed,
        health: _,
        synced,
        fatal_error: _,
        non_fatal_errors: _,
        earliest_block,
        latest_block,
        graft_base,
        graft_block,
        reorg_count: _,
        current_reorg_depth: _,
        max_reorg_depth: _,
    } = deployment;

    let deployment_values = (
        d::id.eq(site.id),
        d::deployment.eq(site.deployment.as_str()),
        d::failed.eq(failed),
        d::synced.eq(synced),
        d::health.eq(SubgraphHealth::Healthy),
        d::fatal_error.eq::<Option<String>>(None),
        d::non_fatal_errors.eq::<Vec<String>>(vec![]),
        d::earliest_ethereum_block_hash.eq(b(&earliest_block)),
        d::earliest_ethereum_block_number.eq(n(&earliest_block)),
        d::latest_ethereum_block_hash.eq(b(&latest_block)),
        d::latest_ethereum_block_number.eq(n(&latest_block)),
        d::entity_count.eq(sql("0")),
        d::graft_base.eq(graft_base.as_ref().map(|s| s.as_str())),
        d::graft_block_hash.eq(b(&graft_block)),
        d::graft_block_number.eq(n(&graft_block)),
    );

    let manifest_values = (
        m::id.eq(site.id),
        m::spec_version.eq(spec_version),
        m::description.eq(description),
        m::repository.eq(repository),
        m::features.eq(features),
        m::schema.eq(schema),
    );

    if exists && replace {
        update(d::table.filter(d::deployment.eq(site.deployment.as_str())))
            .set(deployment_values)
            .execute(conn)?;

        update(m::table.filter(m::id.eq(site.id)))
            .set(manifest_values)
            .execute(conn)?;
    } else {
        insert_into(d::table)
            .values(deployment_values)
            .execute(conn)?;

        insert_into(m::table)
            .values(manifest_values)
            .execute(conn)?;
    }
    Ok(())
}

pub fn update_entity_count(
    conn: &PgConnection,
    site: &Site,
    full_count_query: &str,
    count: i32,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    if count == 0 {
        return Ok(());
    }

    // The big complication in this query is how to determine what the
    // new entityCount should be. We want to make sure that if the entityCount
    // is NULL or the special value `-1`, it gets recomputed. Using `-1` here
    // makes it possible to manually set the `entityCount` to that value
    // to force a recount; setting it to `NULL` is not desirable since
    // `entityCount` on the GraphQL level is not nullable, and so setting
    // `entityCount` to `NULL` could cause errors at that layer; temporarily
    // returning `-1` is more palatable. To be exact, recounts have to be
    // done here, from the subgraph writer.
    //
    // The first argument of `coalesce` will be `NULL` if the entity count
    // is `NULL` or `-1`, forcing `coalesce` to evaluate its second
    // argument, the query to count entities. In all other cases,
    // `coalesce` does not evaluate its second argument
    let count_update = format!(
        "coalesce((nullif(entity_count, -1)) + ({count}),
                  ({full_count_query}))",
        full_count_query = full_count_query,
        count = count
    );
    update(d::table.filter(d::id.eq(site.id)))
        .set(d::entity_count.eq(sql(&count_update)))
        .execute(conn)?;
    Ok(())
}

/// Set the deployment's entity count to whatever `full_count_query` produces
pub fn set_entity_count(
    conn: &PgConnection,
    site: &Site,
    full_count_query: &str,
) -> Result<(), StoreError> {
    use subgraph_deployment as d;

    let full_count_query = format!("({})", full_count_query);
    update(d::table.filter(d::id.eq(site.id)))
        .set(d::entity_count.eq(sql(&full_count_query)))
        .execute(conn)?;
    Ok(())
}
