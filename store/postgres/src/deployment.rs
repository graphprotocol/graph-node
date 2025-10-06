//! Utilities for dealing with deployment metadata. Any connection passed
//! into these methods must be for the shard that holds the actual
//! deployment data and metadata
use crate::{advisory_lock, detail::GraphNodeVersion, primary::DeploymentId, AsyncPgConnection};
use diesel::{
    dsl::{count, delete, insert_into, now, select, sql, update},
    sql_types::{Bool, Integer},
};
use diesel::{
    prelude::{ExpressionMethods, OptionalExtension, QueryDsl},
    sql_query,
    sql_types::{Nullable, Text},
};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use graph::{
    blockchain::block_stream::FirehoseCursor,
    data::subgraph::schema::SubgraphError,
    env::ENV_VARS,
    schema::EntityType,
    slog::{debug, Logger},
};
use graph::{components::store::StoreResult, semver::Version};
use graph::{
    data::store::scalar::ToPrimitive,
    prelude::{
        anyhow, hex, web3::types::H256, BlockNumber, BlockPtr, DeploymentHash, DeploymentState,
        StoreError,
    },
    schema::InputSchema,
};
use graph::{
    data::subgraph::schema::{DeploymentCreate, SubgraphManifestEntity},
    util::backoff::ExponentialBackoff,
};
use stable_hash_legacy::crypto::SetHasher;
use std::sync::Arc;
use std::{convert::TryFrom, ops::Bound, time::Duration};

use crate::ForeignServer;
use crate::{block_range::BLOCK_RANGE_COLUMN, primary::Site};
use graph::internal_error;

#[derive(DbEnum, Debug, Clone, Copy)]
#[PgType = "text"]
pub enum SubgraphHealth {
    Failed,
    Healthy,
    Unhealthy,
}

impl SubgraphHealth {
    fn is_failed(&self) -> bool {
        use graph::data::subgraph::schema::SubgraphHealth as H;

        H::from(*self).is_failed()
    }
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

/// Additional behavior for a deployment when it becomes synced
#[derive(Clone, Copy, Debug)]
pub enum OnSync {
    None,
    /// Activate this deployment
    Activate,
    /// Activate this deployment and unassign any other copies of the same
    /// deployment
    Replace,
}

impl TryFrom<Option<&str>> for OnSync {
    type Error = StoreError;

    fn try_from(value: Option<&str>) -> Result<Self, Self::Error> {
        match value {
            None => Ok(OnSync::None),
            Some("activate") => Ok(OnSync::Activate),
            Some("replace") => Ok(OnSync::Replace),
            _ => Err(internal_error!("illegal value for on_sync: {value}")),
        }
    }
}

impl OnSync {
    pub fn activate(&self) -> bool {
        match self {
            OnSync::None => false,
            OnSync::Activate => true,
            OnSync::Replace => true,
        }
    }

    pub fn replace(&self) -> bool {
        match self {
            OnSync::None => false,
            OnSync::Activate => false,
            OnSync::Replace => true,
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            OnSync::None => "none",
            OnSync::Activate => "activate",
            OnSync::Replace => "replace",
        }
    }

    fn to_sql(&self) -> Option<&str> {
        match self {
            OnSync::None => None,
            OnSync::Activate | OnSync::Replace => Some(self.to_str()),
        }
    }
}

table! {
    /// Deployment metadata that changes on every block
    subgraphs.head (id) {
        id -> Integer,
        block_hash -> Nullable<Binary>,
        block_number -> Nullable<Integer>,
        entity_count -> Int8,
        firehose_cursor -> Nullable<Text>,
    }
}

table! {
    /// Deployment metadata that changes less frequently
    subgraphs.deployment (id) {
        id -> Integer,

        /// The IPFS hash of the deployment. We would like to call this
        /// 'deployment', but Diesel doesn't let us have a column with the
        /// same name as the table
        subgraph -> Text,

        earliest_block_number -> Integer,

        health -> crate::deployment::SubgraphHealthMapping,
        failed -> Bool,
        fatal_error -> Nullable<Text>,
        non_fatal_errors -> Array<Text>,

        graft_base -> Nullable<Text>,
        graft_block_hash -> Nullable<Binary>,
        graft_block_number -> Nullable<Integer>,

        reorg_count -> Integer,
        current_reorg_depth -> Integer,
        max_reorg_depth -> Integer,

        last_healthy_ethereum_block_hash -> Nullable<Binary>,
        last_healthy_ethereum_block_number -> Nullable<Integer>,

        debug_fork -> Nullable<Text>,

        synced_at -> Nullable<Timestamptz>,
        synced_at_block_number -> Nullable<Int4>,
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
        graph_node_version_id -> Nullable<Integer>,
        use_bytea_prefix -> Bool,
        /// Parent of the smallest start block from the manifest
        start_block_number -> Nullable<Integer>,
        start_block_hash -> Nullable<Binary>,
        raw_yaml -> Nullable<Text>,

        // Entity types that have a `causality_region` column.
        // Names stored as present in the schema, not in snake case.
        entities_with_causality_region -> Array<Text>,
        on_sync -> Nullable<Text>,
        // How many blocks of history to keep, defaults to `i32::max` for
        // unlimited history
        history_blocks -> Integer,
    }
}

table! {
    subgraphs.graph_node_versions {
        id -> Integer,
        git_commit_hash -> Text,
        git_repository_dirty -> Bool,
        crate_version -> Text,
        major -> Integer,
        minor -> Integer,
        patch -> Integer,
    }
}

allow_tables_to_appear_in_same_query!(subgraph_error, subgraph_manifest, head, deployment);

joinable!(head -> deployment(id));

/// Look up the graft point for the given subgraph in the database and
/// return it. If `pending_only` is `true`, only return `Some(_)` if the
/// deployment has not progressed past the graft point, i.e., data has not
/// been copied for the graft
async fn graft(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
    pending_only: bool,
) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
    use deployment as sd;
    use head as h;

    let graft_query = sd::table
        .select((sd::graft_base, sd::graft_block_hash, sd::graft_block_number))
        .filter(sd::subgraph.eq(id.as_str()));
    // The name of the base subgraph, the hash, and block number
    let graft: (Option<String>, Option<Vec<u8>>, Option<BlockNumber>) = if pending_only {
        graft_query
            .inner_join(h::table)
            .filter(h::block_number.is_null())
            .first(conn)
            .await
            .optional()?
            .unwrap_or((None, None, None))
    } else {
        graft_query
            .first(conn)
            .await
            .optional()?
            .unwrap_or((None, None, None))
    };
    match graft {
        (None, None, None) => Ok(None),
        (Some(subgraph), Some(hash), Some(block)) => {
            // FIXME:
            //
            // workaround for arweave
            let hash = H256::from_slice(&hash.as_slice()[..32]);
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
pub async fn graft_pending(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
    graft(conn, id, true).await
}

/// Look up the graft point for the given subgraph in the database and
/// return it. Returns `None` if the deployment does not have
/// a graft.
pub async fn graft_point(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
    graft(conn, id, false).await
}

/// Look up the debug fork for the given subgraph in the database and
/// return it. Returns `None` if the deployment does not have
/// a debug fork.
pub async fn debug_fork(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
) -> Result<Option<DeploymentHash>, StoreError> {
    use deployment as sd;

    let debug_fork: Option<String> = sd::table
        .select(sd::debug_fork)
        .filter(sd::subgraph.eq(id.as_str()))
        .first(conn)
        .await?;

    match debug_fork {
        Some(fork) => Ok(Some(DeploymentHash::new(fork.clone()).map_err(|_| {
            StoreError::Unknown(anyhow!(
                "the debug fork for a subgraph must be a valid subgraph id but is `{}`",
                fork
            ))
        })?)),
        None => Ok(None),
    }
}

pub async fn schema(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<(InputSchema, bool), StoreError> {
    use subgraph_manifest as sm;
    let (s, spec_ver, use_bytea_prefix) = sm::table
        .select((sm::schema, sm::spec_version, sm::use_bytea_prefix))
        .filter(sm::id.eq(site.id))
        .first::<(String, String, bool)>(conn)
        .await?;
    let spec_version =
        Version::parse(spec_ver.as_str()).map_err(|err| StoreError::Unknown(err.into()))?;
    InputSchema::parse(&spec_version, s.as_str(), site.deployment.clone())
        .map_err(StoreError::Unknown)
        .map(|schema| (schema, use_bytea_prefix))
}

pub struct ManifestInfo {
    pub description: Option<String>,
    pub repository: Option<String>,
    pub spec_version: String,
    pub instrument: bool,
}

impl ManifestInfo {
    pub async fn load(
        conn: &mut AsyncPgConnection,
        site: &Site,
    ) -> Result<ManifestInfo, StoreError> {
        use subgraph_manifest as sm;
        let (description, repository, spec_version, features): (
            Option<String>,
            Option<String>,
            String,
            Vec<String>,
        ) = sm::table
            .select((
                sm::description,
                sm::repository,
                sm::spec_version,
                sm::features,
            ))
            .filter(sm::id.eq(site.id))
            .first(conn)
            .await?;

        // Using the features field to store the instrument flag is a bit
        // backhanded, but since this will be used very rarely, should not
        // cause any headaches
        let instrument = features.iter().any(|s| s == "instrument");

        Ok(ManifestInfo {
            description,
            repository,
            spec_version,
            instrument,
        })
    }
}

// Return how many blocks of history this subgraph should keep
pub async fn history_blocks(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<BlockNumber, StoreError> {
    use subgraph_manifest as sm;
    sm::table
        .select(sm::history_blocks)
        .filter(sm::id.eq(site.id))
        .first::<BlockNumber>(conn)
        .await
        .map_err(StoreError::from)
}

pub async fn set_history_blocks(
    conn: &mut AsyncPgConnection,
    site: &Site,
    history_blocks: BlockNumber,
) -> Result<(), StoreError> {
    use subgraph_manifest as sm;

    update(sm::table.filter(sm::id.eq(site.id)))
        .set(sm::history_blocks.eq(history_blocks))
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(StoreError::from)
}

/// This migrates subgraphs that existed before the raw_yaml column was added.
pub async fn set_manifest_raw_yaml(
    conn: &mut AsyncPgConnection,
    site: &Site,
    raw_yaml: &str,
) -> Result<(), StoreError> {
    use subgraph_manifest as sm;

    update(sm::table.filter(sm::id.eq(site.id)))
        .filter(sm::raw_yaml.is_null())
        .set(sm::raw_yaml.eq(raw_yaml))
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(|e| e.into())
}

/// Most of the time, this will be a noop; the only time we actually modify
/// the deployment table is the first forward block after a reorg
async fn reset_reorg_count(conn: &mut AsyncPgConnection, site: &Site) -> StoreResult<()> {
    use deployment as d;

    update(d::table.filter(d::id.eq(site.id)))
        .filter(d::current_reorg_depth.gt(0))
        .set(d::current_reorg_depth.eq(0))
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn transact_block(
    conn: &mut AsyncPgConnection,
    site: &Site,
    ptr: &BlockPtr,
    firehose_cursor: &FirehoseCursor,
    count: i32,
) -> Result<BlockNumber, StoreError> {
    use deployment as d;
    use head as h;

    let count_sql = entity_count_sql(count);

    // Sanity check: The processing direction is forward.
    //
    // Performance note: This costs us an extra DB query on every update. We used to put this in the
    // `where` clause of the `update` statement, but that caused Postgres to use bitmap scans instead
    // of a simple primary key lookup. So a separate query it is.
    let block_ptr = block_ptr(conn, &site).await?;
    if let Some(block_ptr_from) = block_ptr {
        if block_ptr_from.number >= ptr.number {
            return Err(StoreError::DuplicateBlockProcessing(
                site.deployment.clone(),
                ptr.number,
            ));
        }
    }

    reset_reorg_count(conn, site).await?;

    let rows = update(h::table.filter(h::id.eq(site.id)))
        .set((
            h::block_number.eq(ptr.number),
            h::block_hash.eq(ptr.hash_slice()),
            h::firehose_cursor.eq(firehose_cursor.as_ref()),
            h::entity_count.eq(sql(&count_sql)),
        ))
        .execute(conn)
        .await
        .map_err(StoreError::from)?;

    match rows {
        // Common case: A single row was updated.
        1 => {
            // It's not strictly necessary to load the earliest block every
            // time this method is called; if these queries slow things down
            // too much, we should cache the earliest block number since it
            // is only needed to determine whether a pruning run should be
            // kicked off
            d::table
                .filter(d::id.eq(site.id))
                .select(d::earliest_block_number)
                .get_result::<BlockNumber>(conn)
                .await
                .map_err(StoreError::from)
        }

        // No matching rows were found. This is logically impossible, as the `block_ptr` would have
        // caught a non-existing deployment.
        0 => Err(StoreError::Unknown(anyhow!(
            "unknown error forwarding block ptr"
        ))),

        // More than one matching row was found.
        _ => Err(StoreError::InternalError(
            "duplicate deployments in shard".to_owned(),
        )),
    }
}

pub async fn forward_block_ptr(
    conn: &mut AsyncPgConnection,
    site: &Site,
    ptr: &BlockPtr,
) -> Result<(), StoreError> {
    use crate::diesel::BoolExpressionMethods;
    use head as h;

    reset_reorg_count(conn, site).await?;

    let row_count = update(h::table.filter(h::id.eq(site.id)).filter(
        // Asserts that the processing direction is forward.
        h::block_number.lt(ptr.number).or(h::block_number.is_null()),
    ))
    .set((
        h::block_number.eq(ptr.number),
        h::block_hash.eq(ptr.hash_slice()),
    ))
    .execute(conn)
    .await
    .map_err(StoreError::from)?;

    match row_count {
        // Common case: A single row was updated.
        1 => Ok(()),

        // No matching rows were found. This is an error. By the filter conditions, this can only be
        // due to a missing deployment (which `block_ptr` catches) or duplicate block processing.
        0 => match block_ptr(conn, &site).await? {
            Some(block_ptr_from) if block_ptr_from.number >= ptr.number => Err(
                StoreError::DuplicateBlockProcessing(site.deployment.clone(), ptr.number),
            ),
            None | Some(_) => Err(StoreError::Unknown(anyhow!(
                "unknown error forwarding block ptr"
            ))),
        },

        // More than one matching row was found.
        _ => Err(StoreError::InternalError(
            "duplicate deployments in shard".to_owned(),
        )),
    }
}

pub async fn get_subgraph_firehose_cursor(
    conn: &mut AsyncPgConnection,
    site: Arc<Site>,
) -> Result<Option<String>, StoreError> {
    use head as h;

    let res = h::table
        .filter(h::id.eq(site.id))
        .select(h::firehose_cursor)
        .first::<Option<String>>(conn)
        .await
        .map_err(StoreError::from);
    res
}

pub async fn revert_block_ptr(
    conn: &mut AsyncPgConnection,
    site: &Site,
    ptr: BlockPtr,
    firehose_cursor: &FirehoseCursor,
) -> Result<(), StoreError> {
    use deployment as d;
    use head as h;

    // Intention is to revert to a block lower than the reorg threshold, on the other
    // hand the earliest we can possibly go is genesys block, so go to genesys even
    // if it's within the reorg threshold.
    let earliest_block = i32::max(ptr.number - ENV_VARS.reorg_threshold(), 0);
    let affected_rows = update(
        d::table
            .filter(d::id.eq(site.id))
            .filter(d::earliest_block_number.le(earliest_block)),
    )
    .set((
        d::reorg_count.eq(d::reorg_count + 1),
        d::current_reorg_depth.eq(d::current_reorg_depth + 1),
        d::max_reorg_depth.eq(sql("greatest(current_reorg_depth + 1, max_reorg_depth)")),
    ))
    .execute(conn)
    .await?;

    update(h::table.filter(h::id.eq(site.id)))
        .set((
            h::block_number.eq(ptr.number),
            h::block_hash.eq(ptr.hash_slice()),
            h::firehose_cursor.eq(firehose_cursor.as_ref()),
        ))
        .execute(conn)
        .await?;

    match affected_rows {
        1 => Ok(()),
        0 => Err(StoreError::Unknown(anyhow!(
            "No rows affected. This could be due to an attempt to revert beyond earliest_block + reorg_threshold",
        ))),
        _ => Err(StoreError::Unknown(anyhow!(
            "Expected to update 1 row, but {} rows were affected",
            affected_rows
        ))),
    }
}

pub async fn block_ptr(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<Option<BlockPtr>, StoreError> {
    use head as h;

    let (number, hash) = h::table
        .filter(h::id.eq(site.id))
        .select((h::block_number, h::block_hash))
        .first::<(Option<BlockNumber>, Option<Vec<u8>>)>(conn)
        .await
        .map_err(|e| match e {
            diesel::result::Error::NotFound => {
                StoreError::DeploymentNotFound(site.deployment.to_string())
            }
            e => e.into(),
        })?;

    let ptr = crate::detail::block(
        site.deployment.as_str(),
        "latest_ethereum_block",
        hash,
        number,
    )?
    .map(|block| block.to_ptr());
    Ok(ptr)
}

/// Initialize the subgraph's block pointer. If the block pointer in
/// `latest_ethereum_block` is set already, do nothing. If it is still
/// `null`, set it to `start_ethereum_block` from `subgraph_manifest`
pub async fn initialize_block_ptr(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<(), StoreError> {
    use head as h;
    use subgraph_manifest as m;

    let needs_init = h::table
        .filter(h::id.eq(site.id))
        .select(h::block_hash)
        .first::<Option<Vec<u8>>>(conn).await
        .map_err(|e| {
            internal_error!(
                "deployment sgd{} must have been created before calling initialize_block_ptr but we got {}",
                site.id, e
            )
        })?
        .is_none();

    if needs_init {
        if let (Some(hash), Some(number)) = m::table
            .filter(m::id.eq(site.id))
            .select((m::start_block_hash, m::start_block_number))
            .first::<(Option<Vec<u8>>, Option<BlockNumber>)>(conn)
            .await?
        {
            update(h::table.filter(h::id.eq(site.id)))
                .set((h::block_hash.eq(&hash), h::block_number.eq(number)))
                .execute(conn)
                .await
                .map(|_| ())
                .map_err(|e| e.into())
        } else {
            Ok(())
        }
    } else {
        Ok(())
    }
}

fn convert_to_u32(number: Option<i32>, field: &str, subgraph: &str) -> Result<u32, StoreError> {
    number
        .ok_or_else(|| internal_error!("missing {} for subgraph `{}`", field, subgraph))
        .and_then(|number| {
            u32::try_from(number).map_err(|_| {
                internal_error!(
                    "invalid value {:?} for {} in subgraph {}",
                    number,
                    field,
                    subgraph
                )
            })
        })
}

pub async fn state(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<DeploymentState, StoreError> {
    use deployment as d;
    use head as h;
    use subgraph_error as e;

    match d::table
        .inner_join(h::table)
        .filter(d::id.eq(site.id))
        .select((
            d::subgraph,
            d::reorg_count,
            d::max_reorg_depth,
            h::block_number,
            h::block_hash,
            d::earliest_block_number,
            d::failed,
            d::health,
        ))
        .first::<(
            String,
            i32,
            i32,
            Option<BlockNumber>,
            Option<Vec<u8>>,
            BlockNumber,
            bool,
            SubgraphHealth,
        )>(conn)
        .await
        .optional()?
    {
        None => Err(StoreError::QueryExecutionError(format!(
            "No data found for subgraph {}",
            site.deployment
        ))),
        Some((
            _,
            reorg_count,
            max_reorg_depth,
            latest_block_number,
            latest_block_hash,
            earliest_block_number,
            failed,
            health,
        )) => {
            let reorg_count = convert_to_u32(Some(reorg_count), "reorg_count", &site.deployment)?;
            let max_reorg_depth =
                convert_to_u32(Some(max_reorg_depth), "max_reorg_depth", &site.deployment)?;
            let latest_block = crate::detail::block(
                &site.deployment,
                "latest_block",
                latest_block_hash,
                latest_block_number,
            )?
            .ok_or_else(|| {
                StoreError::QueryExecutionError(format!(
                    "Subgraph `{}` has not started syncing yet. Wait for it to ingest \
                 a few blocks before querying it",
                    site.deployment
                ))
            })?
            .to_ptr();
            // We skip checking for errors if the subgraph is healthy; since
            // that is a lot harder to determine than one would assume, we try
            // to err on the side of caution
            let first_error_block = if failed || !matches!(health, SubgraphHealth::Healthy) {
                e::table
                    .filter(e::subgraph_id.eq(site.deployment.as_str()))
                    .filter(e::deterministic)
                    .select(sql::<Nullable<Integer>>("min(lower(block_range))"))
                    .first::<Option<i32>>(conn)
                    .await?
            } else {
                None
            };
            Ok(DeploymentState {
                id: site.deployment.clone(),
                reorg_count,
                max_reorg_depth,
                latest_block,
                earliest_block_number,
                first_error_block,
            })
        }
    }
}

/// Mark the deployment `id` as synced
pub async fn set_synced(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
    block_ptr: BlockPtr,
) -> Result<(), StoreError> {
    use deployment as d;

    update(
        d::table
            .filter(d::subgraph.eq(id.as_str()))
            .filter(d::synced_at.is_null()),
    )
    .set((
        d::synced_at.eq(now),
        d::synced_at_block_number.eq(block_ptr.number),
    ))
    .execute(conn)
    .await?;
    Ok(())
}

/// Returns `true` if the deployment (as identified by `site.id`)
pub async fn exists(conn: &mut AsyncPgConnection, site: &Site) -> Result<bool, StoreError> {
    use deployment as d;

    let exists = d::table
        .filter(d::id.eq(site.id))
        .count()
        .get_result::<i64>(conn)
        .await?
        > 0;
    Ok(exists)
}

/// Returns `true` if the deployment `id` exists and is synced
pub async fn exists_and_synced(conn: &mut AsyncPgConnection, id: &str) -> Result<bool, StoreError> {
    use deployment as d;

    let synced = d::table
        .filter(d::subgraph.eq(id))
        .select(d::synced_at.is_not_null())
        .first(conn)
        .await
        .optional()?
        .unwrap_or(false);
    Ok(synced)
}

// Does nothing if the error already exists. Returns the error id.
async fn insert_subgraph_error(
    conn: &mut AsyncPgConnection,
    error: &SubgraphError,
) -> anyhow::Result<String> {
    use subgraph_error as e;

    let error_id = hex::encode(stable_hash_legacy::utils::stable_hash::<SetHasher, _>(
        &error,
    ));
    let SubgraphError {
        subgraph_id,
        message,
        handler,
        block_ptr,
        deterministic,
    } = error;

    let block_num = match &block_ptr {
        None => crate::block_range::BLOCK_UNVERSIONED,
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
        .execute(conn)
        .await?;

    Ok(error_id)
}

pub async fn fail(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
    error: &SubgraphError,
) -> Result<(), StoreError> {
    let error_id = insert_subgraph_error(conn, error).await?;

    update_deployment_status(conn, id, SubgraphHealth::Failed, Some(error_id), None).await?;

    Ok(())
}

pub async fn update_non_fatal_errors(
    conn: &mut AsyncPgConnection,
    deployment_id: &DeploymentHash,
    health: SubgraphHealth,
    non_fatal_errors: Option<&[SubgraphError]>,
) -> Result<(), StoreError> {
    let error_ids = non_fatal_errors.map(|errors| {
        errors
            .iter()
            .map(|error| {
                hex::encode(stable_hash_legacy::utils::stable_hash::<SetHasher, _>(
                    error,
                ))
            })
            .collect::<Vec<_>>()
    });

    update_deployment_status(conn, deployment_id, health, None, error_ids).await?;

    Ok(())
}

/// If `block` is `None`, assumes the latest block.
pub(crate) async fn has_deterministic_errors(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
    block: BlockNumber,
) -> Result<bool, StoreError> {
    use subgraph_error as e;
    select(diesel::dsl::exists(
        e::table
            .filter(e::subgraph_id.eq(id.as_str()))
            .filter(e::deterministic)
            .filter(sql::<Bool>("block_range @> ").bind::<Integer, _>(block)),
    ))
    .get_result(conn)
    .await
    .map_err(|e| e.into())
}

pub async fn update_deployment_status(
    conn: &mut AsyncPgConnection,
    deployment_id: &DeploymentHash,
    health: SubgraphHealth,
    fatal_error: Option<String>,
    non_fatal_errors: Option<Vec<String>>,
) -> Result<(), StoreError> {
    use deployment as d;

    update(d::table.filter(d::subgraph.eq(deployment_id.as_str())))
        .set((
            d::failed.eq(health.is_failed()),
            d::health.eq(health),
            d::fatal_error.eq::<Option<String>>(fatal_error),
            d::non_fatal_errors.eq::<Vec<String>>(non_fatal_errors.unwrap_or(vec![])),
        ))
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(StoreError::from)
}

/// Insert the errors and check if the subgraph needs to be set as
/// unhealthy. The `latest_block` is only used to check whether the subgraph
/// is healthy as of that block; errors are inserted according to the
/// `block_ptr` they contain
pub(crate) async fn insert_subgraph_errors(
    logger: &Logger,
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
    deterministic_errors: &[SubgraphError],
    latest_block: BlockNumber,
) -> Result<(), StoreError> {
    debug!(
        logger,
        "Inserting deterministic errors to the db";
        "subgraph" => id.to_string(),
        "errors" => deterministic_errors.len()
    );

    for error in deterministic_errors {
        insert_subgraph_error(conn, error).await?;
    }

    check_health(logger, conn, id, latest_block).await
}

#[cfg(debug_assertions)]
pub(crate) async fn error_count(
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
) -> Result<usize, StoreError> {
    use subgraph_error as e;

    Ok(e::table
        .filter(e::subgraph_id.eq(id.as_str()))
        .count()
        .get_result::<i64>(conn)
        .await? as usize)
}

/// Checks if the subgraph is healthy or unhealthy as of the given block, or the subgraph latest
/// block if `None`, based on the presence of deterministic errors. Has no effect on failed subgraphs.
async fn check_health(
    logger: &Logger,
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
    block: BlockNumber,
) -> Result<(), StoreError> {
    use deployment as d;

    let has_errors = has_deterministic_errors(conn, id, block).await?;

    let (new, old) = match has_errors {
        true => {
            debug!(
                logger,
                "Subgraph has deterministic errors. Marking as unhealthy";
                "subgraph" => id.to_string(),
                "block" => block
            );
            (SubgraphHealth::Unhealthy, SubgraphHealth::Healthy)
        }
        false => (SubgraphHealth::Healthy, SubgraphHealth::Unhealthy),
    };

    update(
        d::table
            .filter(d::subgraph.eq(id.as_str()))
            .filter(d::health.eq(old)),
    )
    .set(d::health.eq(new))
    .execute(conn)
    .await
    .map(|_| ())
    .map_err(|e| e.into())
}

pub(crate) async fn health(
    conn: &mut AsyncPgConnection,
    id: DeploymentId,
) -> Result<SubgraphHealth, StoreError> {
    use deployment as d;

    d::table
        .filter(d::id.eq(id))
        .select(d::health)
        .get_result(conn)
        .await
        .map_err(|e| e.into())
}

pub(crate) async fn entities_with_causality_region(
    conn: &mut AsyncPgConnection,
    id: DeploymentId,
    schema: &InputSchema,
) -> Result<Vec<EntityType>, StoreError> {
    use subgraph_manifest as sm;

    sm::table
        .filter(sm::id.eq(id))
        .select(sm::entities_with_causality_region)
        .get_result::<Vec<String>>(conn)
        .await
        .map_err(|e| e.into())
        .map(|ents| {
            // It is possible to have entity types in
            // `entities_with_causality_region` that are not mentioned in
            // the schema.
            ents.into_iter()
                .filter_map(|ent| schema.entity_type(&ent).ok())
                .collect()
        })
}

/// Reverts the errors and updates the subgraph health if necessary.
pub(crate) async fn revert_subgraph_errors(
    logger: &Logger,
    conn: &mut AsyncPgConnection,
    id: &DeploymentHash,
    reverted_block: BlockNumber,
) -> Result<(), StoreError> {
    use deployment as d;
    use subgraph_error as e;

    let lower_geq = format!("lower({}) >= ", BLOCK_RANGE_COLUMN);
    delete(
        e::table
            .filter(e::subgraph_id.eq(id.as_str()))
            .filter(sql::<Bool>(&lower_geq).bind::<Integer, _>(reverted_block)),
    )
    .execute(conn)
    .await?;

    // The result will be the same at `reverted_block` or `reverted_block - 1` since the errors at
    // `reverted_block` were just deleted, but semantically we care about `reverted_block - 1` which
    // is the block being reverted to.
    check_health(&logger, conn, id, reverted_block - 1).await?;

    // If the deployment is failed in both `failed` and `status` columns,
    // update both values respectively to `false` and `healthy`. Basically
    // unfail the statuses.
    update(
        d::table
            .filter(d::subgraph.eq(id.as_str()))
            .filter(d::failed.eq(true))
            .filter(d::health.eq(SubgraphHealth::Failed)),
    )
    .set((d::failed.eq(false), d::health.eq(SubgraphHealth::Healthy)))
    .execute(conn)
    .await
    .map(|_| ())
    .map_err(StoreError::from)
}

pub(crate) async fn delete_error(
    conn: &mut AsyncPgConnection,
    error_id: &str,
) -> Result<(), StoreError> {
    use subgraph_error as e;
    delete(e::table.filter(e::id.eq(error_id)))
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(StoreError::from)
}

/// Copy the dynamic data sources for `src` to `dst`. All data sources that
/// were created up to and including `target_block` will be copied.
pub(crate) async fn copy_errors(
    conn: &mut AsyncPgConnection,
    src: &Site,
    dst: &Site,
    target_block: &BlockPtr,
) -> Result<usize, StoreError> {
    use subgraph_error as e;

    let src_nsp = ForeignServer::metadata_schema_in(&src.shard, &dst.shard);

    // Check whether there are any errors for dst which indicates we already
    // did copy
    let count = e::table
        .filter(e::subgraph_id.eq(dst.deployment.as_str()))
        .select(count(e::vid))
        .get_result::<i64>(conn)
        .await?;
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

    Ok(sql_query(query)
        .bind::<Text, _>(src.deployment.as_str())
        .bind::<Text, _>(dst.deployment.as_str())
        .bind::<Integer, _>(target_block.number)
        .execute(conn)
        .await?)
}

/// Drop the schema `namespace`. This deletes all data for the subgraph, and
/// can not be reversed. It does not remove any of the metadata in the
/// `subgraphs` schema for the deployment.
///
/// Since long-running operations, like a vacuum on one of the tables in the
/// schema, could block dropping the schema indefinitely, this operation
/// will wait at most 2s to acquire all necessary locks, and fail if that is
/// not possible.
pub async fn drop_schema(
    conn: &mut AsyncPgConnection,
    namespace: &crate::primary::Namespace,
) -> Result<(), StoreError> {
    let query = format!(
        "set local lock_timeout=2000; drop schema if exists {} cascade",
        namespace
    );
    Ok(conn.batch_execute(&query).await?)
}

pub async fn drop_metadata(conn: &mut AsyncPgConnection, site: &Site) -> Result<(), StoreError> {
    use head as h;

    // We don't need to delete from `deployment`, `subgraph_manifest`,  or
    // `subgraph_error` since that cascades from deleting `head`
    delete(h::table.filter(h::id.eq(site.id)))
        .execute(conn)
        .await?;
    Ok(())
}

pub async fn create_deployment(
    conn: &mut AsyncPgConnection,
    site: &Site,
    create: DeploymentCreate,
    exists: bool,
    replace: bool,
) -> Result<(), StoreError> {
    use deployment as d;
    use head as h;
    use subgraph_manifest as m;

    fn b(ptr: &Option<BlockPtr>) -> Option<&[u8]> {
        ptr.as_ref().map(|ptr| ptr.hash_slice())
    }

    fn n(ptr: &Option<BlockPtr>) -> Option<BlockNumber> {
        ptr.as_ref().map(|ptr| ptr.number)
    }

    let DeploymentCreate {
        manifest:
            SubgraphManifestEntity {
                spec_version,
                description,
                repository,
                features,
                schema,
                raw_yaml,
                entities_with_causality_region,
                history_blocks,
            },
        start_block,
        graft_base,
        graft_block,
        debug_fork,
        history_blocks_override,
    } = create;
    let earliest_block_number = start_block.as_ref().map(|ptr| ptr.number).unwrap_or(0);
    let entities_with_causality_region = Vec::from_iter(
        entities_with_causality_region
            .into_iter()
            .map(|et| et.typename().to_owned()),
    );

    let head_values = (
        h::id.eq(site.id),
        h::block_number.eq(sql("null")),
        h::block_hash.eq(sql("null")),
        h::firehose_cursor.eq(sql("null")),
        h::entity_count.eq(sql("0")),
    );

    let deployment_values = (
        d::id.eq(site.id),
        d::subgraph.eq(site.deployment.as_str()),
        d::failed.eq(false),
        d::health.eq(SubgraphHealth::Healthy),
        d::fatal_error.eq::<Option<String>>(None),
        d::non_fatal_errors.eq::<Vec<String>>(vec![]),
        d::earliest_block_number.eq(earliest_block_number),
        d::graft_base.eq(graft_base.as_ref().map(|s| s.as_str())),
        d::graft_block_hash.eq(b(&graft_block)),
        d::graft_block_number.eq(n(&graft_block)),
        d::debug_fork.eq(debug_fork.as_ref().map(|s| s.as_str())),
    );

    let graph_node_version_id = GraphNodeVersion::create_or_get(conn).await?;

    let manifest_values = (
        m::id.eq(site.id),
        m::spec_version.eq(spec_version),
        m::description.eq(description),
        m::repository.eq(repository),
        m::features.eq(features),
        m::schema.eq(schema),
        m::graph_node_version_id.eq(graph_node_version_id),
        // New subgraphs index only a prefix of bytea columns
        // see: attr-bytea-prefix
        m::use_bytea_prefix.eq(true),
        m::start_block_hash.eq(b(&start_block)),
        m::start_block_number.eq(start_block.as_ref().map(|ptr| ptr.number)),
        m::raw_yaml.eq(raw_yaml),
        m::entities_with_causality_region.eq(entities_with_causality_region),
        m::history_blocks.eq(history_blocks_override.unwrap_or(history_blocks)),
    );

    if exists && replace {
        update(h::table.filter(h::id.eq(site.id)))
            .set(head_values)
            .execute(conn)
            .await?;

        update(d::table.filter(d::subgraph.eq(site.deployment.as_str())))
            .set(deployment_values)
            .execute(conn)
            .await?;

        update(m::table.filter(m::id.eq(site.id)))
            .set(manifest_values)
            .execute(conn)
            .await?;
    } else {
        insert_into(h::table)
            .values(head_values)
            .execute(conn)
            .await?;

        insert_into(d::table)
            .values(deployment_values)
            .execute(conn)
            .await?;

        insert_into(m::table)
            .values(manifest_values)
            .execute(conn)
            .await?;
    }
    Ok(())
}

fn entity_count_sql(count: i32) -> String {
    format!("entity_count + ({count})")
}

pub async fn update_entity_count(
    conn: &mut AsyncPgConnection,
    site: &Site,
    count: i32,
) -> Result<(), StoreError> {
    use head as h;

    if count == 0 {
        return Ok(());
    }

    let count_sql = entity_count_sql(count);
    update(h::table.filter(h::id.eq(site.id)))
        .set(h::entity_count.eq(sql(&count_sql)))
        .execute(conn)
        .await?;
    Ok(())
}

/// Set the deployment's entity count back to `0`
pub async fn clear_entity_count(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<(), StoreError> {
    use head as h;

    update(h::table.filter(h::id.eq(site.id)))
        .set(h::entity_count.eq(0))
        .execute(conn)
        .await?;
    Ok(())
}

/// Set the earliest block of `site` to the larger of `earliest_block` and
/// the current value. This means that the `earliest_block_number` can never
/// go backwards, only forward. This is important so that copying into
/// `site` can not move the earliest block backwards if `site` was also
/// pruned while the copy was running.
pub async fn set_earliest_block(
    conn: &mut AsyncPgConnection,
    site: &Site,
    earliest_block: BlockNumber,
) -> Result<(), StoreError> {
    use deployment as d;

    update(d::table.filter(d::id.eq(site.id)))
        .set(d::earliest_block_number.eq(earliest_block))
        .filter(d::earliest_block_number.lt(earliest_block))
        .execute(conn)
        .await?;
    Ok(())
}

/// Copy the `earliest_block` attribute from `src` to `dst`. The copy might
/// go across shards and use the metadata tables mapped into the shard for
/// `conn` which must be the shard for `dst`
pub async fn copy_earliest_block(
    conn: &mut AsyncPgConnection,
    src: &Site,
    dst: &Site,
) -> Result<(), StoreError> {
    use deployment as d;

    let src_nsp = ForeignServer::metadata_schema_in(&src.shard, &dst.shard);

    let query = format!(
        "(select earliest_block_number from {src_nsp}.deployment where id = {})",
        src.id
    );

    update(d::table.filter(d::id.eq(dst.id)))
        .set(d::earliest_block_number.eq(sql(&query)))
        .execute(conn)
        .await?;

    Ok(())
}

pub async fn on_sync(
    conn: &mut AsyncPgConnection,
    id: impl Into<DeploymentId>,
) -> Result<OnSync, StoreError> {
    use subgraph_manifest as m;

    let s = m::table
        .filter(m::id.eq(id.into()))
        .select(m::on_sync)
        .get_result::<Option<String>>(conn)
        .await?;
    OnSync::try_from(s.as_deref())
}

pub async fn set_on_sync(
    conn: &mut AsyncPgConnection,
    site: &Site,
    on_sync: OnSync,
) -> Result<(), StoreError> {
    use subgraph_manifest as m;

    let n = update(m::table.filter(m::id.eq(site.id)))
        .set(m::on_sync.eq(on_sync.to_sql()))
        .execute(conn)
        .await?;

    match n {
        0 => Err(StoreError::DeploymentNotFound(site.to_string())),
        1 => Ok(()),
        _ => Err(internal_error!(
            "multiple manifests for deployment {}",
            site.to_string()
        )),
    }
}

/// Lock the deployment `site` for writes while `f` is running. The lock can
/// cross transactions, and `f` can therefore execute multiple transactions
/// while other write activity for that deployment is locked out. Block the
/// current thread until we can acquire the lock.
//  see also: deployment-lock-for-update
pub async fn with_lock<F, R>(
    conn: &mut AsyncPgConnection,
    site: &Site,
    f: F,
) -> Result<R, StoreError>
where
    F: AsyncFnOnce(&mut AsyncPgConnection) -> Result<R, StoreError>,
{
    let mut backoff = ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(15));
    while !advisory_lock::lock_deployment_session(conn, site).await? {
        backoff.sleep();
    }
    let res = f(conn).await;
    advisory_lock::unlock_deployment_session(conn, site).await?;
    res
}
