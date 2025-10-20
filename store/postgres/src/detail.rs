//! Queries to support the index node API
//!
// For git_testament_macros
#![allow(unused_macros)]
use diesel::dsl::sql;
use diesel::prelude::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension, QueryDsl,
    SelectableHelper as _,
};
use diesel::sql_types::{Array, BigInt, Integer};
use diesel_async::RunQueryDsl;
use diesel_derives::Associations;
use git_testament::{git_testament, git_testament_macros};
use graph::blockchain::BlockHash;
use graph::data::store::scalar::ToPrimitive;
use graph::data::subgraph::schema::{SubgraphError, SubgraphManifestEntity};
use graph::prelude::BlockNumber;
use graph::prelude::{
    chrono::{DateTime, Utc},
    BlockPtr, DeploymentHash, StoreError, SubgraphDeploymentEntity,
};
use graph::schema::InputSchema;
use graph::{data::subgraph::status, internal_error, prelude::web3::types::H256};
use itertools::Itertools;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::{ops::Bound, sync::Arc};

use crate::deployment::{
    deployment as subgraph_deployment, graph_node_versions, head as subgraph_head, subgraph_error,
    subgraph_manifest, SubgraphHealth as HealthType,
};
use crate::primary::{DeploymentId, Site};
use crate::AsyncPgConnection;

git_testament_macros!(version);
git_testament!(TESTAMENT);

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
const CARGO_PKG_VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
const CARGO_PKG_VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");

type Bytes = Vec<u8>;

pub struct DeploymentDetail {
    pub id: DeploymentId,
    pub subgraph: String,
    /// The earliest block for which we have history
    pub earliest_block_number: i32,
    health: HealthType,
    pub failed: bool,
    graft_base: Option<String>,
    graft_block_hash: Option<Bytes>,
    graft_block_number: Option<BlockNumber>,
    reorg_count: i32,
    current_reorg_depth: i32,
    max_reorg_depth: i32,
    debug_fork: Option<String>,
    pub synced_at: Option<DateTime<Utc>>,
    pub synced_at_block_number: Option<i32>,
    pub block_hash: Option<Bytes>,
    pub block_number: Option<BlockNumber>,
    pub entity_count: usize,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = subgraph_deployment)]
struct Deployment {
    id: DeploymentId,
    subgraph: String,
    /// The earliest block for which we have history
    earliest_block_number: i32,
    health: HealthType,
    failed: bool,
    graft_base: Option<String>,
    graft_block_hash: Option<Bytes>,
    graft_block_number: Option<BlockNumber>,
    reorg_count: i32,
    current_reorg_depth: i32,
    max_reorg_depth: i32,
    debug_fork: Option<String>,
    synced_at: Option<DateTime<Utc>>,
    synced_at_block_number: Option<i32>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = subgraph_head)]
struct Head {
    block_hash: Option<Bytes>,
    block_number: Option<BlockNumber>,
    entity_count: i64,
}

impl From<(Deployment, Head)> for DeploymentDetail {
    fn from((deployment, head): (Deployment, Head)) -> Self {
        let Deployment {
            id,
            subgraph,
            earliest_block_number,
            health,
            failed,
            graft_base,
            graft_block_hash,
            graft_block_number,
            reorg_count,
            current_reorg_depth,
            max_reorg_depth,
            debug_fork,
            synced_at,
            synced_at_block_number,
        } = deployment;

        let Head {
            block_hash,
            block_number,
            entity_count,
        } = head;

        Self {
            id,
            subgraph,
            earliest_block_number,
            health,
            failed,
            graft_base,
            graft_block_hash,
            graft_block_number,
            reorg_count,
            current_reorg_depth,
            max_reorg_depth,
            debug_fork,
            synced_at,
            synced_at_block_number,
            block_hash: block_hash.clone(),
            block_number: block_number.clone(),
            entity_count: entity_count as usize,
        }
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = subgraph_error)]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
pub(crate) struct ErrorDetail {
    pub id: String,
    subgraph_id: String,
    message: String,
    pub block_hash: Option<Bytes>,
    handler: Option<String>,
    pub deterministic: bool,
    pub block_range: (Bound<i32>, Bound<i32>),
}

impl ErrorDetail {
    /// Fetches the fatal error, if present, associated with the given
    /// [`DeploymentHash`].
    pub async fn fatal(
        conn: &mut AsyncPgConnection,
        deployment_id: &DeploymentHash,
    ) -> Result<Option<Self>, StoreError> {
        use subgraph_deployment as d;
        use subgraph_error as e;

        d::table
            .filter(d::subgraph.eq(deployment_id.as_str()))
            .inner_join(e::table.on(e::id.nullable().eq(d::fatal_error)))
            .select(ErrorDetail::as_select())
            .get_result(conn)
            .await
            .optional()
            .map_err(StoreError::from)
    }
}

impl TryFrom<ErrorDetail> for SubgraphError {
    type Error = StoreError;

    fn try_from(value: ErrorDetail) -> Result<Self, Self::Error> {
        let ErrorDetail {
            id: _,
            subgraph_id,
            message,
            block_hash,
            handler,
            deterministic,
            block_range,
        } = value;
        let block_number = crate::block_range::first_block_in_range(&block_range);
        // FIXME:
        //
        // workaround for arweave
        let block_hash = block_hash.map(|hash| H256::from_slice(&hash.as_slice()[..32]));
        // In existing databases, we have errors that have a `block_range` of
        // `UNVERSIONED_RANGE`, which leads to `None` as the block number, but
        // has a hash. Conversely, it is also possible for an error to not have a
        // hash. In both cases, use a block pointer of `None`
        let block_ptr = match (block_number, block_hash) {
            (Some(number), Some(hash)) => Some(BlockPtr::from((hash, number as u64))),
            _ => None,
        };
        let subgraph_id = DeploymentHash::new(subgraph_id).map_err(|id| {
            StoreError::InternalError(format!("invalid subgraph id `{}` in fatal error", id))
        })?;
        Ok(SubgraphError {
            subgraph_id,
            message,
            block_ptr,
            handler,
            deterministic,
        })
    }
}

pub(crate) fn block(
    id: &str,
    name: &str,
    hash: Option<Vec<u8>>,
    number: Option<BlockNumber>,
) -> Result<Option<status::EthereumBlock>, StoreError> {
    match (hash, number) {
        (Some(hash), Some(number)) => Ok(Some(status::EthereumBlock::new(
            BlockHash(hash.into_boxed_slice()),
            number,
        ))),
        (None, None) => Ok(None),
        (hash, number) => Err(internal_error!(
            "the hash and number \
        of a block pointer must either both be null or both have a \
        value, but for `{}` the hash of {} is `{:?}` and the number is `{:?}`",
            id,
            name,
            hash,
            number
        )),
    }
}

pub(crate) fn info_from_details(
    detail: DeploymentDetail,
    fatal: Option<ErrorDetail>,
    non_fatal: Vec<ErrorDetail>,
    sites: &[Arc<Site>],
    subgraph_history_blocks: i32,
    subgraph_size: status::SubgraphSize,
) -> Result<status::Info, StoreError> {
    let DeploymentDetail {
        id,
        subgraph,
        failed: _,
        health,
        synced_at,
        earliest_block_number,
        block_hash,
        block_number,
        entity_count,
        graft_base: _,
        graft_block_hash: _,
        graft_block_number: _,
        synced_at_block_number: _,
        debug_fork: _,
        reorg_count: _,
        current_reorg_depth: _,
        max_reorg_depth: _,
    } = detail;

    let site = sites
        .iter()
        .find(|site| site.deployment.as_str() == subgraph)
        .ok_or_else(|| internal_error!("missing site for subgraph `{}`", subgraph))?;

    // This needs to be filled in later since it lives in a
    // different shard
    let chain_head_block = None;
    let latest_block = block(&subgraph, "latest_ethereum_block", block_hash, block_number)?;
    let health = health.into();
    let chain = status::ChainInfo {
        network: site.network.clone(),
        chain_head_block,
        earliest_block_number,
        latest_block,
    };
    let entity_count = entity_count.to_u64().ok_or_else(|| {
        internal_error!(
            "the entityCount for {} is not representable as a u64",
            subgraph
        )
    })?;
    let fatal_error = fatal.map(SubgraphError::try_from).transpose()?;
    let non_fatal_errors = non_fatal
        .into_iter()
        .map(SubgraphError::try_from)
        .collect::<Result<Vec<SubgraphError>, StoreError>>()?;

    // 'node' needs to be filled in later from a different shard
    Ok(status::Info {
        id: id.into(),
        subgraph,
        synced: synced_at.is_some(),
        health,
        paused: None,
        fatal_error,
        non_fatal_errors,
        chains: vec![chain],
        entity_count,
        node: None,
        history_blocks: subgraph_history_blocks,
        subgraph_size,
    })
}

/// Return the details for `deployments`
pub(crate) async fn deployment_details(
    conn: &mut AsyncPgConnection,
    deployments: Vec<String>,
) -> Result<Vec<DeploymentDetail>, StoreError> {
    use subgraph_deployment as d;
    use subgraph_head as h;

    let cols = <(Deployment, Head)>::as_select();

    // Empty deployments means 'all of them'
    let details = if deployments.is_empty() {
        d::table
            .inner_join(h::table)
            .select(cols)
            .load::<(Deployment, Head)>(conn)
            .await?
    } else {
        d::table
            .inner_join(h::table)
            .filter(d::subgraph.eq_any(&deployments))
            .select(cols)
            .load::<(Deployment, Head)>(conn)
            .await?
    }
    .into_iter()
    .map(DeploymentDetail::from)
    .collect();
    Ok(details)
}

/// Return the details for `deployment`
pub(crate) async fn deployment_details_for_id(
    conn: &mut AsyncPgConnection,
    deployment: &DeploymentId,
) -> Result<DeploymentDetail, StoreError> {
    use subgraph_deployment as d;
    use subgraph_head as h;

    let cols = <(Deployment, Head)>::as_select();

    d::table
        .inner_join(h::table)
        .filter(d::id.eq(&deployment))
        .select(cols)
        .first::<(Deployment, Head)>(conn)
        .await
        .map_err(StoreError::from)
        .map(DeploymentDetail::from)
}

pub(crate) async fn deployment_statuses(
    conn: &mut AsyncPgConnection,
    sites: &[Arc<Site>],
) -> Result<Vec<status::Info>, StoreError> {
    use subgraph_deployment as d;
    use subgraph_error as e;
    use subgraph_head as h;
    use subgraph_manifest as sm;

    // First, we fetch all deployment information along with any fatal errors.
    // Subsequently, we fetch non-fatal errors and we group them by deployment
    // ID.

    let details_with_fatal_error = {
        let join = e::table.on(e::id.nullable().eq(d::fatal_error));

        let cols = <(Deployment, Head, Option<ErrorDetail>)>::as_select();

        // Empty deployments means 'all of them'
        if sites.is_empty() {
            d::table
                .inner_join(h::table)
                .left_outer_join(join)
                .select(cols)
                .load::<(Deployment, Head, Option<ErrorDetail>)>(conn)
                .await?
        } else {
            d::table
                .inner_join(h::table)
                .left_outer_join(join)
                .filter(d::id.eq_any(sites.iter().map(|site| site.id)))
                .select(cols)
                .load::<(Deployment, Head, Option<ErrorDetail>)>(conn)
                .await?
        }
    };

    let mut non_fatal_errors = {
        #[allow(deprecated)]
        let join = e::table.on(e::id.eq(sql("any(subgraphs.deployment.non_fatal_errors)")));

        if sites.is_empty() {
            d::table
                .inner_join(join)
                .select((d::id, ErrorDetail::as_select()))
                .load::<(DeploymentId, ErrorDetail)>(conn)
                .await?
        } else {
            d::table
                .inner_join(join)
                .filter(d::id.eq_any(sites.iter().map(|site| site.id)))
                .select((d::id, ErrorDetail::as_select()))
                .load::<(DeploymentId, ErrorDetail)>(conn)
                .await?
        }
        .into_iter()
        .into_group_map()
    };

    let mut history_blocks_map: HashMap<_, _> = {
        if sites.is_empty() {
            sm::table
                .select((sm::id, sm::history_blocks))
                .load::<(DeploymentId, i32)>(conn)
                .await?
        } else {
            sm::table
                .filter(sm::id.eq_any(sites.iter().map(|site| site.id)))
                .select((sm::id, sm::history_blocks))
                .load::<(DeploymentId, i32)>(conn)
                .await?
        }
        .into_iter()
        .collect()
    };

    let mut deployment_sizes = deployment_sizes(conn, sites)?;

    details_with_fatal_error
        .into_iter()
        .map(|(deployment, head, fatal)| {
            let detail = DeploymentDetail::from((deployment, head));
            let non_fatal = non_fatal_errors.remove(&detail.id).unwrap_or_default();
            let subgraph_history_blocks = history_blocks_map.remove(&detail.id).unwrap_or_default();
            let table_sizes = deployment_sizes.remove(&detail.id).unwrap_or_default();
            info_from_details(
                detail,
                fatal,
                non_fatal,
                sites,
                subgraph_history_blocks,
                table_sizes,
            )
        })
        .collect()
}

fn deployment_sizes(
    conn: &mut PgConnection,
    sites: &[Arc<Site>],
) -> Result<HashMap<DeploymentId, status::SubgraphSize>, StoreError> {
    #[derive(QueryableByName)]
    struct SubgraphSizeRow {
        #[diesel(sql_type = Integer)]
        id: DeploymentId,
        #[diesel(sql_type = BigInt)]
        row_estimate: i64,
        #[diesel(sql_type = BigInt)]
        table_bytes: i64,
        #[diesel(sql_type = BigInt)]
        index_bytes: i64,
        #[diesel(sql_type = BigInt)]
        toast_bytes: i64,
        #[diesel(sql_type = BigInt)]
        total_bytes: i64,
    }

    let mut query = String::from(
        r#"
        SELECT
            ds.id,
            ss.row_estimate::bigint,
            ss.table_bytes::bigint,
            ss.index_bytes::bigint,
            ss.toast_bytes::bigint,
            ss.total_bytes::bigint
        FROM deployment_schemas ds
        JOIN info.subgraph_sizes as ss on ss.name = ds.name
    "#,
    );

    let result = if sites.is_empty() {
        diesel::sql_query(query).load::<SubgraphSizeRow>(conn)
    } else {
        query.push_str(" WHERE ds.id = ANY($1)");
        diesel::sql_query(query)
            .bind::<Array<Integer>, _>(sites.iter().map(|site| site.id).collect::<Vec<_>>())
            .load::<SubgraphSizeRow>(conn)
    };

    let rows = match result {
        Ok(rows) => rows,
        Err(e) if e.to_string().contains("has not been populated") => Vec::new(),
        Err(e) => return Err(e.into()),
    };

    let mut sizes: HashMap<DeploymentId, status::SubgraphSize> = HashMap::new();
    for row in rows {
        sizes.insert(
            row.id,
            status::SubgraphSize {
                row_estimate: row.row_estimate,
                table_bytes: row.table_bytes,
                index_bytes: row.index_bytes,
                toast_bytes: row.toast_bytes,
                total_bytes: row.total_bytes,
            },
        );
    }

    Ok(sizes)
}

#[derive(Queryable, Selectable, Identifiable, Associations)]
#[diesel(table_name = subgraph_manifest)]
#[diesel(belongs_to(GraphNodeVersion))]
// We never read the id field but map it to make the interaction with Diesel
// simpler
struct StoredSubgraphManifest {
    id: i32,
    spec_version: String,
    description: Option<String>,
    repository: Option<String>,
    features: Vec<String>,
    schema: String,
    graph_node_version_id: Option<i32>,
    start_block_number: Option<i32>,
    start_block_hash: Option<Bytes>,
    raw_yaml: Option<String>,
    entities_with_causality_region: Vec<String>,
    history_blocks: i32,
}

impl StoredSubgraphManifest {
    fn as_manifest(self, schema: &InputSchema) -> SubgraphManifestEntity {
        let e: Vec<_> = self
            .entities_with_causality_region
            .into_iter()
            .map(|s| schema.entity_type(&s).unwrap())
            .collect();
        SubgraphManifestEntity {
            spec_version: self.spec_version,
            description: self.description,
            repository: self.repository,
            features: self.features,
            schema: self.schema,
            raw_yaml: self.raw_yaml,
            entities_with_causality_region: e,
            history_blocks: self.history_blocks,
        }
    }
}

struct StoredDeploymentEntity(crate::detail::DeploymentDetail, StoredSubgraphManifest);

impl StoredDeploymentEntity {
    fn as_subgraph_deployment(
        self,
        schema: &InputSchema,
    ) -> Result<SubgraphDeploymentEntity, StoreError> {
        let (detail, manifest) = (self.0, self.1);

        let start_block = block(
            &detail.subgraph,
            "start_block",
            manifest.start_block_hash.clone(),
            manifest.start_block_number.map(|n| n.into()),
        )?
        .map(|block| block.to_ptr());

        let latest_block = block(
            &detail.subgraph,
            "latest_block",
            detail.block_hash,
            detail.block_number,
        )?
        .map(|block| block.to_ptr());

        let graft_block = block(
            &detail.subgraph,
            "graft_block",
            detail.graft_block_hash,
            detail.graft_block_number,
        )?
        .map(|block| block.to_ptr());

        let graft_base = detail
            .graft_base
            .map(DeploymentHash::new)
            .transpose()
            .map_err(|b| internal_error!("invalid graft base `{}`", b))?;

        let debug_fork = detail
            .debug_fork
            .map(DeploymentHash::new)
            .transpose()
            .map_err(|b| internal_error!("invalid debug fork `{}`", b))?;

        Ok(SubgraphDeploymentEntity {
            manifest: manifest.as_manifest(schema),
            failed: detail.failed,
            health: detail.health.into(),
            synced_at: detail.synced_at,
            fatal_error: None,
            non_fatal_errors: vec![],
            earliest_block_number: detail.earliest_block_number,
            start_block,
            latest_block,
            graft_base,
            graft_block,
            debug_fork,
            reorg_count: detail.reorg_count,
            current_reorg_depth: detail.current_reorg_depth,
            max_reorg_depth: detail.max_reorg_depth,
        })
    }
}

pub async fn deployment_entity(
    conn: &mut AsyncPgConnection,
    site: &Site,
    schema: &InputSchema,
) -> Result<SubgraphDeploymentEntity, StoreError> {
    use subgraph_deployment as d;
    use subgraph_head as h;
    use subgraph_manifest as m;

    let manifest = m::table
        .find(site.id)
        .select(StoredSubgraphManifest::as_select())
        .first::<StoredSubgraphManifest>(conn)
        .await?;

    let detail = d::table
        .inner_join(h::table)
        .filter(d::id.eq(site.id))
        .select(<(Deployment, Head)>::as_select())
        .first::<(Deployment, Head)>(conn)
        .await
        .map(DeploymentDetail::from)?;

    StoredDeploymentEntity(detail, manifest).as_subgraph_deployment(schema)
}

#[derive(Queryable, Identifiable, Insertable)]
#[diesel(table_name = graph_node_versions)]
pub struct GraphNodeVersion {
    pub id: i32,
    pub git_commit_hash: String,
    pub git_repository_dirty: bool,
    pub crate_version: String,
    pub major: i32,
    pub minor: i32,
    pub patch: i32,
}

impl GraphNodeVersion {
    pub(crate) async fn create_or_get(conn: &mut AsyncPgConnection) -> anyhow::Result<i32> {
        let git_commit_hash = version_commit_hash!();
        let git_repository_dirty = !&TESTAMENT.modifications.is_empty();
        let crate_version = CARGO_PKG_VERSION;
        let major: i32 = CARGO_PKG_VERSION_MAJOR
            .parse()
            .expect("failed to parse cargo major package version");
        let minor: i32 = CARGO_PKG_VERSION_MINOR
            .parse()
            .expect("failed to parse cargo major package version");
        let patch: i32 = CARGO_PKG_VERSION_PATCH
            .parse()
            .expect("failed to parse cargo major package version");

        let graph_node_version_id = {
            use graph_node_versions::dsl as g;

            // try to insert our current values
            diesel::insert_into(g::graph_node_versions)
                .values((
                    g::git_commit_hash.eq(&git_commit_hash),
                    g::git_repository_dirty.eq(git_repository_dirty),
                    g::crate_version.eq(&crate_version),
                    g::major.eq(&major),
                    g::minor.eq(&minor),
                    g::patch.eq(&patch),
                ))
                .on_conflict_do_nothing()
                .execute(conn)
                .await?;

            // select the id for the row we just inserted
            g::graph_node_versions
                .select(g::id)
                .filter(g::git_commit_hash.eq(&git_commit_hash))
                .filter(g::git_repository_dirty.eq(git_repository_dirty))
                .filter(g::crate_version.eq(&crate_version))
                .filter(g::major.eq(&major))
                .filter(g::minor.eq(&minor))
                .filter(g::patch.eq(&patch))
                .get_result(conn)
                .await?
        };
        Ok(graph_node_version_id)
    }
}
