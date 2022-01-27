// For git_testament_macros
#![allow(unused_macros)]

//! Queries to support the index node API
use crate::primary::Site;
use crate::{
    deployment::{
        get_fatal_error_id, graph_node_versions, subgraph_deployment, subgraph_error,
        subgraph_manifest, SubgraphHealth as HealthType,
    },
    primary::DeploymentId,
};
use diesel::pg::PgConnection;
use diesel::prelude::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, QueryDsl, RunQueryDsl,
};
use diesel::OptionalExtension;
use diesel_derives::Associations;
use git_testament::{git_testament, git_testament_macros};
use graph::{
    constraint_violation,
    data::subgraph::schema::{SubgraphError, SubgraphManifestEntity},
    prelude::{
        bigdecimal::ToPrimitive, BigDecimal, BlockPtr, DeploymentHash, StoreError,
        SubgraphDeploymentEntity,
    },
};
use graph::{data::subgraph::status, prelude::web3::types::H256};
use std::convert::TryFrom;
use std::{ops::Bound, sync::Arc};

git_testament_macros!(version);
git_testament!(TESTAMENT);

const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
const CARGO_PKG_VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
const CARGO_PKG_VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
const CARGO_PKG_VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");

type Bytes = Vec<u8>;

#[derive(Queryable, QueryableByName)]
#[table_name = "subgraph_deployment"]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
#[allow(dead_code)]
pub struct DeploymentDetail {
    pub id: DeploymentId,
    pub deployment: String,
    pub failed: bool,
    health: HealthType,
    pub synced: bool,
    fatal_error: Option<String>,
    non_fatal_errors: Vec<String>,
    earliest_ethereum_block_hash: Option<Bytes>,
    earliest_ethereum_block_number: Option<BigDecimal>,
    pub latest_ethereum_block_hash: Option<Bytes>,
    pub latest_ethereum_block_number: Option<BigDecimal>,
    last_healthy_ethereum_block_hash: Option<Bytes>,
    last_healthy_ethereum_block_number: Option<BigDecimal>,
    pub entity_count: BigDecimal,
    graft_base: Option<String>,
    graft_block_hash: Option<Bytes>,
    graft_block_number: Option<BigDecimal>,
    reorg_count: i32,
    current_reorg_depth: i32,
    max_reorg_depth: i32,
    firehose_cursor: Option<String>,
}

#[derive(Queryable, QueryableByName)]
#[table_name = "subgraph_error"]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
#[allow(dead_code)]
pub(crate) struct ErrorDetail {
    vid: i64,
    pub id: String,
    subgraph_id: String,
    message: String,
    pub block_hash: Option<Bytes>,
    handler: Option<String>,
    pub deterministic: bool,
    pub block_range: (Bound<i32>, Bound<i32>),
}

fn error(conn: &PgConnection, error_id: &str) -> Result<Option<ErrorDetail>, StoreError> {
    use subgraph_error as e;
    e::table
        .filter(e::id.eq(error_id))
        .get_result(conn)
        .optional()
        .map_err(StoreError::from)
}

pub(crate) fn fatal_error(
    conn: &PgConnection,
    deployment_id: &DeploymentHash,
) -> Result<Option<ErrorDetail>, StoreError> {
    let fatal_error_id = match get_fatal_error_id(conn, deployment_id)? {
        Some(fatal_error_id) => fatal_error_id,
        // No fatal error found.
        None => return Ok(None),
    };

    error(conn, &fatal_error_id)
}

struct DetailAndError<'a>(DeploymentDetail, Option<ErrorDetail>, &'a Vec<Arc<Site>>);

pub(crate) fn block(
    id: &str,
    name: &str,
    hash: Option<Vec<u8>>,
    number: Option<BigDecimal>,
) -> Result<Option<status::EthereumBlock>, StoreError> {
    match (&hash, &number) {
        (Some(hash), Some(number)) => {
            let hash = H256::from_slice(hash.as_slice());
            let number = number.to_u64().ok_or_else(|| {
                constraint_violation!(
                    "the block number {} for {} in {} is not representable as a u64",
                    number,
                    name,
                    id
                )
            })?;
            Ok(Some(status::EthereumBlock::new(hash, number)))
        }
        (None, None) => Ok(None),
        _ => Err(constraint_violation!(
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

impl TryFrom<ErrorDetail> for SubgraphError {
    type Error = StoreError;

    fn try_from(value: ErrorDetail) -> Result<Self, Self::Error> {
        let ErrorDetail {
            vid: _,
            id: _,
            subgraph_id,
            message,
            block_hash,
            handler,
            deterministic,
            block_range,
        } = value;
        let block_number = crate::block_range::first_block_in_range(&block_range);
        let block_hash = block_hash.map(|hash| H256::from_slice(hash.as_slice()));
        // In existing databases, we have errors that have a `block_range` of
        // `UNVERSIONED_RANGE`, which leads to `None` as the block number, but
        // has a hash. Conversely, it is also possible for an error to not have a
        // hash. In both cases, use a block pointer of `None`
        let block_ptr = match (block_number, block_hash) {
            (Some(number), Some(hash)) => Some(BlockPtr::from((hash, number as u64))),
            _ => None,
        };
        let subgraph_id = DeploymentHash::new(subgraph_id).map_err(|id| {
            StoreError::ConstraintViolation(format!("invalid subgraph id `{}` in fatal error", id))
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

impl<'a> TryFrom<DetailAndError<'a>> for status::Info {
    type Error = StoreError;

    fn try_from(detail_and_error: DetailAndError) -> Result<Self, Self::Error> {
        let DetailAndError(detail, error, sites) = detail_and_error;

        let DeploymentDetail {
            id,
            deployment,
            failed: _,
            health,
            synced,
            fatal_error: _,
            non_fatal_errors: _,
            earliest_ethereum_block_hash,
            earliest_ethereum_block_number,
            latest_ethereum_block_hash,
            latest_ethereum_block_number,
            entity_count,
            graft_base: _,
            graft_block_hash: _,
            graft_block_number: _,
            ..
        } = detail;

        let site = sites
            .iter()
            .find(|site| site.deployment.as_str() == &deployment)
            .ok_or_else(|| constraint_violation!("missing site for subgraph `{}`", deployment))?;

        // This needs to be filled in later since it lives in a
        // different shard
        let chain_head_block = None;
        let earliest_block = block(
            &deployment,
            "earliest_ethereum_block",
            earliest_ethereum_block_hash,
            earliest_ethereum_block_number,
        )?;
        let latest_block = block(
            &deployment,
            "latest_ethereum_block",
            latest_ethereum_block_hash,
            latest_ethereum_block_number,
        )?;
        let health = health.into();
        let chain = status::ChainInfo {
            network: site.network.clone(),
            chain_head_block,
            earliest_block,
            latest_block,
        };
        let entity_count = entity_count.to_u64().ok_or_else(|| {
            constraint_violation!(
                "the entityCount for {} is not representable as a u64",
                deployment
            )
        })?;
        let fatal_error = error.map(|e| SubgraphError::try_from(e)).transpose()?;
        // 'node' needs to be filled in later from a different shard
        Ok(status::Info {
            id: id.into(),
            subgraph: deployment,
            synced,
            health,
            fatal_error,
            non_fatal_errors: vec![],
            chains: vec![chain],
            entity_count,
            node: None,
        })
    }
}

/// Return the details for `deployments`
pub(crate) fn deployment_details(
    conn: &PgConnection,
    deployments: Vec<String>,
) -> Result<Vec<DeploymentDetail>, StoreError> {
    use subgraph_deployment as d;

    // Empty deployments means 'all of them'
    let details = if deployments.is_empty() {
        d::table.load::<DeploymentDetail>(conn)?
    } else {
        d::table
            .filter(d::deployment.eq_any(&deployments))
            .load::<DeploymentDetail>(conn)?
    };
    Ok(details)
}

pub(crate) fn deployment_statuses(
    conn: &PgConnection,
    sites: &Vec<Arc<Site>>,
) -> Result<Vec<status::Info>, StoreError> {
    use subgraph_deployment as d;
    use subgraph_error as e;

    // Empty deployments means 'all of them'
    if sites.is_empty() {
        d::table
            .left_outer_join(e::table.on(d::fatal_error.eq(e::id.nullable())))
            .load::<(DeploymentDetail, Option<ErrorDetail>)>(conn)?
            .into_iter()
            .map(|(detail, error)| status::Info::try_from(DetailAndError(detail, error, sites)))
            .collect()
    } else {
        let ids: Vec<_> = sites.into_iter().map(|site| site.id).collect();

        d::table
            .left_outer_join(e::table.on(d::fatal_error.eq(e::id.nullable())))
            .filter(d::id.eq_any(&ids))
            .load::<(DeploymentDetail, Option<ErrorDetail>)>(conn)?
            .into_iter()
            .map(|(detail, error)| status::Info::try_from(DetailAndError(detail, error, sites)))
            .collect()
    }
}

#[derive(Queryable, QueryableByName, Identifiable, Associations)]
#[table_name = "subgraph_manifest"]
#[belongs_to(GraphNodeVersion)]
// We never read the id field but map it to make the interaction with Diesel
// simpler
#[allow(dead_code)]
struct StoredSubgraphManifest {
    id: i32,
    spec_version: String,
    description: Option<String>,
    repository: Option<String>,
    features: Vec<String>,
    schema: String,
    graph_node_version_id: Option<i32>,
}

impl From<StoredSubgraphManifest> for SubgraphManifestEntity {
    fn from(value: StoredSubgraphManifest) -> Self {
        SubgraphManifestEntity {
            spec_version: value.spec_version,
            description: value.description,
            repository: value.repository,
            features: value.features,
            schema: value.schema,
        }
    }
}

struct StoredDeploymentEntity(crate::detail::DeploymentDetail, StoredSubgraphManifest);

impl TryFrom<StoredDeploymentEntity> for SubgraphDeploymentEntity {
    type Error = StoreError;

    fn try_from(ent: StoredDeploymentEntity) -> Result<Self, Self::Error> {
        let (detail, manifest) = (ent.0, ent.1.into());

        let earliest_block = block(
            &detail.deployment,
            "earliest_block",
            detail.earliest_ethereum_block_hash,
            detail.earliest_ethereum_block_number,
        )?
        .map(|block| block.to_ptr());

        let latest_block = block(
            &detail.deployment,
            "latest_block",
            detail.latest_ethereum_block_hash,
            detail.latest_ethereum_block_number,
        )?
        .map(|block| block.to_ptr());

        let graft_block = block(
            &detail.deployment,
            "graft_block",
            detail.graft_block_hash,
            detail.graft_block_number,
        )?
        .map(|block| block.to_ptr());

        let graft_base = detail
            .graft_base
            .map(|b| DeploymentHash::new(b))
            .transpose()
            .map_err(|b| constraint_violation!("invalid graft base `{}`", b))?;

        Ok(SubgraphDeploymentEntity {
            manifest,
            failed: detail.failed,
            health: detail.health.into(),
            synced: detail.synced,
            fatal_error: None,
            non_fatal_errors: vec![],
            earliest_block,
            latest_block,
            graft_base,
            graft_block,
            reorg_count: detail.reorg_count,
            current_reorg_depth: detail.current_reorg_depth,
            max_reorg_depth: detail.max_reorg_depth,
        })
    }
}

pub fn deployment_entity(
    conn: &PgConnection,
    site: &Site,
) -> Result<SubgraphDeploymentEntity, StoreError> {
    use subgraph_deployment as d;
    use subgraph_manifest as m;

    let manifest = m::table
        .find(site.id)
        .first::<StoredSubgraphManifest>(conn)?;

    let detail = d::table
        .find(site.id)
        .first::<crate::detail::DeploymentDetail>(conn)?;

    SubgraphDeploymentEntity::try_from(StoredDeploymentEntity(detail, manifest))
}

#[derive(Queryable, Identifiable, Insertable)]
#[table_name = "graph_node_versions"]
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
    pub(crate) fn create_or_get(conn: &PgConnection) -> anyhow::Result<i32> {
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
                .execute(conn)?;

            // select the id for the row we just inserted
            g::graph_node_versions
                .select(g::id)
                .filter(g::git_commit_hash.eq(&git_commit_hash))
                .filter(g::git_repository_dirty.eq(git_repository_dirty))
                .filter(g::crate_version.eq(&crate_version))
                .filter(g::major.eq(&major))
                .filter(g::minor.eq(&minor))
                .filter(g::patch.eq(&patch))
                .get_result(conn)?
        };
        Ok(graph_node_version_id)
    }
}
