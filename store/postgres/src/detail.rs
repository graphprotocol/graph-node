//! Queries to support the index node API
use diesel::pg::PgConnection;
use diesel::prelude::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, QueryDsl, RunQueryDsl,
};
use graph::{
    constraint_violation,
    data::subgraph::schema::SubgraphError,
    prelude::{
        bigdecimal::ToPrimitive, BigDecimal, EthereumBlockPointer, StoreError, SubgraphDeploymentId,
    },
};
use graph::{data::subgraph::status, prelude::web3::types::H256};
use std::convert::TryFrom;
use std::{ops::Bound, sync::Arc};

use crate::deployment::{subgraph_deployment, subgraph_error, SubgraphHealth as HealthType};
use crate::primary::Site;

type Bytes = Vec<u8>;

#[derive(Queryable, QueryableByName)]
#[table_name = "subgraph_deployment"]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
#[allow(dead_code)]
pub struct DeploymentDetail {
    vid: i64,
    pub id: String,
    manifest: String,
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
    block_range: (Bound<i32>, Bound<i32>),
}

#[derive(Queryable, QueryableByName)]
#[table_name = "subgraph_error"]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
#[allow(dead_code)]
struct ErrorDetail {
    vid: i64,
    id: String,
    subgraph_id: String,
    message: String,
    block_hash: Option<Bytes>,
    handler: Option<String>,
    deterministic: bool,
    block_range: (Bound<i32>, Bound<i32>),
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
            (Some(number), Some(hash)) => Some(EthereumBlockPointer::from((hash, number as u64))),
            _ => None,
        };
        let subgraph_id = SubgraphDeploymentId::new(subgraph_id).map_err(|id| {
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
            vid: _,
            id,
            manifest: _,
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
            .find(|site| site.deployment.as_str() == &id)
            .ok_or_else(|| constraint_violation!("missing site for subgraph `{}`", id))?;

        // This needs to be filled in later since it lives in a
        // different shard
        let chain_head_block = None;
        let earliest_block = block(
            &id,
            "earliest_ethereum_block",
            earliest_ethereum_block_hash,
            earliest_ethereum_block_number,
        )?;
        let latest_block = block(
            &id,
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
            constraint_violation!("the entityCount for {} is not representable as a u64", id)
        })?;
        let fatal_error = error.map(|e| SubgraphError::try_from(e)).transpose()?;
        // 'node' needs to be filled in later from a different shard
        Ok(status::Info {
            subgraph: id,
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
            .filter(d::id.eq_any(&deployments))
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
        let ids: Vec<_> = sites
            .into_iter()
            .map(|site| site.deployment.to_string())
            .collect();

        d::table
            .left_outer_join(e::table.on(d::fatal_error.eq(e::id.nullable())))
            .filter(d::id.eq_any(&ids))
            .load::<(DeploymentDetail, Option<ErrorDetail>)>(conn)?
            .into_iter()
            .map(|(detail, error)| status::Info::try_from(DetailAndError(detail, error, sites)))
            .collect()
    }
}
