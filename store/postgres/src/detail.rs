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
use graph::{
    data::subgraph::{schema::SubgraphHealth, status},
    prelude::web3::types::H256,
};
use std::ops::Bound;
use std::{convert::TryFrom, str::FromStr};

// This is not a real table, only a view. We can use diesel to read from it
// but write attempts will fail
table! {
    subgraphs.subgraph_deployment_detail (vid) {
        vid -> BigInt,
        id -> Text,
        manifest -> Text,
        failed -> Bool,
        health -> Text,
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
        network -> Text,
        // We don't map block_range
        // block_range -> Range<Integer>,
    }
}

use crate::deployment::subgraph_error;

allow_tables_to_appear_in_same_query!(subgraph_deployment_detail, subgraph_error);

type Bytes = Vec<u8>;

#[derive(Queryable, QueryableByName)]
#[table_name = "subgraph_deployment_detail"]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
#[allow(dead_code)]
pub struct DeploymentDetail {
    pub vid: i64,
    pub id: String,
    pub manifest: String,
    pub failed: bool,
    pub health: String,
    pub synced: bool,
    pub fatal_error: Option<String>,
    pub non_fatal_errors: Vec<String>,
    pub earliest_ethereum_block_hash: Option<Bytes>,
    pub earliest_ethereum_block_number: Option<BigDecimal>,
    pub latest_ethereum_block_hash: Option<Bytes>,
    pub latest_ethereum_block_number: Option<BigDecimal>,
    pub entity_count: BigDecimal,
    pub graft_base: Option<String>,
    pub graft_block_hash: Option<Bytes>,
    pub graft_block_number: Option<BigDecimal>,
    pub network: String,
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

struct DetailAndError(DeploymentDetail, Option<ErrorDetail>);

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

impl TryFrom<DetailAndError> for status::Info {
    type Error = StoreError;

    fn try_from(detail_and_error: DetailAndError) -> Result<Self, Self::Error> {
        let DetailAndError(detail, error) = detail_and_error;

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
            network,
        } = detail;

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
        let health = SubgraphHealth::from_str(&health)?;
        let chain = status::ChainInfo {
            network,
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
    use subgraph_deployment_detail as d;

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
    deployments: Vec<String>,
) -> Result<Vec<status::Info>, StoreError> {
    use subgraph_deployment_detail as d;
    use subgraph_error as e;

    // Empty deployments means 'all of them'
    if deployments.is_empty() {
        d::table
            .left_outer_join(e::table.on(d::fatal_error.eq(e::id.nullable())))
            .load::<(DeploymentDetail, Option<ErrorDetail>)>(conn)?
            .into_iter()
            .map(|(detail, error)| status::Info::try_from(DetailAndError(detail, error)))
            .collect()
    } else {
        d::table
            .left_outer_join(e::table.on(d::fatal_error.eq(e::id.nullable())))
            .filter(d::id.eq_any(&deployments))
            .load::<(DeploymentDetail, Option<ErrorDetail>)>(conn)?
            .into_iter()
            .map(|(detail, error)| status::Info::try_from(DetailAndError(detail, error)))
            .collect()
    }
}
