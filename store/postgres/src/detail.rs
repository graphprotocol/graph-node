//! Queries to support the index node API
use diesel::pg::PgConnection;
use diesel::prelude::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension, QueryDsl,
    RunQueryDsl,
};
use graph::{
    data::subgraph::schema::SubgraphError,
    prelude::{bigdecimal::ToPrimitive, BigDecimal, StoreError, SubgraphDeploymentId},
};
use graph::{
    data::subgraph::{schema::SubgraphHealth, status},
    prelude::web3::types::H256,
};
use std::{convert::TryFrom, str::FromStr};

use crate::metadata::{subgraph, subgraph_version};

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
        ethereum_head_block_hash -> Nullable<Binary>,
        ethereum_head_block_number -> Nullable<Numeric>,
        network -> Text,
        node_id -> Nullable<Text>,
        // We don't map block_range
        // block_range -> Range<Integer>,
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
        // We don't map block_range
        // block_range -> Range<Integer>,
    }
}

allow_tables_to_appear_in_same_query!(subgraph_deployment_detail, subgraph_error);

type Bytes = Vec<u8>;

#[derive(Queryable, QueryableByName)]
#[table_name = "subgraph_deployment_detail"]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
#[allow(dead_code)]
struct Detail {
    vid: i64,
    id: String,
    manifest: String,
    failed: bool,
    health: String,
    synced: bool,
    fatal_error: Option<String>,
    non_fatal_errors: Vec<String>,
    earliest_ethereum_block_hash: Option<Bytes>,
    earliest_ethereum_block_number: Option<BigDecimal>,
    latest_ethereum_block_hash: Option<Bytes>,
    latest_ethereum_block_number: Option<BigDecimal>,
    entity_count: BigDecimal,
    graft_base: Option<String>,
    graft_block_hash: Option<Bytes>,
    graft_block_number: Option<BigDecimal>,
    ethereum_head_block_hash: Option<Bytes>,
    ethereum_head_block_number: Option<BigDecimal>,
    network: String,
    node_id: Option<String>,
}

#[derive(Queryable, QueryableByName)]
#[table_name = "subgraph_error"]
// We map all fields to make loading `Detail` with diesel easier, but we
// don't need all the fields
#[allow(dead_code)]
struct ErrorDetail {
    vid: i64,
    id: String,
    subgraph_id: Option<String>,
    message: String,
    block_number: Option<BigDecimal>,
    block_hash: Option<Bytes>,
    handler: Option<String>,
    deterministic: bool,
}

struct DetailAndError(Detail, Option<ErrorDetail>);

fn block(
    id: &str,
    name: &str,
    hash: Option<Vec<u8>>,
    number: Option<BigDecimal>,
) -> Result<Option<status::EthereumBlock>, StoreError> {
    match (&hash, &number) {
        (Some(hash), Some(number)) => {
            let hash = H256::from_slice(hash.as_slice());
            let number = number.to_u64().ok_or_else(|| {
                StoreError::ConstraintViolation(format!(
                    "the block number {} for {} in {} is not representable as a u64",
                    number, name, id
                ))
            })?;
            Ok(Some(status::EthereumBlock::new(hash, number)))
        }
        (None, None) => Ok(None),
        _ => Err(StoreError::ConstraintViolation(format!(
            "the hash and number \
        of a block pointer must either both be null or both have a \
        value, but for `{}` the hash of {} is `{:?}` and the number is `{:?}`",
            id, name, hash, number
        ))),
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
            block_number,
            block_hash,
            handler,
            deterministic,
        } = value;
        let block_ptr = block(
            subgraph_id.as_deref().unwrap_or("unknown"),
            "fatal_error",
            block_hash,
            block_number,
        )?
        .map(|block| block.to_ptr());
        let subgraph_id = subgraph_id
            .map(|id| SubgraphDeploymentId::new(id))
            .transpose()
            .map_err(|id| {
                StoreError::ConstraintViolation(format!(
                    "invalid subgraph id `{}` in fatal error",
                    id
                ))
            })?
            .ok_or_else(|| {
                StoreError::ConstraintViolation(format!("missing subgraph id for fatal error"))
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

        let Detail {
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
            ethereum_head_block_hash,
            ethereum_head_block_number,
            network,
            node_id,
        } = detail;

        let chain_head_block = block(
            &id,
            "ethereum_head_block",
            ethereum_head_block_hash,
            ethereum_head_block_number,
        )?;
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
            StoreError::ConstraintViolation(format!(
                "the entityCount for {} is not representable as a u64",
                id
            ))
        })?;
        let fatal_error = error.map(|e| SubgraphError::try_from(e)).transpose()?;
        Ok(status::Info {
            subgraph: id,
            synced,
            health,
            fatal_error,
            non_fatal_errors: vec![],
            chains: vec![chain],
            entity_count,
            node: node_id,
        })
    }
}

pub(crate) fn deployments_for_subgraph(
    conn: &PgConnection,
    name: String,
) -> Result<Vec<String>, StoreError> {
    use subgraph as s;
    use subgraph_version as v;

    Ok(v::table
        .inner_join(s::table.on(v::subgraph.eq(s::id)))
        .filter(s::name.eq(&name))
        .order_by(v::created_at.asc())
        .select(v::deployment)
        .load(conn)?)
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
            .load::<(Detail, Option<ErrorDetail>)>(conn)?
            .into_iter()
            .map(|(detail, error)| status::Info::try_from(DetailAndError(detail, error)))
            .collect()
    } else {
        d::table
            .left_outer_join(e::table.on(d::fatal_error.eq(e::id.nullable())))
            .filter(d::id.eq_any(&deployments))
            .load::<(Detail, Option<ErrorDetail>)>(conn)?
            .into_iter()
            .map(|(detail, error)| status::Info::try_from(DetailAndError(detail, error)))
            .collect()
    }
}

pub fn subgraph_version(
    conn: &PgConnection,
    name: String,
    use_current: bool,
) -> Result<Option<String>, StoreError> {
    use subgraph as s;
    use subgraph_version as v;

    let deployment = if use_current {
        v::table
            .select(v::deployment.nullable())
            .inner_join(s::table.on(s::current_version.eq(v::id.nullable())))
            .filter(s::name.eq(&name))
            .first::<Option<String>>(conn)
    } else {
        v::table
            .select(v::deployment.nullable())
            .inner_join(s::table.on(s::pending_version.eq(v::id.nullable())))
            .filter(s::name.eq(&name))
            .first::<Option<String>>(conn)
    };
    Ok(deployment.optional()?.flatten())
}
