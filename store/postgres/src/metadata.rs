//! Utilities for dealing with subgraph metadata
use diesel::dsl::{sql, update};
use diesel::pg::PgConnection;
use diesel::prelude::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension, QueryDsl,
    RunQueryDsl,
};
use std::convert::TryFrom;

use graph::data::subgraph::schema::{SubgraphManifestEntity, SUBGRAPHS_ID};
use graph::prelude::{
    bigdecimal::ToPrimitive, format_err, web3::types::H256, BigDecimal, BlockNumber,
    DeploymentState, EntityChange, EntityChangeOperation, EthereumBlockPointer, Schema, StoreError,
    StoreEvent, SubgraphDeploymentEntity, SubgraphDeploymentId, SubgraphName, TypedEntity,
};

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

table! {
    subgraphs.subgraph_deployment (vid) {
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
        reorg_count -> Integer,
        current_reorg_depth -> Integer,
        max_reorg_depth -> Integer,
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
