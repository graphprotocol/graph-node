//! SQL queries to load dynamic data sources

use std::ops::Bound;

use diesel::pg::PgConnection;
use diesel::prelude::{ExpressionMethods, QueryDsl, RunQueryDsl};

use graph::{
    components::store::StoredDynamicDataSource,
    constraint_violation,
    data::subgraph::Source,
    prelude::{bigdecimal::ToPrimitive, web3::types::H160, BlockNumber, StoreError},
};

use crate::block_range::first_block_in_range;

// Diesel tables for some of the metadata
// See also: ed42d219c6704a4aab57ce1ea66698e7
// Changes to the GraphQL schema might require changes to these tables.
// The definitions of the tables can be generated with
//    cargo run -p graph-store-postgres --example layout -- \
//      -g diesel store/postgres/src/subgraphs.graphql subgraphs
// BEGIN GENERATED CODE
table! {
    subgraphs.dynamic_ethereum_contract_data_source (vid) {
        vid -> BigInt,
        id -> Text,
        kind -> Text,
        name -> Text,
        network -> Nullable<Text>,
        address -> Binary,
        abi -> Text,
        start_block -> Integer,
        mapping -> Text,
        ethereum_block_hash -> Binary,
        ethereum_block_number -> Numeric,
        deployment -> Text,
        context -> Nullable<Text>,
        block_range -> Range<Integer>,
    }
}
// END GENERATED CODE

fn to_source(
    deployment: &str,
    ds_id: &str,
    address: Vec<u8>,
    abi: String,
    start_block: BlockNumber,
) -> Result<Source, StoreError> {
    if address.len() != 20 {
        return Err(constraint_violation!(
            "Data source address 0x`{:?}` for dynamic data source {} in deployment {} should have be 20 bytes long but is {} bytes long",
            address, ds_id, deployment,
            address.len()
        ));
    }
    let address = Some(H160::from_slice(address.as_slice()));

    // Assume a missing start block is the same as 0
    let start_block = start_block.to_u64().ok_or_else(|| {
        constraint_violation!(
            "Start block {:?} for dynamic data source {} in deployment {} is not a u64",
            start_block,
            ds_id,
            deployment
        )
    })?;

    Ok(Source {
        address,
        abi,
        start_block,
    })
}

pub fn load(conn: &PgConnection, id: &str) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    // Query to load the data sources. Ordering by the creation block and `vid` makes sure they are
    // in insertion order which is important for the correctness of reverts and the execution order
    // of triggers. See also 8f1bca33-d3b7-4035-affc-fd6161a12448.
    let dds: Vec<_> = decds::table
        .filter(decds::deployment.eq(id))
        .select((
            decds::id,
            decds::name,
            decds::context,
            decds::address,
            decds::abi,
            decds::start_block,
            decds::block_range,
        ))
        .order_by((decds::ethereum_block_number, decds::vid))
        .load::<(
            String,
            String,
            Option<String>,
            Vec<u8>,
            String,
            BlockNumber,
            (Bound<i32>, Bound<i32>),
        )>(conn)?;

    let mut data_sources: Vec<StoredDynamicDataSource> = Vec::new();
    for (ds_id, name, context, address, abi, start_block, range) in dds.into_iter() {
        let source = to_source(id, &ds_id, address, abi, start_block)?;
        let creation_block = first_block_in_range(&range);
        let data_source = StoredDynamicDataSource {
            name,
            source,
            context,
            creation_block: creation_block.map(|n| n as u64),
        };

        if !(data_sources.last().and_then(|d| d.creation_block) <= data_source.creation_block) {
            return Err(StoreError::ConstraintViolation(
                "data sources not ordered by creation block".to_string(),
            ));
        }

        data_sources.push(data_source);
    }
    Ok(data_sources)
}
