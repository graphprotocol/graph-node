//! SQL queries to load dynamic data sources

use diesel::pg::PgConnection;
use diesel::prelude::{ExpressionMethods, JoinOnDsl, QueryDsl, RunQueryDsl};

use graph::{
    components::store::StoredDynamicDataSource,
    constraint_violation,
    data::subgraph::Source,
    prelude::{bigdecimal::ToPrimitive, web3::types::H160, BigDecimal, StoreError},
};

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
        source -> Text,
        mapping -> Text,
        templates -> Nullable<Array<Text>>,
        ethereum_block_hash -> Binary,
        ethereum_block_number -> Numeric,
        deployment -> Text,
        context -> Nullable<Text>,
        block_range -> Range<Integer>,
    }
}

table! {
    subgraphs.ethereum_contract_source (vid) {
        vid -> BigInt,
        id -> Text,
        address -> Nullable<Binary>,
        abi -> Text,
        start_block -> Nullable<Numeric>,
        block_range -> Range<Integer>,
    }
}

// END GENERATED CODE

allow_tables_to_appear_in_same_query!(
    dynamic_ethereum_contract_data_source,
    ethereum_contract_source
);

fn to_source(
    deployment: &str,
    ds_id: &str,
    (address, abi, start_block): (Option<Vec<u8>>, String, Option<BigDecimal>),
) -> Result<Source, StoreError> {
    // Treat a missing address as an error. TODO: Is that correct?
    let address = match address {
        Some(address) => address,
        None => {
            return Err(constraint_violation!(
                "Dynamic data source {} for deployment {} is missing an address",
                ds_id,
                deployment
            ));
        }
    };
    if address.len() != 20 {
        return Err(constraint_violation!(
            "Data source address 0x`{:?}` for dynamic data source {} in deployment {} should have be 20 bytes long but is {} bytes long",
            address, ds_id, deployment,
            address.len()
        ));
    }
    let address = Some(H160::from_slice(address.as_slice()));

    // Assume a missing start block is the same as 0
    let start_block = start_block
        .map(|s| {
            s.to_u64().ok_or_else(|| {
                constraint_violation!(
                    "Start block {:?} for dynamic data source {} in deployment {} is not a u64",
                    s,
                    ds_id,
                    deployment
                )
            })
        })
        .transpose()?
        .unwrap_or(0);

    Ok(Source {
        address,
        abi,
        start_block,
    })
}

#[allow(dead_code)]
pub fn load(conn: &PgConnection, id: &str) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
    use dynamic_ethereum_contract_data_source as decds;
    use ethereum_contract_source as ecs;

    let dds: Vec<_> = decds::table
        .inner_join(ecs::table.on(decds::source.eq(ecs::id)))
        .filter(decds::deployment.eq(id))
        .select((
            decds::id,
            decds::name,
            decds::context,
            (ecs::address, ecs::abi, ecs::start_block),
        ))
        .load::<(
            String,
            String,
            Option<String>,
            (Option<Vec<u8>>, String, Option<BigDecimal>),
        )>(conn)?;

    let mut data_sources = Vec::new();
    for (ds_id, name, context, source) in dds.into_iter() {
        let source = to_source(id, &ds_id, source)?;
        let data_source = StoredDynamicDataSource {
            name,
            source,
            context,
        };
        data_sources.push(data_source);
    }
    Ok(data_sources)
}
