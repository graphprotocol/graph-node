//! SQL queries to load dynamic data sources

use std::ops::Bound;

use diesel::{
    dsl::sql,
    prelude::{ExpressionMethods, QueryDsl, RunQueryDsl},
};
use diesel::{insert_into, pg::PgConnection};

use graph::{
    components::store::StoredDynamicDataSource,
    constraint_violation,
    data::subgraph::Source,
    prelude::{
        bigdecimal::ToPrimitive, web3::types::H160, BlockNumber, EthereumBlockPointer, StoreError,
        SubgraphDeploymentId,
    },
};
use uuid::Uuid;

use crate::block_range::{first_block_in_range, BlockRange};

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
        name -> Text,
        address -> Binary,
        abi -> Text,
        start_block -> Integer,
        // Never read
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

pub(crate) fn make_id() -> String {
    format!("{}-dynamic", Uuid::new_v4().to_simple())
}

pub(crate) fn insert(
    conn: &PgConnection,
    deployment: &SubgraphDeploymentId,
    data_sources: Vec<StoredDynamicDataSource>,
    block_ptr: &EthereumBlockPointer,
) -> Result<usize, StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    let dds: Vec<_> = data_sources
        .into_iter()
        .map(|ds| {
            let StoredDynamicDataSource {
                name,
                source:
                    Source {
                        address,
                        abi,
                        start_block,
                    },
                context,
                creation_block: _,
            } = ds;
            // Why Option???
            let address = match address {
                Some(address) => address.as_bytes().to_vec(),
                None => {
                    return Err(constraint_violation!(
                        "dynamic data sources must have an address, but `{}` has none",
                        name
                    ));
                }
            };
            let range = block_ptr.number as i32..;
            Ok((
                decds::deployment.eq(deployment.as_str()),
                decds::id.eq(make_id()),
                decds::name.eq(name),
                decds::context.eq(context),
                decds::address.eq(address),
                decds::abi.eq(abi),
                decds::start_block.eq(start_block as i32),
                decds::ethereum_block_number.eq(sql(&format!("{}::numeric", block_ptr.number))),
                decds::ethereum_block_hash.eq(block_ptr.hash.as_bytes()),
                decds::block_range.eq(BlockRange::from(range).as_pair()),
            ))
        })
        .collect::<Result<_, _>>()?;

    insert_into(decds::table)
        .values(dds)
        .execute(conn)
        .map_err(|e| e.into())
}
