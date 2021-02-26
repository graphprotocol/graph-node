//! SQL queries to load dynamic data sources

use diesel::{
    delete,
    dsl::sql,
    prelude::{ExpressionMethods, QueryDsl, RunQueryDsl},
    sql_query,
    sql_types::Text,
};
use diesel::{insert_into, pg::PgConnection};

use graph::{
    components::store::StoredDynamicDataSource,
    constraint_violation,
    data::subgraph::Source,
    prelude::{
        bigdecimal::ToPrimitive, web3::types::H160, BigDecimal, BlockNumber, EthereumBlockPointer,
        StoreError, SubgraphDeploymentId,
    },
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
        name -> Text,
        address -> Binary,
        abi -> Text,
        start_block -> Integer,
        // Never read
        ethereum_block_hash -> Binary,
        ethereum_block_number -> Numeric,
        deployment -> Text,
        context -> Nullable<Text>,
    }
}
// END GENERATED CODE

fn to_source(
    deployment: &str,
    vid: i64,
    address: Vec<u8>,
    abi: String,
    start_block: BlockNumber,
) -> Result<Source, StoreError> {
    if address.len() != 20 {
        return Err(constraint_violation!(
            "Data source address 0x`{:?}` for dynamic data source {} in deployment {} should have be 20 bytes long but is {} bytes long",
            address, vid, deployment,
            address.len()
        ));
    }
    let address = Some(H160::from_slice(address.as_slice()));

    // Assume a missing start block is the same as 0
    let start_block = start_block.to_u64().ok_or_else(|| {
        constraint_violation!(
            "Start block {:?} for dynamic data source {} in deployment {} is not a u64",
            start_block,
            vid,
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
            decds::vid,
            decds::name,
            decds::context,
            decds::address,
            decds::abi,
            decds::start_block,
            decds::ethereum_block_number,
        ))
        .order_by((decds::ethereum_block_number, decds::vid))
        .load::<(
            i64,
            String,
            Option<String>,
            Vec<u8>,
            String,
            BlockNumber,
            BigDecimal,
        )>(conn)?;

    let mut data_sources: Vec<StoredDynamicDataSource> = Vec::new();
    for (vid, name, context, address, abi, start_block, creation_block) in dds.into_iter() {
        let source = to_source(id, vid, address, abi, start_block)?;
        let creation_block = creation_block.to_u64();
        let data_source = StoredDynamicDataSource {
            name,
            source,
            context,
            creation_block,
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
            Ok((
                decds::deployment.eq(deployment.as_str()),
                decds::name.eq(name),
                decds::context.eq(context),
                decds::address.eq(address),
                decds::abi.eq(abi),
                decds::start_block.eq(start_block as i32),
                decds::ethereum_block_number.eq(sql(&format!("{}::numeric", block_ptr.number))),
                decds::ethereum_block_hash.eq(block_ptr.hash.as_bytes()),
            ))
        })
        .collect::<Result<_, _>>()?;

    insert_into(decds::table)
        .values(dds)
        .execute(conn)
        .map_err(|e| e.into())
}

pub(crate) fn copy(
    conn: &PgConnection,
    src: &SubgraphDeploymentId,
    dst: &SubgraphDeploymentId,
) -> Result<usize, StoreError> {
    const QUERY: &str = "\
      insert into subgraphs.dynamic_ethereum_contract_data_source(name,
             address, abi, start_block, ethereum_block_hash,
             ethereum_block_number, deployment, context)
      select e.name, e.address, e.abi, e.start_block,
             e.ethereum_block_hash, e.ethereum_block_number, $2 as deployment,
             e.context
        from subgraphs.dynamic_ethereum_contract_data_source e
       where e.deployment = $1";

    Ok(sql_query(QUERY)
        .bind::<Text, _>(src.as_str())
        .bind::<Text, _>(dst.as_str())
        .execute(conn)?)
}

pub(crate) fn revert(
    conn: &PgConnection,
    id: &SubgraphDeploymentId,
    block: BlockNumber,
) -> Result<(), StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    let dds = decds::table.filter(decds::deployment.eq(id.as_str()));
    delete(dds.filter(decds::ethereum_block_number.ge(sql(&block.to_string())))).execute(conn)?;
    Ok(())
}

pub(crate) fn drop(conn: &PgConnection, id: &SubgraphDeploymentId) -> Result<usize, StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    delete(decds::table.filter(decds::deployment.eq(id.as_str())))
        .execute(conn)
        .map_err(|e| e.into())
}
