//! SQL queries to load dynamic data sources

use diesel::{
    delete,
    dsl::{count, sql},
    prelude::{ExpressionMethods, QueryDsl, RunQueryDsl},
    sql_query,
    sql_types::{Integer, Text},
};
use diesel::{insert_into, pg::PgConnection};

use graph::{
    components::store::StoredDynamicDataSource,
    constraint_violation,
    data::subgraph::Source,
    prelude::{
        bigdecimal::ToPrimitive, web3::types::H160, BigDecimal, BlockNumber, BlockPtr,
        DeploymentHash, StoreError,
    },
};

use crate::connection_pool::ForeignServer;
use crate::primary::Site;

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

    Ok(Source {
        address,
        abi,
        start_block,
    })
}

pub fn load(
    conn: &PgConnection,
    id: &str,
    block: BlockNumber,
) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
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
        .filter(decds::ethereum_block_number.le(sql(&format!("{}::numeric", block))))
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
        let creation_block = creation_block.to_i32();
        let data_source = StoredDynamicDataSource {
            name,
            source,
            context,
            creation_block,
        };

        if data_sources.last().and_then(|d| d.creation_block) > data_source.creation_block {
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
    deployment: &DeploymentHash,
    data_sources: &[StoredDynamicDataSource],
    block_ptr: &BlockPtr,
) -> Result<usize, StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    if data_sources.is_empty() {
        // Avoids a roundtrip to the DB.
        return Ok(0);
    }

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
                decds::start_block.eq(start_block),
                decds::ethereum_block_number.eq(sql(&format!("{}::numeric", block_ptr.number))),
                decds::ethereum_block_hash.eq(block_ptr.hash_slice()),
            ))
        })
        .collect::<Result<_, _>>()?;

    insert_into(decds::table)
        .values(dds)
        .execute(conn)
        .map_err(|e| e.into())
}

/// Copy the dynamic data sources for `src` to `dst`. All data sources that
/// were created up to and including `target_block` will be copied.
pub(crate) fn copy(
    conn: &PgConnection,
    src: &Site,
    dst: &Site,
    target_block: &BlockPtr,
) -> Result<usize, StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    let src_nsp = if src.shard == dst.shard {
        "subgraphs".to_string()
    } else {
        ForeignServer::metadata_schema(&src.shard)
    };

    // Check whether there are any dynamic data sources for dst which
    // indicates we already did copy
    let count = decds::table
        .filter(decds::deployment.eq(dst.deployment.as_str()))
        .select(count(decds::vid))
        .get_result::<i64>(conn)?;
    if count > 0 {
        return Ok(count as usize);
    }

    let query = format!(
        "\
      insert into subgraphs.dynamic_ethereum_contract_data_source(name,
             address, abi, start_block, ethereum_block_hash,
             ethereum_block_number, deployment, context)
      select e.name, e.address, e.abi, e.start_block,
             e.ethereum_block_hash, e.ethereum_block_number, $2 as deployment,
             e.context
        from {src_nsp}.dynamic_ethereum_contract_data_source e
       where e.deployment = $1
         and e.ethereum_block_number <= $3",
        src_nsp = src_nsp
    );

    Ok(sql_query(&query)
        .bind::<Text, _>(src.deployment.as_str())
        .bind::<Text, _>(dst.deployment.as_str())
        .bind::<Integer, _>(target_block.number)
        .execute(conn)?)
}

pub(crate) fn revert(
    conn: &PgConnection,
    id: &DeploymentHash,
    block: BlockNumber,
) -> Result<(), StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    let dds = decds::table.filter(decds::deployment.eq(id.as_str()));
    delete(dds.filter(decds::ethereum_block_number.ge(sql(&block.to_string())))).execute(conn)?;
    Ok(())
}

pub(crate) fn drop(conn: &PgConnection, id: &DeploymentHash) -> Result<usize, StoreError> {
    use dynamic_ethereum_contract_data_source as decds;

    delete(decds::table.filter(decds::deployment.eq(id.as_str())))
        .execute(conn)
        .map_err(|e| e.into())
}
