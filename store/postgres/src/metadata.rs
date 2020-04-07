//! Utilities for dealing with subgraph metadata
use diesel::pg::PgConnection;
use diesel::prelude::{ExpressionMethods, QueryDsl, RunQueryDsl};

use graph::prelude::{
    bigdecimal::ToPrimitive, format_err, web3::types::H256, BigDecimal, EthereumBlockPointer,
    StoreError, SubgraphDeploymentId,
};

// Diesel tables for some of the metadata
// See also: ed42d219c6704a4aab57ce1ea66698e7
// Changes to the GraphQL schema might require changes to these tables
table! {
    subgraphs.subgraph_deployment (vid) {
        vid -> BigInt,
        id -> Text,
        manifest -> Text,
        failed -> Bool,
        synced -> Bool,
        earliest_ethereum_block_hash -> Nullable<Binary>,
        earliest_ethereum_block_number -> Nullable<Numeric>,
        latest_ethereum_block_hash -> Nullable<Binary>,
        latest_ethereum_block_number -> Nullable<Numeric>,
        ethereum_head_block_number -> Nullable<Numeric>,
        ethereum_head_block_hash -> Nullable<Binary>,
        total_ethereum_blocks_count -> Numeric,
        entity_count -> Numeric,
        graft_base -> Nullable<Text>,
        graft_block_hash -> Nullable<Binary>,
        graft_block_number -> Nullable<Numeric>,
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
