use diesel::{
    pg::PgConnection,
    prelude::*,
    sql_query,
    sql_types::{Binary, Nullable, Text},
};
use diesel_derives::QueryableByName;
use graph::prelude::web3::types::H256;
use itertools::Itertools;
use std::convert::TryFrom;

use graph::prelude::transaction_receipt::LightTransactionReceipt;

use crate::ETHEREUM_BLOCKS_TABLE_NAME;

/// Queries the database for all the transaction receipts in a given block range.
pub fn find_transaction_receipts_in_block(
    conn: &PgConnection,
    blocks_table_name: &str,
    block_hash: H256,
) -> anyhow::Result<Vec<LightTransactionReceipt>> {
    let query = sql_query(format!(
        "
select
    ethereum_hex_to_bytea(receipt ->> 'transactionHash') as transaction_hash,
    ethereum_hex_to_bytea(receipt ->> 'transactionIndex') as transaction_index,
    ethereum_hex_to_bytea(receipt ->> 'blockHash') as block_hash,
    ethereum_hex_to_bytea(receipt ->> 'blockNumber') as block_number,
    ethereum_hex_to_bytea(receipt ->> 'gasUsed') as gas_used,
    ethereum_hex_to_bytea(receipt ->> 'status') as status
from (
    select
        jsonb_array_elements(data -> 'transaction_receipts') as receipt
    from
        {blocks_table}
    where hash = $1) as temp;
",
        blocks_table = blocks_table_name
    ));

    let query_results: Result<Vec<RawTransactionReceipt>, diesel::result::Error> = {
        // The `hash` column has different types between the `public.ethereum_blocks` and the
        // `chain*.blocks` tables, so we must check which one is being queried to bind the
        // `block_hash` parameter to the correct type
        if blocks_table_name == ETHEREUM_BLOCKS_TABLE_NAME {
            query
                .bind::<Text, _>(block_hash.to_string())
                .get_results(conn)
        } else {
            query
                .bind::<Binary, _>(block_hash.as_bytes())
                .get_results(conn)
        }
    };

    query_results
        .map_err(|error| {
            anyhow::anyhow!(
                "Error fetching transaction receipt from database: {}",
                error
            )
        })?
        .into_iter()
        .map(LightTransactionReceipt::try_from)
        .collect()
}

/// Type that comes straight out of a SQL query
#[derive(QueryableByName)]
struct RawTransactionReceipt {
    #[sql_type = "Binary"]
    transaction_hash: Vec<u8>,
    #[sql_type = "Binary"]
    transaction_index: Vec<u8>,
    #[sql_type = "Nullable<Binary>"]
    block_hash: Option<Vec<u8>>,
    #[sql_type = "Nullable<Binary>"]
    block_number: Option<Vec<u8>>,
    #[sql_type = "Nullable<Binary>"]
    gas_used: Option<Vec<u8>>,
    #[sql_type = "Nullable<Binary>"]
    status: Option<Vec<u8>>,
}

impl TryFrom<RawTransactionReceipt> for LightTransactionReceipt {
    type Error = anyhow::Error;

    fn try_from(value: RawTransactionReceipt) -> Result<Self, Self::Error> {
        let RawTransactionReceipt {
            transaction_hash,
            transaction_index,
            block_hash,
            block_number,
            gas_used,
            status,
        } = value;

        let transaction_hash = drain_vector(transaction_hash)?;
        let transaction_index = drain_vector(transaction_index)?;
        let block_hash = block_hash.map(drain_vector).transpose()?;
        let block_number = block_number.map(drain_vector).transpose()?;
        let gas_used = gas_used.map(drain_vector).transpose()?;
        let status = status.map(drain_vector).transpose()?;

        Ok(LightTransactionReceipt {
            transaction_hash: transaction_hash.into(),
            transaction_index: transaction_index.into(),
            block_hash: block_hash.map(Into::into),
            block_number: block_number.map(Into::into),
            gas_used: gas_used.map(Into::into),
            status: status.map(Into::into),
        })
    }
}

/// Converts Vec<u8> to [u8; N], where N is the vector's expected lenght.
/// Fails if input size is larger than output size.
pub(crate) fn drain_vector<const N: usize>(input: Vec<u8>) -> Result<[u8; N], anyhow::Error> {
    anyhow::ensure!(input.len() <= N, "source is larger than output");
    let mut output = [0u8; N];
    let start = output.len() - input.len();
    output[start..].iter_mut().set_from(input);
    Ok(output)
}

#[test]
fn test_drain_vector() {
    let input = vec![191, 153, 17];
    let expected_output = [0, 0, 0, 0, 0, 191, 153, 17];
    let result = drain_vector(input).expect("failed to drain vector into array");
    assert_eq!(result, expected_output);
}
