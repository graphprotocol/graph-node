//! Code for retrieving transaction receipts from the database.
//!
//! This module exposes:
//! 1. the [`find_transaction_receipts_in_block`] function, that queries the database and returns
//!    transaction receipts present in a given block.
//! 2. the [`LightTransactionReceipt`] type, which holds basic information about the retrieved
//!    transaction receipts.

use diesel::{
    pg::{Pg, PgConnection},
    prelude::*,
    query_builder::{Query, QueryFragment},
    sql_types::{Binary, Nullable},
};
use diesel_derives::{Queryable, QueryableByName};
use itertools::Itertools;
use std::convert::TryFrom;
use web3::types::*;

/// Parameters for querying for all transaction receipts of a given block.
struct TransactionReceiptQuery<'a> {
    block_hash: &'a [u8],
    schema_name: &'a str,
}

impl<'a> diesel::query_builder::QueryId for TransactionReceiptQuery<'a> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> QueryFragment<Pg> for TransactionReceiptQuery<'a> {
    /// Writes the following SQL:
    ///
    /// ```sql
    /// select
    ///     ethereum_hex_to_bytea(receipt ->> 'transactionHash') as transaction_hash,
    ///     ethereum_hex_to_bytea(receipt ->> 'transactionIndex') as transaction_index,
    ///     ethereum_hex_to_bytea(receipt ->> 'blockHash') as block_hash,
    ///     ethereum_hex_to_bytea(receipt ->> 'blockNumber') as block_number,
    ///     ethereum_hex_to_bytea(receipt ->> 'gasUsed') as gas_used,
    ///     ethereum_hex_to_bytea(receipt ->> 'status') as status
    /// from (
    ///     select
    ///         jsonb_array_elements(data -> 'transaction_receipts') as receipt
    ///     from
    ///         $CHAIN_SCHEMA.blocks
    ///     where hash = $BLOCK_HASH) as temp;
    ///```
    fn walk_ast(&self, mut out: diesel::query_builder::AstPass<Pg>) -> QueryResult<()> {
        out.push_sql(
            r#"
select
    ethereum_hex_to_bytea(receipt ->> 'transactionHash') as transaction_hash,
    ethereum_hex_to_bytea(receipt ->> 'transactionIndex') as transaction_index,
    ethereum_hex_to_bytea(receipt ->> 'blockHash') as block_hash,
    ethereum_hex_to_bytea(receipt ->> 'blockNumber') as block_number,
    ethereum_hex_to_bytea(receipt ->> 'gasUsed') as gas_used,
    ethereum_hex_to_bytea(receipt ->> 'status') as status
from (
    select jsonb_array_elements(data -> 'transaction_receipts') as receipt
    from"#,
        );
        out.push_identifier(&self.schema_name)?;
        out.push_sql(".");
        out.push_identifier("blocks")?;
        out.push_sql(" where hash = ");
        out.push_bind_param::<Binary, _>(&self.block_hash)?;
        out.push_sql(") as temp;");
        Ok(())
    }
}

impl<'a> Query for TransactionReceiptQuery<'a> {
    type SqlType = (
        Binary,
        Binary,
        Nullable<Binary>,
        Nullable<Binary>,
        Nullable<Binary>,
        Nullable<Binary>,
    );
}

impl<'a> RunQueryDsl<PgConnection> for TransactionReceiptQuery<'a> {}

/// Type that comes straight out of a SQL query
#[derive(QueryableByName, Queryable)]
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

/// Like web3::types::Receipt, but with fewer fields.
pub struct LightTransactionReceipt {
    pub transaction_hash: H256,
    pub transaction_index: U64,
    pub block_hash: Option<H256>,
    pub block_number: Option<U64>,
    pub gas_used: Option<U256>,
    pub status: Option<U64>,
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

/// Queries the database for all the transaction receipts in a given block range.
pub fn find_transaction_receipts_in_block(
    conn: &PgConnection,
    schema_name: &str,
    block_hash: &H256,
) -> anyhow::Result<Vec<LightTransactionReceipt>> {
    let query = TransactionReceiptQuery {
        schema_name,
        block_hash: block_hash.as_bytes(),
    };

    query
        .get_results::<RawTransactionReceipt>(conn)
        .or_else(|error| {
            Err(anyhow::anyhow!(
                "Error fetching transaction receipt from database: {}",
                error
            ))
        })?
        .into_iter()
        .map(LightTransactionReceipt::try_from)
        .collect()
}

impl From<TransactionReceipt> for LightTransactionReceipt {
    fn from(receipt: TransactionReceipt) -> Self {
        let TransactionReceipt {
            transaction_hash,
            transaction_index,
            block_hash,
            block_number,
            gas_used,
            status,
            ..
        } = receipt;
        LightTransactionReceipt {
            transaction_hash,
            transaction_index,
            block_hash,
            block_number,
            gas_used,
            status,
        }
    }
}
