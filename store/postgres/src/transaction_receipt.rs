use diesel::sql_types::{Binary, Nullable};
use diesel_derives::QueryableByName;
use graph::prelude::transaction_receipt::LightTransactionReceipt;
use itertools::Itertools;
use std::convert::TryFrom;

/// Type that comes straight out of a SQL query
#[derive(QueryableByName)]
pub(crate) struct RawTransactionReceipt {
    #[diesel(sql_type = Binary)]
    transaction_hash: Vec<u8>,
    #[diesel(sql_type = Binary)]
    transaction_index: Vec<u8>,
    #[diesel(sql_type = Nullable<Binary>)]
    block_hash: Option<Vec<u8>>,
    #[diesel(sql_type = Nullable<Binary>)]
    block_number: Option<Vec<u8>>,
    #[diesel(sql_type = Nullable<Binary>)]
    gas_used: Option<Vec<u8>>,
    #[diesel(sql_type = Nullable<Binary>)]
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

        // Convert big-endian bytes to numbers
        let transaction_index = u64::from_be_bytes(transaction_index);
        let block_number = block_number.map(u64::from_be_bytes);
        let gas_used = gas_used.map(u64::from_be_bytes).unwrap_or(0);

        // Handle both old U64 format and new boolean format
        let status = status
            .map(|bytes| {
                match bytes.len() {
                    1 => bytes[0] != 0, // New format: single byte
                    8 => {
                        u64::from_be_bytes(drain_vector::<8>(bytes.to_vec()).unwrap_or([0; 8])) != 0
                    } // Old format: U64
                    _ => false,         // Fallback
                }
            })
            .unwrap_or(false);

        Ok(LightTransactionReceipt {
            transaction_hash: transaction_hash.into(),
            transaction_index,
            block_hash: block_hash.map(Into::into),
            block_number,
            gas_used,
            status,
        })
    }
}

/// Converts Vec<u8> to [u8; N], where N is the vector's expected length.
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
