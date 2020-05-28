use futures::Stream;
use serde::de::{Deserializer, Error as DeserializerError};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use web3::types::H256;

/// Deserialize an H256 hash (with or without '0x' prefix).
fn deserialize_h256<'de, D>(deserializer: D) -> Result<H256, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let block_hash = s.trim_start_matches("0x");
    H256::from_str(block_hash).map_err(D::Error::custom)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainHeadUpdate {
    pub network_name: String,
    #[serde(deserialize_with = "deserialize_h256")]
    pub head_block_hash: H256,
    pub head_block_number: u64,
}

/// The updates have no payload, receivers should call `Store::chain_head_ptr`
/// to check what the latest block is.
pub type ChainHeadUpdateStream = Box<dyn Stream<Item = (), Error = ()> + Send>;

pub trait ChainHeadUpdateListener {
    // Subscribe to chain head updates for the given network.
    fn subscribe(&self, network: String) -> ChainHeadUpdateStream;
}
