use futures::prelude::*;
use serde::de::{Deserialize, Deserializer, Error as DeserializerError};
use std::str::FromStr;
use web3::types::H256;

use components::EventProducer;

/// Deserialize an H256 hash (with or without '0x' prefix).
fn deserialize_h256<'de, D>(deserializer: D) -> Result<H256, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let block_hash = s.trim_left_matches("0x");
    H256::from_str(block_hash).map_err(D::Error::custom)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainHeadUpdate {
    pub network_name: String,
    #[serde(deserialize_with = "deserialize_h256")]
    pub head_block_hash: H256,
    pub head_block_number: u64,
}

pub trait ChainHeadUpdateListener: EventProducer<ChainHeadUpdate> {
    /// Begin processing notifications coming in from Postgres.
    fn start(&mut self);
}
