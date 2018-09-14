use futures::prelude::*;
use web3::types::H256;

use components::EventProducer;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainHeadUpdate {
    pub network_name: String,
    pub head_block_hash: H256,
    pub head_block_number: u64,
}

pub trait ChainHeadUpdateListener: EventProducer<ChainHeadUpdate> {
    /// Begin processing notifications coming in from Postgres.
    fn start(&mut self);
}
