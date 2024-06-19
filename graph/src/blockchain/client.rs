use std::sync::Arc;

use super::Blockchain;
use crate::firehose::{FirehoseEndpoint, FirehoseEndpoints};
use anyhow::anyhow;

// EthereumClient represents the mode in which the ethereum chain block can be retrieved,
// alongside their requirements.
// Rpc requires an rpc client which have different `NodeCapabilities`
// Firehose requires FirehoseEndpoints and an adapter that can at least resolve eth calls
// Substreams only requires the FirehoseEndpoints.
#[derive(Debug)]
pub enum ChainClient<C: Blockchain> {
    Firehose(FirehoseEndpoints),
    Rpc(C::Client),
}

impl<C: Blockchain> ChainClient<C> {
    pub fn new_firehose(firehose_endpoints: FirehoseEndpoints) -> Self {
        Self::Firehose(firehose_endpoints)
    }

    pub fn new_rpc(rpc: C::Client) -> Self {
        Self::Rpc(rpc)
    }

    pub fn is_firehose(&self) -> bool {
        match self {
            ChainClient::Firehose(_) => true,
            ChainClient::Rpc(_) => false,
        }
    }

    pub async fn firehose_endpoint(&self) -> anyhow::Result<Arc<FirehoseEndpoint>> {
        match self {
            ChainClient::Firehose(endpoints) => endpoints.endpoint().await,
            _ => Err(anyhow!("firehose endpoint requested on rpc chain client")),
        }
    }

    pub fn rpc(&self) -> anyhow::Result<&C::Client> {
        match self {
            Self::Rpc(rpc) => Ok(rpc),
            _ => Err(anyhow!("rpc endpoint requested on firehose chain client")),
        }
    }
}
