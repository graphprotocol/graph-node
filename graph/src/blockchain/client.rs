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
    Firehose(FirehoseEndpoints, Option<C::Client>),
    Rpc(C::Client),
}

impl<C: Blockchain> ChainClient<C> {
    pub fn new_firehose(firehose_endpoints: FirehoseEndpoints) -> Self {
        Self::Firehose(firehose_endpoints, None)
    }

    pub fn new_firehose_with_rpc(firehose_endpoints: FirehoseEndpoints, rpc: C::Client) -> Self {
        Self::Firehose(firehose_endpoints, Some(rpc))
    }

    pub fn new_rpc(rpc: C::Client) -> Self {
        Self::Rpc(rpc)
    }

    pub fn is_firehose(&self) -> bool {
        match self {
            ChainClient::Firehose(_, _) => true,
            ChainClient::Rpc(_) => false,
        }
    }

    pub async fn firehose_endpoint(&self) -> anyhow::Result<Arc<FirehoseEndpoint>> {
        match self {
            ChainClient::Firehose(endpoints, _) => endpoints.endpoint().await,
            _ => Err(anyhow!("firehose endpoint requested on rpc chain client")),
        }
    }

    pub fn rpc(&self) -> anyhow::Result<&C::Client> {
        match self {
            Self::Rpc(rpc) => Ok(rpc),
            Self::Firehose(_, Some(rpc)) => Ok(rpc),
            _ => Err(anyhow!("rpc endpoint requested on firehose chain client")),
        }
    }
}
