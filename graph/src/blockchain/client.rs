use crate::firehose::FirehoseEndpoints;

use super::Blockchain;

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
        Self::new(firehose_endpoints, C::Client::default())
    }
    pub fn new(firehose_endpoints: FirehoseEndpoints, adapters: C::Client) -> Self {
        // If we can get a firehose endpoint then we should prioritise it.
        // the reason we want to test this by getting an adapter is because
        // adapter limits in the configuration can effectively disable firehose
        // by setting a limit to 0.
        // In this case we should fallback to an rpc client.
        let firehose_available = firehose_endpoints.random().is_ok();

        match firehose_available {
            true => Self::Firehose(firehose_endpoints),
            false => Self::Rpc(adapters),
        }
    }

    pub fn is_firehose(&self) -> bool {
        match self {
            ChainClient::Firehose(_) => true,
            ChainClient::Rpc(_) => false,
        }
    }
}
