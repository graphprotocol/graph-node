use graph::prelude::blockchain::*;
use graph::prelude::*;

use super::network::EthereumNetwork;

pub struct Ethereum {
    options: BlockchainOptions,
}

impl Blockchain for Ethereum {
    type Network = EthereumNetwork;

    fn new(options: BlockchainOptions) -> Self {
        Self { options }
    }

    fn network(&self, name: String) -> Result<Self::Network, Error> {
        let config = self
            .options
            .networks
            .get(&name)
            .ok_or_else(|| format_err!("network `{}` not configured", name))?;

        EthereumNetwork::new(config, self.options.metrics_registry.clone())
    }
}
