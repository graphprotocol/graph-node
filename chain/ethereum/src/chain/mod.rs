use std::sync::Arc;

use async_trait::async_trait;

use graph::prelude::{
    format_err, info, Error, EthereumAdapter as EthereumAdapterTrait, Logger, MetricsRegistry,
    NetworkInstance, NetworkInstanceId, ProviderEthRpcMetrics,
};

use crate::{EthereumAdapter, Transport};

pub struct ChainOptions<T: MetricsRegistry> {
    pub logger: Logger,
    pub metrics_registry: Arc<T>,
}

pub struct Chain {
    id: NetworkInstanceId,
    url: String,
    adapter: Arc<dyn EthereumAdapterTrait>,
}

impl Chain {
    pub async fn from_descriptor<T: MetricsRegistry>(
        descriptor: &str,
        options: ChainOptions<T>,
    ) -> Result<Self, Error> {
        // Try to parse the "mainnet:http://..." type descriptor into a network
        // instance ID and URL
        let (id, url) = parse_descriptor(descriptor)?;

        info!(
            options.logger,
            "Add Ethereum chain";
            "name" => &id.name,
            "url" => &url,
        );

        // Establish an Ethereum adapter for this chain
        let (transport_event_loop, transport) = Transport::new_rpc(url.as_str());
        let adapter_metrics = Arc::new(ProviderEthRpcMetrics::new(options.metrics_registry));
        let adapter = Arc::new(EthereumAdapter::new(transport, adapter_metrics));

        // If we drop the event loop the transport will stop working.
        // For now it's fine to just leak it.
        std::mem::forget(transport_event_loop);

        Ok(Self { id, url, adapter })
    }
}

#[async_trait]
impl NetworkInstance for Chain {
    fn id(&self) -> &NetworkInstanceId {
        &self.id
    }

    fn url(&self) -> &str {
        self.url.as_str()
    }

    fn compat_ethereum_adapter(&self) -> Option<Arc<dyn EthereumAdapterTrait>> {
        Some(self.adapter.clone())
    }
}

fn parse_descriptor(descriptor: &str) -> Result<(NetworkInstanceId, String), Error> {
    if descriptor.starts_with("wss://")
        || descriptor.starts_with("http://")
        || descriptor.starts_with("https://")
    {
        return Err(format_err!(
            "Is your Ethereum node string missing a chain name? \
                 Try 'mainnet:' + the Ethereum node URL."
        ));
    }

    // Parse string (format is "NETWORK_NAME:URL")
    let split_at = descriptor.find(':').ok_or_else(|| {
        format_err!(
            "A chain name must be provided alongside the \
                 Ethereum node location. Try e.g. 'mainnet:URL'."
        )
    })?;

    let (name, loc_with_delim) = descriptor.split_at(split_at);
    let loc = &loc_with_delim[1..];

    if name.is_empty() {
        Err(format_err!("Ethereum chain name cannot be an empty string"))?;
    }

    if loc.is_empty() {
        Err(format_err!("Ethereum node URL cannot be an empty string"))?;
    }

    Ok((
        NetworkInstanceId {
            network: "ethereum".into(),
            name: name.into(),
        },
        loc.into(),
    ))
}
