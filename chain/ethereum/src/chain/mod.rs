use std::sync::Arc;

use async_trait::async_trait;

use graph::prelude::{
    format_err, info, BlockPointer, BlockchainStore, BlockchainStoreId, ChainStore, CheapClone,
    Error, EthereumAdapter as EthereumAdapterTrait, Future01CompatExt, Logger, MetricsRegistry,
    NetworkInstance, NetworkInstanceId, NetworkStoreFactory, ProviderEthRpcMetrics,
};
use graph::util::security::SafeDisplay;

use crate::{EthereumAdapter, Transport};

pub struct ChainOptions<MR, NSF> {
    pub logger: Logger,
    pub store_factory: Arc<NSF>,
    pub metrics_registry: Arc<MR>,
}

pub struct Chain {
    id: NetworkInstanceId,
    url: String,
    adapter: Arc<dyn EthereumAdapterTrait>,
    store: Arc<Box<dyn BlockchainStore>>,
}

impl Chain {
    pub async fn from_descriptor<MR, NSF>(
        descriptor: &str,
        options: ChainOptions<MR, NSF>,
    ) -> Result<Self, Error>
    where
        MR: MetricsRegistry,
        NSF: NetworkStoreFactory,
    {
        // Try to parse the "mainnet:http://..." type descriptor into a network
        // instance ID and URL
        let (id, url) = parse_descriptor(descriptor)?;

        info!(
            options.logger,
            "Add Ethereum chain";
            "url" => SafeDisplay(&url),
            "name" => &id.name,
        );

        // Establish an Ethereum adapter for this chain
        let (transport_event_loop, transport) = Transport::new_rpc(url.as_str());
        let adapter_metrics = Arc::new(ProviderEthRpcMetrics::new(options.metrics_registry));
        let adapter = Arc::new(EthereumAdapter::new(transport, adapter_metrics));

        info!(
            options.logger,
            "Connecting to Ethereum chain";
            "url" => SafeDisplay(&url),
            "name" => &id.name,
        );

        // Connect to the node and obtain the network ID and version
        let identifier = adapter
            .net_identifiers(&options.logger)
            .compat()
            .await
            .map_err(|e| {
                format_err!(
                    "Failed to connect to Ethereum chain `{}` at `{}`: {}",
                    id.name,
                    SafeDisplay(&url),
                    e
                )
            })?;

        info!(
             options.logger,
             "Successfully connected to Ethereum chain";
             "url" => SafeDisplay(&url),
             "name" => &id.name,
             "net_version" => &identifier.net_version,
        );

        // Establish a store for this chain
        let store = options
            .store_factory
            .blockchain_store(&BlockchainStoreId {
                network_instance: id.clone(),
                genesis_block: BlockPointer {
                    number: 0,
                    hash: identifier.genesis_block_hash.as_bytes().into(),
                },
            })
            .await?;

        // If we drop the event loop the transport will stop working.
        // For now it's fine to just leak it.
        std::mem::forget(transport_event_loop);

        Ok(Self {
            id,
            url,
            adapter,
            store,
        })
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
        Some(self.adapter.cheap_clone())
    }

    fn compat_blockchain_store(&self) -> Option<Arc<Box<dyn BlockchainStore>>> {
        Some(self.store.cheap_clone())
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
