use std::sync::Arc;

use async_trait::async_trait;

use graph::prelude::{
    format_err, BlockPointer, ChainStore, CheapClone, Error,
    EthereumAdapter as EthereumAdapterTrait, Future01CompatExt, Logger, MetricsRegistry,
    NetworkInstance, NetworkInstanceId, ProviderEthRpcMetrics, Store,
};

use crate::{EthereumAdapter, Transport};

pub struct Descriptor {
    pub id: NetworkInstanceId,
    pub url: String,
}

pub struct ConnectOptions<'a, MR> {
    pub descriptor: Descriptor,
    pub logger: &'a Logger,
    pub metrics_registry: Arc<MR>,
}

pub struct Connection {
    pub id: NetworkInstanceId,
    pub url: String,
    pub version: String,
    pub genesis_block: BlockPointer,
    pub adapter: Arc<dyn EthereumAdapterTrait>,
}

pub struct ChainOptions<MR, S> {
    pub conn: Connection,
    pub logger: Logger,
    pub store: Arc<S>,
    pub metrics_registry: Arc<MR>,
}

pub fn parse_descriptor(descriptor: &str) -> Result<Descriptor, Error> {
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

    Ok(Descriptor {
        id: NetworkInstanceId {
            network: "ethereum".into(),
            name: name.into(),
        },
        url: loc.into(),
    })
}

pub async fn connect<'a, MR>(options: ConnectOptions<'a, MR>) -> Result<Connection, Error>
where
    MR: MetricsRegistry,
{
    let ConnectOptions {
        descriptor,
        logger,
        metrics_registry,
    } = options;

    // Establish an Ethereum adapter for this chain
    let (transport_event_loop, transport) = Transport::new_rpc(descriptor.url.as_str());
    let adapter_metrics = Arc::new(ProviderEthRpcMetrics::new(metrics_registry));
    let adapter = Arc::new(EthereumAdapter::new(transport, adapter_metrics));

    // Connect to the node and obtain the network ID and version
    let identifier = adapter.net_identifiers(&logger).compat().await?;

    // If we drop the event loop the transport will stop working.
    // For now it's fine to just leak it.
    std::mem::forget(transport_event_loop);

    Ok(Connection {
        id: descriptor.id,
        url: descriptor.url,
        adapter: adapter,
        version: identifier.net_version,
        genesis_block: BlockPointer {
            number: 0,
            hash: identifier.genesis_block_hash.as_bytes().into(),
        },
    })
}

pub struct Chain<S> {
    id: NetworkInstanceId,
    url: String,
    version: String,
    genesis_block: BlockPointer,
    adapter: Arc<dyn EthereumAdapterTrait>,
    store: Arc<S>,
    metrics_registry: Arc<dyn MetricsRegistry>,
}

impl<S> Chain<S>
where
    S: Store + ChainStore,
{
    pub async fn new<MR>(options: ChainOptions<MR, S>) -> Result<Self, Error>
    where
        MR: MetricsRegistry,
    {
        options
            .store
            .initialize_chain(&options.conn.version, &options.conn.genesis_block)?;

        Ok(Self {
            id: options.conn.id,
            url: options.conn.url,
            version: options.conn.version,
            genesis_block: options.conn.genesis_block,
            adapter: options.conn.adapter,
            store: options.store,
            metrics_registry: options.metrics_registry,
        })
    }
}

#[async_trait]
impl<S> NetworkInstance for Chain<S>
where
    S: Store + ChainStore,
{
    fn id(&self) -> &NetworkInstanceId {
        &self.id
    }

    fn url(&self) -> &str {
        self.url.as_str()
    }

    fn compat_ethereum_adapter(&self) -> Option<Arc<dyn EthereumAdapterTrait>> {
        Some(self.adapter.cheap_clone())
    }
}
