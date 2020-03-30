use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::prelude::*;
use lazy_static;

use futures03::future::AbortHandle;
use graph::prelude::{
    format_err, futures03, info, o, BlockPointer, ChainStore, CheapClone, Error,
    EthereumAdapter as EthereumAdapterTrait, EthereumCallCache, EventProducer, Future01CompatExt,
    LinkResolver, Logger, LoggerFactory, MetricsRegistry, NetworkInstance, NetworkInstanceId,
    ProviderEthRpcMetrics, Store, SubgraphDeploymentStore,
    SubgraphInstanceManager as SubgraphInstanceManagerTrait, SubgraphManifest,
};
use graph_core::SubgraphInstanceManager;
use graph_runtime_wasm::RuntimeHostBuilder as WASMRuntimeHostBuilder;

use crate::{
    network_indexer::NetworkIndexer, BlockIngestor, BlockStreamBuilder, EthereumAdapter, Transport,
};

lazy_static! {
    // Default to an Ethereum reorg threshold to 50 blocks
    static ref REORG_THRESHOLD: u64 = env::var("ETHEREUM_REORG_THRESHOLD")
        .ok()
        .map(|s| u64::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_REORG_THRESHOLD")))
        .unwrap_or(50);

    // Default to an ancestor count of 50 blocks
    static ref ANCESTOR_COUNT: u64 = env::var("ETHEREUM_ANCESTOR_COUNT")
        .ok()
        .map(|s| u64::from_str(&s)
             .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_ANCESTOR_COUNT")))
        .unwrap_or(50);

    // Default to a block polling interval of 500ms
    static ref POLLING_INTERVAL: Duration = env::var("ETHEREUM_POLLING_INTERVAL")
        .ok()
        .map(|s| u64::from_str(&s).unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_POLLING_INTERVAL")))
        .map(|n| Duration::from_millis(n))
        .unwrap_or(Duration::from_millis(500));
}

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

pub struct ChainOptions<'a, MR, S> {
    pub conn: Connection,
    pub logger_factory: &'a LoggerFactory,
    pub store: Arc<S>,
    pub link_resolver: Arc<dyn LinkResolver>,
    pub metrics_registry: Arc<MR>,
    pub block_ingestion: bool,
    pub chain_indexing: bool,
}

pub struct Chain<S> {
    id: NetworkInstanceId,
    url: String,
    _version: String,
    _genesis_block: BlockPointer,

    logger: Logger,
    adapter: Arc<dyn EthereumAdapterTrait>,
    store: Arc<S>,
    metrics_registry: Arc<dyn MetricsRegistry>,

    subgraph_instance_manager: Box<dyn SubgraphInstanceManagerTrait>,
}

impl<S> Chain<S>
where
    S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
{
    pub async fn new<'a, MR>(options: ChainOptions<'a, MR, S>) -> Result<Self, Error>
    where
        MR: MetricsRegistry,
    {
        options
            .store
            .initialize_chain(&options.conn.version, &options.conn.genesis_block)?;

        let logger = options
            .logger_factory
            .component_logger("EthereumChain", None)
            .new(o!(
                "chain" => format!("{}", options.conn.id),
            ));
        let logger_factory = options.logger_factory.with_parent(logger.clone());

        let block_stream_builder = BlockStreamBuilder::new(
            options.store.clone(),
            options.store.clone(),
            options.conn.adapter.clone(),
            *REORG_THRESHOLD,
            options.metrics_registry.clone(),
        );

        let runtime_host_builder = WASMRuntimeHostBuilder::new(
            options.conn.adapter.clone(),
            options.link_resolver,
            options.store.clone(),
        );

        let subgraph_instance_manager = Box::new(SubgraphInstanceManager::new(
            &logger_factory,
            options.store.clone(),
            options.conn.adapter.clone(),
            runtime_host_builder,
            block_stream_builder,
            options.metrics_registry.clone(),
        ));

        let this = Self {
            // Properties
            id: options.conn.id,
            url: options.conn.url,
            _version: options.conn.version,
            _genesis_block: options.conn.genesis_block,

            // Components
            logger: logger,
            adapter: options.conn.adapter,
            store: options.store,
            metrics_registry: options.metrics_registry,

            // Subgraph indexing components
            subgraph_instance_manager,
        };

        if options.block_ingestion {
            this.spawn_block_ingestor(options.logger_factory);
        }

        if options.chain_indexing {
            this.spawn_chain_indexer();
        }

        Ok(this)
    }

    fn spawn_block_ingestor(&self, logger_factory: &LoggerFactory) {
        // BlockIngestor must be configured to keep at least REORG_THRESHOLD ancestors,
        // otherwise BlockStream will not work properly.
        // BlockStream expects the blocks after the reorg threshold to be present in the
        // database.
        assert!(*ANCESTOR_COUNT >= *REORG_THRESHOLD);

        info!(self.logger, "Starting block ingestor for Ethereum chain");

        let block_ingestor = BlockIngestor::new(
            self.store.clone(),
            self.adapter.clone(),
            *ANCESTOR_COUNT,
            self.id().name.to_string(),
            logger_factory,
            *POLLING_INTERVAL,
        )
        .expect("failed to create Ethereum block ingestor");

        // Run the Ethereum block ingestor in the background
        graph::spawn(block_ingestor.into_polling_stream().compat());
    }

    fn spawn_chain_indexer(&self) {
        let mut indexer = NetworkIndexer::new(
            &self.logger,
            self.adapter.clone(),
            self.store.clone(),
            self.metrics_registry.clone(),
            format!("network/{}", self.id()).into(),
            None,
        );

        graph::spawn(
            indexer
                .take_event_stream()
                .unwrap()
                .for_each(|_| {
                    // For now we simply ignore these events; we may later use them
                    // to drive subgraph indexing
                    Ok(())
                })
                .compat(),
        );
    }
}

#[async_trait]
impl<S> NetworkInstance for Chain<S>
where
    S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
{
    fn id(&self) -> &NetworkInstanceId {
        &self.id
    }

    fn url(&self) -> &str {
        self.url.as_str()
    }

    async fn start_subgraph(&self, subgraph: SubgraphManifest) -> Result<AbortHandle, Error> {
        self.subgraph_instance_manager.start(subgraph).await
    }

    fn compat_ethereum_adapter(&self) -> Option<Arc<dyn EthereumAdapterTrait>> {
        Some(self.adapter.cheap_clone())
    }
}
