use graph::components::blockchain::*;
use graph::prelude::*;

use super::block::EthereumBlock;
use super::indexer::*;
use crate::{EthereumAdapter, *};

pub struct EthereumNetwork {
    logger: Logger,
    name: String,
    clients: Vec<Arc<EthereumAdapter<Transport>>>,
    metrics_registry: Arc<dyn MetricsRegistry>,
}

impl EthereumNetwork {
    pub fn new(
        options: &NetworkOptions,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Result<Self, Error> {
        let logger = options.logger.new(o!("network" => options.name));

        let clients: Vec<Arc<EthereumAdapter<Transport>>> =
            options
                .providers
                .iter()
                .try_fold(vec![], |mut clients, provider| {
                    if provider.kind.as_str() != "rpc" {
                        return Err(format_err!(
                            "unsupported type `{}` for Ethereum provider `{}`",
                            provider.kind,
                            provider.url
                        ));
                    }

                    let (transport_event_loop, transport) = Transport::new_rpc(&provider.url);

                    // If we drop the event loop the transport will stop working.
                    // For now it's fine to just leak it.
                    std::mem::forget(transport_event_loop);

                    let metrics = Arc::new(ProviderEthRpcMetrics::new(metrics_registry.clone()));
                    let adapter = Arc::new(EthereumAdapter::new(transport, metrics));
                    clients.push(adapter);
                    Ok(clients)
                })?;

        Ok(Self {
            name: options.name.clone(),
            logger,
            clients,
            metrics_registry,
        })
    }
}

impl Network for EthereumNetwork {
    type Block = EthereumBlock;
    type Indexer = EthereumIndexer;

    fn latest_block(&self, options: LatestBlockOptions) -> LatestBlockFuture {
        unimplemented!();
    }

    fn block_by_number(&self, options: BlockByNumberOptions) -> BlockByNumberFuture {
        unimplemented!();
    }

    fn block_by_hash(&self, options: BlockByHashOptions) -> BlockByHashFuture {
        unimplemented!();
    }

    fn indexer(
        &self,
        options: NetworkIndexerOptions,
    ) -> Box<dyn Future<Item = Self::Indexer, Error = Error> + Send> {
        EthereumIndexer::create(EthereumIndexerOptions {
            subgraph_name: format!("ethereum/{}", self.name),
            logger: self.logger.clone(),
            clients: self.clients.clone(),
            metrics_registry: self.metrics_registry.clone(),
            options,
        })
    }
}
