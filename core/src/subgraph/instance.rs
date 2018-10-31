use failure::Error;
use futures::prelude::*;
use std::sync::Arc;

use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::web3::types::{Log, Transaction};

pub struct SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    /// The manifest of the subgraph instance.
    _manifest: SubgraphManifest,

    /// Runtime hosts, one for each data source mapping.
    ///
    /// The runtime hosts are created and added in the same order the
    /// data sources appear in the subgraph manifest. Incoming block
    /// stream events are processed by the mappings in this same order.
    hosts: Vec<Arc<T::Host>>,
}

impl<T> SubgraphInstanceTrait<T> for SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    fn from_manifest(manifest: SubgraphManifest, host_builder: T) -> Self {
        // Create a new runtime host for each data source in the subgraph manifest;
        // we use the same order here as in the subgraph manifest to make the
        // event processing behavior predictable
        let hosts = manifest
            .data_sources
            .iter()
            .map(|d| host_builder.build(manifest.clone(), d.clone()))
            .map(Arc::new)
            .collect();

        // Remember manifest and managed hosts
        SubgraphInstance {
            _manifest: manifest,
            hosts: hosts,
        }
    }

    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool {
        self.hosts.iter().any(|host| host.matches_log(log))
    }

    fn process_log(
        &self,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Log,
        entity_operations: Vec<EntityOperation>,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send> {
        // Identify runtime hosts that will handle this event
        let matching_hosts: Vec<_> = self
            .hosts
            .iter()
            .filter(|host| host.matches_log(&log))
            .cloned()
            .collect();

        let log = Arc::new(log);

        // Process the log in each host in the same order the corresponding
        // data sources appear in the subgraph manifest
        Box::new(stream::iter_ok(matching_hosts).fold(
            entity_operations,
            move |entity_operations, host| {
                host.process_log(
                    block.clone(),
                    transaction.clone(),
                    log.clone(),
                    entity_operations,
                )
            },
        ))
    }
}
