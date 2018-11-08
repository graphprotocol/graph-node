use failure::Error;
use futures::prelude::*;
use std::sync::Arc;

use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::web3::types::{Log, Transaction};

pub struct SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
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
    fn from_manifest(
        logger: &Logger,
        manifest: SubgraphManifest,
        host_builder: T,
    ) -> Result<Self, Error> {
        // Create a new runtime host for each data source in the subgraph manifest;
        // we use the same order here as in the subgraph manifest to make the
        // event processing behavior predictable
        let manifest_id = manifest.id;
        let (hosts, errors): (_, Vec<_>) = manifest
            .data_sources
            .into_iter()
            .map(|d| host_builder.build(&logger, manifest_id.clone(), d))
            .partition(|res| res.is_ok());

        if !errors.is_empty() {
            let joined_errors = errors
                .into_iter()
                .map(Result::unwrap_err)
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format_err!(
                "errors starting data sources: {}",
                joined_errors
            ));
        }

        Ok(SubgraphInstance {
            hosts: hosts
                .into_iter()
                .map(Result::unwrap)
                .map(Arc::new)
                .collect(),
        })
    }

    /// Returns true if the subgraph has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool {
        self.hosts.iter().any(|host| host.matches_log(log))
    }

    fn process_log(
        &self,
        logger: &Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Log,
        entity_operations: Vec<EntityOperation>,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send> {
        let logger = logger.to_owned();

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
                    &logger,
                    block.clone(),
                    transaction.clone(),
                    log.clone(),
                    entity_operations,
                )
            },
        ))
    }
}
