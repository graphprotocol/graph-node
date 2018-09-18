use failure::Error;
use futures::prelude::*;

use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::web3::types::Log;

pub struct SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    manifest: SubgraphManifest,
    hosts: Vec<T::Host>,
}

impl<T> SubgraphInstanceTrait<T> for SubgraphInstance<T>
where
    T: RuntimeHostBuilder,
{
    fn from_manifest(manifest: SubgraphManifest, host_builder: T) -> Self {
        // Create a new runtime host for each data source in the subgraph manifest
        let hosts = manifest
            .data_sources
            .iter()
            .map(|d| host_builder.build(manifest.clone(), d.clone()))
            .collect();

        // Remember manifest and managed hosts
        SubgraphInstance { manifest, hosts }
    }

    fn parse_log(&self, log: &Log) -> Result<EthereumEvent, Error> {
        unimplemented!();
    }

    fn process_event(
        &self,
        event: EthereumEvent,
    ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send> {
        // Identify runtime hosts that will handle this event
        let matching_hosts = self.hosts.iter().filter(|host| host.matches_event(&event));

        // Process the event in each of these hosts in order; the result of each
        // host.process_event() call is a Vec<EntityOperation> future
        let futures = matching_hosts
            .map(|host| host.process_event(event.clone()))
            .collect::<Vec<_>>();

        // Collect the results of the event handlers in the right order and
        // flatten the entity operations
        Box::new(
            // FIXME: `futures_ordered` does not guarantee the futures are executed
            // in the desired order; their results will just be returned in the same
            // order
            stream::futures_ordered(futures)
                .collect()
                .map(|vecs| vecs.into_iter().flatten().collect()),
        )
    }
}
