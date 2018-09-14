use failure::Error;

use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};

pub struct SubgraphInstance {
    manifest: SubgraphManifest,
}

impl SubgraphInstanceTrait for SubgraphInstance {
    fn from_manifest<T>(manifest: SubgraphManifest, host_builder: T) -> Self
    where
        T: RuntimeHostBuilder,
    {
        SubgraphInstance { manifest }
    }

    fn process_event(
        &mut self,
        event: EthereumEvent,
    ) -> Box<Future<Item = Vec<EntityChange>, Error = Error>> {
        Box::new(future::ok(vec![]))
    }
}
