use components::EventProducer;
use data::schema::Schema;
use data::subgraph::{SubgraphManifest, SubgraphProviderError};
use tokio::prelude::*;

/// Events emitted by [SubgraphProvider](trait.SubgraphProvider.html) implementations.
#[derive(Clone, Debug)]
pub enum SubgraphProviderEvent {
    /// A subgraph was added to the provider.
    SubgraphAdded(SubgraphManifest),
    /// A subgraph was removed from the provider.
    SubgraphRemoved(SubgraphManifest),
}

/// Schema-only events emitted by a [SubgraphProvider](trait.SubgraphProvider.html).
#[derive(Clone, Debug)]
pub enum SchemaEvent {
    /// A subgraph with a new schema was added.
    SchemaAdded(Schema),
    /// A subgraph with an existing schema was removed.
    SchemaRemoved(Schema),
}

/// Common trait for subgraph providers.
pub trait SubgraphProvider:
    EventProducer<SubgraphProviderEvent> + EventProducer<SchemaEvent> + Send + Sync + 'static
{
    fn add(
        &self,
        link: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;
}
