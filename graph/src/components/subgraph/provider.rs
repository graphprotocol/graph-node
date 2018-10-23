use prelude::*;

/// Events emitted by [SubgraphProvider](trait.SubgraphProvider.html) implementations.
#[derive(Clone, Debug, PartialEq)]
pub enum SubgraphProviderEvent {
    /// A subgraph with the given manifest should start processing.
    SubgraphStart(SubgraphManifest),
    /// The subgraph with the given ID should stop processing.
    SubgraphStop(SubgraphId),
}

/// Schema-only events emitted by a [SubgraphProvider](trait.SubgraphProvider.html).
#[derive(Clone, Debug, PartialEq)]
pub enum SchemaEvent {
    /// A subgraph with a new schema was added.
    SchemaAdded(Schema),
    /// A subgraph with the given id was removed.
    SchemaRemoved(SubgraphId),
}

/// Common trait for subgraph providers.
pub trait SubgraphProvider:
    EventProducer<SubgraphProviderEvent> + EventProducer<SchemaEvent> + Send + Sync + 'static
{
    fn deploy(
        &self,
        name: String,
        link: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn remove(
        &self,
        name: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static>;

    fn list(&self) -> Result<Vec<(String, Option<SubgraphId>)>, Error>;
}
