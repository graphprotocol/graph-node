use components::EventConsumer;

use super::SubgraphProviderEvent;

/// A `RuntimeManager` manages subgraph runtimes based on which subgraphs
/// are available in the system. These are provided through
/// `SubgraphProviderEvent`s.
///
/// When a subgraph is added, the runtime manager creates and starts
/// one or more runtime hosts for the subgraph. When a subgraph is removed,
/// the runtime manager stops and removes the runtime hosts for this subgraph.
pub trait RuntimeManager: EventConsumer<SubgraphProviderEvent> {}
