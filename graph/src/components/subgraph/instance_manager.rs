use components::EventConsumer;

use super::SubgraphProviderEvent;

/// A `SubgraphInstanceManager` loads and manages subgraph instances.
///
/// It consumes subgraph added/removed events from a `SubgraphProvider`.
/// When a subgraph is added, the subgraph instance manager creates and starts
/// a subgraph instances for the subgraph. When a subgraph is removed, the
/// subgraph instance manager stops and removes the corresponding instance.
pub trait SubgraphInstanceManager: EventConsumer<SubgraphProviderEvent> {}
