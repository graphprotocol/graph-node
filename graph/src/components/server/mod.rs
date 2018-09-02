/// Component for running GraphQL queries over HTTP.
pub mod query;

/// Component for running GraphQL subscriptions over WebSockets.
pub mod subscription;

/// Component for the JSON-RPC admin API.
pub mod admin;

/// Component to manage and resolve registered subgraphs.
mod subgraph_registry;
pub use self::subgraph_registry::SubgraphRegistry;
