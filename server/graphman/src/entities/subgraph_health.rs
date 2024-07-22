use async_graphql::Enum;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(remote = "graph::data::subgraph::schema::SubgraphHealth")]
pub enum SubgraphHealth {
    /// Syncing without errors.
    Healthy,

    /// Syncing but has errors.
    Unhealthy,

    /// No longer syncing due to a fatal error.
    Failed,
}
