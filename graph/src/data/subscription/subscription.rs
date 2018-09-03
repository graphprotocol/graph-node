use prelude::Query;

/// A GraphQL subscription made by a client.
#[derive(Clone, Debug)]
pub struct Subscription {
    /// The GraphQL subscription query.
    pub query: Query,
}
