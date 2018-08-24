use prelude::Query;

/// A GraphQL subscription made by a client.
#[derive(Debug)]
pub struct Subscription {
    /// The GraphQL subscription query.
    pub query: Query,
}
