use futures::prelude::*;

use prelude::{QueryError, QueryResult, SubscriptionError};

/// A stream of query results for a subscription.
pub type QueryResultStream = Box<Stream<Item = QueryResult, Error = ()> + Send>;

/// The result of running a subscription, if successful.
pub struct SubscriptionResult {
    pub stream: QueryResultStream,
}

impl SubscriptionResult {
    pub fn new(stream: QueryResultStream) -> Self {
        SubscriptionResult { stream }
    }
}
