use futures::prelude::*;

use prelude::QueryResult;

/// A stream of query results for a subscription.
pub type QueryResultStream = Box<Stream<Item = QueryResult, Error = ()> + Send>;

/// The result of running a subscription, if successful.
pub type SubscriptionResult = QueryResultStream;
