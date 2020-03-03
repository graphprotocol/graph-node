use crate::prelude::QueryResult;
use std::marker::Unpin;

/// A stream of query results for a subscription.
pub type QueryResultStream = Box<dyn futures03::stream::Stream<Item = QueryResult> + Send + Unpin>;

/// The result of running a subscription, if successful.
pub type SubscriptionResult = QueryResultStream;
