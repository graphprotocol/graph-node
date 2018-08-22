use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::channel;
use graphql_parser::query as gqlq;
use std::collections::BTreeMap;

use graph::prelude::*;

/// A mock `GraphQLRunner`.
pub struct MockGraphQLRunner {
    logger: slog::Logger,
}

impl MockGraphQLRunner {
    /// Creates a new mock `GraphQLRunner`.
    pub fn new(logger: &slog::Logger) -> Self {
        MockGraphQLRunner {
            logger: logger.new(o!("component" => "MockGraphQLRunner")),
        }
    }
}

impl GraphQLRunner for MockGraphQLRunner {
    fn run_query(&self, query: Query) -> QueryResultFuture {
        info!(self.logger, "Run query"; "query" => format!("{:?}", query));

        // Here we would access the store.
        let mut data = BTreeMap::new();
        data.insert(
            String::from("allUsers"),
            gqlq::Value::String("placeholder".to_string()),
        );
        data.insert(
            String::from("allItems"),
            gqlq::Value::String("placeholder".to_string()),
        );
        let data = gqlq::Value::Object(data);

        Box::new(future::ok(QueryResult::new(Some(data))))
    }

    fn run_subscription(&self, subscription: Subscription) -> SubscriptionResultFuture {
        info!(self.logger, "Run subscription"; "subscription" => format!("{:?}", subscription));
        let (_, receiver) = channel(2);
        Box::new(future::ok(SubscriptionResult::new(Box::new(receiver))))
    }
}
