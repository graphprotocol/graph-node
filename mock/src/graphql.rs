use futures::future;
use futures::sync::mpsc::channel;
use graphql_parser::query as gqlq;
use std::collections::BTreeMap;

use graph::prelude::*;

/// A mock `GraphQlRunner`.
pub struct MockGraphQlRunner {
    logger: Logger,
}

impl MockGraphQlRunner {
    /// Creates a new mock `GraphQlRunner`.
    pub fn new(logger: &Logger) -> Self {
        MockGraphQlRunner {
            logger: logger.new(o!("component" => "MockGraphQlRunner")),
        }
    }
}

impl GraphQlRunner for MockGraphQlRunner {
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

    fn run_query_with_complexity(
        &self,
        _query: Query,
        _max_complexity: Option<u64>,
    ) -> QueryResultFuture {
        unimplemented!();
    }

    fn run_subscription(&self, subscription: Subscription) -> SubscriptionResultFuture {
        info!(self.logger, "Run subscription"; "subscription" => format!("{:?}", subscription));
        let (_, receiver) = channel(2);
        Box::new(future::ok(Box::new(receiver) as SubscriptionResult))
    }
}
