use graphql_parser::query as gqlq;
use std::collections::BTreeMap;

use graph::prelude::*;

/// A mock `QueryRunner`.
pub struct MockQueryRunner {
    logger: slog::Logger,
}

impl MockQueryRunner {
    /// Creates a new mock `QueryRunner`.
    pub fn new(logger: &slog::Logger) -> Self {
        MockQueryRunner {
            logger: logger.new(o!("component" => "MockQueryRunner")),
        }
    }
}

impl QueryRunner for MockQueryRunner {
    fn run_query(
        &self,
        query: Query,
    ) -> Box<Future<Item = QueryResult, Error = QueryError> + Send> {
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
}
