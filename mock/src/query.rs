use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser::query as gqlq;
use slog;
use std::collections::BTreeMap;

use graph::prelude::*;

/// A mock `QueryRunner`.
pub struct MockQueryRunner<S> {
    logger: slog::Logger,
    query_sink: Sender<Query>,
    _store: Arc<S>,
}

impl<S> MockQueryRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new mock `QueryRunner`.
    pub fn new(logger: &slog::Logger, store: S) -> Self {
        let (sink, stream) = channel(100);
        let runner = MockQueryRunner {
            logger: logger.new(o!("component" => "MockQueryRunner")),
            query_sink: sink,
            _store: Arc::new(store),
        };
        runner.run_queries(stream);
        runner
    }

    /// Spawns a Tokio task to run any queries received through the given stream.
    fn run_queries(&self, stream: Receiver<Query>) {
        info!(self.logger, "Preparing to run queries");

        let logger = self.logger.clone();

        tokio::spawn(stream.for_each(move |query| {
            info!(logger, "Running query"; "query" => format!("{:?}", query));

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

            query
                .result_sender
                .send(QueryResult::new(Some(data)))
                .expect("Failed to send query result back");
            Ok(())
        }));
    }
}

impl<S> QueryRunner for MockQueryRunner<S> {
    fn query_sink(&mut self) -> Sender<Query> {
        self.query_sink.clone()
    }
}
