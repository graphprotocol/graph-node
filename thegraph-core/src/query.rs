use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_core::reactor::Handle;

use thegraph::prelude::{Query, QueryResult, QueryRunner as QueryRunnerTrait, Store};

/// A mock `QueryRunner`.
pub struct QueryRunner<S> {
    logger: slog::Logger,
    query_sink: Sender<Query>,
    _store: Arc<S>,
    runtime: Handle,
}

impl<S> QueryRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new mock `QueryRunner`.
    pub fn new(logger: &slog::Logger, runtime: Handle, store: S) -> Self {
        let (sink, stream) = channel(100);
        let runner = QueryRunner {
            logger: logger.new(o!("component" => "QueryRunner")),
            query_sink: sink,
            _store: Arc::new(store),
            runtime,
        };
        runner.run_queries(stream);
        runner
    }

    /// Spawns a Tokio task to run any queries received through the given stream.
    fn run_queries(&self, stream: Receiver<Query>) {
        info!(self.logger, "Preparing to run queries");

        let logger = self.logger.clone();

        self.runtime.spawn(stream.for_each(move |query| {
            info!(logger, "Running query"; "query" => format!("{:?}", query));

            // Here we would access the store.
            let mut data = HashMap::new();
            data.insert(String::from("allUsers"), String::from("placeholder"));
            data.insert(String::from("allItems"), String::from("placeholder"));

            query
                .result_sender
                .send(QueryResult::new(Some(data)))
                .expect("Failed to send query result back");
            Ok(())
        }));
    }
}

impl<S> QueryRunnerTrait for QueryRunner<S> {
    fn query_sink(&mut self) -> Sender<Query> {
        self.query_sink.clone()
    }
}
