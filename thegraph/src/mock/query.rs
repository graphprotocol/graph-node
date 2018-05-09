use prelude::*;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::Arc;
use tokio_core::reactor::Handle;

use common::query::{Query, QueryResult};

/// A mock [QueryRunner](../common/query/trait.QueryRunner.html).
pub struct MockQueryRunner<S> {
    logger: slog::Logger,
    query_sink: Sender<Query>,
    _store: Arc<S>,
    runtime: Handle,
}

impl<S> MockQueryRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new mock [QueryRunner](../common/query/trait.QueryRunner.html).
    pub fn new(logger: &slog::Logger, runtime: Handle, store: S) -> Self {
        let (sink, stream) = channel(100);
        let runner = MockQueryRunner {
            logger: logger.new(o!("component" => "MockQueryRunner")),
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

            query
                .result_sender
                .send(QueryResult {})
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
