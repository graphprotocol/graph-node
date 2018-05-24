use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::Arc;
use tokio_core::reactor::Handle;

use thegraph::prelude::{Query, QueryRunner as QueryRunnerTrait, Store};

use super::execution;
use super::resolver::StoreResolver;

/// Common query runner implementation for The Graph.
pub struct QueryRunner<S> {
    logger: slog::Logger,
    query_sink: Sender<Query>,
    store: Arc<S>,
    runtime: Handle,
}

impl<S> QueryRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new query runner.
    pub fn new(logger: &slog::Logger, runtime: Handle, store: S) -> Self {
        let (sink, stream) = channel(100);
        let runner = QueryRunner {
            logger: logger.new(o!("component" => "QueryRunner")),
            query_sink: sink,
            store: Arc::new(store),
            runtime,
        };
        runner.run_queries(stream);
        runner
    }

    /// Spawns a Tokio task to run any queries received through the given stream.
    fn run_queries(&self, stream: Receiver<Query>) {
        info!(self.logger, "Preparing to run queries");

        let logger = self.logger.clone();
        let store = self.store.clone();

        self.runtime.spawn(stream.for_each(move |query| {
            let options = execution::ExecutionOptions {
                logger: logger.clone(),
                resolver: StoreResolver::new(&logger, store.clone()),
            };
            let result = execution::execute(&query, options);

            query
                .result_sender
                .send(result)
                .expect("Failed to deliver query result");
            Ok(())
        }));
    }
}

impl<S> QueryRunnerTrait for QueryRunner<S> {
    fn query_sink(&mut self) -> Sender<Query> {
        self.query_sink.clone()
    }
}
