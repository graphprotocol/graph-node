use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use tokio_core::reactor::Handle;

use thegraph::prelude::{Query, QueryRunner as QueryRunnerTrait, StoreRequest};
use thegraph_graphql_utils::execution;

use super::resolver::StoreResolver;

/// Common query runner implementation for The Graph.
pub struct QueryRunner {
    logger: slog::Logger,
    query_sink: Sender<Query>,
    store_requests: Sender<StoreRequest>,
    runtime: Handle,
}

impl QueryRunner {
    /// Creates a new query runner.
    pub fn new(
        logger: &slog::Logger,
        runtime: Handle,
        store_requests: Sender<StoreRequest>,
    ) -> Self {
        let (sink, stream) = channel(100);
        let runner = QueryRunner {
            logger: logger.new(o!("component" => "QueryRunner")),
            query_sink: sink,
            store_requests,
            runtime,
        };
        runner.run_queries(stream);
        runner
    }

    /// Spawns a Tokio task to run any queries received through the given stream.
    fn run_queries(&self, stream: Receiver<Query>) {
        info!(self.logger, "Preparing to run queries");

        let logger = self.logger.clone();
        let store_requests = self.store_requests.clone();
        let runtime = self.runtime.clone();

        self.runtime.spawn(stream.for_each(move |query| {
            let options = execution::ExecutionOptions {
                logger: logger.clone(),
                resolver: StoreResolver::new(&logger, runtime.clone(), store_requests.clone()),
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

impl QueryRunnerTrait for QueryRunner {
    fn query_sink(&mut self) -> Sender<Query> {
        self.query_sink.clone()
    }
}
