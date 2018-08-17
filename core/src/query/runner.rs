use futures::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

use graph::prelude::{QueryRunner as QueryRunnerTrait, *};
use graph_graphql::prelude::*;

/// Common query runner implementation for The Graph.
pub struct QueryRunner<S> {
    logger: Logger,
    query_sink: Sender<Query>,
    store: Arc<Mutex<S>>,
}

impl<S> QueryRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new query runner.
    pub fn new(logger: &Logger, store: Arc<Mutex<S>>) -> Self {
        let (sink, stream) = channel(100);
        let runner = QueryRunner {
            logger: logger.new(o!("component" => "QueryRunner")),
            query_sink: sink,
            store: store,
        };
        runner.run_queries(stream);
        runner
    }

    /// Spawns a Tokio task to run any queries received through the given stream.
    fn run_queries(&self, stream: Receiver<Query>) {
        info!(self.logger, "Preparing to run queries");

        let logger = self.logger.clone();
        let store = self.store.clone();

        tokio::spawn(stream.for_each(move |query| {
            let options = QueryExecutionOptions {
                logger: logger.clone(),
                resolver: StoreResolver::new(&logger, store.clone()),
            };
            let result = execute_query(&query, options);

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
