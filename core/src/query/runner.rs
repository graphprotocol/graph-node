use std::sync::Mutex;

use graph::prelude::{QueryRunner as QueryRunnerTrait, *};
use graph_graphql::prelude::*;

/// Common query runner implementation for The Graph.
pub struct QueryRunner<S> {
    logger: Logger,
    store: Arc<Mutex<S>>,
}

impl<S> QueryRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new query runner.
    pub fn new(logger: &Logger, store: Arc<Mutex<S>>) -> Self {
        QueryRunner {
            logger: logger.new(o!("component" => "QueryRunner")),
            store: store,
        }
    }
}

impl<S> QueryRunnerTrait for QueryRunner<S>
where
    S: Store + 'static,
{
    fn run_query(
        &self,
        query: Query,
    ) -> Box<Future<Item = QueryResult, Error = QueryError> + Send> {
        let logger = self.logger.clone();
        let store = self.store.clone();

        let options = QueryExecutionOptions {
            logger: logger.clone(),
            resolver: StoreResolver::new(&logger, store.clone()),
        };

        let result = execute_query(&query, options);
        Box::new(future::ok(result))
    }
}
