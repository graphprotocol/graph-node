use std::sync::Mutex;

use graph::prelude::{GraphQLRunner as GraphQLRunnerTrait, *};
use graph_graphql::prelude::*;

/// GraphQL runner implementation for The Graph.
pub struct GraphQLRunner<S> {
    logger: Logger,
    store: Arc<Mutex<S>>,
}

impl<S> GraphQLRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new query runner.
    pub fn new(logger: &Logger, store: Arc<Mutex<S>>) -> Self {
        GraphQLRunner {
            logger: logger.new(o!("component" => "GraphQLRunner")),
            store: store,
        }
    }
}

impl<S> GraphQLRunnerTrait for GraphQLRunner<S>
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
