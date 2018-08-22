use futures::future;
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
    fn run_query(&self, query: Query) -> QueryResultFuture {
        let result = execute_query(
            &query,
            QueryExecutionOptions {
                logger: self.logger.clone(),
                resolver: StoreResolver::new(&self.logger, self.store.clone()),
            },
        );
        Box::new(future::ok(result))
    }

    fn run_subscription(&self, subscription: Subscription) -> SubscriptionResultFuture {
        let result = execute_subscription(
            &subscription,
            SubscriptionExecutionOptions {
                logger: self.logger.clone(),
                resolver: StoreResolver::new(&self.logger, self.store.clone()),
            },
        );

        Box::new(future::result(result))
    }
}
