use futures::future;

use graph::prelude::{GraphQlRunner as GraphQlRunnerTrait, *};
use graph_graphql::prelude::*;

/// GraphQL runner implementation for The Graph.
pub struct GraphQlRunner<S> {
    logger: Logger,
    store: Arc<S>,
}

impl<S> GraphQlRunner<S>
where
    S: Store + Send + Sync + 'static,
{
    /// Creates a new query runner.
    pub fn new(logger: &Logger, store: Arc<S>) -> Self {
        GraphQlRunner {
            logger: logger.new(o!("component" => "GraphQlRunner")),
            store: store,
        }
    }
}

impl<S> GraphQlRunnerTrait for GraphQlRunner<S>
where
    S: Store + Send + Sync + 'static,
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
