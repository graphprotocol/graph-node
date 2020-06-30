use futures01::future;
use graphql_parser::query as q;
use std::collections::BTreeMap;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::prelude::{
    object, object_value, QueryExecutionOptions, StoreResolver, SubscriptionExecutionOptions,
};
use crate::query::execute_query;
use crate::subscription::execute_prepared_subscription;
use graph::data::graphql::effort::LoadManager;
use graph::prelude::{
    async_trait, o, EthereumBlockPointer, GraphQlRunner as GraphQlRunnerTrait, Logger, Query,
    QueryExecutionError, QueryResult, Store, StoreError, SubgraphDeploymentId,
    SubgraphDeploymentStore, Subscription, SubscriptionError, SubscriptionResultFuture,
};

use lazy_static::lazy_static;

/// GraphQL runner implementation for The Graph.
pub struct GraphQlRunner<S> {
    logger: Logger,
    store: Arc<S>,
    load_manager: Arc<LoadManager>,
}

lazy_static! {
    static ref GRAPHQL_QUERY_TIMEOUT: Option<Duration> = env::var("GRAPH_GRAPHQL_QUERY_TIMEOUT")
        .ok()
        .map(|s| Duration::from_secs(
            u64::from_str(&s)
                .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_GRAPHQL_QUERY_TIMEOUT"))
        ));
    static ref GRAPHQL_MAX_COMPLEXITY: Option<u64> = env::var("GRAPH_GRAPHQL_MAX_COMPLEXITY")
        .ok()
        .map(|s| u64::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_GRAPHQL_MAX_COMPLEXITY")));
    static ref GRAPHQL_MAX_DEPTH: u8 = env::var("GRAPH_GRAPHQL_MAX_DEPTH")
        .ok()
        .map(|s| u8::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_GRAPHQL_MAX_DEPTH")))
        .unwrap_or(u8::max_value());
    static ref GRAPHQL_MAX_FIRST: u32 = env::var("GRAPH_GRAPHQL_MAX_FIRST")
        .ok()
        .map(|s| u32::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_GRAPHQL_MAX_FIRST")))
        .unwrap_or(1000);
}

impl<S> GraphQlRunner<S>
where
    S: Store + SubgraphDeploymentStore,
{
    /// Creates a new query runner.
    pub fn new(logger: &Logger, store: Arc<S>, load_manager: Arc<LoadManager>) -> Self {
        let logger = logger.new(o!("component" => "GraphQlRunner"));
        GraphQlRunner {
            logger,
            store,
            load_manager,
        }
    }

    /// Create a JSON value that contains the block information for our
    /// response
    #[allow(dead_code)]
    fn make_extensions(
        &self,
        subgraph: &SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    ) -> Result<BTreeMap<q::Name, q::Value>, Vec<QueryExecutionError>> {
        let network = self
            .store
            .network_name(subgraph)
            .map_err(|e| vec![QueryExecutionError::StoreError(e.into())])?
            .map(|s| format!("ethereum/{}", s))
            .unwrap_or("unknown".to_string());
        let network_info = if self
            .store
            .uses_relational_schema(subgraph)
            .map_err(|e| vec![StoreError::from(e).into()])?
        {
            // Relational storage: produce the full output
            object_value(vec![(
                network.as_str(),
                object! {
                        hash: block_ptr.hash_hex(),
                        number: q::Number::from(block_ptr.number as i32)
                },
            )])
        } else {
            // JSONB storage: we can not reliably report anything about
            // the block where the query happened
            object_value(vec![(network.as_str(), object_value(vec![]))])
        };
        let mut exts = BTreeMap::new();
        exts.insert(
            "subgraph".to_owned(),
            object! {
                id: subgraph.to_string(),
                blocks: network_info
            },
        );
        Ok(exts)
    }

    fn execute(
        &self,
        query: Query,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
    ) -> Result<Arc<QueryResult>, QueryResult> {
        let max_depth = max_depth.unwrap_or(*GRAPHQL_MAX_DEPTH);
        let query = crate::execution::Query::new(query, max_complexity, max_depth)?;
        if self
            .load_manager
            .decline(query.shape_hash, query.query_text.as_ref())
        {
            return Err(QueryExecutionError::TooExpensive.into());
        }

        let execute = |selection_set, block_ptr, resolver| {
            execute_query(
                query.clone(),
                Some(&selection_set),
                Some(block_ptr),
                QueryExecutionOptions {
                    logger: self.logger.clone(),
                    resolver,
                    deadline: GRAPHQL_QUERY_TIMEOUT.map(|t| Instant::now() + t),
                    max_first: max_first.unwrap_or(*GRAPHQL_MAX_FIRST),
                    load_manager: self.load_manager.clone(),
                },
            )
        };

        // Unwrap: There is always at least one block constraint, even if it
        // is an implicit 'BlockContraint::Latest'.
        let mut by_block_constraint = query.block_constraint()?.into_iter();
        let (bc, selection_set) = by_block_constraint.next().unwrap();
        let (resolver, block_ptr) =
            StoreResolver::at_block(&self.logger, self.store.clone(), bc, &query.schema.id)?;
        let mut result = execute(selection_set, block_ptr, resolver);

        // We want to optimize for the common case of a single block constraint, where we can avoid
        // cloning the result. If there are multiple constraints we have to clone.
        if by_block_constraint.len() > 0 {
            let mut partial_res = result.as_ref().clone();
            for (bc, selection_set) in by_block_constraint {
                let (resolver, block_ptr) = StoreResolver::at_block(
                    &self.logger,
                    self.store.clone(),
                    bc,
                    &query.schema.id,
                )?;
                partial_res.append(execute(selection_set, block_ptr, resolver).as_ref().clone());
            }
            result = Arc::new(partial_res);
        }

        Ok(result)
    }
}

#[async_trait]
impl<S> GraphQlRunnerTrait for GraphQlRunner<S>
where
    S: Store + SubgraphDeploymentStore,
{
    async fn run_query(self: Arc<Self>, query: Query) -> Arc<QueryResult> {
        self.run_query_with_complexity(
            query,
            *GRAPHQL_MAX_COMPLEXITY,
            Some(*GRAPHQL_MAX_DEPTH),
            Some(*GRAPHQL_MAX_FIRST),
        )
        .await
    }

    async fn run_query_with_complexity(
        &self,
        query: Query,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
    ) -> Arc<QueryResult> {
        self.execute(query, max_complexity, max_depth, max_first)
            .unwrap_or_else(|e| Arc::new(e))
    }

    fn run_subscription(&self, subscription: Subscription) -> SubscriptionResultFuture {
        let query = match crate::execution::Query::new(
            subscription.query,
            *GRAPHQL_MAX_COMPLEXITY,
            *GRAPHQL_MAX_DEPTH,
        ) {
            Ok(query) => query,
            Err(e) => return Box::new(future::err(e.into())),
        };

        if self
            .load_manager
            .decline(query.shape_hash, query.query_text.as_ref())
        {
            let err = SubscriptionError::GraphQLError(vec![QueryExecutionError::TooExpensive]);
            return Box::new(future::result(Err(err)));
        }

        let result = execute_prepared_subscription(
            query,
            SubscriptionExecutionOptions {
                logger: self.logger.clone(),
                resolver: StoreResolver::for_subscription(&self.logger, self.store.clone()),
                timeout: GRAPHQL_QUERY_TIMEOUT.clone(),
                max_complexity: *GRAPHQL_MAX_COMPLEXITY,
                max_depth: *GRAPHQL_MAX_DEPTH,
                max_first: *GRAPHQL_MAX_FIRST,
            },
        );

        Box::new(future::result(result))
    }

    fn load_manager(&self) -> Arc<LoadManager> {
        self.load_manager.clone()
    }
}
