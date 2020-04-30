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
use graph::prelude::{
    o, EthereumBlockPointer, GraphQlRunner as GraphQlRunnerTrait, Logger, Query,
    QueryExecutionError, QueryResult, QueryResultFuture, Store, StoreError, SubgraphDeploymentId,
    SubgraphDeploymentStore, Subscription, SubscriptionResultFuture,
};

use lazy_static::lazy_static;

/// GraphQL runner implementation for The Graph.
pub struct GraphQlRunner<S> {
    logger: Logger,
    store: Arc<S>,
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
    pub fn new(logger: &Logger, store: Arc<S>) -> Self {
        GraphQlRunner {
            logger: logger.new(o!("component" => "GraphQlRunner")),
            store,
        }
    }

    /// Create a JSON value that contains the block information for our
    /// response
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

    pub fn execute(
        &self,
        query: Query,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
    ) -> Result<QueryResult, Vec<QueryExecutionError>> {
        let max_depth = max_depth.unwrap_or(*GRAPHQL_MAX_DEPTH);
        let query = crate::execution::Query::new(query, max_complexity, max_depth)?;
        let bc = query.block_constraint()?;
        let (resolver, block_ptr) =
            StoreResolver::at_block(&self.logger, self.store.clone(), bc, &query.schema.id)?;
        let exts = self.make_extensions(&query.schema.id, &block_ptr)?;
        execute_query(
            query,
            QueryExecutionOptions {
                logger: self.logger.clone(),
                resolver,
                deadline: GRAPHQL_QUERY_TIMEOUT.map(|t| Instant::now() + t),
                max_first: max_first.unwrap_or(*GRAPHQL_MAX_FIRST),
            },
        )
        .map(|values| QueryResult::new(Some(values)).with_extensions(exts))
    }
}

impl<S> GraphQlRunnerTrait for GraphQlRunner<S>
where
    S: Store + SubgraphDeploymentStore,
{
    fn run_query(&self, query: Query) -> QueryResultFuture {
        self.run_query_with_complexity(
            query,
            *GRAPHQL_MAX_COMPLEXITY,
            Some(*GRAPHQL_MAX_DEPTH),
            Some(*GRAPHQL_MAX_FIRST),
        )
    }

    fn run_query_with_complexity(
        &self,
        query: Query,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
    ) -> QueryResultFuture {
        let result = match self.execute(query, max_complexity, max_depth, max_first) {
            Ok(result) => result,
            Err(e) => QueryResult::from(e),
        };
        Box::new(future::ok(result))
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

        let result = execute_prepared_subscription(
            query,
            SubscriptionExecutionOptions {
                logger: self.logger.clone(),
                resolver: StoreResolver::new(&self.logger, self.store.clone()),
                timeout: GRAPHQL_QUERY_TIMEOUT.clone(),
                max_complexity: *GRAPHQL_MAX_COMPLEXITY,
                max_depth: *GRAPHQL_MAX_DEPTH,
                max_first: *GRAPHQL_MAX_FIRST,
            },
        );

        Box::new(future::result(result))
    }
}
