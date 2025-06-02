use std::sync::Arc;
use std::time::Instant;

use crate::metrics::GraphQLMetrics;
use crate::prelude::{QueryExecutionOptions, StoreResolver};
use crate::query::execute_query;
use graph::futures03::future;
use graph::prelude::MetricsRegistry;
use graph::prelude::{
    async_trait, o, CheapClone, DeploymentState, GraphQLMetrics as GraphQLMetricsTrait,
    GraphQlRunner as GraphQlRunnerTrait, Logger, Query, QueryExecutionError, ENV_VARS,
};
use graph::{data::graphql::load_manager::LoadManager, prelude::QueryStoreManager};
use graph::{
    data::query::{LatestBlockInfo, QueryResults, QueryTarget},
    prelude::QueryStore,
};

/// GraphQL runner implementation for The Graph.
pub struct GraphQlRunner<S> {
    logger: Logger,
    store: Arc<S>,
    load_manager: Arc<LoadManager>,
    graphql_metrics: Arc<GraphQLMetrics>,
}

#[cfg(debug_assertions)]
lazy_static::lazy_static! {
    // Test only, see c435c25decbc4ad7bbbadf8e0ced0ff2
    pub static ref INITIAL_DEPLOYMENT_STATE_FOR_TESTS: std::sync::Mutex<Option<DeploymentState>> = std::sync::Mutex::new(None);
}

impl<S> GraphQlRunner<S>
where
    S: QueryStoreManager,
{
    /// Creates a new query runner.
    pub fn new(
        logger: &Logger,
        store: Arc<S>,
        load_manager: Arc<LoadManager>,
        registry: Arc<MetricsRegistry>,
    ) -> Self {
        let logger = logger.new(o!("component" => "GraphQlRunner"));
        let graphql_metrics = Arc::new(GraphQLMetrics::new(registry));
        GraphQlRunner {
            logger,
            store,
            load_manager,
            graphql_metrics,
        }
    }

    /// Check if the subgraph state differs from `state` now in a way that
    /// would affect a query that looked at data as fresh as `latest_block`.
    /// If the subgraph did change, return the `Err` that should be sent back
    /// to clients to indicate that condition
    async fn deployment_changed(
        &self,
        store: &dyn QueryStore,
        state: DeploymentState,
        latest_block: u64,
    ) -> Result<(), QueryExecutionError> {
        if ENV_VARS.graphql.allow_deployment_change {
            return Ok(());
        }
        let new_state = store.deployment_state().await?;
        assert!(new_state.reorg_count >= state.reorg_count);
        if new_state.reorg_count > state.reorg_count {
            // One or more reorgs happened; each reorg can't have gone back
            // farther than `max_reorg_depth`, so that querying at blocks
            // far enough away from the previous latest block is fine. Taking
            // this into consideration is important, since most of the time
            // there is only one reorg of one block, and we therefore avoid
            // flagging a lot of queries a bit behind the head
            let n_blocks = new_state.max_reorg_depth * (new_state.reorg_count - state.reorg_count);
            if latest_block + n_blocks as u64 > state.latest_block.number as u64 {
                return Err(QueryExecutionError::DeploymentReverted);
            }
        }
        Ok(())
    }

    async fn execute(
        &self,
        query: Query,
        target: QueryTarget,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
        max_skip: Option<u32>,
        metrics: Arc<GraphQLMetrics>,
    ) -> Result<QueryResults, QueryResults> {
        let execute_start = Instant::now();

        // We need to use the same `QueryStore` for the entire query to ensure
        // we have a consistent view if the world, even when replicas, which
        // are eventually consistent, are in use. If we run different parts
        // of the query against different replicas, it would be possible for
        // them to be at wildly different states, and we might unwittingly
        // mix data from different block heights even if no reverts happen
        // while the query is running. `self.store` can not be used after this
        // point, and everything needs to go through the `store` we are
        // setting up here

        let store = self.store.query_store(target.clone()).await?;
        let state = store.deployment_state().await?;
        let network = Some(store.network_name().to_string());
        let schema = store.api_schema()?;

        let latest_block = match store.block_ptr().await.ok().flatten() {
            Some(block) => Some(LatestBlockInfo {
                timestamp: store
                    .block_pointer(&block.hash)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|(_, t, _)| t.map(|ts| ts.as_secs_since_epoch() as u64)),
                hash: block.hash,
                number: block.number,
            }),
            None => None,
        };

        // Test only, see c435c25decbc4ad7bbbadf8e0ced0ff2
        #[cfg(debug_assertions)]
        let state = INITIAL_DEPLOYMENT_STATE_FOR_TESTS
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(state);

        let max_depth = max_depth.unwrap_or(ENV_VARS.graphql.max_depth);
        let do_trace = query.trace;
        let query = crate::execution::Query::new(
            &self.logger,
            schema,
            network,
            query,
            max_complexity,
            max_depth,
            metrics.cheap_clone(),
        )?;
        self.load_manager
            .decide(
                &store.wait_stats(),
                store.shard(),
                store.deployment_id(),
                query.shape_hash,
                query.query_text.as_ref(),
            )
            .to_result()?;
        let by_block_constraint =
            StoreResolver::locate_blocks(store.as_ref(), &state, &query).await?;
        let mut max_block = 0;
        let mut result: QueryResults =
            QueryResults::empty(query.root_trace(do_trace), latest_block);
        let mut query_res_futures: Vec<_> = vec![];
        let setup_elapsed = execute_start.elapsed();

        // Note: This will always iterate at least once.
        for (ptr, (selection_set, error_policy)) in by_block_constraint {
            let resolver = StoreResolver::at_block(
                &self.logger,
                store.cheap_clone(),
                &state,
                ptr,
                error_policy,
                query.schema.id().clone(),
                metrics.cheap_clone(),
                self.load_manager.cheap_clone(),
            )
            .await?;
            max_block = max_block.max(resolver.block_number());
            query_res_futures.push(execute_query(
                query.clone(),
                Some(selection_set),
                resolver.block_ptr.clone(),
                QueryExecutionOptions {
                    resolver,
                    deadline: ENV_VARS.graphql.query_timeout.map(|t| Instant::now() + t),
                    max_first: max_first.unwrap_or(ENV_VARS.graphql.max_first),
                    max_skip: max_skip.unwrap_or(ENV_VARS.graphql.max_skip),
                    trace: do_trace,
                },
            ));
        }

        let results: Vec<_> = if ENV_VARS.graphql.parallel_block_constraints {
            future::join_all(query_res_futures).await
        } else {
            let mut results = vec![];
            for query_res_future in query_res_futures {
                results.push(query_res_future.await);
            }
            results
        };

        for (query_res, cache_status) in results {
            result.append(query_res, cache_status);
        }

        query.log_execution(max_block);
        result.trace.finish(setup_elapsed, execute_start.elapsed());
        self.deployment_changed(store.as_ref(), state, max_block as u64)
            .await
            .map_err(QueryResults::from)
            .map(|()| result)
    }
}

#[async_trait]
impl<S> GraphQlRunnerTrait for GraphQlRunner<S>
where
    S: QueryStoreManager,
{
    async fn run_query(self: Arc<Self>, query: Query, target: QueryTarget) -> QueryResults {
        self.run_query_with_complexity(
            query,
            target,
            ENV_VARS.graphql.max_complexity,
            Some(ENV_VARS.graphql.max_depth),
            Some(ENV_VARS.graphql.max_first),
            Some(ENV_VARS.graphql.max_skip),
        )
        .await
    }

    async fn run_query_with_complexity(
        self: Arc<Self>,
        query: Query,
        target: QueryTarget,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
        max_skip: Option<u32>,
    ) -> QueryResults {
        self.execute(
            query,
            target,
            max_complexity,
            max_depth,
            max_first,
            max_skip,
            self.graphql_metrics.clone(),
        )
        .await
        .unwrap_or_else(|e| e)
    }

    fn metrics(&self) -> Arc<dyn GraphQLMetricsTrait> {
        self.graphql_metrics.clone()
    }
}
