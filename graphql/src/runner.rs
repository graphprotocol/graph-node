use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::prelude::{QueryExecutionOptions, StoreResolver, SubscriptionExecutionOptions};
use crate::query::execute_query;
use crate::subscription::execute_prepared_subscription;
use graph::{
    components::store::SubscriptionManager,
    prelude::{
        async_trait, o, CheapClone, DeploymentState, GraphQlRunner as GraphQlRunnerTrait, Logger,
        Query, QueryExecutionError, Subscription, SubscriptionError, SubscriptionResult,
    },
};
use graph::{data::graphql::effort::LoadManager, prelude::QueryStoreManager};
use graph::{
    data::query::{QueryResults, QueryTarget},
    prelude::QueryStore,
};

use lazy_static::lazy_static;

/// GraphQL runner implementation for The Graph.
pub struct GraphQlRunner<S, SM> {
    logger: Logger,
    store: Arc<S>,
    subscription_manager: Arc<SM>,
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
    static ref GRAPHQL_MAX_SKIP: u32 = env::var("GRAPH_GRAPHQL_MAX_SKIP")
        .ok()
        .map(|s| u32::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_GRAPHQL_MAX_SKIP")))
        .unwrap_or(std::u32::MAX);
    // Allow skipping the check whether a deployment has changed while
    // we were running a query. Once we are sure that the check mechanism
    // is reliable, this variable should be removed
    static ref GRAPHQL_ALLOW_DEPLOYMENT_CHANGE: bool = env::var("GRAPHQL_ALLOW_DEPLOYMENT_CHANGE")
        .ok()
        .map(|s| s == "true")
        .unwrap_or(false);
}

#[cfg(debug_assertions)]
lazy_static! {
    // Test only, see c435c25decbc4ad7bbbadf8e0ced0ff2
    pub static ref INITIAL_DEPLOYMENT_STATE_FOR_TESTS: std::sync::Mutex<Option<DeploymentState>> = std::sync::Mutex::new(None);
}

impl<S, SM> GraphQlRunner<S, SM>
where
    S: QueryStoreManager,
    SM: SubscriptionManager,
{
    /// Creates a new query runner.
    pub fn new(
        logger: &Logger,
        store: Arc<S>,
        subscription_manager: Arc<SM>,
        load_manager: Arc<LoadManager>,
    ) -> Self {
        let logger = logger.new(o!("component" => "GraphQlRunner"));
        GraphQlRunner {
            logger,
            store,
            subscription_manager,
            load_manager,
        }
    }

    /// Check if the subgraph state differs from `state` now in a way that
    /// would affect a query that looked at data as fresh as `latest_block`.
    /// If the subgraph did change, return the `Err` that should be sent back
    /// to clients to indicate that condition
    fn deployment_changed(
        &self,
        store: &dyn QueryStore,
        state: DeploymentState,
        latest_block: u64,
    ) -> Result<(), QueryExecutionError> {
        if *GRAPHQL_ALLOW_DEPLOYMENT_CHANGE {
            return Ok(());
        }
        let new_state = store.deployment_state()?;
        assert!(new_state.reorg_count >= state.reorg_count);
        if new_state.reorg_count > state.reorg_count {
            // One or more reorgs happened; each reorg can't have gone back
            // farther than `max_reorg_depth`, so that querying at blocks
            // far enough away from the previous latest block is fine. Taking
            // this into consideration is important, since most of the time
            // there is only one reorg of one block, and we therefore avoid
            // flagging a lot of queries a bit behind the head
            let n_blocks = new_state.max_reorg_depth * (new_state.reorg_count - state.reorg_count);
            if latest_block + n_blocks as u64 > state.latest_ethereum_block_number as u64 {
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
        nested_resolver: bool,
    ) -> Result<QueryResults, QueryResults> {
        // We need to use the same `QueryStore` for the entire query to ensure
        // we have a consistent view if the world, even when replicas, which
        // are eventually consistent, are in use. If we run different parts
        // of the query against different replicas, it would be possible for
        // them to be at wildly different states, and we might unwittingly
        // mix data from different block heights even if no reverts happen
        // while the query is running. `self.store` can not be used after this
        // point, and everything needs to go through the `store` we are
        // setting up here
        let store = self
            .store
            .query_store(target, false)
            .map_err(|e| QueryExecutionError::from(e))?;
        let state = store.deployment_state()?;
        let network = Some(store.network_name().to_string());
        let schema = store.api_schema()?;

        // Test only, see c435c25decbc4ad7bbbadf8e0ced0ff2
        #[cfg(debug_assertions)]
        let state = INITIAL_DEPLOYMENT_STATE_FOR_TESTS
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(state);

        let max_depth = max_depth.unwrap_or(*GRAPHQL_MAX_DEPTH);
        let query = crate::execution::Query::new(
            &self.logger,
            schema,
            network,
            query,
            max_complexity,
            max_depth,
        )?;
        self.load_manager
            .decide(
                store.wait_stats(),
                query.shape_hash,
                query.query_text.as_ref(),
            )
            .to_result()?;
        let by_block_constraint = query.block_constraint()?;
        let mut max_block = 0;
        let mut result: QueryResults = QueryResults::empty();

        // Note: This will always iterate at least once.
        for (bc, (selection_set, error_policy)) in by_block_constraint {
            let resolver = StoreResolver::at_block(
                &self.logger,
                store.cheap_clone(),
                self.subscription_manager.cheap_clone(),
                bc,
                error_policy,
                query.schema.id().clone(),
            )
            .await?;
            max_block = max_block.max(resolver.block_number());
            let query_res = execute_query(
                query.clone(),
                Some(selection_set),
                resolver.block_ptr.clone(),
                QueryExecutionOptions {
                    resolver,
                    deadline: GRAPHQL_QUERY_TIMEOUT.map(|t| Instant::now() + t),
                    max_first: max_first.unwrap_or(*GRAPHQL_MAX_FIRST),
                    max_skip: max_skip.unwrap_or(*GRAPHQL_MAX_SKIP),
                    load_manager: self.load_manager.clone(),
                },
                nested_resolver,
            )
            .await;
            result.append(query_res);
        }

        query.log_execution(max_block);
        self.deployment_changed(store.as_ref(), state, max_block as u64)
            .map_err(QueryResults::from)
            .map(|()| result)
    }
}

#[async_trait]
impl<S, SM> GraphQlRunnerTrait for GraphQlRunner<S, SM>
where
    S: QueryStoreManager,
    SM: SubscriptionManager,
{
    async fn run_query(
        self: Arc<Self>,
        query: Query,
        target: QueryTarget,
        nested_resolver: bool,
    ) -> QueryResults {
        self.run_query_with_complexity(
            query,
            target,
            *GRAPHQL_MAX_COMPLEXITY,
            Some(*GRAPHQL_MAX_DEPTH),
            Some(*GRAPHQL_MAX_FIRST),
            Some(*GRAPHQL_MAX_SKIP),
            nested_resolver,
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
        nested_resolver: bool,
    ) -> QueryResults {
        self.execute(
            query,
            target,
            max_complexity,
            max_depth,
            max_first,
            max_skip,
            nested_resolver,
        )
        .await
        .unwrap_or_else(|e| e)
    }

    async fn run_subscription(
        self: Arc<Self>,
        subscription: Subscription,
        target: QueryTarget,
    ) -> Result<SubscriptionResult, SubscriptionError> {
        let store = self.store.query_store(target, true)?;
        let schema = store.api_schema()?;
        let network = store.network_name().to_string();

        let query = crate::execution::Query::new(
            &self.logger,
            schema,
            Some(network.clone()),
            subscription.query,
            *GRAPHQL_MAX_COMPLEXITY,
            *GRAPHQL_MAX_DEPTH,
        )?;

        if let Err(err) = self
            .load_manager
            .decide(
                store.wait_stats(),
                query.shape_hash,
                query.query_text.as_ref(),
            )
            .to_result()
        {
            return Err(SubscriptionError::GraphQLError(vec![err]));
        }

        execute_prepared_subscription(
            query,
            SubscriptionExecutionOptions {
                logger: self.logger.clone(),
                store,
                subscription_manager: self.subscription_manager.cheap_clone(),
                timeout: GRAPHQL_QUERY_TIMEOUT.clone(),
                max_complexity: *GRAPHQL_MAX_COMPLEXITY,
                max_depth: *GRAPHQL_MAX_DEPTH,
                max_first: *GRAPHQL_MAX_FIRST,
                max_skip: *GRAPHQL_MAX_SKIP,
                load_manager: self.load_manager.cheap_clone(),
            },
        )
    }

    fn load_manager(&self) -> Arc<LoadManager> {
        self.load_manager.clone()
    }
}
