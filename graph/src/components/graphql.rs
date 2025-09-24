use crate::data::query::{Query, QueryTarget};
use crate::data::query::{QueryResults, SqlQueryReq};
use crate::data::store::SqlQueryObject;
use crate::prelude::{DeploymentHash, QueryExecutionError};

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

pub enum GraphQlTarget {
    SubgraphName(String),
    Deployment(DeploymentHash),
}
/// A component that can run GraphqL queries against a [Store](../store/trait.Store.html).
#[async_trait]
pub trait GraphQlRunner: Send + Sync + 'static {
    /// Runs a GraphQL query and returns its result.
    async fn run_query(self: Arc<Self>, query: Query, target: QueryTarget) -> QueryResults;

    /// Runs a GraphqL query up to the given complexity. Overrides the global complexity limit.
    async fn run_query_with_complexity(
        self: Arc<Self>,
        query: Query,
        target: QueryTarget,
        max_complexity: Option<u64>,
        max_depth: Option<u8>,
        max_first: Option<u32>,
        max_skip: Option<u32>,
    ) -> QueryResults;

    fn metrics(&self) -> Arc<dyn GraphQLMetrics>;

    async fn run_sql_query(
        self: Arc<Self>,
        req: SqlQueryReq,
    ) -> Result<Vec<SqlQueryObject>, QueryExecutionError>;
}

pub trait GraphQLMetrics: Send + Sync + 'static {
    fn observe_query_execution(&self, duration: Duration, results: &QueryResults);
    fn observe_query_parsing(&self, duration: Duration, results: &QueryResults);
    fn observe_query_validation(&self, duration: Duration, id: &DeploymentHash);
    fn observe_query_validation_error(&self, error_codes: Vec<&str>, id: &DeploymentHash);
    fn observe_query_blocks_behind(&self, blocks_behind: i32, id: &DeploymentHash);
}
