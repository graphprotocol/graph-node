use std::sync::Arc;

use async_graphql::Context;
use async_graphql::Object;
use async_graphql::Result;
use graph_store_postgres::graphman::GraphmanStore;
use graphman_store::GraphmanStore as _;

use crate::entities::Execution;
use crate::entities::ExecutionId;

pub struct ExecutionQuery;

/// Queries related to command executions.
#[Object]
impl ExecutionQuery {
    /// Returns all stored command execution data.
    pub async fn info(&self, ctx: &Context<'_>, id: ExecutionId) -> Result<Execution> {
        let store = ctx.data::<Arc<GraphmanStore>>()?.to_owned();
        let execution = store.load_execution(id.into())?;

        Ok(execution.try_into()?)
    }
}
