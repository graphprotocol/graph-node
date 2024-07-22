use std::sync::Arc;

use async_graphql::{Context, Result};
use graph_core::graphman::GraphmanContext;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graph_store_postgres::Store;
use slog::Logger;

pub fn make_graphman_context(ctx: &Context<'_>) -> Result<GraphmanContext> {
    let pool = ctx.data::<ConnectionPool>()?.to_owned();
    let notification_sender = ctx.data::<Arc<NotificationSender>>()?.to_owned();
    let store = ctx.data::<Arc<Store>>()?.to_owned();
    let logger = ctx.data::<Logger>()?.to_owned();

    Ok(GraphmanContext {
        pool,
        notification_sender,
        store,
        logger,
    })
}
