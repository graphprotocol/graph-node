use std::sync::Arc;

use async_graphql::Context;
use async_graphql::Result;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graph_store_postgres::Store;

pub struct GraphmanContext {
    pub primary_pool: ConnectionPool,
    pub notification_sender: Arc<NotificationSender>,
    pub store: Arc<Store>,
}

impl GraphmanContext {
    pub fn new(ctx: &Context<'_>) -> Result<GraphmanContext> {
        let primary_pool = ctx.data::<ConnectionPool>()?.to_owned();
        let notification_sender = ctx.data::<Arc<NotificationSender>>()?.to_owned();
        let store = ctx.data::<Arc<Store>>()?.to_owned();

        Ok(GraphmanContext {
            primary_pool,
            notification_sender,
            store,
        })
    }
}
