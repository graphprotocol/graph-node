use std::sync::Arc;

use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graph_store_postgres::Store;
use slog::Logger;

/// The data that is needed to execute graphman commands.
pub struct GraphmanContext {
    pub pool: ConnectionPool,
    pub notification_sender: Arc<NotificationSender>,
    pub store: Arc<Store>,
    pub logger: Logger,
}

impl AsRef<GraphmanContext> for GraphmanContext {
    fn as_ref(&self) -> &GraphmanContext {
        self
    }
}

impl AsMut<GraphmanContext> for GraphmanContext {
    fn as_mut(&mut self) -> &mut GraphmanContext {
        self
    }
}
