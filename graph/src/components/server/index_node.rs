use std::sync::Arc;

use futures::prelude::*;

use crate::prelude::Schema;

#[derive(Debug)]
/// This is only needed to support the explorer API
pub struct VersionInfo {
    pub created_at: String,
    pub deployment_id: String,
    pub latest_ethereum_block_number: Option<u64>,
    pub total_ethereum_blocks_count: Option<u64>,
    pub synced: bool,
    pub failed: bool,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub schema: Arc<Schema>,
    pub network: String,
}

/// Common trait for index node server implementations.
pub trait IndexNodeServer {
    type ServeError;

    /// Creates a new Tokio task that, when spawned, brings up the index node server.
    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<dyn Future<Item = (), Error = ()> + Send>, Self::ServeError>;
}
