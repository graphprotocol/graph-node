use std::io;
use std::sync::Arc;

use components::subgraph::SubgraphProvider;
use prelude::Logger;

/// Common trait for JSON-RPC admin server implementations.
pub trait JsonRpcServer {
    type Server;

    fn serve(
        port: u16,
        provider: Arc<impl SubgraphProvider>,
        logger: Logger,
    ) -> Result<Self::Server, io::Error>;
}
