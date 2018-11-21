use std::io;
use std::sync::Arc;

use prelude::Logger;

/// Common trait for JSON-RPC admin server implementations.
pub trait JsonRpcServer<P, S> {
    type Server;

    fn serve(
        port: u16,
        http_port: u16,
        ws_port: u16,
        provider: Arc<P>,
        store: Arc<S>,
        logger: Logger,
    ) -> Result<Self::Server, io::Error>;
}
