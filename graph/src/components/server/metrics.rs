use futures::prelude::*;

/// Common trait for index node server implementations.
pub trait MetricsServer {
    type ServeError;

    /// Creates a new Tokio task that, when spawned, brings up the index node server.
    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<dyn Future<Item = (), Error = ()> + Send>, Self::ServeError>;
}
