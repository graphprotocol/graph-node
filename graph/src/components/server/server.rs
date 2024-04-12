use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::Request;

use crate::cheap_clone::CheapClone;
use crate::hyper::server::conn::http1;
use crate::hyper::service::service_fn;
use crate::hyper_util::rt::TokioIo;
use crate::slog::error;
use crate::tokio::net::TcpListener;
use crate::tokio::task::JoinHandle;
use crate::{anyhow, tokio};

use crate::prelude::Logger;

use super::query::ServerResult;

/// A handle to the server that can be used to shut it down. The `accepting`
/// field is only used in tests to check if the server is running
pub struct ServerHandle {
    pub handle: JoinHandle<()>,
    pub accepting: Arc<AtomicBool>,
}

pub async fn start<F, S>(
    logger: Logger,
    port: u16,
    handler: F,
) -> Result<ServerHandle, anyhow::Error>
where
    F: Fn(Request<Incoming>) -> S + Send + Clone + 'static,
    S: Future<Output = ServerResult> + Send + 'static,
{
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    let accepting = Arc::new(AtomicBool::new(false));
    let accepting2 = accepting.cheap_clone();
    let handle = crate::spawn(async move {
        accepting2.store(true, std::sync::atomic::Ordering::SeqCst);
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(res) => res,
                Err(e) => {
                    error!(logger, "Error accepting connection"; "error" => e.to_string());
                    continue;
                }
            };

            // Use an adapter to access something implementing `tokio::io` traits as if they implement
            // `hyper::rt` IO traits.
            let io = TokioIo::new(stream);

            let handler = handler.clone();
            // Spawn a tokio task to serve multiple connections concurrently
            tokio::task::spawn(async move {
                let new_service = service_fn(handler);
                // Finally, we bind the incoming connection to our `hello` service
                http1::Builder::new()
                    // `service_fn` converts our function in a `Service`
                    .serve_connection(io, new_service)
                    .await
            });
        }
    });
    Ok(ServerHandle { handle, accepting })
}
