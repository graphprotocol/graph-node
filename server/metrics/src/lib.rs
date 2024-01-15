use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use anyhow::Error;
use graph::tokio::net::TcpListener;
use hyper::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::Response;
use hyper_util::rt::TokioIo;
use thiserror::Error;

use graph::prelude::*;
use graph::prometheus::{Encoder, Registry, TextEncoder};

/// Errors that may occur when starting the server.
#[derive(Debug, Error)]
pub enum PrometheusMetricsServeError {
    #[error("Bind error: {0}")]
    BindError(String),
}

#[derive(Clone)]
pub struct PrometheusMetricsServer {
    logger: Logger,
    registry: Arc<Registry>,
}

impl PrometheusMetricsServer {
    pub fn new(logger_factory: &LoggerFactory, registry: Arc<Registry>) -> Self {
        PrometheusMetricsServer {
            logger: logger_factory.component_logger("MetricsServer", None),
            registry,
        }
    }

    /// Creates a new Tokio task that, when spawned, brings up the index node server.
    pub async fn serve(
        &mut self,
        port: u16,
    ) -> Result<Result<(), ()>, PrometheusMetricsServeError> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting metrics server at: http://localhost:{}", port,
        );

        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port))
            .await
            .map_err(|e| {
                PrometheusMetricsServeError::BindError(format!("unable to bind: {}", e))
            })?;

        let server = self.clone();
        let (stream, _) = listener.accept().await.map_err(|e| {
            PrometheusMetricsServeError::BindError(format!("unable to accept connections: {}", e))
        })?;
        let stream = TokioIo::new(stream);

        let mut builder =
            hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());

        Ok(builder
            .http1()
            .serve_connection(
                stream,
                service_fn(move |_| {
                    let metric_families = server.registry.gather();
                    let encoder = TextEncoder::new();
                    let body = encoder.encode_to_string(&metric_families).unwrap();
                    futures03::future::ok::<_, Error>(
                        Response::builder()
                            .status(200)
                            .header(CONTENT_TYPE, encoder.format_type())
                            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                            .body(body)
                            .unwrap(),
                    )
                }),
            )
            .await
            .map_err(move |e| error!(logger, "Metrics Server error"; "error" => format!("{}", e))))
    }
}
