use std::sync::Arc;

use graph::components::server::server::{start, ServerHandle};
use graph::http_body_util::Full;
use graph::hyper::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE};
use graph::hyper::Response;

use graph::prelude::*;
use graph::prometheus::{Encoder, Registry, TextEncoder};

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
    pub async fn start(&self, port: u16) -> Result<ServerHandle, anyhow::Error> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting metrics server at: http://localhost:{}", port,
        );

        let server = self.clone();
        start(logger, port, move |_| {
            let server = server.clone();
            async move {
                let metric_families = server.registry.gather();
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                Ok(Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, encoder.format_type())
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Full::from(buffer))
                    .unwrap())
            }
        })
        .await
    }
}
