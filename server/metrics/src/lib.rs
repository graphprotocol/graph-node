use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use hyper;
use hyper::service::service_fn_ok;
use hyper::{Body, Response, Server};
use prometheus::{Encoder, Registry, TextEncoder};

use graph::prelude::{MetricsServer as MetricsServerTrait, *};

/// Errors that may occur when starting the server.
#[derive(Debug)]
pub enum PrometheusMetricsServeError {
    BindError(hyper::Error),
}

impl Error for PrometheusMetricsServeError {
    fn description(&self) -> &str {
        "Failed to start the server"
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl Display for PrometheusMetricsServeError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            PrometheusMetricsServeError::BindError(e) => {
                write!(f, "Failed to bind index node server: {}", e)
            }
        }
    }
}

impl From<hyper::Error> for PrometheusMetricsServeError {
    fn from(err: hyper::Error) -> Self {
        PrometheusMetricsServeError::BindError(err)
    }
}

pub struct PrometheusMetricsServer {
    logger: Logger,
    registry: Arc<Registry>,
}

impl Clone for PrometheusMetricsServer {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            registry: self.registry.clone(),
        }
    }
}

impl PrometheusMetricsServer {
    pub fn new(logger_factory: &LoggerFactory, registry: Arc<Registry>) -> Self {
        PrometheusMetricsServer {
            logger: logger_factory.component_logger("MetricsServer", None),
            registry,
        }
    }
}

impl MetricsServerTrait for PrometheusMetricsServer {
    type ServeError = PrometheusMetricsServeError;

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<dyn Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        info!(
            logger,
            "Starting metrics server at: http://localhost:{}", port,
        );

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        let server = self.clone();
        let new_service = move || {
            let registry = server.registry.clone();
            service_fn_ok(move |_req| {
                let metric_families = registry.gather();
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                Response::builder()
                    .status(200)
                    .header(hyper::header::CONTENT_TYPE, encoder.format_type())
                    .body(Body::from(buffer))
                    .unwrap()
            })
        };

        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Metrics server error"; "error" => format!("{}", e)));

        Ok(Box::new(task))
    }
}
