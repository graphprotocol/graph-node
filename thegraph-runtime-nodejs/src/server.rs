use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::Sender;
use hyper;
use hyper::server::conn::AddrIncoming;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use serde_json;
use slog;

use super::event::RuntimeEvent;
use super::request::{RuntimeRequest, RuntimeRequestError};

struct RuntimeAdapterService {
    logger: slog::Logger,
    event_sender: Sender<RuntimeEvent>,
}

impl RuntimeAdapterService {
    pub fn new(logger: slog::Logger, event_sender: Sender<RuntimeEvent>) -> Self {
        RuntimeAdapterService {
            logger,
            event_sender,
        }
    }
}

impl Service for RuntimeAdapterService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = RuntimeRequestError;
    type Future = Box<Future<Item = Response<Self::ReqBody>, Error = Self::Error> + Send>;

    fn call(&mut self, request: Request<Self::ReqBody>) -> Self::Future {
        let read_logger = self.logger.clone();
        let deserialize_logger = self.logger.clone();
        let send_logger = self.logger.clone();

        let event_sender = self.event_sender.clone();

        match request.method() {
            &Method::POST => Box::new({
                let logger = self.logger.clone();

                // Read the request body into a single chunk
                request
                    .into_body()
                    .concat2()
                    .map_err(RuntimeRequestError::from)
                    .and_then(RuntimeRequest::new)
                    .and_then(move |event| {
                        event_sender
                            .clone()
                            .send(event)
                            .map_err(|e| panic!("Failed to forward runtime event: {}", e))
                    })
                    .then(move |result| {
                        if let Err(ref e) = result {
                            error!(logger, "Failed to handle runtime event";
                                   "error" => format!("{:?}", e));
                        }

                        // Once we're done with the above, send a response back
                        match result {
                            Ok(_) => future::ok(
                                Response::builder()
                                    .status(StatusCode::CREATED)
                                    .body(Body::from("OK"))
                                    .unwrap(),
                            ),
                            Err(e) => future::ok(
                                Response::builder()
                                    .status(StatusCode::BAD_REQUEST)
                                    .body(Body::from(format!("{}", e)))
                                    .unwrap(),
                            ),
                        }
                    })
            }),
            _ => Box::new(future::ok(
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("Not found"))
                    .unwrap(),
            )),
        }
    }
}

/// Starts the runtime adpater's HTTP server to handle events from the runtime.
pub fn start_server(
    logger: slog::Logger,
    event_sender: Sender<RuntimeEvent>,
    addr: &str,
) -> Box<Future<Item = (), Error = ()> + Send> {
    // Parse the server address
    let addr = addr.parse().expect(
        "Invalid network address provided \
         for the Node.js runtime adapter server",
    );

    // Create a new service factory
    let new_service = move || {
        future::ok::<RuntimeAdapterService, hyper::Error>(RuntimeAdapterService::new(
            logger.clone(),
            event_sender.clone(),
        ))
    };

    // Start serving requests
    Box::new(
        Server::bind(&addr)
            .serve(new_service)
            .map_err(|e| panic!("Failed to start runtime adapter server: {}", e)),
    )
}
