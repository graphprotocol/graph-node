use futures::future;
use futures::prelude::*;
use hyper;
use hyper::server::conn::AddrIncoming;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, Server, StatusCode};

#[derive(Default)]
struct RuntimeAdapterService;

impl Service for RuntimeAdapterService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response<Self::ReqBody>, Error = hyper::Error> + Send>;

    fn call(&mut self, _req: Request<Self::ReqBody>) -> Self::Future {
        let response = match _req.method() {
            &Method::POST => {
                // TODO: Parse body and forward to store
                Response::builder()
                    .status(StatusCode::CREATED)
                    .body(Body::from("OK"))
                    .unwrap()
            }
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not found"))
                .unwrap(),
        };

        Box::new(future::ok(response))
    }
}

pub fn start_server(addr: &str) {
    let addr = addr.parse().expect(
        "Invalid network address provided \
         for the Node.js runtime adapter server",
    );

    let new_service = || {
        let service = RuntimeAdapterService::default();
        future::ok::<RuntimeAdapterService, hyper::Error>(service)
    };

    Server::bind(&addr)
        .serve(new_service)
        .wait()
        .expect("Failed to start Node.js runtime adapter server");
}
