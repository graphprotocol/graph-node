extern crate futures;
extern crate http;
extern crate hyper;
use self::futures::future::Future;
use self::http::Uri;
use self::hyper::server::{Http, Request, Response, Service};
use self::hyper::{Method, StatusCode};

struct RuntimeAdapterServer;

impl Service for RuntimeAdapterServer {
  type Request = Request;
  type Response = Response;
  type Error = hyper::Error;
  type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

  fn call(&self, _req: Request) -> Self::Future {
    let mut response = Response::new();

    match _req.method() {
      &Method::Post => {
        let uri = _req.path().parse::<Uri>().unwrap();
        let (_, type_name) = uri.path().split_at(1);

        // TODO: Parse body and forward to store

        response.set_status(StatusCode::Created);
      }
      &Method::Put => {
        let uri = _req.path().parse::<Uri>().unwrap();
        let (_, type_name) = uri.path().split_at(1);

        // TODO: Parse body and forward to store

        response.set_status(StatusCode::Ok);
      }
      _ => {
        response.set_status(StatusCode::NotFound);
      }
    };

    Box::new(futures::future::ok(response))
  }
}

// fn get_path(path: &str) -> &str {
//   let uri = path.parse::<Uri>().unwrap();
//   let (_, type_name) = uri.path().split_at(1);
//   type_name
// }

pub fn start_server() {
  let addr = "127.0.0.1:8889".parse().unwrap();
  let server = Http::new()
    .bind(&addr, || Ok(RuntimeAdapterServer))
    .unwrap();
  server.run().unwrap();
}
