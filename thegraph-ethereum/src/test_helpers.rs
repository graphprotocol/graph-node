#[macro_use]
#[cfg(test)]
pub mod transport {
    use futures::prelude::*;
    use futures::{failed, finished};
    use jsonrpc_core;
    use serde_json;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use web3::error::{Error, ErrorKind};
    use web3::helpers::*;
    use web3::{RequestId, Transport};

    pub type Result<T> = Box<Future<Item = T, Error = Error> + Send + 'static>;

    #[derive(Debug, Default, Clone)]
    pub struct TestTransport {
        asserted: usize,
        requests: RefCell<Vec<(String, Vec<jsonrpc_core::Value>)>>,
        response: RefCell<VecDeque<jsonrpc_core::Value>>,
    }

    impl Transport for TestTransport {
        type Out = Result<jsonrpc_core::Value>;

        fn prepare(
            &self,
            method: &str,
            params: Vec<jsonrpc_core::Value>,
        ) -> (RequestId, jsonrpc_core::Call) {
            let request = build_request(1, method, params.clone());
            self.requests.borrow_mut().push((method.into(), params));
            (self.requests.borrow().len(), request)
        }

        fn send(&self, id: RequestId, request: jsonrpc_core::Call) -> Result<jsonrpc_core::Value> {
            match self.response.borrow_mut().pop_front() {
                Some(response) => Box::new(finished(response)),
                None => {
                    println!("Unexpected request (id: {:?}): {:?}", id, request);
                    Box::new(failed(ErrorKind::Unreachable.into()))
                }
            }
        }
    }

    impl TestTransport {
        pub fn set_response(&mut self, value: jsonrpc_core::Value) {
            *self.response.borrow_mut() = vec![value].into();
        }

        pub fn add_response(&mut self, value: jsonrpc_core::Value) {
            self.response.borrow_mut().push_back(value);
        }

        pub fn assert_request(&mut self, method: &str, params: &[String]) {
            let idx = self.asserted;
            self.asserted += 1;

            let (m, p) = self.requests
                .borrow()
                .get(idx)
                .expect("Expected result.")
                .clone();
            assert_eq!(&m, method);
            let p: Vec<String> = p.into_iter()
                .map(|p| serde_json::to_string(&p).unwrap())
                .collect();
            assert_eq!(p, params);
        }

        pub fn assert_no_more_requests(&mut self) {
            let requests = self.requests.borrow();
            assert_eq!(
                self.asserted,
                requests.len(),
                "Expected no more requests, got: {:?}",
                &requests[self.asserted..]
            );
        }
    }
}
