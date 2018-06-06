use futures::prelude::*;
use hyper;
use hyper::Chunk;
use serde_json;
use std::error::Error;
use std::fmt;

use super::event::RuntimeEvent;

/// Errors that can occur while handling a data source runtime event.
#[derive(Debug)]
pub enum RuntimeRequestError {
    DeserializeError(serde_json::Error),
    ServerError(hyper::Error),
}

impl Error for RuntimeRequestError {
    fn description(&self) -> &str {
        "Runtime request error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            RuntimeRequestError::DeserializeError(ref e) => Some(e),
            RuntimeRequestError::ServerError(ref e) => Some(e),
        }
    }
}

impl fmt::Display for RuntimeRequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RuntimeRequestError::DeserializeError(ref e) => {
                write!(f, "Failed to deserialize the runtime request: {}", e)
            }
            RuntimeRequestError::ServerError(ref e) => {
                write!(f, "Failed to handle runtime request: {}", e)
            }
        }
    }
}

impl From<serde_json::Error> for RuntimeRequestError {
    fn from(e: serde_json::Error) -> Self {
        RuntimeRequestError::DeserializeError(e)
    }
}

impl From<hyper::Error> for RuntimeRequestError {
    fn from(e: hyper::Error) -> Self {
        RuntimeRequestError::ServerError(e)
    }
}

/// Future for a runtime event received via an HTTP request.
pub struct RuntimeRequest {
    body: Chunk,
}

impl RuntimeRequest {
    /// Creates a new RuntimeRequest future based on an HTTP request.
    pub fn new(body: Chunk) -> Self {
        RuntimeRequest { body }
    }
}

impl Future for RuntimeRequest {
    type Item = RuntimeEvent;
    type Error = RuntimeRequestError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Parse the request body into a RuntimeEvent
        let event: RuntimeEvent =
            serde_json::from_slice(&self.body).map_err(RuntimeRequestError::from)?;
        Ok(Async::Ready(event))
    }
}
