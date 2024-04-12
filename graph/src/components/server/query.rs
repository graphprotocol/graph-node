use http_body_util::Full;
use hyper::body::Bytes;
use hyper::Response;

use crate::data::query::QueryError;
use std::error::Error;
use std::fmt;

use crate::components::store::StoreError;

pub type ServerResponse = Response<Full<Bytes>>;
pub type ServerResult = Result<ServerResponse, ServerError>;

/// Errors that can occur while processing incoming requests.
#[derive(Debug)]
pub enum ServerError {
    ClientError(String),
    QueryError(QueryError),
    InternalError(String),
}

impl From<QueryError> for ServerError {
    fn from(e: QueryError) -> Self {
        ServerError::QueryError(e)
    }
}

impl From<StoreError> for ServerError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::ConstraintViolation(s) => ServerError::InternalError(s),
            _ => ServerError::ClientError(e.to_string()),
        }
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ServerError::ClientError(ref s) => {
                write!(f, "GraphQL server error (client error): {}", s)
            }
            ServerError::QueryError(ref e) => {
                write!(f, "GraphQL server error (query error): {}", e)
            }
            ServerError::InternalError(ref s) => {
                write!(f, "GraphQL server error (internal error): {}", s)
            }
        }
    }
}

impl Error for ServerError {
    fn description(&self) -> &str {
        "Failed to process the GraphQL request"
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            ServerError::ClientError(_) => None,
            ServerError::QueryError(ref e) => Some(e),
            ServerError::InternalError(_) => None,
        }
    }
}
