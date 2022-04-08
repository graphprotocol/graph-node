use crate::components::store::StoreError;
use crate::prelude::q;
use graphql_parser::Pos;
use std::error::Error;
use std::fmt::{self, Display};
use std::sync::Arc;

#[derive(Debug)]
pub struct CloneableAnyhowError(Arc<anyhow::Error>);

impl Clone for CloneableAnyhowError {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl From<anyhow::Error> for CloneableAnyhowError {
    fn from(f: anyhow::Error) -> Self {
        Self(Arc::new(f))
    }
}

/// Error caused while executing a [Query](struct.Query.html).
#[derive(Debug, Clone)]
pub enum QueryExecutionError {
    InvalidArgumentError(Pos, String, q::Value),
    Timeout,
    EnumCoercionError(Pos, String, q::Value, String, Vec<String>),
    ScalarCoercionError(Pos, String, q::Value, String),
    IncorrectPrefetchResult { slow: q::Value, prefetch: q::Value },
    StoreError(CloneableAnyhowError),
}

impl Display for QueryExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl Error for QueryExecutionError {
    fn description(&self) -> &str {
        "Query execution error"
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

impl From<StoreError> for QueryExecutionError {
    fn from(e: StoreError) -> Self {
        QueryExecutionError::StoreError(CloneableAnyhowError(Arc::new(e.into())))
    }
}
