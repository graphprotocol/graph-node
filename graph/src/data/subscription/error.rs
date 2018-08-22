use serde::ser::*;
use std::error::Error;
use std::fmt;

use prelude::QueryExecutionError;

/// Error caused while processing a [Subscription](struct.Subscription.html) request.
#[derive(Debug)]
pub enum SubscriptionError {
    GraphQLError(QueryExecutionError),
}

impl From<QueryExecutionError> for SubscriptionError {
    fn from(e: QueryExecutionError) -> Self {
        SubscriptionError::GraphQLError(e)
    }
}

impl Error for SubscriptionError {
    fn description(&self) -> &str {
        "Subscription error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            SubscriptionError::GraphQLError(e) => Some(e),
        }
    }
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SubscriptionError::GraphQLError(e) => write!(f, "{}", e),
        }
    }
}

impl Serialize for SubscriptionError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        let msg = format!("{}", self);
        map.serialize_entry("message", msg.as_str())?;
        map.end()
    }
}
