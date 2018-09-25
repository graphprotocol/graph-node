use serde::ser::*;

use prelude::QueryExecutionError;

/// Error caused while processing a [Subscription](struct.Subscription.html) request.
#[derive(Debug, Fail)]
pub enum SubscriptionError {
    #[fail(display = "GraphQL error: {}", _0)]
    GraphQLError(QueryExecutionError),
    #[fail(display = "GraphQL errors: {:?}", _0)]
    GraphQLErrorList(Vec<QueryExecutionError>),
}

impl From<QueryExecutionError> for SubscriptionError {
    fn from(e: QueryExecutionError) -> Self {
        SubscriptionError::GraphQLError(e)
    }
}

impl From<Vec<QueryExecutionError>> for SubscriptionError {
    fn from(e: Vec<QueryExecutionError>) -> Self {
        SubscriptionError::GraphQLErrorList(e)
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
