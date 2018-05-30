use graphql_parser::query as q;

use super::error::{QueryError, QueryExecutionError};

/// The result of running a query, if successful.
#[derive(Debug)]
pub struct QueryResult {
    pub data: Option<q::Value>,
    pub errors: Option<Vec<QueryError>>,
}

impl QueryResult {
    pub fn new(data: Option<q::Value>) -> Self {
        QueryResult { data, errors: None }
    }

    pub fn add_error(&mut self, e: QueryError) {
        let errors = self.errors.get_or_insert(vec![]);
        errors.push(e);
    }
}

impl From<QueryExecutionError> for QueryResult {
    fn from(e: QueryExecutionError) -> Self {
        let mut result = Self::new(None);
        result.errors = Some(vec![QueryError::from(e)]);
        result
    }
}
