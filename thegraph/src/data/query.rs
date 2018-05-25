use futures::sync::oneshot;
use graphql_parser::Pos;
use graphql_parser::query;
use serde::ser::*;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::string::FromUtf8Error;

use data::schema::Schema;

#[derive(Deserialize)]
#[serde(untagged, remote = "query::Value")]
enum GraphQLValue {
    String(String),
}

/// Variable value for a GraphQL query.
#[derive(Debug, Deserialize)]
pub struct QueryVariableValue(#[serde(with = "GraphQLValue")] query::Value);

impl Deref for QueryVariableValue {
    type Target = query::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for QueryVariableValue {
    fn deref_mut(&mut self) -> &mut query::Value {
        &mut self.0
    }
}

impl PartialEq for QueryVariableValue {
    fn eq(&self, other: &QueryVariableValue) -> bool {
        self.0 == other.0
    }
}

impl<'a> From<&'a str> for QueryVariableValue {
    fn from(s: &'a str) -> Self {
        QueryVariableValue(query::Value::String(s.to_string()))
    }
}

/// Variable values for a GraphQL query.
#[derive(Debug, Deserialize)]
pub struct QueryVariables(HashMap<String, QueryVariableValue>);

impl QueryVariables {
    pub fn new() -> Self {
        QueryVariables(HashMap::new())
    }
}

impl Deref for QueryVariables {
    type Target = HashMap<String, QueryVariableValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for QueryVariables {
    fn deref_mut(&mut self) -> &mut HashMap<String, QueryVariableValue> {
        &mut self.0
    }
}

impl PartialEq for QueryVariables {
    fn eq(&self, other: &QueryVariables) -> bool {
        self.0 == other.0
    }
}

/// A GraphQL query as submitted by a client, either directly or through a subscription.
#[derive(Debug)]
pub struct Query {
    pub schema: Schema,
    pub document: query::Document,
    pub variables: Option<QueryVariables>,
    pub result_sender: oneshot::Sender<QueryResult>,
}

/// The result of running a query, if successful.
#[derive(Debug)]
pub struct QueryResult {
    pub data: Option<query::Value>,
    pub errors: Option<Vec<QueryError>>,
}

impl QueryResult {
    pub fn new(data: Option<query::Value>) -> Self {
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

/// Error caused while executing a [Query](struct.Query.html).
#[derive(Debug)]
pub enum QueryExecutionError {
    OperationNameRequired,
    OperationNotFound(String),
    NotSupported(String),
    NoRootQueryObjectType,
    ResolveEntityError(Pos, String),
    NonNullError(Pos, String),
    ListValueError(Pos, String),
    NamedTypeError(String),
    AbstractTypeError(String),
    InvalidArgumentError(Pos, String, query::Value),
    MissingArgumentError(Pos, String),
}

impl Error for QueryExecutionError {
    fn description(&self) -> &str {
        "Query execution error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for QueryExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryExecutionError::OperationNameRequired => write!(f, "Operation name required"),
            QueryExecutionError::OperationNotFound(s) => {
                write!(f, "Operation name not found: {}", s)
            }
            QueryExecutionError::NotSupported(s) => write!(f, "Not supported: {}", s),
            QueryExecutionError::NoRootQueryObjectType => {
                write!(f, "No root Query type defined in the schema")
            }
            QueryExecutionError::ResolveEntityError(_, s) => {
                write!(f, "Failed to resolve entity: {}", s)
            }
            QueryExecutionError::NonNullError(_, s) => {
                write!(f, "Null value resolved for non-null field: {}", s)
            }
            QueryExecutionError::ListValueError(_, s) => {
                write!(f, "Non-list value resolved for list field: {}", s)
            }
            QueryExecutionError::NamedTypeError(s) => {
                write!(f, "Failed to resolve named type: {}", s)
            }
            QueryExecutionError::AbstractTypeError(s) => {
                write!(f, "Failed to resolve abstract type: {}", s)
            }
            QueryExecutionError::InvalidArgumentError(_, s, v) => {
                write!(f, "Invalid value provided for argument \"{}\": {:?}", s, v)
            }
            QueryExecutionError::MissingArgumentError(_, s) => {
                write!(f, "No value provided for required argument: {}", s)
            }
        }
    }
}

/// Error caused while processing a [Query](struct.Query.html) request.
#[derive(Debug)]
pub enum QueryError {
    EncodingError(FromUtf8Error),
    ParseError(query::ParseError),
    ExecutionError(QueryExecutionError),
}

impl From<FromUtf8Error> for QueryError {
    fn from(e: FromUtf8Error) -> Self {
        QueryError::EncodingError(e)
    }
}

impl From<query::ParseError> for QueryError {
    fn from(e: query::ParseError) -> Self {
        QueryError::ParseError(e)
    }
}

impl From<QueryExecutionError> for QueryError {
    fn from(e: QueryExecutionError) -> Self {
        QueryError::ExecutionError(e)
    }
}

impl Error for QueryError {
    fn description(&self) -> &str {
        "Query error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            &QueryError::EncodingError(ref e) => Some(e),
            &QueryError::ExecutionError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &QueryError::EncodingError(ref e) => write!(f, "{}", e),
            &QueryError::ExecutionError(ref e) => write!(f, "{}", e),
            &QueryError::ParseError(ref e) => write!(f, "{}", e),
        }
    }
}

impl Serialize for QueryError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;

        let msg = match self {
            // Serialize parse errors with their location (line, column) to make it easier
            // for users to find where the errors are; this is likely to change as the
            // graphql_parser team makes improvements to their error reporting
            QueryError::ParseError(_) => {
                // Split the inner message into (first line, rest)
                let mut msg = format!("{}", self);
                let inner_msg = msg.replace("query parse error:", "");
                let inner_msg = inner_msg.trim();
                let parts: Vec<&str> = inner_msg.splitn(2, "\n").collect();

                // Find the colon in the first line and split there
                let colon_pos = parts[0].rfind(":").unwrap();
                let (a, b) = parts[0].split_at(colon_pos);

                // Find the line and column numbers and convert them to u32
                let line: u32 = a.matches(char::is_numeric)
                    .collect::<String>()
                    .parse()
                    .unwrap();
                let column: u32 = b.matches(char::is_numeric)
                    .collect::<String>()
                    .parse()
                    .unwrap();

                // Generate the list of locations
                let mut location = HashMap::new();
                location.insert("line", line);
                location.insert("column", column);
                map.serialize_entry("locations", &vec![location])?;

                // Only use the remainder after the location as the error message
                parts[1].to_string()
            }

            // Serialize entity resolution errors using their position
            QueryError::ExecutionError(QueryExecutionError::ResolveEntityError(pos, _))
            | QueryError::ExecutionError(QueryExecutionError::NonNullError(pos, _))
            | QueryError::ExecutionError(QueryExecutionError::ListValueError(pos, _))
            | QueryError::ExecutionError(QueryExecutionError::InvalidArgumentError(pos, _, _))
            | QueryError::ExecutionError(QueryExecutionError::MissingArgumentError(pos, _)) => {
                let mut location = HashMap::new();
                location.insert("line", pos.line);
                location.insert("column", pos.column);
                map.serialize_entry("locations", &vec![location])?;
                format!("{}", self)
            }
            _ => format!("{}", self),
        };

        map.serialize_entry("message", msg.as_str())?;
        map.end()
    }
}
