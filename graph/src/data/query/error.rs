use graphql_parser::{query as q, Pos};
use serde::ser::*;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::string::FromUtf8Error;

/// Error caused while executing a [Query](struct.Query.html).
#[derive(Debug)]
pub enum QueryExecutionError {
    OperationNameRequired,
    OperationNotFound(String),
    NotSupported(String),
    NoRootQueryObjectType,
    NoRootSubscriptionObjectType,
    ResolveEntityError(Pos, String),
    ResolveListError,
    NonNullError(Pos, String),
    ListValueError(Pos, String),
    NamedTypeError(String),
    ObjectFieldError,
    AbstractTypeError(String),
    InvalidArgumentError(Pos, String, q::Value),
    MissingArgumentError(Pos, String),
    StoreQueryError(String),
    FilterNotSupportedError(String, String),
    //    ExecutionErrorList(Vec<QueryExecutionError>),
    UnknownField(Pos, String, String),
    EmptyQuery,
    MultipleSubscriptionFields,
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
            QueryExecutionError::NoRootSubscriptionObjectType => {
                write!(f, "No root Subscription type defined in the schema")
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
            QueryExecutionError::ObjectFieldError => write!(f, "Object field contains 0 types"),
            QueryExecutionError::AbstractTypeError(s) => {
                write!(f, "Failed to resolve abstract type: {}", s)
            }
            QueryExecutionError::InvalidArgumentError(_, s, v) => {
                write!(f, "Invalid value provided for argument \"{}\": {:?}", s, v)
            }
            QueryExecutionError::MissingArgumentError(_, s) => {
                write!(f, "No value provided for required argument: {}", s)
            }
            QueryExecutionError::StoreQueryError(s) => {
                write!(f, "Failed to resolve store query, error: {}", s)
            }
            QueryExecutionError::FilterNotSupportedError(value, filter) => write!(
                f,
                "Value does not support this filter, value: {}, filter: {}",
                value, filter
            ),
            QueryExecutionError::ResolveListError => {
                write!(f, "Failed to resolve named type within list type")
            }
            QueryExecutionError::UnknownField(_, t, s) => {
                write!(f, "Type \"{}\" has no field \"{}\"", t, s)
            }
            QueryExecutionError::EmptyQuery => write!(f, "The query is empty"),
            QueryExecutionError::MultipleSubscriptionFields => write!(
                f,
                "Only a single top-level field is allowed in subscriptions"
            ),
        }
    }
}

impl From<QueryExecutionError> for Vec<QueryExecutionError> {
    fn from(e: QueryExecutionError) -> Self {
        vec![e]
    }
}

/// Error caused while processing a [Query](struct.Query.html) request.
#[derive(Debug)]
pub enum QueryError {
    EncodingError(FromUtf8Error),
    ParseError(q::ParseError),
    ExecutionError(QueryExecutionError),
}

impl From<FromUtf8Error> for QueryError {
    fn from(e: FromUtf8Error) -> Self {
        QueryError::EncodingError(e)
    }
}

impl From<q::ParseError> for QueryError {
    fn from(e: q::ParseError) -> Self {
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
                let line: u32 = a
                    .matches(char::is_numeric)
                    .collect::<String>()
                    .parse()
                    .unwrap();
                let column: u32 = b
                    .matches(char::is_numeric)
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
