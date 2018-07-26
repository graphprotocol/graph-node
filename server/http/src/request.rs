use futures::prelude::*;
use futures::sync::oneshot;
use graphql_parser;
use hyper::Chunk;
use serde_json;

use graph::components::server::GraphQLServerError;
use graph::prelude::*;

/// Future for a query parsed from an HTTP request.
pub struct GraphQLRequest {
    body: Chunk,
    schema: Option<Schema>,
}

impl GraphQLRequest {
    /// Creates a new GraphQLRequest future based on an HTTP request and a result sender.
    pub fn new(body: Chunk, schema: Option<Schema>) -> Self {
        GraphQLRequest { body, schema }
    }
}

impl Future for GraphQLRequest {
    type Item = (Query, oneshot::Receiver<QueryResult>);
    type Error = GraphQLServerError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Fail if no schema is available
        let schema = self.schema
            .clone()
            .ok_or(GraphQLServerError::InternalError(
                "No schema available to query".to_string(),
            ))?;

        // Parse request body as JSON
        let json: serde_json::Value = serde_json::from_slice(&self.body)
            .map_err(|e| GraphQLServerError::ClientError(format!("{}", e)))?;

        // Ensure the JSON data is an object
        let obj = json.as_object()
            .ok_or(GraphQLServerError::ClientError(String::from(
                "Request data is not an object",
            )))?;

        // Ensure the JSON data has a "query" field
        let query_value = obj.get("query")
            .ok_or(GraphQLServerError::ClientError(String::from(
                "The \"query\" field missing in request data",
            )))?;

        // Ensure the "query" field is a string
        let query_string = query_value.as_str().ok_or(GraphQLServerError::ClientError(
            String::from("The\"query\" field is not a string"),
        ))?;

        // Parse the "query" field of the JSON body
        let document = graphql_parser::parse_query(query_string)
            .map_err(|e| GraphQLServerError::from(QueryError::from(e)))?;

        // Parse the "variables" field of the JSON body, if present
        let variables = match obj.get("variables") {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(variables @ serde_json::Value::Object(_)) => {
                serde_json::from_value(variables.clone())
                    .map_err(|e| GraphQLServerError::ClientError(format!("{}", e)))
                    .map(|v| Some(v))
            }
            _ => Err(GraphQLServerError::ClientError(format!(
                "Invalid query variables provided"
            ))),
        }?;

        // Create a one-shot channel to allow another part of the system
        // to notify the service when the query has completed
        let (sender, receiver) = oneshot::channel();

        Ok(Async::Ready((
            Query {
                document,
                variables,
                schema: schema,
                result_sender: sender,
            },
            receiver,
        )))
    }
}

#[cfg(test)]
mod tests {
    use graphql_parser;
    use hyper;
    use tokio_core::reactor::Core;

    use graph::prelude::*;

    use super::GraphQLRequest;

    const EXAMPLE_SCHEMA: &'static str = "type Query { users: [User!] }";

    #[test]
    fn rejects_invalid_json() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(hyper::Chunk::from("!@#)%"), Some(schema));
        let result = core.run(request);
        result.expect_err("Should reject invalid JSON");
    }

    #[test]
    fn rejects_json_without_query_field() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(hyper::Chunk::from("{}"), Some(schema));
        let result = core.run(request);
        result.expect_err("Should reject JSON without query field");
    }

    #[test]
    fn rejects_json_with_non_string_query_field() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(hyper::Chunk::from("{\"query\": 5}"), Some(schema));
        let result = core.run(request);
        result.expect_err("Should reject JSON with a non-string query field");
    }

    #[test]
    fn rejects_broken_queries() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(hyper::Chunk::from("{\"query\": \"foo\"}"), Some(schema));
        let result = core.run(request);
        result.expect_err("Should reject broken queries");
    }

    #[test]
    fn accepts_valid_queries() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(
            hyper::Chunk::from("{\"query\": \"{ user { name } }\"}"),
            Some(schema),
        );
        let result = core.run(request);
        let (query, _) = result.expect("Should accept valid queries");
        assert_eq!(
            query.document,
            graphql_parser::parse_query("{ user { name } }").unwrap()
        );
    }

    #[test]
    fn accepts_null_variables() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(
            hyper::Chunk::from(
                "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": null \
                 }",
            ),
            Some(schema),
        );
        let result = core.run(request);
        let (query, _) = result.expect("Should accept null variables");

        let expected_query = graphql_parser::parse_query("{ user { name } }").unwrap();
        assert_eq!(query.document, expected_query);
        assert_eq!(query.variables, None);
    }

    #[test]
    fn rejects_non_map_variables() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(
            hyper::Chunk::from(
                "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": 5 \
                 }",
            ),
            Some(schema),
        );
        let result = core.run(request);
        result.expect_err("Should reject non-map variables");
    }

    #[test]
    fn parses_variables() {
        let mut core = Core::new().unwrap();
        let schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(EXAMPLE_SCHEMA).unwrap(),
        };
        let request = GraphQLRequest::new(
            hyper::Chunk::from(
                "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": { \"foo\": \"bar\" } \
                 }",
            ),
            Some(schema),
        );
        let result = core.run(request);
        let (query, _) = result.expect("Should accept valid queries");

        let expected_query = graphql_parser::parse_query("{ user { name } }").unwrap();
        let mut expected_variables = QueryVariables::new();
        expected_variables.insert("foo".to_string(), QueryVariableValue::from("bar"));

        assert_eq!(query.document, expected_query);
        assert_eq!(query.variables, Some(expected_variables));
    }
}
