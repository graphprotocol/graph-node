use std::sync::Arc;
use std::time::Duration;

use graph::blockchain::BlockchainMap;
use graph::cheap_clone::CheapClone;
use graph::components::graphql::GraphQLMetrics;
use graph::components::link_resolver::LinkResolver;
use graph::components::server::query::{ServerResponse, ServerResult};
use graph::data::subgraph::DeploymentHash;
use graph::http_body_util::{BodyExt, Full};
use graph::hyper::body::{Bytes, Incoming};
use graph::hyper::header::{
    self, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    CONTENT_TYPE, LOCATION,
};
use graph::hyper::{body::Body, Method, Request, Response, StatusCode};

use graph::components::{server::query::ServerError, store::Store};
use graph::data::query::{Query, QueryError, QueryResult, QueryResults};
use graph::prelude::{q, serde_json};
use graph::slog::{debug, error, Logger};
use graph_graphql::prelude::{execute_query, Query as PreparedQuery, QueryExecutionOptions};

use crate::auth::bearer_token;

use crate::explorer::Explorer;
use crate::resolver::IndexNodeResolver;
use crate::schema::SCHEMA;

struct NoopGraphQLMetrics;

impl GraphQLMetrics for NoopGraphQLMetrics {
    fn observe_query_execution(&self, _duration: Duration, _results: &QueryResults) {}
    fn observe_query_parsing(&self, _duration: Duration, _results: &QueryResults) {}
    fn observe_query_validation(&self, _duration: Duration, _id: &DeploymentHash) {}
    fn observe_query_validation_error(&self, _error_codes: Vec<&str>, _id: &DeploymentHash) {}
    fn observe_query_blocks_behind(&self, _blocks_behind: i32, _id: &DeploymentHash) {}
}

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct IndexNodeService<S> {
    logger: Logger,
    blockchain_map: Arc<BlockchainMap>,
    store: Arc<S>,
    explorer: Arc<Explorer<S>>,
    link_resolver: Arc<dyn LinkResolver>,
}

impl<S> IndexNodeService<S>
where
    S: Store,
{
    pub fn new(
        logger: Logger,
        blockchain_map: Arc<BlockchainMap>,
        store: Arc<S>,
        link_resolver: Arc<dyn LinkResolver>,
    ) -> Self {
        let explorer = Arc::new(Explorer::new(store.clone()));

        IndexNodeService {
            logger,
            blockchain_map,
            store,
            explorer,
            link_resolver,
        }
    }

    fn graphiql_html() -> &'static str {
        include_str!("../assets/index.html")
    }

    /// Serves a static file.
    fn serve_file(contents: &'static str, content_type: &'static str) -> ServerResponse {
        Response::builder()
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, content_type)
            .status(200)
            .body(Full::from(contents))
            .unwrap()
    }

    fn index() -> ServerResponse {
        Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/html")
            .body(Full::from("OK"))
            .unwrap()
    }

    fn handle_graphiql() -> ServerResponse {
        Self::serve_file(Self::graphiql_html(), "text/html")
    }

    pub async fn handle_graphql_query<T: Body>(
        &self,
        request: Request<T>,
    ) -> Result<QueryResults, ServerError> {
        let store = self.store.clone();

        let bearer_token = bearer_token(request.headers())
            .map(<[u8]>::to_vec)
            .map(String::from_utf8)
            .transpose()
            .map_err(|_| ServerError::ClientError("Bearer token is invalid UTF-8".to_string()))?;

        let body = request
            .collect()
            .await
            .map_err(|_| ServerError::InternalError("Failed to read request body".into()))?
            .to_bytes();

        let validated = ValidatedRequest::new(body, bearer_token)?;
        let query = validated.query;

        let schema = SCHEMA.clone();
        let query = match PreparedQuery::new(
            &self.logger,
            schema,
            None,
            query,
            None,
            100,
            Arc::new(NoopGraphQLMetrics),
        ) {
            Ok(query) => query,
            Err(e) => return Ok(QueryResults::from(QueryResult::from(e))),
        };

        // Run the query using the index node resolver
        let query_clone = query.cheap_clone();
        let logger = self.logger.cheap_clone();
        let result: QueryResult = {
            let resolver = IndexNodeResolver::new(
                &logger,
                store,
                self.link_resolver.clone(),
                validated.bearer_token,
                self.blockchain_map.clone(),
            );
            let options = QueryExecutionOptions {
                resolver,
                deadline: None,
                max_first: std::u32::MAX,
                max_skip: std::u32::MAX,
                trace: false,
            };
            let (result, _) = execute_query(query_clone.cheap_clone(), None, None, options).await;
            query_clone.log_execution(0);
            // Index status queries are not cacheable, so we may unwrap this.
            Arc::try_unwrap(result).unwrap()
        };

        Ok(QueryResults::from(result))
    }

    // Handles OPTIONS requests
    fn handle_graphql_options<T>(_request: Request<T>) -> ServerResponse {
        Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/plain")
            .header(ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, User-Agent")
            .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS, POST")
            .body(Full::from(""))
            .unwrap()
    }

    /// Handles 302 redirects
    fn handle_temp_redirect(destination: &str) -> ServerResult {
        header::HeaderValue::from_str(destination)
            .map_err(|_| ServerError::ClientError("invalid characters in redirect URL".into()))
            .map(|loc_header_val| {
                Response::builder()
                    .status(StatusCode::FOUND)
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(LOCATION, loc_header_val)
                    .body(Full::from("Redirecting..."))
                    .unwrap()
            })
    }

    /// Handles 404s.
    pub(crate) fn handle_not_found() -> ServerResponse {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/plain")
            .body(Full::from("Not found\n"))
            .unwrap()
    }

    async fn handle_call<T: Body>(&self, req: Request<T>) -> ServerResult {
        let method = req.method().clone();

        let path = req.uri().path().to_owned();
        let path_segments = {
            let mut segments = path.split('/');

            // Remove leading '/'
            assert_eq!(segments.next(), Some(""));

            segments.collect::<Vec<_>>()
        };

        match (method, path_segments.as_slice()) {
            (Method::GET, [""]) => Ok(Self::index()),
            (Method::GET, ["graphiql.css"]) => Ok(Self::serve_file(
                include_str!("../assets/graphiql.css"),
                "text/css",
            )),
            (Method::GET, ["graphiql.min.js"]) => Ok(Self::serve_file(
                include_str!("../assets/graphiql.min.js"),
                "text/javascript",
            )),

            (Method::GET, path @ ["graphql"]) => {
                let dest = format!("/{}/playground", path.join("/"));
                Self::handle_temp_redirect(&dest)
            }
            (Method::GET, ["graphql", "playground"]) => Ok(Self::handle_graphiql()),

            (Method::POST, ["graphql"]) => {
                Ok(self.handle_graphql_query(req).await?.as_http_response())
            }
            (Method::OPTIONS, ["graphql"]) => Ok(Self::handle_graphql_options(req)),

            (Method::GET, ["explorer", rest @ ..]) => self.explorer.handle(&self.logger, rest),

            _ => Ok(Self::handle_not_found()),
        }
    }

    pub async fn call(&self, req: Request<Incoming>) -> ServerResponse {
        let logger = self.logger.clone();

        // Returning Err here will prevent the client from receiving any response.
        // Instead, we generate a Response with an error code and return Ok
        let result = self.handle_call(req).await;
        match result {
            Ok(response) => response,
            Err(err @ ServerError::ClientError(_)) => {
                debug!(logger, "IndexNodeService call failed: {}", err);

                Response::builder()
                    .status(400)
                    .header(CONTENT_TYPE, "text/plain")
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Full::from(format!("Invalid request: {}", err)))
                    .unwrap()
            }
            Err(err @ ServerError::QueryError(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Response::builder()
                    .status(400)
                    .header(CONTENT_TYPE, "text/plain")
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Full::from(format!("Query error: {}", err)))
                    .unwrap()
            }
            Err(err @ ServerError::InternalError(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Response::builder()
                    .status(500)
                    .header(CONTENT_TYPE, "text/plain")
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .body(Full::from(format!("Internal server error: {}", err)))
                    .unwrap()
            }
        }
    }
}

struct ValidatedRequest {
    pub query: Query,
    pub bearer_token: Option<String>,
}

impl ValidatedRequest {
    pub fn new(req_body: Bytes, bearer_token: Option<String>) -> Result<Self, ServerError> {
        // Parse request body as JSON
        let json: serde_json::Value = serde_json::from_slice(&req_body)
            .map_err(|e| ServerError::ClientError(format!("{}", e)))?;

        // Ensure the JSON data is an object
        let obj = json.as_object().ok_or_else(|| {
            ServerError::ClientError(String::from("ValidatedRequest data is not an object"))
        })?;

        // Ensure the JSON data has a "query" field
        let query_value = obj.get("query").ok_or_else(|| {
            ServerError::ClientError(String::from(
                "The \"query\" field is missing in request data",
            ))
        })?;

        // Ensure the "query" field is a string
        let query_string = query_value.as_str().ok_or_else(|| {
            ServerError::ClientError(String::from("The\"query\" field is not a string"))
        })?;

        // Parse the "query" field of the JSON body
        let document = q::parse_query(query_string)
            .map_err(|e| ServerError::from(QueryError::ParseError(Arc::new(e.into()))))?
            .into_static();

        // Parse the "variables" field of the JSON body, if present
        let variables = match obj.get("variables") {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(variables @ serde_json::Value::Object(_)) => {
                serde_json::from_value(variables.clone())
                    .map_err(|e| ServerError::ClientError(e.to_string()))
                    .map(Some)
            }
            _ => Err(ServerError::ClientError(
                "Invalid query variables provided".to_string(),
            )),
        }?;

        let query = Query::new(document, variables, false);

        Ok(Self {
            query,
            bearer_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use graph::{
        data::value::{Object, Word},
        prelude::*,
    };

    use graph::hyper::body::Bytes;
    use std::collections::HashMap;

    use super::{ServerError, ValidatedRequest};

    fn validate_req(req_body: Bytes) -> Result<Query, ServerError> {
        Ok(ValidatedRequest::new(req_body, None)?.query)
    }

    #[test]
    fn rejects_invalid_json() {
        let request = validate_req(Bytes::from("!@#)%"));
        request.expect_err("Should reject invalid JSON");
    }

    #[test]
    fn rejects_json_without_query_field() {
        let request = validate_req(Bytes::from("{}"));
        request.expect_err("Should reject JSON without query field");
    }

    #[test]
    fn rejects_json_with_non_string_query_field() {
        let request = validate_req(Bytes::from("{\"query\": 5}"));
        request.expect_err("Should reject JSON with a non-string query field");
    }

    #[test]
    fn rejects_broken_queries() {
        let request = validate_req(Bytes::from("{\"query\": \"foo\"}"));
        request.expect_err("Should reject broken queries");
    }

    #[test]
    fn accepts_valid_queries() {
        let request = validate_req(Bytes::from("{\"query\": \"{ user { name } }\"}"));
        let query = request.expect("Should accept valid queries");
        assert_eq!(
            query.document,
            q::parse_query("{ user { name } }").unwrap().into_static()
        );
    }

    #[test]
    fn accepts_null_variables() {
        let request = validate_req(Bytes::from(
            "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": null \
                 }",
        ));
        let query = request.expect("Should accept null variables");

        let expected_query = q::parse_query("{ user { name } }").unwrap().into_static();
        assert_eq!(query.document, expected_query);
        assert_eq!(query.variables, None);
    }

    #[test]
    fn rejects_non_map_variables() {
        let request = validate_req(Bytes::from(
            "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": 5 \
                 }",
        ));
        request.expect_err("Should reject non-map variables");
    }

    #[test]
    fn parses_variables() {
        let request = validate_req(Bytes::from(
            "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": { \
                 \"string\": \"s\", \"map\": {\"k\": \"v\"}, \"int\": 5 \
                 } \
                 }",
        ));
        let query = request.expect("Should accept valid queries");

        let expected_query = q::parse_query("{ user { name } }").unwrap().into_static();
        let expected_variables = QueryVariables::new(HashMap::from_iter(
            vec![
                (String::from("string"), r::Value::String(String::from("s"))),
                (
                    String::from("map"),
                    r::Value::Object(Object::from_iter(
                        vec![(Word::from("k"), r::Value::String(String::from("v")))].into_iter(),
                    )),
                ),
                (String::from("int"), r::Value::Int(5)),
            ]
            .into_iter(),
        ));

        assert_eq!(query.document, expected_query);
        assert_eq!(query.variables, Some(expected_variables));
    }
}
