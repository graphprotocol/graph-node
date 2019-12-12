use http::header;
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use graph::components::server::query::GraphQLServerError;
use graph::prelude::*;
use graph_graphql::prelude::{execute_query, QueryExecutionOptions};

use crate::request::IndexNodeRequest;
use crate::resolver::IndexNodeResolver;
use crate::response::IndexNodeResponse;
use crate::schema::SCHEMA;

/// An asynchronous response to a GraphQL request.
pub type IndexNodeServiceResponse =
    Pin<Box<dyn std::future::Future<Output = Result<Response<Body>, GraphQLServerError>> + Send>>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct IndexNodeService<Q, S> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    node_id: NodeId,
}

impl<Q, S> Clone for IndexNodeService<Q, S> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            graphql_runner: self.graphql_runner.clone(),
            store: self.store.clone(),
            node_id: self.node_id.clone(),
        }
    }
}

impl<Q, S> IndexNodeService<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    /// Creates a new GraphQL service.
    pub fn new(logger: Logger, graphql_runner: Arc<Q>, store: Arc<S>, node_id: NodeId) -> Self {
        IndexNodeService {
            logger,
            graphql_runner,
            store,
            node_id,
        }
    }

    fn graphiql_html(&self) -> String {
        include_str!("../assets/index.html").into()
    }

    /// Serves a static file.
    fn serve_file(&self, contents: &'static str) -> IndexNodeServiceResponse {
        async move {
            Ok(Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap())
        }
            .boxed()
    }

    /// Serves a dynamically created file.
    fn serve_dynamic_file(&self, contents: String) -> IndexNodeServiceResponse {
        async {
            Ok(Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap())
        }
            .boxed()
    }

    fn index(&self) -> IndexNodeServiceResponse {
        Box::pin(async {
            Ok(Response::builder()
                .status(200)
                .body(Body::from("OK"))
                .unwrap())
        })
    }

    fn handle_graphiql(&self) -> IndexNodeServiceResponse {
        self.serve_dynamic_file(self.graphiql_html())
    }

    fn handle_graphql_query(&self, request_body: Body) -> IndexNodeServiceResponse {
        let logger = self.logger.clone();
        let store = self.store.clone();
        let result_logger = self.logger.clone();
        let graphql_runner = self.graphql_runner.clone();

        // Obtain the schema for the index node GraphQL API
        let schema = SCHEMA.clone();

        let start = Instant::now();

        hyper::body::to_bytes(request_body)
            .map_err(|_| GraphQLServerError::from("Failed to read request body"))
            .and_then(move |body| IndexNodeRequest::new(body, schema).compat())
            .and_then(move |query| {
                let logger = logger.clone();
                let graphql_runner = graphql_runner.clone();

                // Run the query using the index node resolver
                futures03::future::ok(execute_query(
                    query,
                    QueryExecutionOptions {
                        logger: logger.clone(),
                        resolver: IndexNodeResolver::new(&logger, graphql_runner, store),
                        deadline: None,
                        max_complexity: None,
                        max_depth: 100,
                        max_first: std::u32::MAX,
                    },
                ))
            })
            .then(move |result| {
                let elapsed = start.elapsed().as_millis();
                match result {
                    Ok(_) => info!(
                        result_logger,
                        "GraphQL query served";
                        "query_time_ms" => elapsed,
                        "code" => LogCode::GraphQlQuerySuccess,
                    ),
                    Err(ref e) => error!(
                        result_logger,
                        "GraphQL query failed";
                        "error" => e.to_string(),
                        "query_time_ms" => elapsed,
                        "code" => LogCode::GraphQlQueryFailure,
                    ),
                }
                IndexNodeResponse::new(result).compat()
            })
            .boxed()
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(&self, _request: Request<Body>) -> IndexNodeServiceResponse {
        Box::pin(async {
            Ok(Response::builder()
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "Content-Type")
                .header("Access-Control-Allow-Methods", "GET, OPTIONS, POST")
                .body(Body::from(""))
                .unwrap())
        })
    }

    /// Handles 302 redirects
    fn handle_temp_redirect(&self, destination: &str) -> IndexNodeServiceResponse {
        Box::pin(futures03::future::ready(
            header::HeaderValue::from_str(destination)
                .map_err(|_| GraphQLServerError::from("invalid characters in redirect URL"))
                .map(|loc_header_val| {
                    Response::builder()
                        .status(StatusCode::FOUND)
                        .header(header::LOCATION, loc_header_val)
                        .body(Body::from("Redirecting..."))
                        .unwrap()
                }),
        ))
    }

    /// Handles 404s.
    fn handle_not_found(&self) -> IndexNodeServiceResponse {
        Box::pin(futures03::future::ok(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not found"))
                .unwrap(),
        ))
    }

    fn handle_call(&mut self, req: Request<Body>) -> IndexNodeServiceResponse {
        let method = req.method().clone();

        let path = req.uri().path().to_owned();
        let path_segments = {
            let mut segments = path.split('/');

            // Remove leading '/'
            assert_eq!(segments.next(), Some(""));

            segments.collect::<Vec<_>>()
        };

        match (method, path_segments.as_slice()) {
            (Method::GET, [""]) => self.index(),
            (Method::GET, ["graphiql.css"]) => {
                self.serve_file(include_str!("../assets/graphiql.css"))
            }
            (Method::GET, ["graphiql.min.js"]) => {
                self.serve_file(include_str!("../assets/graphiql.min.js"))
            }

            (Method::GET, path @ ["graphql"]) => {
                let dest = format!("/{}/playground", path.join("/"));
                self.handle_temp_redirect(&dest)
            }
            (Method::GET, ["graphql", "playground"]) => self.handle_graphiql(),

            (Method::POST, ["graphql"]) => self.handle_graphql_query(req.into_body()),
            (Method::OPTIONS, ["graphql"]) => self.handle_graphql_options(req),

            _ => self.handle_not_found(),
        }
    }
}

impl<Q, S> Service<Request<Body>> for IndexNodeService<Q, S>
where
    Q: GraphQlRunner,
    S: SubgraphDeploymentStore + Store,
{
    type Response = Response<Body>;
    type Error = GraphQLServerError;
    type Future = IndexNodeServiceResponse;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let logger = self.logger.clone();

        // Returning Err here will prevent the client from receiving any response.
        // Instead, we generate a Response with an error code and return Ok
        Box::pin(self.handle_call(req).map(move |result| match result {
            Ok(response) => Ok(response),
            Err(err @ GraphQLServerError::Canceled(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
                    .header("Content-Type", "text/plain")
                    .body(Body::from("Internal server error (operation canceled)"))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::ClientError(_)) => {
                debug!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(400)
                    .header("Content-Type", "text/plain")
                    .body(Body::from(format!("Invalid request: {}", err)))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::QueryError(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
                    .header("Content-Type", "text/plain")
                    .body(Body::from(format!("Query error: {}", err)))
                    .unwrap())
            }
            Err(err @ GraphQLServerError::InternalError(_)) => {
                error!(logger, "IndexNodeService call failed: {}", err);

                Ok(Response::builder()
                    .status(500)
                    .header("Content-Type", "text/plain")
                    .body(Body::from(format!("Internal server error: {}", err)))
                    .unwrap())
            }
        }))
    }
}
