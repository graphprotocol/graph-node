use http::header::{
    self, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    CONTENT_TYPE, LOCATION,
};
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::task::Context;
use std::task::Poll;

use graph::{components::server::query::GraphQLServerError, data::query::QueryResults};
use graph::{components::store::StatusStore, prelude::*};
use graph_graphql::prelude::{execute_query, Query as PreparedQuery, QueryExecutionOptions};

use crate::explorer::Explorer;
use crate::request::IndexNodeRequest;
use crate::resolver::IndexNodeResolver;
use crate::schema::SCHEMA;

/// An asynchronous response to a GraphQL request.
pub type IndexNodeServiceResponse = DynTryFuture<'static, Response<Body>, GraphQLServerError>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct IndexNodeService<Q, S, R, St> {
    logger: Logger,
    graphql_runner: Arc<Q>,
    store: Arc<S>,
    explorer: Arc<Explorer<S>>,
    link_resolver: Arc<R>,
    subgraph_store: Arc<St>,
}

impl<Q, S, R, St> Clone for IndexNodeService<Q, S, R, St> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            graphql_runner: self.graphql_runner.clone(),
            store: self.store.clone(),
            explorer: self.explorer.clone(),
            link_resolver: self.link_resolver.clone(),
            subgraph_store: self.subgraph_store.clone(),
        }
    }
}

impl<Q, S, R, St> CheapClone for IndexNodeService<Q, S, R, St> {}

impl<Q, S, R, St> IndexNodeService<Q, S, R, St>
where
    Q: GraphQlRunner,
    S: StatusStore,
    R: LinkResolver,
    St: SubgraphStore,
{
    /// Creates a new GraphQL service.
    pub fn new(
        logger: Logger,
        graphql_runner: Arc<Q>,
        store: Arc<S>,
        link_resolver: Arc<R>,
        subgraph_store: Arc<St>,
    ) -> Self {
        let explorer = Arc::new(Explorer::new(store.clone()));

        IndexNodeService {
            logger,
            graphql_runner,
            store,
            explorer,
            link_resolver,
            subgraph_store,
        }
    }

    fn graphiql_html() -> &'static str {
        include_str!("../assets/index.html")
    }

    /// Serves a static file.
    fn serve_file(contents: &'static str, content_type: &'static str) -> Response<Body> {
        Response::builder()
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, content_type)
            .status(200)
            .body(Body::from(contents))
            .unwrap()
    }

    fn index() -> Response<Body> {
        Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/html")
            .body(Body::from("OK"))
            .unwrap()
    }

    fn handle_graphiql() -> Response<Body> {
        Self::serve_file(Self::graphiql_html(), "text/html")
    }

    async fn handle_graphql_query(
        &self,
        request_body: Body,
    ) -> Result<Response<Body>, GraphQLServerError> {
        let store = self.store.clone();

        // Obtain the schema for the index node GraphQL API
        let schema = SCHEMA.clone();

        let body = hyper::body::to_bytes(request_body)
            .map_err(|_| GraphQLServerError::InternalError("Failed to read request body".into()))
            .await?;

        let query = IndexNodeRequest::new(body).compat().await?;
        let query = match PreparedQuery::new(&self.logger, schema, None, query, None, 100) {
            Ok(query) => query,
            Err(e) => return Ok(QueryResults::from(QueryResult::from(e)).as_http_response()),
        };

        let load_manager = self.graphql_runner.load_manager();

        // Run the query using the index node resolver
        let query_clone = query.cheap_clone();
        let logger = self.logger.cheap_clone();
        let result = {
            let options = QueryExecutionOptions {
                resolver: IndexNodeResolver::new(
                    &logger,
                    store,
                    self.link_resolver.clone(),
                    self.subgraph_store.clone(),
                ),
                deadline: None,
                max_first: std::u32::MAX,
                max_skip: std::u32::MAX,
                load_manager,
            };
            let result = execute_query(query_clone.cheap_clone(), None, None, options).await;
            query_clone.log_execution(0);
            QueryResult::from(
                // Index status queries are not cacheable, so we may unwrap this.
                Arc::try_unwrap(result).unwrap(),
            )
        };

        Ok(QueryResults::from(result).as_http_response())
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(_request: Request<Body>) -> Response<Body> {
        Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/plain")
            .header(ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, User-Agent")
            .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS, POST")
            .body(Body::from(""))
            .unwrap()
    }

    /// Handles 302 redirects
    fn handle_temp_redirect(destination: &str) -> Result<Response<Body>, GraphQLServerError> {
        header::HeaderValue::from_str(destination)
            .map_err(|_| {
                GraphQLServerError::ClientError("invalid characters in redirect URL".into())
            })
            .map(|loc_header_val| {
                Response::builder()
                    .status(StatusCode::FOUND)
                    .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                    .header(CONTENT_TYPE, "text/plain")
                    .header(LOCATION, loc_header_val)
                    .body(Body::from("Redirecting..."))
                    .unwrap()
            })
    }

    /// Handles 404s.
    pub(crate) fn handle_not_found() -> Response<Body> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "text/plain")
            .body(Body::from("Not found\n"))
            .unwrap()
    }

    async fn handle_call(self, req: Request<Body>) -> Result<Response<Body>, GraphQLServerError> {
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

            (Method::POST, ["graphql"]) => self.handle_graphql_query(req.into_body()).await,
            (Method::OPTIONS, ["graphql"]) => Ok(Self::handle_graphql_options(req)),

            (Method::GET, ["explorer", rest @ ..]) => self.explorer.handle(&self.logger, rest),

            _ => Ok(Self::handle_not_found()),
        }
    }
}

impl<Q, S, R, St> Service<Request<Body>> for IndexNodeService<Q, S, R, St>
where
    Q: GraphQlRunner,
    S: StatusStore,
    R: LinkResolver,
    St: SubgraphStore,
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
        Box::pin(
            self.cheap_clone()
                .handle_call(req)
                .map(move |result| match result {
                    Ok(response) => Ok(response),
                    Err(err @ GraphQLServerError::ClientError(_)) => {
                        debug!(logger, "IndexNodeService call failed: {}", err);

                        Ok(Response::builder()
                            .status(400)
                            .header(CONTENT_TYPE, "text/plain")
                            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                            .body(Body::from(format!("Invalid request: {}", err)))
                            .unwrap())
                    }
                    Err(err @ GraphQLServerError::QueryError(_)) => {
                        error!(logger, "IndexNodeService call failed: {}", err);

                        Ok(Response::builder()
                            .status(400)
                            .header(CONTENT_TYPE, "text/plain")
                            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                            .body(Body::from(format!("Query error: {}", err)))
                            .unwrap())
                    }
                    Err(err @ GraphQLServerError::InternalError(_)) => {
                        error!(logger, "IndexNodeService call failed: {}", err);

                        Ok(Response::builder()
                            .status(500)
                            .header(CONTENT_TYPE, "text/plain")
                            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                            .body(Body::from(format!("Internal server error: {}", err)))
                            .unwrap())
                    }
                }),
        )
    }
}
