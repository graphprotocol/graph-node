use graphql_parser;
use std::error::Error;
use std::fmt;
use std::sync::Mutex;

use graph::prelude::*;

#[derive(Debug)]
pub struct MockServeError;

impl Error for MockServeError {
    fn description(&self) -> &str {
        "Mock serve error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for MockServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Mock serve error")
    }
}

/// A mock `GraphQLServer`.
pub struct MockGraphQLServer<Q> {
    logger: Logger,
    schema: Arc<Mutex<Option<Schema>>>,
    query_runner: Arc<Q>,
}

impl<Q> MockGraphQLServer<Q> {
    /// Creates a new mock `GraphQLServer`.
    pub fn new(logger: &Logger, query_runner: Arc<Q>) -> Self {
        MockGraphQLServer {
            logger: logger.new(o!("component" => "MockGraphQLServer")),
            schema: Arc::new(Mutex::new(None)),
            query_runner,
        }
    }
}

impl<Q> GraphQLServer for MockGraphQLServer<Q>
where
    Q: GraphQlRunner,
{
    type ServeError = MockServeError;

    fn serve(
        &mut self,
        _port: u16,
        _ws_port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let schema = self.schema.clone();
        let query_runner = self.query_runner.clone();
        let logger = self.logger.clone();

        // Generate mock query requests
        let requests = (0..5)
            .map(|_| {
                let schema = schema.lock().unwrap();
                Query {
                    schema: Arc::new(schema.clone().unwrap()),
                    document: graphql_parser::parse_query("{ allUsers { name }}").unwrap(),
                    variables: None,
                }
            })
            .collect::<Vec<Query>>();

        // Create task to generate mock queries
        Ok(Box::new(stream::iter_ok(requests).for_each(move |query| {
            let logger = logger.clone();
            query_runner.run_query(query).then(move |result| {
                info!(logger, "Query result: {:?}", result);
                Ok(())
            })
        })))
    }
}
