pub mod flight_client;

use std::error::Error;

use arrow::{array::RecordBatch, datatypes::Schema};
use futures03::{future::BoxFuture, stream::BoxStream};
use slog::Logger;

use crate::nozzle::error;

/// Client for connecting to Nozzle core and executing SQL queries.
pub trait Client {
    type Error: Error + error::IsDeterministic + Send + Sync + 'static;

    /// Executes a SQL query and returns the corresponding schema.
    fn schema(
        &self,
        logger: &Logger,
        query: impl ToString,
    ) -> BoxFuture<'static, Result<Schema, Self::Error>>;

    /// Executes a SQL query and streams the requested data in batches.
    fn query(
        &self,
        logger: &Logger,
        query: impl ToString,
    ) -> BoxStream<'static, Result<RecordBatch, Self::Error>>;
}
