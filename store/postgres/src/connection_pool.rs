use diesel::pg::PgConnection;
use diesel::r2d2::{self, ConnectionManager, Pool};

use graph::prelude::*;
use graph::util::security::SafeDisplay;

use std::time::Duration;

#[derive(Debug)]
struct ErrorHandler(Logger);

impl r2d2::HandleError<r2d2::Error> for ErrorHandler {
    fn handle_error(&self, error: r2d2::Error) {
        error!(self.0, "Postgres connection error"; "error" => error.to_string());
    }
}

pub fn create_connection_pool(
    postgres_url: String,
    pool_size: u32,
    logger: &Logger,
) -> Pool<ConnectionManager<PgConnection>> {
    let logger_store = logger.new(o!("component" => "Store"));
    let logger_pool = logger.new(o!("component" => "PostgresConnectionPool"));
    let error_handler = Box::new(ErrorHandler(logger_pool.clone()));

    // Connect to Postgres
    let conn_manager = ConnectionManager::new(postgres_url.clone());
    let pool = Pool::builder()
        .error_handler(error_handler)
        // Set the time we wait for a connection to 6h. The default is 30s
        // which can be too little if database connections are highly
        // contended; if we don't get a connection within the timeout,
        // ultimately subgraphs get marked as failed. This effectively
        // turns off this timeout and makes it possible that work needing
        // a database connection blocks for a very long time
        .connection_timeout(Duration::from_secs(6 * 60 * 60))
        .max_size(pool_size)
        .build(conn_manager)
        .unwrap();
    info!(
        logger_store,
        "Connected to Postgres";
        "url" => SafeDisplay(postgres_url.as_str())
    );
    pool
}
