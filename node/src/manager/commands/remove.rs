use std::sync::Arc;

use graph::prelude::Error;
use graph_store_postgres::SubgraphStore;

use graph_core::graphman::core;

pub fn run(store: Arc<SubgraphStore>, name: &str) -> Result<(), Error> {
    core::remove::run(store, name, true)?;

    Ok(())
}
