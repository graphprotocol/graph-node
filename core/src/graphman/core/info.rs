use std::sync::Arc;

use graph::{
    components::store::StatusStore,
    data::subgraph::status::{self, Info},
    prelude::anyhow,
};
use graph_store_postgres::{connection_pool::ConnectionPool, Store};

use crate::graphman::deployment::{Deployment, DeploymentSearch};

pub struct InfoResult {
    pub deployments: Vec<Deployment>,
    pub statuses: Vec<Info>,
}
pub fn run(
    pool: ConnectionPool,
    store: Option<Arc<Store>>,
    search: DeploymentSearch,
    current: bool,
    pending: bool,
    used: bool,
) -> Result<InfoResult, anyhow::Error> {
    let deployments = search.find(pool, current, pending, used)?;

    let ids: Vec<_> = deployments.iter().map(|d| d.locator().id).collect();

    let statuses = match store {
        Some(store) => store.status(status::Filter::DeploymentIds(ids))?,
        None => vec![],
    };

    Ok(InfoResult {
        deployments,
        statuses,
    })
}
