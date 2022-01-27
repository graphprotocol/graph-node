use diesel::{dsl::sql, prelude::*};
use diesel::{sql_types::Text, PgConnection};

use graph::components::store::DeploymentId;
use graph::{
    components::store::DeploymentLocator,
    data::subgraph::status,
    prelude::{
        anyhow::{self, anyhow, bail},
        DeploymentHash, Error, SubgraphStore as _,
    },
};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::{command_support::catalog as store_catalog, Shard, SubgraphStore};

use crate::manager::deployment;
use crate::manager::display::List;

#[derive(Queryable, PartialEq, Eq, Hash, Debug)]
pub struct Deployment {
    pub name: String,
    pub status: String,
    pub deployment: String,
    pub namespace: String,
    pub id: i32,
    pub node_id: Option<String>,
    pub shard: String,
    pub chain: String,
    pub active: bool,
}

impl Deployment {
    pub fn lookup(primary: &ConnectionPool, name: String) -> Result<Vec<Self>, anyhow::Error> {
        let conn = primary.get()?;
        Self::lookup_with_conn(&conn, name)
    }

    pub fn lookup_with_conn(conn: &PgConnection, name: String) -> Result<Vec<Self>, anyhow::Error> {
        use store_catalog::deployment_schemas as ds;
        use store_catalog::subgraph as s;
        use store_catalog::subgraph_deployment_assignment as a;
        use store_catalog::subgraph_version as v;

        let query = ds::table
            .inner_join(v::table.on(v::deployment.eq(ds::subgraph)))
            .inner_join(s::table.on(v::subgraph.eq(s::id)))
            .left_outer_join(a::table.on(a::id.eq(ds::id)))
            .select((
                s::name,
                sql::<Text>(
                    "(case
                    when subgraphs.subgraph.pending_version = subgraphs.subgraph_version.id then 'pending'
                    when subgraphs.subgraph.current_version = subgraphs.subgraph_version.id then 'current'
                    else 'unused' end) status",
                ),
                v::deployment,
                ds::name,
                ds::id,
                a::node_id.nullable(),
                ds::shard,
                ds::network,
                ds::active,
            ));

        let deployments: Vec<Deployment> = if name.starts_with("sgd") {
            query.filter(ds::name.eq(&name)).load(conn)?
        } else if name.starts_with("Qm") {
            query.filter(ds::subgraph.eq(&name)).load(conn)?
        } else {
            // A subgraph name
            let pattern = format!("%{}%", name);
            query.filter(s::name.ilike(&pattern)).load(conn)?
        };
        Ok(deployments)
    }

    pub fn locator(&self) -> DeploymentLocator {
        DeploymentLocator::new(
            DeploymentId(self.id),
            DeploymentHash::new(self.deployment.clone()).unwrap(),
        )
    }

    pub fn print_table(deployments: Vec<Self>, statuses: Vec<status::Info>) {
        let mut rows = vec![
            "name",
            "status",
            "id",
            "namespace",
            "shard",
            "active",
            "chain",
            "node_id",
        ];
        if !statuses.is_empty() {
            rows.extend(vec!["synced", "health", "latest block", "chain head block"]);
        }

        let mut list = List::new(rows);

        for deployment in deployments {
            let status = statuses
                .iter()
                .find(|status| &status.id.0 == &deployment.id);

            let mut rows = vec![
                deployment.name,
                deployment.status,
                deployment.deployment,
                deployment.namespace,
                deployment.shard,
                deployment.active.to_string(),
                deployment.chain,
                deployment.node_id.unwrap_or("---".to_string()),
            ];
            if let Some(status) = status {
                let chain = &status.chains[0];
                rows.extend(vec![
                    status.synced.to_string(),
                    status.health.as_str().to_string(),
                    chain
                        .latest_block
                        .as_ref()
                        .map(|b| b.number().to_string())
                        .unwrap_or("-".to_string()),
                    chain
                        .chain_head_block
                        .as_ref()
                        .map(|b| b.number().to_string())
                        .unwrap_or("-".to_string()),
                ])
            }
            list.append(rows);
        }

        list.render();
    }
}

pub fn locate(
    store: &SubgraphStore,
    hash: String,
    shard: Option<String>,
) -> Result<DeploymentLocator, Error> {
    let hash = deployment::as_hash(hash)?;

    fn locate_unique(store: &SubgraphStore, hash: String) -> Result<DeploymentLocator, Error> {
        let locators = store.locators(&hash)?;

        match locators.len() {
            0 => {
                bail!("no matching assignment");
            }
            1 => Ok(locators[0].clone()),
            _ => {
                bail!(
                    "deployment hash `{}` is ambiguous: {} locations found",
                    hash,
                    locators.len()
                );
            }
        }
    }

    match shard {
        Some(shard) => store
            .locate_in_shard(&hash, Shard::new(shard.clone())?)?
            .ok_or_else(|| anyhow!("no deployment with hash `{}` in shard {}", hash, shard)),
        None => locate_unique(store, hash.to_string()),
    }
}

pub fn as_hash(hash: String) -> Result<DeploymentHash, Error> {
    DeploymentHash::new(hash).map_err(|s| anyhow!("illegal deployment hash `{}`", s))
}
