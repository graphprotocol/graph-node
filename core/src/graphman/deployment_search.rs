use anyhow::anyhow;
use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::Text;
use graph::components::store::{DeploymentId, DeploymentLocator};
use graph::data::subgraph::DeploymentHash;
use graph_store_postgres::command_support::catalog;
use graph_store_postgres::connection_pool::ConnectionPool;
use itertools::Itertools;

use crate::graphman::GraphmanError;

#[derive(Clone)]
pub struct DeploymentSearch {
    pool: ConnectionPool,
}

#[derive(Clone, Debug, Default)]
pub enum DeploymentSelector {
    Subgraph {
        name: String,
    },
    Ipfs {
        hash: String,
        shard: Option<String>,
    },
    Namespace(String),

    #[default]
    All,
}

#[derive(Clone, Debug, Default)]
pub struct DeploymentFilters {
    pub included_versions: Option<DeploymentVersionFilter>,
}

#[derive(Clone, Debug, Default)]
pub enum DeploymentVersionFilter {
    Current,
    Pending,
    Used,

    #[default]
    All,
}

#[derive(Debug, PartialEq, Eq, Hash, Queryable)]
pub struct Deployment {
    pub id: i32,
    pub hash: String,
    pub namespace: String,
    pub name: String,
    pub node_id: Option<String>,
    pub shard: String,
    pub chain: String,
    pub version_status: String,
    pub is_active: bool,
}

impl DeploymentSearch {
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }

    pub fn by_deployment(
        &self,
        deployment: DeploymentSelector,
    ) -> Result<Vec<Deployment>, GraphmanError> {
        use catalog::deployment_schemas as ds;
        use catalog::subgraph as s;
        use catalog::subgraph_deployment_assignment as sda;
        use catalog::subgraph_version as sv;

        let mut conn = self.pool.get()?;

        let mut query = ds::table
            .inner_join(sv::table.on(sv::deployment.eq(ds::subgraph)))
            .inner_join(s::table.on(sv::subgraph.eq(s::id)))
            .left_outer_join(sda::table.on(sda::id.eq(ds::id)))
            .select((
                ds::id,
                sv::deployment,
                ds::name,
                s::name,
                sda::node_id.nullable(),
                ds::shard,
                ds::network,
                sql::<Text>(
                    "(case
                        when subgraphs.subgraph.pending_version = subgraphs.subgraph_version.id then 'pending'
                        when subgraphs.subgraph.current_version = subgraphs.subgraph_version.id then 'current'
                        else 'unused' end) status",
                ),
                ds::active,
            )).into_boxed();

        match deployment {
            DeploymentSelector::Subgraph { name } => {
                let pattern = format!("%{}%", name.replace("%", ""));
                query = query.filter(s::name.ilike(pattern));
            }
            DeploymentSelector::Ipfs { hash, shard } => {
                query = query.filter(ds::subgraph.eq(hash));

                if let Some(shard) = shard {
                    query = query.filter(ds::shard.eq(shard));
                }
            }
            DeploymentSelector::Namespace(namespace) => {
                query = query.filter(ds::name.eq(namespace));
            }
            DeploymentSelector::All => {
                // No changes required.
            }
        };

        query.load(&mut conn).map_err(Into::into)
    }

    pub fn by_deployment_and_filters(
        &self,
        deployment: DeploymentSelector,
        filters: DeploymentFilters,
    ) -> Result<Vec<Deployment>, GraphmanError> {
        let DeploymentFilters { included_versions } = filters;

        let deployments = self.by_deployment(deployment)?.into_iter();

        let deployments = Self::included_versions_filter(deployments, included_versions);

        Ok(deployments.collect())
    }

    pub fn one_by_deployment(
        &self,
        deployment: DeploymentSelector,
    ) -> Result<Deployment, GraphmanError> {
        let deployment = self
            .by_deployment(deployment)?
            .into_iter()
            .exactly_one()
            .map_err(|err| {
                let count = err.into_iter().count();
                GraphmanError::Datastore(anyhow!("expected exactly one deployment, found {count}"))
            })?;

        Ok(deployment)
    }

    fn included_versions_filter(
        deployments: impl Iterator<Item = Deployment>,
        included_versions: Option<DeploymentVersionFilter>,
    ) -> impl Iterator<Item = Deployment> {
        use DeploymentVersionFilter::*;

        deployments.filter(move |d| {
            let Some(included_versions) = &included_versions else {
                return true;
            };

            match included_versions {
                Current => d.version_status == "current",
                Pending => d.version_status == "pending",
                Used => d.version_status == "current" || d.version_status == "pending",
                All => true,
            }
        })
    }
}

impl Deployment {
    pub fn locator(&self) -> DeploymentLocator {
        DeploymentLocator::new(
            DeploymentId::new(self.id),
            DeploymentHash::new(self.hash.clone()).unwrap(),
        )
    }
}
