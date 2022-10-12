use graph::components::store::DeploymentSchemaVersion;
use graph::prelude::StoreError;

use crate::deployment_store::DeploymentStore;
use crate::subgraph_store::SubgraphStore;
use crate::{dynds::DataSourcesTable, primary::Site};
use std::sync::Arc;

pub struct SubgraphMigrator<'a> {
    site: Arc<Site>,
    subgraph_store: &'a SubgraphStore,
    store: Arc<DeploymentStore>,
    current_version: DeploymentSchemaVersion,
}

impl<'a> SubgraphMigrator<'a> {
    pub fn new(
        site: Arc<Site>,
        subgraph_store: &SubgraphStore,
        store: Arc<DeploymentStore>,
    ) -> SubgraphMigrator {
        return SubgraphMigrator {
            current_version: site.schema_version,
            subgraph_store: &subgraph_store,
            store,
            site,
        };
    }

    pub fn run(mut self) -> Result<(), StoreError> {
        if self.current_version == DeploymentSchemaVersion::V1 {
            self.migrate_to_v2()?;
        }
        Ok(())
    }

    fn migrate_to_v2(&mut self) -> Result<(), StoreError> {
        let migration = format!(
            "
        alter table {table}
        add done bool not null default false;
        ",
            table = DataSourcesTable::table_name(&self.site.namespace)
        );
        self.store.execute_raw(migration)?;
        self.update_site_version(DeploymentSchemaVersion::V2)?;
        Ok(())
    }

    fn update_site_version(&mut self, version: DeploymentSchemaVersion) -> Result<(), StoreError> {
        let primary = self.subgraph_store.primary_conn()?;
        primary.update_site_version(&self.site.deployment, &version)?;
        self.current_version = version;
        Ok(())
    }
}
