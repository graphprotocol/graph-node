use graph::components::store::DeploymentSchemaVersion;
use graph::prelude::StoreError;

use crate::deployment_store::DeploymentStore;
use crate::primary;
use crate::{dynds::DataSourcesTable, primary::Site};
use std::sync::Arc;

pub struct SubgraphMigrator<'a> {
    site: Arc<Site>,
    primary: primary::Connection<'a>,
    store: Arc<DeploymentStore>,
    current_version: DeploymentSchemaVersion,
}

impl<'a> SubgraphMigrator<'a> {
    pub fn new(
        site: Arc<Site>,
        primary: primary::Connection<'a>,
        store: Arc<DeploymentStore>,
    ) -> SubgraphMigrator<'a> {
        return SubgraphMigrator {
            primary: primary,
            store: store,
            current_version: site.schema_version,
            site: site,
        };
    }

    pub fn run(mut self) -> Result<(), StoreError> {
        // subgraph deployment is already in latest version.
        if self.current_version == DeploymentSchemaVersion::LATEST {
            return Ok(());
        }
        match self.current_version {
            DeploymentSchemaVersion::V1 => {
                self.migrate_to_v2()?;
            }
            _ => {
                todo!(
                    "migration doesn't exist for the schema version: {schema_version}",
                    schema_version = self.current_version
                )
            }
        }
        // recursively migrate till site reaches latest schema version
        self.run()
    }

    fn migrate_to_v2(&mut self) -> Result<(), StoreError> {
        let migration = format!(
            "
        alter table {table}
        add done bool;
        update {table} set done = true where block_range = 'empty'::int4range;
        ",
            table = DataSourcesTable::table_name(&self.site.namespace)
        );
        self.store.execute_raw(migration)?;
        self.update_site_version(DeploymentSchemaVersion::V2)?;
        Ok(())
    }

    fn update_site_version(&mut self, version: DeploymentSchemaVersion) -> Result<(), StoreError> {
        self.primary
            .update_site_version(&self.site.deployment, &version)?;
        self.current_version = version;
        Ok(())
    }
}
