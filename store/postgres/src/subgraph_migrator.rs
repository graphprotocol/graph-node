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
        // upgrade the schema till it reaches lastest version.
        while self.current_version != DeploymentSchemaVersion::LATEST {
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
        self.primary
            .update_site_version(&self.site.deployment, &version)?;
        self.current_version = version;
        Ok(())
    }
}
