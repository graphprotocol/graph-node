mod private;
pub(crate) mod shared;

pub(crate) use private::DataSourcesTable;

use crate::primary::Site;
use diesel::PgConnection;
use graph::{
    components::store::{write, StoredDynamicDataSource},
    data_source::CausalityRegion,
    internal_error,
    prelude::{BlockNumber, StoreError},
};

pub async fn load(
    conn: &mut PgConnection,
    site: &Site,
    block: BlockNumber,
    manifest_idx_and_name: Vec<(u32, String)>,
) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
    match site.schema_version.private_data_sources() {
        true => {
            DataSourcesTable::new(site.namespace.clone())
                .load(conn, block)
                .await
        }
        false => shared::load(conn, site.deployment.as_str(), block, manifest_idx_and_name),
    }
}

pub(crate) async fn insert(
    conn: &mut PgConnection,
    site: &Site,
    data_sources: &write::DataSources,
    manifest_idx_and_name: &[(u32, String)],
) -> Result<usize, StoreError> {
    match site.schema_version.private_data_sources() {
        true => {
            DataSourcesTable::new(site.namespace.clone())
                .insert(conn, data_sources)
                .await
        }
        false => shared::insert(conn, &site.deployment, data_sources, manifest_idx_and_name),
    }
}

pub(crate) fn revert(
    conn: &mut PgConnection,
    site: &Site,
    block: BlockNumber,
) -> Result<(), StoreError> {
    match site.schema_version.private_data_sources() {
        true => DataSourcesTable::new(site.namespace.clone()).revert(conn, block),
        false => shared::revert(conn, &site.deployment, block),
    }
}

pub(crate) fn update_offchain_status(
    conn: &mut PgConnection,
    site: &Site,
    data_sources: &write::DataSources,
) -> Result<(), StoreError> {
    if data_sources.is_empty() {
        return Ok(());
    }

    match site.schema_version.private_data_sources() {
        true => {
            DataSourcesTable::new(site.namespace.clone()).update_offchain_status(conn, data_sources)
        }
        false => Err(internal_error!(
            "shared schema does not support data source offchain_found",
        )),
    }
}

/// The maximum assigned causality region. Any higher number is therefore free to be assigned.
pub(crate) fn causality_region_curr_val(
    conn: &mut PgConnection,
    site: &Site,
) -> Result<Option<CausalityRegion>, StoreError> {
    match site.schema_version.private_data_sources() {
        true => DataSourcesTable::new(site.namespace.clone()).causality_region_curr_val(conn),

        // Subgraphs on the legacy shared table do not use offchain data sources.
        false => Ok(None),
    }
}
