mod private;
pub(crate) mod shared;

pub(crate) use private::DataSourcesTable;

use crate::{primary::Site, AsyncPgConnection};
use graph::{
    components::store::{write, StoredDynamicDataSource},
    data_source::CausalityRegion,
    internal_error,
    prelude::{BlockNumber, StoreError},
};

pub async fn load(
    conn: &mut AsyncPgConnection,
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
        false => shared::load(conn, site.deployment.as_str(), block, manifest_idx_and_name).await,
    }
}

pub(crate) async fn insert(
    conn: &mut AsyncPgConnection,
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
        false => shared::insert(conn, &site.deployment, data_sources, manifest_idx_and_name).await,
    }
}

pub(crate) async fn revert(
    conn: &mut AsyncPgConnection,
    site: &Site,
    block: BlockNumber,
) -> Result<(), StoreError> {
    match site.schema_version.private_data_sources() {
        true => {
            DataSourcesTable::new(site.namespace.clone())
                .revert(conn, block)
                .await
        }
        false => shared::revert(conn, &site.deployment, block).await,
    }
}

pub(crate) async fn update_offchain_status(
    conn: &mut AsyncPgConnection,
    site: &Site,
    data_sources: &write::DataSources,
) -> Result<(), StoreError> {
    if data_sources.is_empty() {
        return Ok(());
    }

    match site.schema_version.private_data_sources() {
        true => {
            DataSourcesTable::new(site.namespace.clone())
                .update_offchain_status(conn, data_sources)
                .await
        }
        false => Err(internal_error!(
            "shared schema does not support data source offchain_found",
        )),
    }
}

/// The maximum assigned causality region. Any higher number is therefore free to be assigned.
pub(crate) async fn causality_region_curr_val(
    conn: &mut AsyncPgConnection,
    site: &Site,
) -> Result<Option<CausalityRegion>, StoreError> {
    match site.schema_version.private_data_sources() {
        true => {
            DataSourcesTable::new(site.namespace.clone())
                .causality_region_curr_val(conn)
                .await
        }

        // Subgraphs on the legacy shared table do not use offchain data sources.
        false => Ok(None),
    }
}
