mod private;
pub(crate) mod shared;

pub(crate) use private::DataSourcesTable;

use crate::primary::Site;
use diesel::PgConnection;
use graph::{
    blockchain::BlockPtr,
    components::store::StoredDynamicDataSource,
    constraint_violation,
    prelude::{BlockNumber, StoreError},
};

pub fn load(
    conn: &PgConnection,
    site: &Site,
    block: BlockNumber,
    manifest_idx_and_name: Vec<(u32, String)>,
) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
    match site.schema_version.private_data_sources() {
        true => DataSourcesTable::new(site.namespace.clone()).load(conn, block),
        false => shared::load(conn, site.deployment.as_str(), block, manifest_idx_and_name),
    }
}

pub(crate) fn insert(
    conn: &PgConnection,
    site: &Site,
    data_sources: &[StoredDynamicDataSource],
    block_ptr: &BlockPtr,
    manifest_idx_and_name: &[(u32, String)],
) -> Result<usize, StoreError> {
    match site.schema_version.private_data_sources() {
        true => DataSourcesTable::new(site.namespace.clone()).insert(
            conn,
            data_sources,
            block_ptr.number,
        ),
        false => shared::insert(
            conn,
            &site.deployment,
            data_sources,
            block_ptr,
            manifest_idx_and_name,
        ),
    }
}

pub(crate) fn revert(
    conn: &PgConnection,
    site: &Site,
    block: BlockNumber,
) -> Result<(), StoreError> {
    match site.schema_version.private_data_sources() {
        true => DataSourcesTable::new(site.namespace.clone()).revert(conn, block),
        false => shared::revert(conn, &site.deployment, block),
    }
}

pub(crate) fn update_offchain_status(
    conn: &PgConnection,
    site: &Site,
    data_sources: &[StoredDynamicDataSource],
) -> Result<(), StoreError> {
    if data_sources.len() == 0 {
        return Ok(());
    }

    match site.schema_version.private_data_sources() {
        true => {
            DataSourcesTable::new(site.namespace.clone()).update_offchain_status(conn, data_sources)
        }
        false => Err(constraint_violation!(
            "shared schema does not support data source offchain_found",
        )),
    }
}
