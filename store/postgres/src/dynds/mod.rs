mod private;
pub(crate) mod shared;

pub(crate) use private::DataSourcesTable;

use crate::primary::Site;
use diesel::PgConnection;
use graph::{
    blockchain::BlockPtr,
    components::store::StoredDynamicDataSource,
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
