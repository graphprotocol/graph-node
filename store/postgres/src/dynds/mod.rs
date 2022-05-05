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
) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
    match site.schema_version.private_data_sources() {
        true => DataSourcesTable::new(site.namespace.clone()).load(conn, block),
        false => shared::load(conn, site.deployment.as_str(), block),
    }
}

pub(crate) fn insert(
    conn: &PgConnection,
    site: &Site,
    data_sources: &[StoredDynamicDataSource],
    block_ptr: &BlockPtr,
) -> Result<usize, StoreError> {
    match site.schema_version.private_data_sources() {
        true => DataSourcesTable::new(site.namespace.clone()).insert(
            conn,
            data_sources,
            block_ptr.number,
        ),
        false => shared::insert(conn, &site.deployment, data_sources, block_ptr),
    }
}

pub(crate) fn copy(
    conn: &PgConnection,
    src: &Site,
    dst: &Site,
    target_block: &BlockPtr,
) -> Result<usize, StoreError> {
    if src.schema_version != dst.schema_version {
        return Err(StoreError::ConstraintViolation(format!(
            "attempted to copy between different schema versions, \
             source version is {} but destination version is {}",
            src.schema_version, dst.schema_version
        )));
    }
    match src.schema_version.private_data_sources() {
        true => todo!(),
        false => shared::copy(conn, src, dst, target_block),
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
