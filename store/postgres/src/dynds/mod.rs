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
    shared::load(conn, site.deployment.as_str(), block)
}

pub(crate) fn insert(
    conn: &PgConnection,
    site: &Site,
    data_sources: &[StoredDynamicDataSource],
    block_ptr: &BlockPtr,
) -> Result<usize, StoreError> {
    shared::insert(conn, &site.deployment, data_sources, block_ptr)
}

pub(crate) fn copy(
    conn: &PgConnection,
    src: &Site,
    dst: &Site,
    target_block: &BlockPtr,
) -> Result<usize, StoreError> {
    shared::copy(conn, src, dst, target_block)
}

pub(crate) fn revert(
    conn: &PgConnection,
    site: &Site,
    block: BlockNumber,
) -> Result<(), StoreError> {
    shared::revert(conn, &site.deployment, block)
}
