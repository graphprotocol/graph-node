mod private;
mod shared;

pub(crate) use private::DataSourcesTable;

use crate::primary::Site;
use diesel::PgConnection;
use graph::{
    blockchain::BlockPtr,
    components::store::StoredDynamicDataSource,
    prelude::{BlockNumber, DeploymentHash, StoreError},
};

pub fn load(
    conn: &PgConnection,
    id: &str,
    block: BlockNumber,
) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
    shared::load(conn, id, block)
}

pub(crate) fn insert(
    conn: &PgConnection,
    deployment: &DeploymentHash,
    data_sources: &[StoredDynamicDataSource],
    block_ptr: &BlockPtr,
) -> Result<usize, StoreError> {
    shared::insert(conn, deployment, data_sources, block_ptr)
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
    id: &DeploymentHash,
    block: BlockNumber,
) -> Result<(), StoreError> {
    shared::revert(conn, id, block)
}

pub(crate) fn drop(conn: &PgConnection, id: &DeploymentHash) -> Result<usize, StoreError> {
    shared::drop(conn, id)
}
