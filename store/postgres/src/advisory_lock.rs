//! Helpers for using advisory locks. We centralize all use of these locks
//! in this module to make it easier to see what they are used for.
//!
//! The [Postgres
//! documentation](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
//! has more details on advisory locks.
//!
//! We use the following 64 bit locks:
//!   * 1,2: to synchronize on migratons
//!
//! We use the following 2x 32-bit locks
//!   * 1, n: to lock copying of the deployment with id n in the destination
//!           shard

use diesel::{sql_query, PgConnection, RunQueryDsl};
use graph::prelude::StoreError;

use crate::command_support::catalog::Site;

/// Get a lock for running migrations. Blocks until we get
/// the lock.
pub(crate) fn lock_migration(conn: &PgConnection) -> Result<(), StoreError> {
    sql_query("select pg_advisory_lock(1)").execute(conn)?;

    Ok(())
}

/// Release the migration lock.
pub(crate) fn unlock_migration(conn: &PgConnection) -> Result<(), StoreError> {
    sql_query("select pg_advisory_unlock(1)").execute(conn)?;
    Ok(())
}

pub(crate) fn lock_copying(conn: &PgConnection, dst: &Site) -> Result<(), StoreError> {
    sql_query(&format!("select pg_advisory_lock(1, {})", dst.id))
        .execute(conn)
        .map(|_| ())
        .map_err(StoreError::from)
}

pub(crate) fn unlock_copying(conn: &PgConnection, dst: &Site) -> Result<(), StoreError> {
    sql_query(&format!("select pg_advisory_unlock(1, {})", dst.id))
        .execute(conn)
        .map(|_| ())
        .map_err(StoreError::from)
}
