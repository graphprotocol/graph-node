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
//!   * 2, n: to lock the deployment with id n to make sure only one write
//!           happens to it

use diesel::sql_types::Bool;
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

/// Try to lock deployment `site` with a session lock. Return `true` if we
/// got the lock, and `false` if we did not. You don't want to use this
/// directly. Instead, use `deployment::with_lock`
pub(crate) fn lock_deployment_session(
    conn: &PgConnection,
    site: &Site,
) -> Result<bool, StoreError> {
    #[derive(QueryableByName)]
    struct Locked {
        #[sql_type = "Bool"]
        locked: bool,
    }

    sql_query(&format!(
        "select pg_try_advisory_lock(2, {}) as locked",
        site.id
    ))
    .get_result::<Locked>(conn)
    .map(|res| res.locked)
    .map_err(StoreError::from)
}

/// Release the lock acquired with `lock_deployment_session`.
pub(crate) fn unlock_deployment_session(
    conn: &PgConnection,
    site: &Site,
) -> Result<(), StoreError> {
    sql_query(&format!("select pg_advisory_unlock(2, {})", site.id))
        .execute(conn)
        .map(|_| ())
        .map_err(StoreError::from)
}
