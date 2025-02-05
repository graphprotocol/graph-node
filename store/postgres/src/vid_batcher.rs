use std::time::Duration;

use diesel::{
    deserialize::FromSql,
    pg::Pg,
    serialize::{Output, ToSql},
    sql_types::BigInt,
};
use graph::env::ENV_VARS;

use crate::relational::Table;

/// The initial batch size for tables that do not have an array column
const INITIAL_BATCH_SIZE: i64 = 10_000;
/// The initial batch size for tables that do have an array column; those
/// arrays can be large and large arrays will slow down copying a lot. We
/// therefore tread lightly in that case
const INITIAL_BATCH_SIZE_LIST: i64 = 100;

/// Track the desired size of a batch in such a way that doing the next
/// batch gets close to TARGET_DURATION for the time it takes to copy one
/// batch, but don't step up the size by more than 2x at once
#[derive(Debug, Queryable)]
pub(crate) struct AdaptiveBatchSize {
    pub size: i64,
}

impl AdaptiveBatchSize {
    pub fn new(table: &Table) -> Self {
        let size = if table.columns.iter().any(|col| col.is_list()) {
            INITIAL_BATCH_SIZE_LIST
        } else {
            INITIAL_BATCH_SIZE
        };

        Self { size }
    }

    // adjust batch size by trying to extrapolate in such a way that we
    // get close to TARGET_DURATION for the time it takes to copy one
    // batch, but don't step up batch_size by more than 2x at once
    pub fn adapt(&mut self, duration: Duration) {
        // Avoid division by zero
        let duration = duration.as_millis().max(1);
        let new_batch_size = self.size as f64
            * ENV_VARS.store.batch_target_duration.as_millis() as f64
            / duration as f64;
        self.size = (2 * self.size).min(new_batch_size.round() as i64);
    }
}

impl ToSql<BigInt, Pg> for AdaptiveBatchSize {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        <i64 as ToSql<BigInt, Pg>>::to_sql(&self.size, out)
    }
}

impl FromSql<BigInt, Pg> for AdaptiveBatchSize {
    fn from_sql(bytes: diesel::pg::PgValue) -> diesel::deserialize::Result<Self> {
        let size = <i64 as FromSql<BigInt, Pg>>::from_sql(bytes)?;
        Ok(AdaptiveBatchSize { size })
    }
}
