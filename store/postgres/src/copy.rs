//! This module if repsonsible for copying the data of an existing subgraph
//! `src` to a new subgraph `dst`. The copy is done in batches so that
//! copying does not cause long-running transactions since they lead to
//! table bloat in the rest of the system.
//!
//! The `TARGET_DURATION` defines how long any individual copy operation
//! should take at most, and the code adapts the number of entities that are
//! copied in each batch to be close to that time span.
//!
//! The progress of copy operations is recorded in the tables
//! `subgraphs.copy_state` and `subgraphs.copy_table_state` so that a copy
//! operation can resume after an interruption, for example, because
//! `graph-node` was restarted while the copy was running.
use std::{
    convert::TryFrom,
    sync::Arc,
    time::{Duration, Instant},
};

use diesel::{
    dsl::sql, insert_into, select, sql_query, sql_types::Integer, update, Connection as _,
    ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, RunQueryDsl,
};
use graph::{
    components::store::EntityType,
    constraint_violation,
    prelude::{info, BlockNumber, EthereumBlockPointer, Logger, StoreError},
};

use crate::{connection_pool::ConnectionPool, primary::Site, relational::Layout};
use crate::{relational::Table, relational_queries as rq};

const INITIAL_BATCH_SIZE: i64 = 10_000;
const TARGET_DURATION: Duration = Duration::from_secs(5 * 60);
const LOG_INTERVAL: Duration = Duration::from_secs(5 * 60);

table! {
    subgraphs.copy_state(dst) {
        // deployment_schemas.id
        src -> Integer,
        // deployment_schemas.id
        dst -> Integer,
        target_block_hash -> Binary,
        target_block_number -> Integer,
        started_at -> Date,
        finished_at -> Nullable<Date>,
        cancelled_at -> Nullable<Date>,
    }
}

table! {
    subgraphs.copy_table_state {
        id -> Integer,
        entity_type -> Text,
        // references copy_state.dst
        dst -> Integer,
        next_vid -> BigInt,
        target_vid -> BigInt,
        batch_size -> BigInt,
        started_at -> Date,
        finished_at -> Nullable<Date>,
        // Measures just the time we spent working, not any wait time for
        // connections or the like
        duration_ms -> BigInt,
    }
}

#[allow(dead_code)]
struct CopyState {
    src: Arc<Layout>,
    dst: Arc<Layout>,
    target_block: EthereumBlockPointer,
    tables: Vec<TableState>,
}

impl CopyState {
    fn new(
        conn: &PgConnection,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: EthereumBlockPointer,
    ) -> Result<CopyState, StoreError> {
        use copy_state as cs;

        let crosses_shards = dst.site.shard != src.site.shard;
        if crosses_shards {
            src.import_schema(conn)?;
        }

        let state = match cs::table
            .filter(cs::dst.eq(dst.site.id))
            .select((cs::src, cs::target_block_hash, cs::target_block_number))
            .first::<(i32, Vec<u8>, BlockNumber)>(conn)
            .optional()?
        {
            Some((src_id, hash, number)) => {
                let stored_target_block = EthereumBlockPointer::from((hash, number));
                if stored_target_block != target_block {
                    return Err(constraint_violation!(
                        "CopyState {} for copying {} to {} has incompatible block pointer {} instead of {}",
                        dst.site.id,
                        src.site.deployment,
                        dst.site.deployment,
                        stored_target_block,
                        target_block));
                }
                if src_id != src.site.id {
                    return Err(constraint_violation!(
                        "CopyState {} for copying {} to {} has incompatible source {} instead of {}",
                        dst.site.id,
                        src.site.deployment,
                        dst.site.deployment,
                        src_id,
                        src.site.id
                    ));
                }
                Self::load(conn, src, dst, target_block)
            }
            None => Self::create(conn, src, dst, target_block),
        }?;

        Ok(state)
    }

    fn load(
        conn: &PgConnection,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: EthereumBlockPointer,
    ) -> Result<CopyState, StoreError> {
        let tables = TableState::load(conn, src.as_ref(), dst.as_ref())?;
        Ok(CopyState {
            src,
            dst,
            target_block,
            tables,
        })
    }

    fn create(
        conn: &PgConnection,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: EthereumBlockPointer,
    ) -> Result<CopyState, StoreError> {
        use copy_state as cs;
        use copy_table_state as cts;

        insert_into(cs::table)
            .values((
                cs::src.eq(src.site.id),
                cs::dst.eq(dst.site.id),
                cs::target_block_hash.eq(target_block.hash_slice()),
                cs::target_block_number.eq(target_block.number),
            ))
            .execute(conn)?;

        let tables: Vec<_> = dst
            .tables
            .values()
            .filter_map(|dst_table| {
                src.table_for_entity(&dst_table.object)
                    .ok()
                    .map(|src_table| {
                        TableState::init(
                            conn,
                            dst.site.clone(),
                            src_table.clone(),
                            dst_table.clone(),
                            &target_block,
                        )
                    })
            })
            .collect::<Result<_, _>>()?;

        let values = tables
            .iter()
            .map(|table| {
                (
                    cts::entity_type.eq(table.dst.object.as_str()),
                    cts::dst.eq(dst.site.id),
                    cts::next_vid.eq(table.next_vid),
                    cts::target_vid.eq(table.target_vid),
                    cts::batch_size.eq(table.batch_size),
                )
            })
            .collect::<Vec<_>>();
        insert_into(cts::table).values(values).execute(conn)?;

        Ok(CopyState {
            src,
            dst,
            target_block,
            tables,
        })
    }

    fn crosses_shards(&self) -> bool {
        self.dst.site.shard != self.src.site.shard
    }

    fn finished(&self, conn: &PgConnection) -> Result<(), StoreError> {
        use copy_state as cs;

        update(cs::table.filter(cs::dst.eq(self.dst.site.id)))
            .set(cs::finished_at.eq(sql("now()")))
            .execute(conn)?;

        // If we imported the schema for `src`, and no other in-progress
        // copy is using it, get rid of it again
        if self.crosses_shards() {
            let has_active_copies = select(diesel::dsl::exists(
                cs::table
                    .filter(cs::src.eq(self.src.site.id))
                    .filter(cs::finished_at.is_null()),
            ))
            .get_result::<bool>(conn)?;
            if !has_active_copies {
                // This is a foreign schema that nobody is using anymore,
                // get rid of it. As a safety check (on top of the one that
                // drop_foreign_schema does), see that we do not have
                // metadata for `src`
                if crate::deployment::exists(conn, &self.src.site)? {
                    return Err(constraint_violation!(
                        "we think we are copying {}[{}] across shards from {} to {}, but the \
                        source subgraph is actually in this shard",
                        self.src.site.deployment,
                        self.src.site.id,
                        self.src.site.shard,
                        self.dst.site.shard
                    ));
                }
                crate::catalog::drop_foreign_schema(conn, self.src.site.as_ref())?;
            }
        }
        Ok(())
    }
}

struct TableState {
    dst_site: Arc<Site>,
    src: Arc<Table>,
    dst: Arc<Table>,
    /// The `vid` of the next entity version that we will copy
    next_vid: i64,
    target_vid: i64,
    batch_size: i64,
    duration_ms: i64,
}

impl TableState {
    fn init(
        conn: &PgConnection,
        dst_site: Arc<Site>,
        src: Arc<Table>,
        dst: Arc<Table>,
        target_block: &EthereumBlockPointer,
    ) -> Result<Self, StoreError> {
        #[derive(QueryableByName)]
        struct MaxVid {
            #[sql_type = "diesel::sql_types::BigInt"]
            max_vid: i64,
        }

        let target_vid = sql_query(&format!(
            "select coalesce(max(vid), 0) as max_vid from {} where lower(block_range) <= $1",
            src.qualified_name.as_str()
        ))
        .bind::<Integer, _>(&target_block.number)
        .load::<MaxVid>(conn)?
        .first()
        .map(|v| v.max_vid)
        .unwrap_or(0);

        Ok(Self {
            dst_site,
            src,
            dst,
            next_vid: 0,
            target_vid,
            batch_size: INITIAL_BATCH_SIZE,
            duration_ms: 0,
        })
    }

    fn finished(&self) -> bool {
        self.next_vid >= self.target_vid
    }

    fn load(
        conn: &PgConnection,
        src_layout: &Layout,
        dst_layout: &Layout,
    ) -> Result<Vec<TableState>, StoreError> {
        use copy_table_state as cts;

        fn resolve_entity(
            layout: &Layout,
            kind: &str,
            entity_type: &EntityType,
            dst: i32,
            id: i32,
        ) -> Result<Arc<Table>, StoreError> {
            layout
                .table_for_entity(&entity_type)
                .map_err(|e| {
                    constraint_violation!(
                        "invalid {} table {} in CopyState {} (table {}): {}",
                        kind,
                        entity_type,
                        dst,
                        id,
                        e
                    )
                })
                .map(|table| table.clone())
        }

        cts::table
            .filter(cts::dst.eq(dst_layout.site.id))
            .select((
                cts::id,
                cts::entity_type,
                cts::next_vid,
                cts::target_vid,
                cts::batch_size,
                cts::duration_ms,
            ))
            .load::<(i32, String, i64, i64, i64, i64)>(conn)?
            .into_iter()
            .map(
                |(id, entity_type, current_vid, target_vid, batch_size, duration_ms)| {
                    let entity_type = EntityType::new(entity_type);
                    let src =
                        resolve_entity(src_layout, "source", &entity_type, dst_layout.site.id, id);
                    let dst = resolve_entity(
                        dst_layout,
                        "destination",
                        &entity_type,
                        dst_layout.site.id,
                        id,
                    );
                    match (src, dst) {
                        (Ok(src), Ok(dst)) => Ok(TableState {
                            dst_site: dst_layout.site.clone(),
                            src,
                            dst,
                            next_vid: current_vid,
                            target_vid,
                            batch_size,
                            duration_ms,
                        }),
                        (Err(e), _) => Err(e),
                        (_, Err(e)) => Err(e),
                    }
                },
            )
            .collect()
    }

    fn record_progress(&self, conn: &PgConnection, elapsed: Duration) -> Result<(), StoreError> {
        use copy_table_state as cts;

        // This conversion will become a problem if a copy takes longer than
        // 300B years
        let elapsed = i64::try_from(elapsed.as_millis()).unwrap_or(0);
        let duration_ms = self.duration_ms + elapsed;
        let values = (
            cts::next_vid.eq(self.next_vid),
            cts::batch_size.eq(self.batch_size),
            cts::duration_ms.eq(duration_ms),
        );
        update(
            cts::table
                .filter(cts::dst.eq(self.dst_site.id))
                .filter(cts::entity_type.eq(self.dst.object.as_str())),
        )
        .set(values)
        .execute(conn)?;
        Ok(())
    }

    fn record_finished(&self, conn: &PgConnection) -> Result<(), StoreError> {
        use copy_table_state as cts;

        update(
            cts::table
                .filter(cts::dst.eq(self.dst_site.id))
                .filter(cts::entity_type.eq(self.dst.object.as_str())),
        )
        .set(cts::finished_at.eq(sql("now()")))
        .execute(conn)?;
        Ok(())
    }

    fn copy_batch(&mut self, conn: &PgConnection) -> Result<(), StoreError> {
        let start = Instant::now();

        rq::CopyEntityBatchQuery::new(
            self.dst.as_ref(),
            &self.src,
            self.next_vid,
            self.next_vid + self.batch_size,
        )?
        .execute(conn)?;

        let duration = start.elapsed();

        // remember how far we got
        self.next_vid += self.batch_size;

        // adjust batch size by trying to extrapolate in such a way that we
        // get close to TARGET_DURATION for the time it takes to copy one
        // batch, but don't step up batch_size by more than 2x at once
        let new_batch_size = self.batch_size as f64 * TARGET_DURATION.as_millis() as f64
            / duration.as_millis() as f64;
        self.batch_size = (2 * self.batch_size).min(new_batch_size.round() as i64);

        self.record_progress(conn, duration)?;

        if self.finished() {
            self.record_finished(conn)?;
        }

        Ok(())
    }
}

// A helper for logging progress while data is being copied
struct CopyProgress<'a> {
    logger: &'a Logger,
    last_log: Instant,
    src: Arc<Site>,
    dst: Arc<Site>,
    current_vid: i64,
    target_vid: i64,
}

impl<'a> CopyProgress<'a> {
    fn new(logger: &'a Logger, state: &CopyState) -> Self {
        let target_vid: i64 = state.tables.iter().map(|table| table.target_vid).sum();
        Self {
            logger,
            last_log: Instant::now(),
            src: state.src.site.clone(),
            dst: state.dst.site.clone(),
            current_vid: 0,
            target_vid,
        }
    }

    fn start(&self) {
        info!(
            self.logger,
            "Initialize data copy from {}[{}] to {}[{}]",
            self.src.deployment,
            self.src.namespace,
            self.dst.deployment,
            self.dst.namespace
        );
    }

    fn progress_pct(current_vid: i64, target_vid: i64) -> f64 {
        if target_vid == 0 {
            100.0
        } else {
            current_vid as f64 / target_vid as f64 * 100.0
        }
    }

    fn update(&mut self, table: &TableState) {
        if self.last_log.elapsed() > LOG_INTERVAL {
            info!(
                self.logger,
                "Copied {:.2}% of `{}` entities ({}/{} entity versions), {:.2}% of overall data",
                Self::progress_pct(table.next_vid, table.target_vid),
                table.dst.object,
                table.next_vid,
                table.target_vid,
                Self::progress_pct(self.current_vid + table.next_vid, self.target_vid)
            );
            self.last_log = Instant::now();
        }
    }

    fn table_finished(&mut self, table: &TableState) {
        self.current_vid += table.next_vid;
    }

    fn finished(&self) {
        info!(
            self.logger,
            "Finished copying data into {}[{}]", self.dst.deployment, self.dst.namespace
        );
    }
}

/// A helper for copying subgraphs
pub struct Connection {
    /// The connection pool for the shard that will contain the destination
    /// of the copy
    pool: ConnectionPool,
}

impl Connection {
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }

    fn transaction<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&PgConnection) -> Result<T, StoreError>,
    {
        let conn = self.pool.get()?;
        conn.transaction(|| f(&conn)).map_err(|e| e.into())
    }

    /// Copy the data for the subgraph `src` to the subgraph `dst`. The
    /// schema for both subgraphs must have already been set up. The
    /// `target_block` must by far enough behind the chain head so that the
    /// block is guaranteed to not be subject to chain reorgs.
    ///
    /// The copy logic makes heavy use of the fact that the `vid` and
    /// `block_range` of entity versions are related since for two entity
    /// versions `v1` and `v2` such that `v1.vid <= v2.vid`, we know that
    /// `lower(v1.block_range) <= lower(v2.block_range)`. Conversely,
    /// therefore, we have that `lower(v2.block_range) >
    /// lower(v1.block_range) => v2.vid > v1.vid` and we can therefore stop
    /// the copying of each table as soon as we hit `max_vid = max { v.vid |
    /// lower(v.block_range) <= target_block.number }`.
    pub fn copy_data(
        &self,
        logger: &Logger,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        let mut state =
            self.transaction(|conn| CopyState::new(conn, src, dst.clone(), target_block))?;

        let mut progress = CopyProgress::new(logger, &state);
        progress.start();

        for table in state.tables.iter_mut().filter(|table| !table.finished()) {
            while !table.finished() {
                self.transaction(|conn| table.copy_batch(conn))?;
                progress.update(table);
            }
            progress.table_finished(table);
        }

        self.transaction(|conn| state.finished(conn))?;
        progress.finished();

        Ok(())
    }
}
