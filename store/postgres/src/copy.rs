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
    dsl::sql,
    insert_into,
    r2d2::{ConnectionManager, PooledConnection},
    select, sql_query,
    sql_types::Integer,
    update, Connection as _, ExpressionMethods, OptionalExtension, PgConnection, QueryDsl,
    RunQueryDsl,
};
use graph::{
    components::store::EntityType,
    constraint_violation,
    prelude::{info, o, warn, BlockNumber, BlockPtr, Logger, StoreError},
};

use crate::{
    advisory_lock,
    primary::{DeploymentId, Site},
};
use crate::{connection_pool::ConnectionPool, relational::Layout};
use crate::{relational::Table, relational_queries as rq};

const INITIAL_BATCH_SIZE: i64 = 10_000;
const TARGET_DURATION: Duration = Duration::from_secs(5 * 60);
const LOG_INTERVAL: Duration = Duration::from_secs(3 * 60);

table! {
    subgraphs.copy_state(dst) {
        // deployment_schemas.id
        src -> Integer,
        // deployment_schemas.id
        dst -> Integer,
        target_block_hash -> Binary,
        target_block_number -> Integer,
        started_at -> Timestamptz,
        finished_at -> Nullable<Timestamptz>,
        cancelled_at -> Nullable<Timestamptz>,
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
        started_at -> Timestamptz,
        finished_at -> Nullable<Timestamptz>,
        // Measures just the time we spent working, not any wait time for
        // connections or the like
        duration_ms -> BigInt,
    }
}

// This is the same as primary::active_copies, but mapped into each shard
table! {
    primary_public.active_copies(dst) {
        src -> Integer,
        dst -> Integer,
        cancelled_at -> Nullable<Date>,
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum Status {
    Finished,
    Cancelled,
}

#[allow(dead_code)]
struct CopyState {
    src: Arc<Layout>,
    dst: Arc<Layout>,
    target_block: BlockPtr,
    tables: Vec<TableState>,
}

impl CopyState {
    fn new(
        conn: &PgConnection,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: BlockPtr,
    ) -> Result<CopyState, StoreError> {
        use copy_state as cs;

        let crosses_shards = dst.site.shard != src.site.shard;
        if crosses_shards {
            src.import_schema(conn)?;
        }

        let state = match cs::table
            .filter(cs::dst.eq(dst.site.id))
            .select((cs::src, cs::target_block_hash, cs::target_block_number))
            .first::<(DeploymentId, Vec<u8>, BlockNumber)>(conn)
            .optional()?
        {
            Some((src_id, hash, number)) => {
                let stored_target_block = BlockPtr::from((hash, number));
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
        target_block: BlockPtr,
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
        target_block: BlockPtr,
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
        target_block: &BlockPtr,
    ) -> Result<Self, StoreError> {
        #[derive(QueryableByName)]
        struct MaxVid {
            #[sql_type = "diesel::sql_types::BigInt"]
            max_vid: i64,
        }

        let target_vid = sql_query(&format!(
            "select coalesce(max(vid), -1) as max_vid from {} where lower(block_range) <= $1",
            src.qualified_name.as_str()
        ))
        .bind::<Integer, _>(&target_block.number)
        .load::<MaxVid>(conn)?
        .first()
        .map(|v| v.max_vid)
        .unwrap_or(-1);

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
        self.next_vid > self.target_vid
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
            dst: DeploymentId,
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

    fn record_progress(
        &mut self,
        conn: &PgConnection,
        elapsed: Duration,
        first_batch: bool,
    ) -> Result<(), StoreError> {
        use copy_table_state as cts;

        // This conversion will become a problem if a copy takes longer than
        // 300B years
        self.duration_ms += i64::try_from(elapsed.as_millis()).unwrap_or(0);

        if first_batch {
            // Reset started_at so that finished_at - started_at is an
            // accurate indication of how long we worked on a table.
            update(
                cts::table
                    .filter(cts::dst.eq(self.dst_site.id))
                    .filter(cts::entity_type.eq(self.dst.object.as_str())),
            )
            .set(cts::started_at.eq(sql("now()")))
            .execute(conn)?;
        }
        let values = (
            cts::next_vid.eq(self.next_vid),
            cts::batch_size.eq(self.batch_size),
            cts::duration_ms.eq(self.duration_ms),
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

    fn copy_batch(&mut self, conn: &PgConnection) -> Result<Status, StoreError> {
        fn is_cancelled(dst: &Site, conn: &PgConnection) -> Result<bool, StoreError> {
            use active_copies as ac;

            let canceled = ac::table
                .filter(ac::dst.eq(dst.id))
                .select(ac::cancelled_at.is_not_null())
                .get_result::<bool>(conn)?;
            if canceled {
                use copy_state as cs;

                update(cs::table.filter(cs::dst.eq(dst.id)))
                    .set(cs::cancelled_at.eq(sql("now()")))
                    .execute(conn)?;
            }
            Ok(canceled)
        }

        let start = Instant::now();

        if is_cancelled(self.dst_site.as_ref(), conn)? {
            return Ok(Status::Cancelled);
        }

        // Copy all versions with next_vid <= vid <= next_vid + batch_size - 1,
        // but do not go over target_vid
        let first_batch = self.next_vid == 0;
        let last_vid = (self.next_vid + self.batch_size - 1).min(self.target_vid);
        rq::CopyEntityBatchQuery::new(self.dst.as_ref(), &self.src, self.next_vid, last_vid)?
            .execute(conn)?;

        let duration = start.elapsed();

        // remember how far we got
        self.next_vid = last_vid + 1;

        // adjust batch size by trying to extrapolate in such a way that we
        // get close to TARGET_DURATION for the time it takes to copy one
        // batch, but don't step up batch_size by more than 2x at once
        let new_batch_size = self.batch_size as f64 * TARGET_DURATION.as_millis() as f64
            / duration.as_millis() as f64;
        self.batch_size = (2 * self.batch_size).min(new_batch_size.round() as i64);

        self.record_progress(conn, duration, first_batch)?;

        if self.finished() {
            self.record_finished(conn)?;
        }

        if is_cancelled(self.dst_site.as_ref(), conn)? {
            return Ok(Status::Cancelled);
        }

        Ok(Status::Finished)
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
        // When a step is done, current_vid == target_vid + 1; don't report
        // more than 100% completion
        if target_vid == 0 || current_vid >= target_vid {
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
    logger: Logger,
    conn: PooledConnection<ConnectionManager<PgConnection>>,
    src: Arc<Layout>,
    dst: Arc<Layout>,
    target_block: BlockPtr,
}

impl Connection {
    /// Create a new copy connection. It takes a connection from the
    /// dedicated fdw pool in `pool`, and releases it only after the entire
    /// copy has finished, which might take hours or even days. This method
    /// will block until it was able to get a fdw connection. The overall
    /// effect is that new copy requests will not start until a connection
    /// is available.
    pub fn new(
        logger: &Logger,
        pool: ConnectionPool,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: BlockPtr,
    ) -> Result<Self, StoreError> {
        let logger = logger.new(o!("dst" => dst.site.namespace.to_string()));
        let mut last_log = Instant::now();
        let conn = pool.get_fdw(&logger, || {
            if last_log.elapsed() > LOG_INTERVAL {
                info!(&logger, "waiting for other copy operations to finish");
                last_log = Instant::now();
            }
            false
        })?;
        Ok(Self {
            logger,
            conn,
            src,
            dst,
            target_block,
        })
    }

    fn transaction<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&PgConnection) -> Result<T, StoreError>,
    {
        self.conn.transaction(|| f(&self.conn))
    }

    pub fn copy_data_internal(&self) -> Result<Status, StoreError> {
        let mut state = self.transaction(|conn| {
            CopyState::new(
                conn,
                self.src.clone(),
                self.dst.clone(),
                self.target_block.clone(),
            )
        })?;

        let mut progress = CopyProgress::new(&self.logger, &state);
        progress.start();

        for table in state.tables.iter_mut().filter(|table| !table.finished()) {
            while !table.finished() {
                let status = self.transaction(|conn| table.copy_batch(conn))?;
                if status == Status::Cancelled {
                    return Ok(status);
                }
                progress.update(table);
            }
            progress.table_finished(table);
        }

        self.transaction(|conn| state.finished(conn))?;
        progress.finished();

        Ok(Status::Finished)
    }

    /// Copy the data for the subgraph `src` to the subgraph `dst`. The
    /// schema for both subgraphs must have already been set up. The
    /// `target_block` must be far enough behind the chain head so that the
    /// block is guaranteed to not be subject to chain reorgs. All data up
    /// to and including `target_block` will be copied.
    ///
    /// The copy logic makes heavy use of the fact that the `vid` and
    /// `block_range` of entity versions are related since for two entity
    /// versions `v1` and `v2` such that `v1.vid <= v2.vid`, we know that
    /// `lower(v1.block_range) <= lower(v2.block_range)`. Conversely,
    /// therefore, we have that `lower(v2.block_range) >
    /// lower(v1.block_range) => v2.vid > v1.vid` and we can therefore stop
    /// the copying of each table as soon as we hit `max_vid = max { v.vid |
    /// lower(v.block_range) <= target_block.number }`.
    pub fn copy_data(&self) -> Result<Status, StoreError> {
        // We require sole access to the destination site, and that we get a
        // consistent view of what has been copied so far. In general, that
        // is always true. It can happen though that this function runs when
        // a query for copying from a previous run of graph-node is still in
        // progress. Since each transaction during copying takes a very long
        // time, it might take several minutes for Postgres to notice that
        // the old process has been terminated. The locking strategy here
        // ensures that we wait until that has happened.
        info!(
            &self.logger,
            "Obtaining copy lock (this might take a long time if another process is still copying)"
        );
        advisory_lock::lock_copying(&self.conn, self.dst.site.as_ref())?;
        let res = self.copy_data_internal();
        advisory_lock::unlock_copying(&self.conn, self.dst.site.as_ref())?;
        if matches!(res, Ok(Status::Cancelled)) {
            warn!(&self.logger, "Copying was cancelled and is incomplete");
        }
        res
    }
}
