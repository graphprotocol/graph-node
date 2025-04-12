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
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use diesel::{
    connection::SimpleConnection as _,
    dsl::sql,
    insert_into,
    r2d2::{ConnectionManager, PooledConnection},
    select, sql_query, update, Connection as _, ExpressionMethods, OptionalExtension, PgConnection,
    QueryDsl, RunQueryDsl,
};
use graph::{
    constraint_violation,
    futures03::{future::select_all, FutureExt as _},
    prelude::{
        info, lazy_static, o, warn, BlockNumber, BlockPtr, CheapClone, Logger, StoreError, ENV_VARS,
    },
    schema::EntityType,
    slog::{debug, error},
    tokio,
};
use itertools::Itertools;

use crate::{
    advisory_lock, catalog, deployment,
    dynds::DataSourcesTable,
    primary::{DeploymentId, Primary, Site},
    relational::index::IndexList,
    vid_batcher::{VidBatcher, VidRange},
};
use crate::{connection_pool::ConnectionPool, relational::Layout};
use crate::{relational::Table, relational_queries as rq};

const LOG_INTERVAL: Duration = Duration::from_secs(3 * 60);

/// If replicas are lagging by more than this, the copying code will pause
/// for a while to allow replicas to catch up
const MAX_REPLICATION_LAG: Duration = Duration::from_secs(60);
/// If replicas need to catch up, do not resume copying until the lag is
/// less than this
const ACCEPTABLE_REPLICATION_LAG: Duration = Duration::from_secs(30);
/// When replicas are lagging too much, sleep for this long before checking
/// the lag again
const REPLICATION_SLEEP: Duration = Duration::from_secs(10);

lazy_static! {
    static ref STATEMENT_TIMEOUT: Option<String> = ENV_VARS
        .store
        .batch_timeout
        .map(|duration| format!("set local statement_timeout={}", duration.as_millis()));
}

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

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Status {
    Finished,
    Cancelled,
}

struct CopyState {
    src: Arc<Layout>,
    dst: Arc<Layout>,
    target_block: BlockPtr,
    finished: Vec<TableState>,
    unfinished: Vec<TableState>,
}

impl CopyState {
    fn new(
        conn: &mut PgConnection,
        primary: Primary,
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
                Self::load(conn, primary, src, dst, target_block)
            }
            None => Self::create(conn, primary.cheap_clone(), src, dst, target_block),
        }?;

        Ok(state)
    }

    fn load(
        conn: &mut PgConnection,
        primary: Primary,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: BlockPtr,
    ) -> Result<CopyState, StoreError> {
        let tables = TableState::load(conn, primary, src.as_ref(), dst.as_ref())?;
        let (finished, mut unfinished): (Vec<_>, Vec<_>) =
            tables.into_iter().partition(|table| table.finished());
        unfinished.sort_by_key(|table| table.dst.object.to_string());
        Ok(CopyState {
            src,
            dst,
            target_block,
            finished,
            unfinished,
        })
    }

    fn create(
        conn: &mut PgConnection,
        primary: Primary,
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

        let mut unfinished: Vec<_> = dst
            .tables
            .values()
            .filter_map(|dst_table| {
                src.table_for_entity(&dst_table.object)
                    .ok()
                    .map(|src_table| {
                        TableState::init(
                            conn,
                            primary.cheap_clone(),
                            dst.site.clone(),
                            &src,
                            src_table.clone(),
                            dst_table.clone(),
                            &target_block,
                        )
                    })
            })
            .collect::<Result<_, _>>()?;
        unfinished.sort_by_key(|table| table.dst.object.to_string());

        let values = unfinished
            .iter()
            .map(|table| {
                (
                    cts::entity_type.eq(table.dst.object.as_str()),
                    cts::dst.eq(dst.site.id),
                    cts::next_vid.eq(table.batcher.next_vid()),
                    cts::target_vid.eq(table.batcher.target_vid()),
                    cts::batch_size.eq(table.batcher.batch_size() as i64),
                )
            })
            .collect::<Vec<_>>();
        insert_into(cts::table).values(values).execute(conn)?;

        Ok(CopyState {
            src,
            dst,
            target_block,
            finished: Vec::new(),
            unfinished,
        })
    }

    fn crosses_shards(&self) -> bool {
        self.dst.site.shard != self.src.site.shard
    }

    fn finished(&self, conn: &mut PgConnection) -> Result<(), StoreError> {
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

    fn all_tables(&self) -> impl Iterator<Item = &TableState> {
        self.finished.iter().chain(self.unfinished.iter())
    }
}

pub(crate) fn source(
    conn: &mut PgConnection,
    dst: &Site,
) -> Result<Option<DeploymentId>, StoreError> {
    use copy_state as cs;

    cs::table
        .filter(cs::dst.eq(dst.id))
        .select(cs::src)
        .get_result::<DeploymentId>(conn)
        .optional()
        .map_err(StoreError::from)
}

/// A helper to copy entities from one table to another in batches that are
/// small enough to not interfere with the rest of the operations happening
/// in the database. The `src` and `dst` table must have the same structure
/// so that we can copy rows from one to the other with very little
/// transformation. See `CopyEntityBatchQuery` for the details of what
/// exactly that means
struct TableState {
    primary: Primary,
    src: Arc<Table>,
    dst: Arc<Table>,
    dst_site: Arc<Site>,
    batcher: VidBatcher,
    duration_ms: i64,
}

impl TableState {
    fn init(
        conn: &mut PgConnection,
        primary: Primary,
        dst_site: Arc<Site>,
        src_layout: &Layout,
        src: Arc<Table>,
        dst: Arc<Table>,
        target_block: &BlockPtr,
    ) -> Result<Self, StoreError> {
        let vid_range = VidRange::for_copy(conn, &src, target_block)?;
        let batcher = VidBatcher::load(conn, &src_layout.site.namespace, src.as_ref(), vid_range)?;
        Ok(Self {
            primary,
            src,
            dst,
            dst_site,
            batcher,
            duration_ms: 0,
        })
    }

    fn finished(&self) -> bool {
        self.batcher.finished()
    }

    fn load(
        conn: &mut PgConnection,
        primary: Primary,
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
                .table_for_entity(entity_type)
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
            .order_by(cts::entity_type)
            .load::<(i32, String, i64, i64, i64, i64)>(conn)?
            .into_iter()
            .map(
                |(id, entity_type, current_vid, target_vid, size, duration_ms)| {
                    let entity_type = src_layout.input_schema.entity_type(&entity_type)?;
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
                        (Ok(src), Ok(dst)) => {
                            let batcher = VidBatcher::load(
                                conn,
                                &src_layout.site.namespace,
                                &src,
                                VidRange::new(current_vid, target_vid),
                            )?
                            .with_batch_size(size as usize);

                            Ok(TableState {
                                primary: primary.cheap_clone(),
                                src,
                                dst,
                                dst_site: dst_layout.site.clone(),
                                batcher,
                                duration_ms,
                            })
                        }
                        (Err(e), _) => Err(e),
                        (_, Err(e)) => Err(e),
                    }
                },
            )
            .collect()
    }

    fn record_progress(
        &mut self,
        conn: &mut PgConnection,
        elapsed: Duration,
    ) -> Result<(), StoreError> {
        use copy_table_state as cts;

        // This conversion will become a problem if a copy takes longer than
        // 300B years
        self.duration_ms += i64::try_from(elapsed.as_millis()).unwrap_or(0);

        // Reset started_at so that finished_at - started_at is an accurate
        // indication of how long we worked on a table if we haven't worked
        // on the table yet.
        update(
            cts::table
                .filter(cts::dst.eq(self.dst_site.id))
                .filter(cts::entity_type.eq(self.dst.object.as_str()))
                .filter(cts::duration_ms.eq(0)),
        )
        .set(cts::started_at.eq(sql("now()")))
        .execute(conn)?;
        let values = (
            cts::next_vid.eq(self.batcher.next_vid()),
            cts::batch_size.eq(self.batcher.batch_size() as i64),
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

    fn record_finished(&self, conn: &mut PgConnection) -> Result<(), StoreError> {
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

    fn is_cancelled(&self, conn: &mut PgConnection) -> Result<bool, StoreError> {
        let dst = self.dst_site.as_ref();
        let canceled = self.primary.is_copy_cancelled(dst)?;
        if canceled {
            use copy_state as cs;

            update(cs::table.filter(cs::dst.eq(dst.id)))
                .set(cs::cancelled_at.eq(sql("now()")))
                .execute(conn)?;
        }
        Ok(canceled)
    }

    fn copy_batch(&mut self, conn: &mut PgConnection) -> Result<Status, StoreError> {
        let (duration, count) = self.batcher.step(|start, end| {
            let count = rq::CopyEntityBatchQuery::new(self.dst.as_ref(), &self.src, start, end)?
                .count_current()
                .get_result::<i64>(conn)
                .optional()?;
            Ok(count.unwrap_or(0) as i32)
        })?;

        let count = count.unwrap_or(0);

        deployment::update_entity_count(conn, &self.dst_site, count)?;

        self.record_progress(conn, duration)?;

        if self.finished() {
            self.record_finished(conn)?;
        }

        Ok(Status::Finished)
    }

    fn set_batch_size(&mut self, conn: &mut PgConnection, size: usize) -> Result<(), StoreError> {
        use copy_table_state as cts;

        self.batcher.set_batch_size(size);

        update(
            cts::table
                .filter(cts::dst.eq(self.dst_site.id))
                .filter(cts::entity_type.eq(self.dst.object.as_str())),
        )
        .set(cts::batch_size.eq(self.batcher.batch_size() as i64))
        .execute(conn)?;

        Ok(())
    }
}

// A helper for logging progress while data is being copied and
// communicating across all copy workers
struct CopyProgress {
    logger: Logger,
    last_log: Arc<Mutex<Instant>>,
    src: Arc<Site>,
    dst: Arc<Site>,
    /// The sum of all `target_vid` of tables that have finished
    current_vid: AtomicI64,
    target_vid: i64,
    cancelled: AtomicBool,
}

impl CopyProgress {
    fn new(logger: Logger, state: &CopyState) -> Self {
        let target_vid: i64 = state
            .all_tables()
            .map(|table| table.batcher.target_vid())
            .sum();
        let current_vid = state
            .all_tables()
            .filter(|table| table.finished())
            .map(|table| table.batcher.next_vid())
            .sum();
        let current_vid = AtomicI64::new(current_vid);
        Self {
            logger,
            last_log: Arc::new(Mutex::new(Instant::now())),
            src: state.src.site.clone(),
            dst: state.dst.site.clone(),
            current_vid,
            target_vid,
            cancelled: AtomicBool::new(false),
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

    fn start_table(&self, table: &TableState) {
        info!(
            self.logger,
            "Starting to copy `{}` entities from {} to {}",
            table.dst.object,
            table.src.qualified_name,
            table.dst.qualified_name
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

    fn update(&self, entity_type: &EntityType, batcher: &VidBatcher) {
        let mut last_log = self.last_log.lock().unwrap_or_else(|err| {
            // Better to clear the poison error and skip a log message than
            // crash for no important reason
            warn!(
                self.logger,
                "Lock for progress locking was poisoned, skipping a log message"
            );
            let mut last_log = err.into_inner();
            *last_log = Instant::now();
            self.last_log.clear_poison();
            last_log
        });
        if last_log.elapsed() > LOG_INTERVAL {
            let total_current_vid = self.current_vid.load(Ordering::SeqCst) + batcher.next_vid();
            info!(
                self.logger,
                "Copied {:.2}% of `{}` entities ({}/{} entity versions), {:.2}% of overall data",
                Self::progress_pct(batcher.next_vid(), batcher.target_vid()),
                entity_type,
                batcher.next_vid(),
                batcher.target_vid(),
                Self::progress_pct(total_current_vid, self.target_vid)
            );
            *last_log = Instant::now();
        }
    }

    fn table_finished(&self, batcher: &VidBatcher) {
        self.current_vid
            .fetch_add(batcher.next_vid(), Ordering::SeqCst);
    }

    fn finished(&self) {
        info!(
            self.logger,
            "Finished copying data into {}[{}]", self.dst.deployment, self.dst.namespace
        );
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

enum WorkerResult {
    Ok(CopyTableWorker),
    Err(StoreError),
    Wake,
}

impl From<Result<CopyTableWorker, StoreError>> for WorkerResult {
    fn from(result: Result<CopyTableWorker, StoreError>) -> Self {
        match result {
            Ok(worker) => WorkerResult::Ok(worker),
            Err(e) => WorkerResult::Err(e),
        }
    }
}

/// We pass connections back and forth between the control loop and various
/// workers. We need to make sure that we end up with the connection that
/// was used to acquire the copy lock in the right place so we can release
/// the copy lock which is only possible with the connection that acquired
/// it.
///
/// This struct helps us with that. It wraps a connection and tracks whether
/// the connection was used to acquire the copy lock
struct LockTrackingConnection {
    inner: PooledConnection<ConnectionManager<PgConnection>>,
    has_lock: bool,
}

impl LockTrackingConnection {
    fn new(inner: PooledConnection<ConnectionManager<PgConnection>>) -> Self {
        Self {
            inner,
            has_lock: false,
        }
    }

    fn transaction<T, F>(&mut self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut PgConnection) -> Result<T, StoreError>,
    {
        let conn = &mut self.inner;
        conn.transaction(|conn| f(conn))
    }

    /// Put `self` into `other` if `self` has the lock.
    fn extract(self, other: &mut Option<Self>) {
        if self.has_lock {
            *other = Some(self);
        }
    }

    fn lock(&mut self, logger: &Logger, dst: &Site) -> Result<(), StoreError> {
        if self.has_lock {
            warn!(logger, "already acquired copy lock for {}", dst);
            return Ok(());
        }
        advisory_lock::lock_copying(&mut self.inner, dst)?;
        self.has_lock = true;
        Ok(())
    }

    fn unlock(&mut self, logger: &Logger, dst: &Site) -> Result<(), StoreError> {
        if !self.has_lock {
            error!(
                logger,
                "tried to release copy lock for {} even though we are not the owner", dst
            );
            return Ok(());
        }
        advisory_lock::unlock_copying(&mut self.inner, dst)?;
        self.has_lock = false;
        Ok(())
    }
}

/// A helper to run copying of one table. We need to thread `conn` and
/// `table` from the control loop to the background worker and back again to
/// the control loop. This worker facilitates that
struct CopyTableWorker {
    conn: LockTrackingConnection,
    table: TableState,
    result: Result<Status, StoreError>,
}

impl CopyTableWorker {
    fn new(conn: LockTrackingConnection, table: TableState) -> Self {
        Self {
            conn,
            table,
            result: Ok(Status::Cancelled),
        }
    }

    async fn run(mut self, logger: Logger, progress: Arc<CopyProgress>) -> WorkerResult {
        let object = self.table.dst.object.cheap_clone();
        graph::spawn_blocking_allow_panic(move || {
            self.result = self.run_inner(logger, &progress);
            self
        })
        .await
        .map_err(|e| constraint_violation!("copy worker for {} panicked: {}", object, e))
        .into()
    }

    fn run_inner(&mut self, logger: Logger, progress: &CopyProgress) -> Result<Status, StoreError> {
        use Status::*;

        let conn = &mut self.conn.inner;
        progress.start_table(&self.table);
        while !self.table.finished() {
            // It is important that this check happens outside the write
            // transaction so that we do not hold on to locks acquired
            // by the check
            if self.table.is_cancelled(conn)? || progress.is_cancelled() {
                progress.cancel();
                return Ok(Cancelled);
            }

            // Pause copying if replication is lagging behind to avoid
            // overloading replicas
            let mut lag = catalog::replication_lag(conn)?;
            if lag > MAX_REPLICATION_LAG {
                loop {
                    info!(logger,
                             "Replicas are lagging too much; pausing copying for {}s to allow them to catch up",
                             REPLICATION_SLEEP.as_secs();
                             "lag_s" => lag.as_secs());
                    std::thread::sleep(REPLICATION_SLEEP);
                    lag = catalog::replication_lag(conn)?;
                    if lag <= ACCEPTABLE_REPLICATION_LAG {
                        break;
                    }
                }
            }

            let status = {
                loop {
                    if progress.is_cancelled() {
                        break Cancelled;
                    }

                    match conn.transaction(|conn| {
                        if let Some(timeout) = STATEMENT_TIMEOUT.as_ref() {
                            conn.batch_execute(timeout)?;
                        }
                        self.table.copy_batch(conn)
                    }) {
                        Ok(status) => {
                            break status;
                        }
                        Err(StoreError::StatementTimeout) => {
                            let timeout = ENV_VARS
                                .store
                                .batch_timeout
                                .map(|t| t.as_secs().to_string())
                                .unwrap_or_else(|| "unlimted".to_string());
                            warn!(
                                logger,
                                "Current batch timed out. Retrying with a smaller batch size.";
                                "timeout_s" => timeout,
                                "table" => self.table.dst.qualified_name.as_str(),
                                "current_vid" => self.table.batcher.next_vid(),
                                "current_batch_size" => self.table.batcher.batch_size(),
                            );
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                    // We hit a timeout. Reset the batch size to 1.
                    // That's small enough that we will make _some_
                    // progress, assuming the timeout is set to a
                    // reasonable value (several minutes)
                    //
                    // Our estimation of batch sizes is generally good
                    // and stays within the prescribed bounds, but there
                    // are cases where proper estimation of the batch
                    // size is nearly impossible since the size of the
                    // rows in the table jumps sharply at some point
                    // that is hard to predict. This mechanism ensures
                    // that if our estimation is wrong, the consequences
                    // aren't too severe.
                    conn.transaction(|conn| self.table.set_batch_size(conn, 1))?;
                }
            };

            if status == Cancelled {
                progress.cancel();
                return Ok(Cancelled);
            }
            progress.update(&self.table.dst.object, &self.table.batcher);
        }
        progress.table_finished(&self.table.batcher);
        Ok(Finished)
    }
}

/// A helper to manage the workers that are copying data. Besides the actual
/// workers it also keeps a worker that wakes us up periodically to give us
/// a chance to create more workers if there are database connections
/// available
struct Workers {
    /// The list of workers that are currently running. This will always
    /// include a future that wakes us up periodically
    futures: Vec<Pin<Box<dyn Future<Output = WorkerResult>>>>,
}

impl Workers {
    fn new() -> Self {
        Self {
            futures: vec![Self::waker()],
        }
    }

    fn add(&mut self, worker: Pin<Box<dyn Future<Output = WorkerResult>>>) {
        self.futures.push(worker);
    }

    fn has_work(&self) -> bool {
        self.futures.len() > 1
    }

    async fn select(&mut self) -> WorkerResult {
        use WorkerResult::*;

        let futures = std::mem::take(&mut self.futures);
        let (result, _idx, remaining) = select_all(futures).await;
        self.futures = remaining;
        match result {
            Ok(_) | Err(_) => { /* nothing to do */ }
            Wake => {
                self.futures.push(Self::waker());
            }
        }
        result
    }

    fn waker() -> Pin<Box<dyn Future<Output = WorkerResult>>> {
        let sleep = tokio::time::sleep(ENV_VARS.store.batch_target_duration);
        Box::pin(sleep.map(|()| WorkerResult::Wake))
    }

    /// Return the number of workers that are not the waker
    fn len(&self) -> usize {
        self.futures.len() - 1
    }
}

/// A helper for copying subgraphs
pub struct Connection {
    /// The connection pool for the shard that will contain the destination
    /// of the copy
    logger: Logger,
    /// We always have one database connection to make sure that copy jobs,
    /// once started, can eventually finished so that we don't have
    /// different copy jobs that are all half done and have to wait for
    /// other jobs to finish
    ///
    /// This is an `Option` because we need to take this connection out of
    /// `self` at some point to spawn a background task to copy an
    /// individual table. Except for that case, this will always be
    /// `Some(..)`. Most code shouldn't access `self.conn` directly, but use
    /// `self.transaction`
    conn: Option<LockTrackingConnection>,
    pool: ConnectionPool,
    primary: Primary,
    workers: usize,
    src: Arc<Layout>,
    dst: Arc<Layout>,
    target_block: BlockPtr,
    src_manifest_idx_and_name: Arc<Vec<(i32, String)>>,
    dst_manifest_idx_and_name: Arc<Vec<(i32, String)>>,
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
        primary: Primary,
        pool: ConnectionPool,
        src: Arc<Layout>,
        dst: Arc<Layout>,
        target_block: BlockPtr,
        src_manifest_idx_and_name: Vec<(i32, String)>,
        dst_manifest_idx_and_name: Vec<(i32, String)>,
    ) -> Result<Self, StoreError> {
        let logger = logger.new(o!("dst" => dst.site.namespace.to_string()));

        if src.site.schema_version != dst.site.schema_version {
            return Err(StoreError::ConstraintViolation(format!(
                "attempted to copy between different schema versions, \
                 source version is {} but destination version is {}",
                src.site.schema_version, dst.site.schema_version
            )));
        }

        let mut last_log = Instant::now();
        let conn = pool.get_fdw(&logger, || {
            if last_log.elapsed() > LOG_INTERVAL {
                info!(&logger, "waiting for other copy operations to finish");
                last_log = Instant::now();
            }
            false
        })?;
        let src_manifest_idx_and_name = Arc::new(src_manifest_idx_and_name);
        let dst_manifest_idx_and_name = Arc::new(dst_manifest_idx_and_name);
        let conn = Some(LockTrackingConnection::new(conn));
        Ok(Self {
            logger,
            conn,
            pool,
            primary,
            workers: ENV_VARS.store.batch_workers,
            src,
            dst,
            target_block,
            src_manifest_idx_and_name,
            dst_manifest_idx_and_name,
        })
    }

    fn transaction<T, F>(&mut self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut PgConnection) -> Result<T, StoreError>,
    {
        let Some(conn) = self.conn.as_mut() else {
            return Err(constraint_violation!(
                "copy connection has been handed to background task but not returned yet (transaction)"
            ));
        };
        conn.transaction(|conn| f(conn))
    }

    /// Copy private data sources if the source uses a schema version that
    /// has a private data sources table. The copying is done in its own
    /// transaction.
    fn copy_private_data_sources(&mut self, state: &CopyState) -> Result<(), StoreError> {
        let src_manifest_idx_and_name = self.src_manifest_idx_and_name.cheap_clone();
        let dst_manifest_idx_and_name = self.dst_manifest_idx_and_name.cheap_clone();
        if state.src.site.schema_version.private_data_sources() {
            self.transaction(|conn| {
                DataSourcesTable::new(state.src.site.namespace.clone()).copy_to(
                    conn,
                    &DataSourcesTable::new(state.dst.site.namespace.clone()),
                    state.target_block.number,
                    &src_manifest_idx_and_name,
                    &dst_manifest_idx_and_name,
                )
            })?;
        }
        Ok(())
    }

    /// Create a worker using the connection in `self.conn`. This may return
    /// `None` if there are no more tables that need to be copied. It is an
    /// error to call this if `self.conn` is `None`
    fn default_worker(
        &mut self,
        state: &mut CopyState,
        progress: &Arc<CopyProgress>,
    ) -> Option<Pin<Box<dyn Future<Output = WorkerResult>>>> {
        let Some(conn) = self.conn.take() else {
            return None;
        };
        let Some(table) = state.unfinished.pop() else {
            self.conn = Some(conn);
            return None;
        };

        let worker = CopyTableWorker::new(conn, table);
        Some(Box::pin(
            worker.run(self.logger.cheap_clone(), progress.cheap_clone()),
        ))
    }

    /// Opportunistically create an extra worker if we have more tables to
    /// copy and there are idle fdw connections. If there are no more tables
    /// or no idle connections, this will return `None`.
    async fn extra_worker(
        &mut self,
        state: &mut CopyState,
        progress: &Arc<CopyProgress>,
    ) -> Option<Pin<Box<dyn Future<Output = WorkerResult>>>> {
        // It's important that we get the connection before the table since
        // we remove the table from the state and could drop it otherwise
        let conn = self
            .pool
            .try_get_fdw(&self.logger, ENV_VARS.store.batch_worker_wait)
            .await?;
        let Some(table) = state.unfinished.pop() else {
            return None;
        };
        let conn = LockTrackingConnection::new(conn);

        let worker = CopyTableWorker::new(conn, table);
        Some(Box::pin(
            worker.run(self.logger.cheap_clone(), progress.cheap_clone()),
        ))
    }

    /// Check that we can make progress, i.e., that we have at least one
    /// worker that copies as long as there are unfinished tables. This is a
    /// safety check to guard against `copy_data_internal` looping forever
    /// because of some internal inconsistency
    fn assert_progress(&self, num_workers: usize, state: &CopyState) -> Result<(), StoreError> {
        if num_workers == 0 && !state.unfinished.is_empty() {
            // Something bad happened. We should have at least one
            // worker if there are still tables to copy
            if self.conn.is_none() {
                return Err(constraint_violation!(
                    "copy connection has been handed to background task but not returned yet (copy_data_internal)"
                ));
            } else {
                return Err(constraint_violation!(
                    "no workers left but still tables to copy"
                ));
            }
        }
        Ok(())
    }

    /// Wait for all workers to finish. This is called when we a worker has
    /// failed with an error that forces us to abort copying
    async fn cancel_workers(&mut self, progress: Arc<CopyProgress>, mut workers: Workers) {
        progress.cancel();
        error!(
            self.logger,
            "copying encountered an error; waiting for all workers to finish"
        );
        while workers.has_work() {
            use WorkerResult::*;
            let result = workers.select().await;
            match result {
                Ok(worker) => {
                    worker.conn.extract(&mut self.conn);
                }
                Err(e) => {
                    /* Ignore; we had an error previously */
                    error!(self.logger, "copy worker panicked: {}", e);
                }
                Wake => { /* Ignore; this is just a waker */ }
            }
        }
    }

    async fn copy_data_internal(&mut self, index_list: IndexList) -> Result<Status, StoreError> {
        let src = self.src.clone();
        let dst = self.dst.clone();
        let target_block = self.target_block.clone();
        let primary = self.primary.cheap_clone();
        let mut state =
            self.transaction(|conn| CopyState::new(conn, primary, src, dst, target_block))?;

        let progress = Arc::new(CopyProgress::new(self.logger.cheap_clone(), &state));
        progress.start();

        // Run as many copy jobs as we can in parallel, up to `self.workers`
        // many. We can always start at least one worker because of the
        // connection in `self.conn`. If the fdw pool has idle connections
        // and there are more tables to be copied, we can start more
        // workers, up to `self.workers` many
        //
        // The loop has to be very careful about terminating early so that
        // we do not ever leave the loop with `self.conn == None`
        let mut workers = Workers::new();
        while !state.unfinished.is_empty() || workers.has_work() {
            debug!(self.logger, "copy_data_internal: looking for more work";
                   "workers" => workers.len(), "unfinished" => state.unfinished.len());
            // We usually add at least one job here, except if we are out of
            // tables to copy. In that case, we go through the `while` loop
            // every time one of the tables we are currently copying
            // finishes
            if let Some(worker) = self.default_worker(&mut state, &progress) {
                workers.add(worker);
                debug!(self.logger, "copy_data_internal: found more work for default worker";
                "workers" => workers.len(), "unfinished" => state.unfinished.len());
            } else {
                debug!(self.logger, "copy_data_internal: no more work for default worker";
                "workers" => workers.len(), "unfinished" => state.unfinished.len());
            }
            loop {
                if workers.len() >= self.workers {
                    debug!(self.logger, "copy_data_internal: max workers reached, not looking for extra work";
                    "workers" => workers.len(), "unfinished" => state.unfinished.len());

                    break;
                }
                let Some(worker) = self.extra_worker(&mut state, &progress).await else {
                    debug!(self.logger, "copy_data_internal: no more work for extra worker";
                    "workers" => workers.len(), "unfinished" => state.unfinished.len());
                    break;
                };
                debug!(self.logger, "copy_data_internal: found more work for extra worker";
                "workers" => workers.len(), "unfinished" => state.unfinished.len());
                workers.add(worker);
            }

            self.assert_progress(workers.len(), &state)?;
            debug!(self.logger, "copy_data_internal: running workers";
            "workers" => workers.len(), "unfinished" => state.unfinished.len());
            let result = workers.select().await;

            match &result {
                W::Ok(copy_table_worker) => {
                    debug!(self.logger, "copy_data_internal: worker finished successfully";
                    "workers" => workers.len(),
                    "unfinished" => state.unfinished.len(),
                    "table" => copy_table_worker.table.dst.name.as_str());
                }
                W::Err(store_error) => {
                    debug!(self.logger, "copy_data_internal: worker finished with error";
                    "workers" => workers.len(),
                    "unfinished" => state.unfinished.len(),
                    "error" => store_error.to_string());
                }
                W::Wake => {
                    debug!(self.logger, "copy_data_internal: waker finished";
                    "workers" => workers.len(), "unfinished" => state.unfinished.len());
                }
            }

            // Analyze `result` and take another trip through the loop if
            // everything is ok; wait for pending workers and return if
            // there was an error or if copying was cancelled.
            use WorkerResult as W;
            match result {
                W::Err(e) => {
                    // This is a panic in the background task. We need to
                    // cancel all other tasks and return the error
                    error!(self.logger, "copy worker panicked: {}", e);
                    self.cancel_workers(progress, workers).await;
                    return Err(e);
                }
                W::Ok(worker) => {
                    // Put the connection back into self.conn so that we can use it
                    // in the next iteration.
                    worker.conn.extract(&mut self.conn);

                    match (worker.result, progress.is_cancelled()) {
                        (Ok(Status::Finished), false) => {
                            // The worker finished successfully, and nothing was
                            // cancelled; take another trip through the loop
                            state.finished.push(worker.table);
                        }
                        (Ok(Status::Finished), true) => {
                            state.finished.push(worker.table);
                            self.cancel_workers(progress, workers).await;
                            return Ok(Status::Cancelled);
                        }
                        (Ok(Status::Cancelled), _) => {
                            self.cancel_workers(progress, workers).await;
                            return Ok(Status::Cancelled);
                        }
                        (Err(e), _) => {
                            error!(self.logger, "copy worker had an error: {}", e);
                            self.cancel_workers(progress, workers).await;
                            return Err(e);
                        }
                    }
                }
                W::Wake => {
                    // nothing to do, just try to create more workers by
                    // going through the loop again
                }
            };
        }
        debug_assert!(self.conn.is_some());
        debug!(self.logger, "copy_data_internal: finished all tables";
        "workers" => workers.len(), "unfinished" => state.unfinished.len());

        // Create indexes for all the attributes that were postponed at the start of
        // the copy/graft operations.
        // First recreate the indexes that existed in the original subgraph.
        for table in state.all_tables() {
            let arr = index_list.indexes_for_table(
                &self.dst.site.namespace,
                &table.src.name.to_string(),
                &table.dst,
                true,
                false,
                true,
            )?;

            for (_, sql) in arr {
                let query = sql_query(format!("{};", sql));
                self.transaction(|conn| query.execute(conn).map_err(StoreError::from))?;
            }
        }

        // Second create the indexes for the new fields.
        // Here we need to skip those created in the first step for the old fields.
        for table in state.all_tables() {
            let orig_colums = table
                .src
                .columns
                .iter()
                .map(|c| c.name.to_string())
                .collect_vec();
            for sql in table
                .dst
                .create_postponed_indexes(orig_colums, false)
                .into_iter()
            {
                let query = sql_query(sql);
                self.transaction(|conn| query.execute(conn).map_err(StoreError::from))?;
            }
        }

        self.copy_private_data_sources(&state)?;

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
    /// The parameter index_list is a list of indexes that exist on the `src`.
    ///
    /// The copy logic makes heavy use of the fact that the `vid` and
    /// `block_range` of entity versions are related since for two entity
    /// versions `v1` and `v2` such that `v1.vid <= v2.vid`, we know that
    /// `lower(v1.block_range) <= lower(v2.block_range)`. Conversely,
    /// therefore, we have that `lower(v2.block_range) >
    /// lower(v1.block_range) => v2.vid > v1.vid` and we can therefore stop
    /// the copying of each table as soon as we hit `max_vid = max { v.vid |
    /// lower(v.block_range) <= target_block.number }`.
    pub async fn copy_data(mut self, index_list: IndexList) -> Result<Status, StoreError> {
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

        let dst_site = self.dst.site.cheap_clone();
        let Some(conn) = self.conn.as_mut() else {
            return Err(constraint_violation!(
                "copy connection went missing (copy_data)"
            ));
        };
        conn.lock(&self.logger, &dst_site)?;

        let res = self.copy_data_internal(index_list).await;

        match self.conn.as_mut() {
            None => {
                // A background worker panicked and left us without our
                // dedicated connection; we would need to get that
                // connection to unlock the advisory lock. We can't do that,
                // so we just log an error
                warn!(
                    self.logger,
                    "can't unlock copy lock since the default worker panicked; lock will linger until session ends"
                );
            }
            Some(conn) => {
                conn.unlock(&self.logger, &dst_site)?;
            }
        }

        if matches!(res, Ok(Status::Cancelled)) {
            warn!(&self.logger, "Copying was cancelled and is incomplete");
        }
        res
    }
}
