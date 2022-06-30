use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};

use graph::data::subgraph::schema;
use graph::env::env_var;
use graph::prelude::{
    BlockNumber, Entity, MetricsRegistry, Schema, SubgraphStore as _, BLOCK_NUMBER_MAX,
};
use graph::slog::info;
use graph::util::bounded_queue::BoundedQueue;
use graph::{
    cheap_clone::CheapClone,
    components::store::{self, EntityType, WritableStore as WritableStoreTrait},
    data::subgraph::schema::SubgraphError,
    prelude::{
        BlockPtr, DeploymentHash, EntityKey, EntityModification, Error, Logger, StopwatchMetrics,
        StoreError, StoreEvent, UnfailOutcome, ENV_VARS,
    },
    slog::{error, warn},
    util::backoff::ExponentialBackoff,
};
use store::StoredDynamicDataSource;

use crate::deployment_store::DeploymentStore;
use crate::{primary, primary::Site, relational::Layout, SubgraphStore};

graph::prelude::lazy_static! {
    /// The size of the write queue; this many blocks can be buffered for
    /// writing before calls to transact block operations will block.
    /// Setting this to `0` disables pipelined writes, and writes will be
    /// done synchronously.
    pub static ref WRITE_QUEUE_SIZE: usize = {
        env_var("GRAPH_STORE_WRITE_QUEUE", 5)
    };
}

/// A wrapper around `SubgraphStore` that only exposes functions that are
/// safe to call from `WritableStore`, i.e., functions that either do not
/// deal with anything that depends on a specific deployment
/// location/instance, or where the result is independent of the deployment
/// instance
struct WritableSubgraphStore(SubgraphStore);

impl WritableSubgraphStore {
    fn primary_conn(&self) -> Result<primary::Connection, StoreError> {
        self.0.primary_conn()
    }

    pub(crate) fn send_store_event(&self, event: &StoreEvent) -> Result<(), StoreError> {
        self.0.send_store_event(event)
    }

    fn layout(&self, id: &DeploymentHash) -> Result<Arc<Layout>, StoreError> {
        self.0.layout(id)
    }
}

/// Write synchronously to the actual store, i.e., once a method returns,
/// the changes have been committed to the store and are visible to anybody
/// else connecting to that database
struct SyncStore {
    logger: Logger,
    store: WritableSubgraphStore,
    writable: Arc<DeploymentStore>,
    site: Arc<Site>,
    input_schema: Arc<Schema>,
}

impl SyncStore {
    const BACKOFF_BASE: Duration = Duration::from_millis(100);
    const BACKOFF_CEIL: Duration = Duration::from_secs(10);

    fn new(
        subgraph_store: SubgraphStore,
        logger: Logger,
        site: Arc<Site>,
    ) -> Result<Self, StoreError> {
        let store = WritableSubgraphStore(subgraph_store.clone());
        let writable = subgraph_store.for_site(site.as_ref())?.clone();
        let input_schema = subgraph_store.input_schema(&site.deployment)?;
        Ok(Self {
            logger,
            store,
            writable,
            site,
            input_schema,
        })
    }

    fn log_backoff_warning(&self, op: &str, backoff: &ExponentialBackoff) {
        warn!(self.logger,
            "database unavailable, will retry";
            "operation" => op,
            "attempt" => backoff.attempt,
            "delay_ms" => backoff.delay().as_millis());
    }

    fn retry<T, F>(&self, op: &str, f: F) -> Result<T, StoreError>
    where
        F: Fn() -> Result<T, StoreError>,
    {
        let mut backoff = ExponentialBackoff::new(Self::BACKOFF_BASE, Self::BACKOFF_CEIL);
        loop {
            match f() {
                Ok(v) => return Ok(v),
                Err(StoreError::DatabaseUnavailable) => {
                    self.log_backoff_warning(op, &backoff);
                }
                Err(e) => return Err(e),
            }
            backoff.sleep();
        }
    }

    async fn retry_async<T, F, Fut>(&self, op: &str, f: F) -> Result<T, StoreError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, StoreError>>,
    {
        let mut backoff = ExponentialBackoff::new(Self::BACKOFF_BASE, Self::BACKOFF_CEIL);
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(StoreError::DatabaseUnavailable) => {
                    self.log_backoff_warning(op, &backoff);
                }
                Err(e) => return Err(e),
            }
            backoff.sleep_async().await;
        }
    }

    /// Try to send a `StoreEvent`; if sending fails, log the error but
    /// return `Ok(())`
    fn try_send_store_event(&self, event: StoreEvent) -> Result<(), StoreError> {
        if !ENV_VARS.store.disable_subscription_notifications {
            let _ = self.store.send_store_event(&event).map_err(
                |e| error!(self.logger, "Could not send store event"; "error" => e.to_string()),
            );
            Ok(())
        } else {
            Ok(())
        }
    }
}

// Methods that mirror `WritableStoreTrait`
impl SyncStore {
    async fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        self.retry_async("block_ptr", || {
            let site = self.site.clone();
            async move { self.writable.block_ptr(site).await }
        })
        .await
    }

    async fn block_cursor(&self) -> Result<Option<String>, StoreError> {
        self.writable.block_cursor(self.site.cheap_clone()).await
    }

    async fn delete_block_cursor(&self) -> Result<(), StoreError> {
        self.writable
            .delete_block_cursor(self.site.cheap_clone())
            .await
    }

    fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError> {
        self.retry("start_subgraph_deployment", || {
            let graft_base = match self.writable.graft_pending(&self.site.deployment)? {
                Some((base_id, base_ptr)) => {
                    let src = self.store.layout(&base_id)?;
                    Some((src, base_ptr))
                }
                None => None,
            };
            self.writable
                .start_subgraph(logger, self.site.clone(), graft_base)?;
            self.store.primary_conn()?.copy_finished(self.site.as_ref())
        })
    }

    fn revert_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<&str>,
    ) -> Result<(), StoreError> {
        self.retry("revert_block_operations", || {
            let event = self.writable.revert_block_operations(
                self.site.clone(),
                block_ptr_to.clone(),
                firehose_cursor,
            )?;

            self.try_send_store_event(event)
        })
    }

    fn unfail_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        self.retry("unfail_deterministic_error", || {
            self.writable
                .unfail_deterministic_error(self.site.clone(), current_ptr, parent_ptr)
        })
    }

    fn unfail_non_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        self.retry("unfail_non_deterministic_error", || {
            self.writable
                .unfail_non_deterministic_error(self.site.clone(), current_ptr)
        })
    }

    async fn fail_subgraph(&self, error: SubgraphError) -> Result<(), StoreError> {
        self.retry_async("fail_subgraph", || {
            let error = error.clone();
            async {
                self.writable
                    .clone()
                    .fail_subgraph(self.site.deployment.clone(), error)
                    .await
            }
        })
        .await
    }

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError> {
        self.retry_async("supports_proof_of_indexing", || async {
            self.writable
                .supports_proof_of_indexing(self.site.clone())
                .await
        })
        .await
    }

    fn get(&self, key: &EntityKey, block: BlockNumber) -> Result<Option<Entity>, StoreError> {
        self.retry("get", || {
            self.writable.get(self.site.cheap_clone(), key, block)
        })
    }

    fn transact_block_operations(
        &self,
        block_ptr_to: &BlockPtr,
        firehose_cursor: Option<&str>,
        mods: &[EntityModification],
        stopwatch: &StopwatchMetrics,
        data_sources: &[StoredDynamicDataSource],
        deterministic_errors: &[SubgraphError],
    ) -> Result<(), StoreError> {
        fn same_subgraph(mods: &[EntityModification], id: &DeploymentHash) -> bool {
            mods.iter().all(|md| &md.entity_key().subgraph_id == id)
        }

        assert!(
            same_subgraph(mods, &self.site.deployment),
            "can only transact operations within one shard"
        );
        self.retry("transact_block_operations", move || {
            let event = self.writable.transact_block_operations(
                self.site.clone(),
                block_ptr_to,
                firehose_cursor,
                mods,
                stopwatch,
                data_sources,
                deterministic_errors,
            )?;

            let _section = stopwatch.start_section("send_store_event");
            self.try_send_store_event(event)?;
            Ok(())
        })
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
        block: BlockNumber,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        self.retry("get_many", || {
            self.writable
                .get_many(self.site.cheap_clone(), &ids_for_type, block)
        })
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        self.retry_async("is_deployment_synced", || async {
            self.writable
                .exists_and_synced(self.site.deployment.cheap_clone())
                .await
        })
        .await
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        self.retry("unassign_subgraph", || {
            let pconn = self.store.primary_conn()?;
            pconn.transaction(|| -> Result<_, StoreError> {
                let changes = pconn.unassign_subgraph(self.site.as_ref())?;
                self.store.send_store_event(&StoreEvent::new(changes))
            })
        })
    }

    async fn load_dynamic_data_sources(
        &self,
        block: BlockNumber,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.retry_async("load_dynamic_data_sources", || async {
            self.writable
                .load_dynamic_data_sources(self.site.cheap_clone(), block)
                .await
        })
        .await
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        self.retry("deployment_synced", || {
            let event = {
                // Make sure we drop `pconn` before we call into the deployment
                // store so that we do not hold two database connections which
                // might come from the same pool and could therefore deadlock
                let pconn = self.store.primary_conn()?;
                pconn.transaction(|| -> Result<_, Error> {
                    let changes = pconn.promote_deployment(&self.site.deployment)?;
                    Ok(StoreEvent::new(changes))
                })?
            };

            self.writable.deployment_synced(&self.site.deployment)?;

            self.store.send_store_event(&event)
        })
    }

    fn shard(&self) -> &str {
        self.site.shard.as_str()
    }

    async fn health(&self) -> Result<schema::SubgraphHealth, StoreError> {
        self.retry_async("health", || async {
            self.writable.health(&self.site).await.map(Into::into)
        })
        .await
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.input_schema.clone()
    }
}

/// Track block numbers we see in a few methods that traverse the queue to
/// help determine which changes in the queue will actually be visible in
/// the database once the whole queue has been processed and the block
/// number at which queries should run so that they only consider data that
/// is not affected by any requests currently queued.
///
/// The tracker relies on `update` being called in the order newest request
/// in the queue to oldest request so that reverts are seen before the
/// writes that they revert.
struct BlockTracker {
    /// The smallest block number that has been reverted to
    revert: BlockNumber,
    /// The largest block number that is not affected by entries in the
    /// queue
    block: BlockNumber,
}

impl BlockTracker {
    fn new() -> Self {
        Self {
            revert: BLOCK_NUMBER_MAX,
            block: BLOCK_NUMBER_MAX,
        }
    }

    fn update(&mut self, req: &Request) {
        match req {
            Request::Write { block_ptr, .. } => {
                self.block = self.block.min(block_ptr.number - 1);
            }
            Request::RevertTo { block_ptr, .. } => {
                // `block_ptr` is the block pointer we are reverting _to_,
                // and is not affected by the revert
                self.revert = self.revert.min(block_ptr.number);
                self.block = self.block.min(block_ptr.number);
            }
        }
    }

    /// The block at which a query should run so it does not see the result
    /// of any writes that might have happened concurrently but have already
    /// been accounted for by inspecting the in-memory queue
    fn query_block(&self) -> BlockNumber {
        self.block
    }

    /// Return `true` if a write at this block will be visible, i.e., not
    /// reverted by a previous queue entry
    fn visible(&self, block_ptr: &BlockPtr) -> bool {
        block_ptr.number <= self.revert
    }
}

/// A write request received from the `WritableStore` frontend that gets
/// queued
enum Request {
    Write {
        store: Arc<SyncStore>,
        stopwatch: StopwatchMetrics,
        /// The block at which we are writing the changes
        block_ptr: BlockPtr,
        firehose_cursor: Option<String>,
        mods: Vec<EntityModification>,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    },
    RevertTo {
        store: Arc<SyncStore>,
        /// The subgraph head will be at this block pointer after the revert
        block_ptr: BlockPtr,
        firehose_cursor: Option<String>,
    },
}

impl Request {
    fn execute(&self) -> Result<(), StoreError> {
        match self {
            Request::Write {
                store,
                stopwatch,
                block_ptr: block_ptr_to,
                firehose_cursor,
                mods,
                data_sources,
                deterministic_errors,
            } => store.transact_block_operations(
                block_ptr_to,
                firehose_cursor.as_deref(),
                mods,
                stopwatch,
                data_sources,
                deterministic_errors,
            ),
            Request::RevertTo {
                store,
                block_ptr,
                firehose_cursor,
            } => store.revert_block_operations(block_ptr.clone(), firehose_cursor.as_deref()),
        }
    }
}

/// A queue that asynchronously writes requests queued with `push` to the
/// underlying store and allows retrieving information that is a combination
/// of queued changes and changes already committed to the store.
struct Queue {
    store: Arc<SyncStore>,
    /// A queue of pending requests. New requests are appended at the back,
    /// and popped off the front for processing. When the queue only
    /// contains `Write` requests block numbers in the requests are
    /// increasing going front-to-back. When `Revert` requests are queued,
    /// that is not true anymore
    queue: BoundedQueue<Arc<Request>>,

    /// The write task puts errors from `transact_block_operations` here so
    /// we can report them on the next call to transact block operations.
    write_err: Mutex<Option<StoreError>>,

    /// True if the background worker ever encountered an error. Once that
    /// happens, no more changes will be written, and any attempt to write
    /// or revert will result in an error
    poisoned: AtomicBool,

    stopwatch: StopwatchMetrics,
}

/// Support for controlling the background writer (pause/resume) only for
/// use in tests. In release builds, the checks that pause the writer are
/// compiled out. Before `allow_steps` is called, the background writer is
/// allowed to process as many requests as it can
#[cfg(debug_assertions)]
pub(crate) mod test_support {
    use std::sync::atomic::{AtomicBool, Ordering};

    use graph::{prelude::lazy_static, util::bounded_queue::BoundedQueue};

    lazy_static! {
        static ref DO_STEP: AtomicBool = AtomicBool::new(false);
        static ref ALLOWED_STEPS: BoundedQueue<()> = BoundedQueue::with_capacity(1_000);
    }

    pub(super) async fn take_step() {
        if DO_STEP.load(Ordering::SeqCst) {
            ALLOWED_STEPS.pop().await
        }
    }

    /// Allow the writer to process `steps` requests. After calling this,
    /// the writer will only process the number of requests it is allowed to
    pub async fn allow_steps(steps: usize) {
        for _ in 0..steps {
            ALLOWED_STEPS.push(()).await
        }
        DO_STEP.store(true, Ordering::SeqCst);
    }
}

impl Queue {
    /// Create a new queue and spawn a task that processes write requests
    fn start(
        logger: Logger,
        store: Arc<SyncStore>,
        capacity: usize,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Arc<Self> {
        async fn start_writer(queue: Arc<Queue>, logger: Logger) {
            loop {
                #[cfg(debug_assertions)]
                test_support::take_step().await;

                // We peek at the front of the queue, rather than pop it
                // right away, so that query methods like `get` have access
                // to the data while it is being written. If we popped here,
                // these methods would not be able to see that data until
                // the write transaction commits, causing them to return
                // incorrect results.
                let req = {
                    let _section = queue.stopwatch.start_section("queue_wait");
                    queue.queue.peek().await
                };
                let res = {
                    let _section = queue.stopwatch.start_section("queue_execute");
                    graph::spawn_blocking_allow_panic(move || req.execute()).await
                };

                let _section = queue.stopwatch.start_section("queue_pop");
                match res {
                    Ok(Ok(())) => {
                        // The request has been handled. It's now safe to remove it
                        // from the queue
                        queue.queue.pop().await;
                    }
                    Ok(Err(e)) => {
                        error!(logger, "Subgraph writer failed"; "error" => e.to_string());
                        queue.record_err(e);
                        return;
                    }
                    Err(e) => {
                        error!(logger, "Subgraph writer paniced"; "error" => e.to_string());
                        queue.record_err(StoreError::WriterPanic(e));
                        return;
                    }
                }
            }
        }

        let queue = BoundedQueue::with_capacity(capacity);
        let write_err = Mutex::new(None);

        // Use a separate instance of the `StopwatchMetrics` for background
        // work since that has its own call hierarchy, and using the
        // foreground metrics will lead to incorrect nesting of sections
        let stopwatch = StopwatchMetrics::new(
            logger.clone(),
            store.site.deployment.clone(),
            "writer",
            registry,
        );

        let queue = Self {
            store,
            queue,
            write_err,
            poisoned: AtomicBool::new(false),
            stopwatch,
        };
        let queue = Arc::new(queue);

        graph::spawn(start_writer(queue.cheap_clone(), logger));

        queue
    }

    /// Add a write request to the queue
    async fn push(&self, req: Request) -> Result<(), StoreError> {
        self.check_err()?;
        self.queue.push(Arc::new(req)).await;
        Ok(())
    }

    /// Wait for the background writer to finish processing queued entries
    async fn flush(&self) -> Result<(), StoreError> {
        self.queue.wait_empty().await;
        self.check_err()
    }

    fn check_err(&self) -> Result<(), StoreError> {
        if let Some(err) = self.write_err.lock().unwrap().take() {
            return Err(err);
        }
        match self.poisoned.load(Ordering::SeqCst) {
            true => Err(StoreError::Poisoned),
            false => Ok(()),
        }
    }

    /// Record the error `e`, mark the queue as poisoned, and remove all
    /// pending requests. The queue can not be used anymore
    fn record_err(&self, e: StoreError) {
        *self.write_err.lock().unwrap() = Some(e);
        self.poisoned.store(true, Ordering::SeqCst);
        self.queue.clear();
    }

    /// Get the entity for `key` if it exists by looking at both the queue
    /// and the store
    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        enum Op {
            Write(Entity),
            Remove,
        }

        // Going from newest to oldest entry in the queue as `find_map` does
        // ensures that we see reverts before we see the corresponding write
        // request. We ignore any write request that writes blocks that have
        // a number strictly higher than the revert with the smallest block
        // number, as all such writes will be undone once the revert is
        // processed.
        let mut tracker = BlockTracker::new();

        let op = self.queue.find_map(|req| {
            tracker.update(req.as_ref());
            match req.as_ref() {
                Request::Write {
                    block_ptr, mods, ..
                } => {
                    if tracker.visible(block_ptr) {
                        mods.iter()
                            .find(|emod| emod.entity_key() == key)
                            .map(|emod| match emod {
                                EntityModification::Insert { data, .. }
                                | EntityModification::Overwrite { data, .. } => {
                                    Op::Write(data.clone())
                                }
                                EntityModification::Remove { .. } => Op::Remove,
                            })
                    } else {
                        None
                    }
                }
                Request::RevertTo { .. } => None,
            }
        });

        match op {
            Some(Op::Write(entity)) => Ok(Some(entity)),
            Some(Op::Remove) => Ok(None),
            None => self.store.get(key, tracker.query_block()),
        }
    }

    /// Get many entities at once by looking at both the queue and the store
    fn get_many(
        &self,
        mut ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        // See the implementation of `get` for how we handle reverts
        let mut tracker = BlockTracker::new();

        // Get entities from entries in the queue
        let mut map = self.queue.fold(
            BTreeMap::new(),
            |mut map: BTreeMap<EntityType, Vec<Entity>>, req| {
                tracker.update(req.as_ref());
                match req.as_ref() {
                    Request::Write {
                        block_ptr, mods, ..
                    } => {
                        if tracker.visible(block_ptr) {
                            for emod in mods {
                                let key = emod.entity_key();
                                if let Some(ids) = ids_for_type.get_mut(&key.entity_type) {
                                    if let Some(idx) =
                                        ids.iter().position(|id| *id == &key.entity_id)
                                    {
                                        // We are looking for the entity
                                        // underlying this modification. Add
                                        // it to the result map, but also
                                        // remove it from `ids_for_type` so
                                        // that we don't look for it any
                                        // more
                                        if let Some(entity) = emod.entity() {
                                            map.entry(key.entity_type.clone())
                                                .or_default()
                                                .push(entity.clone());
                                        }
                                        ids.swap_remove(idx);
                                        if ids.is_empty() {
                                            ids_for_type.remove(&key.entity_type);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Request::RevertTo { .. } => { /* nothing to do */ }
                }
                map
            },
        );

        // Whatever remains in `ids_for_type` needs to be gotten from the
        // store. Take extra care to not unnecessarily copy maps
        if !ids_for_type.is_empty() {
            let store_map = self.store.get_many(ids_for_type, tracker.query_block())?;
            if !store_map.is_empty() {
                if map.is_empty() {
                    map = store_map
                } else {
                    for (entity_type, mut entities) in store_map {
                        map.entry(entity_type).or_default().append(&mut entities);
                    }
                }
            }
        }
        Ok(map)
    }

    /// Load dynamic data sources by looking at both the queue and the store
    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        // See the implementation of `get` for how we handle reverts
        let mut tracker = BlockTracker::new();

        // We need to produce a list of dynamic data sources that are
        // ordered by their creation block. We first look through all the
        // dds that are still in the queue, and then load dds from the store
        // as long as they were written at a block before whatever is still
        // in the queue. The overall list of dds is the list of dds from the
        // store plus the ones still in memory sorted by their block number.
        let mut queue_dds = self.queue.fold(Vec::new(), |mut dds, req| {
            tracker.update(req.as_ref());
            match req.as_ref() {
                Request::Write {
                    block_ptr,
                    data_sources,
                    ..
                } => {
                    if tracker.visible(block_ptr) {
                        dds.extend(data_sources.clone());
                    }
                }
                Request::RevertTo { .. } => { /* nothing to do */ }
            }
            dds
        });
        // Using a stable sort is important here so that dds created at the
        // same block stay in the order in which they were added (and
        // therefore will be loaded from the store in that order once the
        // queue has been written)
        queue_dds.sort_by_key(|dds| dds.creation_block);

        let mut dds = self
            .store
            .load_dynamic_data_sources(tracker.query_block())
            .await?;
        dds.append(&mut queue_dds);

        Ok(dds)
    }
}

/// A shim to allow bypassing any pipelined store handling if need be
enum Writer {
    Sync(Arc<SyncStore>),
    Async(Arc<Queue>),
}

impl Writer {
    fn new(
        logger: Logger,
        store: Arc<SyncStore>,
        capacity: usize,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        info!(logger, "Starting subgraph writer"; "queue_size" => capacity);
        if capacity == 0 {
            Self::Sync(store)
        } else {
            Self::Async(Queue::start(logger, store, capacity, registry))
        }
    }

    async fn write(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<String>,
        mods: Vec<EntityModification>,
        stopwatch: &StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        match self {
            Writer::Sync(store) => store.transact_block_operations(
                &block_ptr_to,
                firehose_cursor.as_deref(),
                &mods,
                &stopwatch,
                &data_sources,
                &deterministic_errors,
            ),
            Writer::Async(queue) => {
                let req = Request::Write {
                    store: queue.store.cheap_clone(),
                    stopwatch: queue.stopwatch.cheap_clone(),
                    block_ptr: block_ptr_to,
                    firehose_cursor,
                    mods,
                    data_sources,
                    deterministic_errors,
                };
                queue.push(req).await
            }
        }
    }

    async fn revert(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<&str>,
    ) -> Result<(), StoreError> {
        match self {
            Writer::Sync(store) => store.revert_block_operations(block_ptr_to, firehose_cursor),
            Writer::Async(queue) => {
                let firehose_cursor = firehose_cursor.map(|c| c.to_string());
                let req = Request::RevertTo {
                    store: queue.store.cheap_clone(),
                    block_ptr: block_ptr_to,
                    firehose_cursor,
                };
                queue.push(req).await
            }
        }
    }

    async fn flush(&self) -> Result<(), StoreError> {
        match self {
            Writer::Sync { .. } => Ok(()),
            Writer::Async(queue) => queue.flush().await,
        }
    }

    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        match self {
            Writer::Sync(store) => store.get(key, BLOCK_NUMBER_MAX),
            Writer::Async(queue) => queue.get(key),
        }
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        match self {
            Writer::Sync(store) => store.get_many(ids_for_type, BLOCK_NUMBER_MAX),
            Writer::Async(queue) => queue.get_many(ids_for_type),
        }
    }

    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        match self {
            Writer::Sync(store) => store.load_dynamic_data_sources(BLOCK_NUMBER_MAX).await,
            Writer::Async(queue) => queue.load_dynamic_data_sources().await,
        }
    }
}

pub struct WritableStore {
    store: Arc<SyncStore>,
    block_ptr: Mutex<Option<BlockPtr>>,
    block_cursor: Mutex<Option<String>>,
    writer: Writer,
}

impl WritableStore {
    pub(crate) async fn new(
        subgraph_store: SubgraphStore,
        logger: Logger,
        site: Arc<Site>,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Result<Self, StoreError> {
        let store = Arc::new(SyncStore::new(subgraph_store, logger.clone(), site)?);
        let block_ptr = Mutex::new(store.block_ptr().await?);
        let block_cursor = Mutex::new(store.block_cursor().await?);
        let writer = Writer::new(
            logger,
            store.clone(),
            ENV_VARS.store.write_queue_size,
            registry,
        );

        Ok(Self {
            store,
            block_ptr,
            block_cursor,
            writer,
        })
    }
}

#[async_trait::async_trait]
impl WritableStoreTrait for WritableStore {
    async fn block_ptr(&self) -> Option<BlockPtr> {
        self.block_ptr.lock().unwrap().clone()
    }

    async fn block_cursor(&self) -> Option<String> {
        self.block_cursor.lock().unwrap().clone()
    }

    async fn delete_block_cursor(&self) -> Result<(), StoreError> {
        self.store.delete_block_cursor().await?;
        *self.block_cursor.lock().unwrap() = None;
        Ok(())
    }

    async fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError> {
        let store = self.store.cheap_clone();
        let logger = logger.cheap_clone();
        graph::spawn_blocking_allow_panic(move || store.start_subgraph_deployment(&logger))
            .await
            .map_err(Error::from)??;

        // Refresh all in memory state in case this instance was used before
        *self.block_ptr.lock().unwrap() = self.store.block_ptr().await?;
        *self.block_cursor.lock().unwrap() = self.store.block_cursor().await?;

        Ok(())
    }

    async fn revert_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<&str>,
    ) -> Result<(), StoreError> {
        *self.block_ptr.lock().unwrap() = Some(block_ptr_to.clone());
        *self.block_cursor.lock().unwrap() = firehose_cursor.map(|c| c.to_string());

        self.writer.revert(block_ptr_to, firehose_cursor).await
    }

    fn unfail_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        let outcome = self
            .store
            .unfail_deterministic_error(current_ptr, parent_ptr)?;

        if let UnfailOutcome::Unfailed = outcome {
            *self.block_ptr.lock().unwrap() = Some(parent_ptr.clone());
        }

        Ok(outcome)
    }

    fn unfail_non_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        // We don't have to update in memory self.block_ptr
        // because the method call below doesn't rewind/revert
        // any block.
        self.store.unfail_non_deterministic_error(current_ptr)
    }

    async fn fail_subgraph(&self, error: SubgraphError) -> Result<(), StoreError> {
        self.store.fail_subgraph(error).await
    }

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError> {
        self.store.supports_proof_of_indexing().await
    }

    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        self.writer.get(key)
    }

    async fn transact_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<String>,
        mods: Vec<EntityModification>,
        stopwatch: &StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        self.writer
            .write(
                block_ptr_to.clone(),
                firehose_cursor.clone(),
                mods,
                stopwatch,
                data_sources,
                deterministic_errors,
            )
            .await?;

        *self.block_ptr.lock().unwrap() = Some(block_ptr_to);
        *self.block_cursor.lock().unwrap() = firehose_cursor;

        Ok(())
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        self.writer.get_many(ids_for_type)
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        self.store.deployment_synced()
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        self.store.is_deployment_synced().await
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        self.store.unassign_subgraph()
    }

    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.writer.load_dynamic_data_sources().await
    }

    fn shard(&self) -> &str {
        self.store.shard()
    }

    async fn health(&self) -> Result<schema::SubgraphHealth, StoreError> {
        self.store.health().await
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.store.input_schema()
    }

    async fn flush(&self) -> Result<(), StoreError> {
        self.writer.flush().await
    }
}
