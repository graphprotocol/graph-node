use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::{collections::BTreeMap, sync::Arc};

use graph::blockchain::block_stream::FirehoseCursor;
use graph::components::store::{DeploymentCursorTracker, DerivedEntityQuery, EntityKey, ReadStore};
use graph::constraint_violation;
use graph::data::subgraph::schema;
use graph::data_source::CausalityRegion;
use graph::prelude::{
    BlockNumber, Entity, MetricsRegistry, SubgraphDeploymentEntity, SubgraphStore as _,
    BLOCK_NUMBER_MAX,
};
use graph::schema::InputSchema;
use graph::slog::{info, warn};
use graph::tokio::task::JoinHandle;
use graph::util::bounded_queue::BoundedQueue;
use graph::{
    cheap_clone::CheapClone,
    components::store::{self, EntityType, WritableStore as WritableStoreTrait},
    data::subgraph::schema::SubgraphError,
    prelude::{
        BlockPtr, DeploymentHash, EntityModification, Error, Logger, StopwatchMetrics, StoreError,
        StoreEvent, UnfailOutcome, ENV_VARS,
    },
    slog::error,
};
use store::StoredDynamicDataSource;

use crate::deployment_store::DeploymentStore;
use crate::primary::DeploymentId;
use crate::retry;
use crate::{primary, primary::Site, relational::Layout, SubgraphStore};

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

    fn load_deployment(&self, site: &Site) -> Result<SubgraphDeploymentEntity, StoreError> {
        self.0.load_deployment(site)
    }

    fn find_site(&self, id: DeploymentId) -> Result<Arc<Site>, StoreError> {
        self.0.find_site(id)
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
    input_schema: Arc<InputSchema>,
}

impl SyncStore {
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
        retry::forever_async(&self.logger, "block_ptr", || {
            let site = self.site.clone();
            async move { self.writable.block_ptr(site).await }
        })
        .await
    }

    async fn block_cursor(&self) -> Result<FirehoseCursor, StoreError> {
        self.writable
            .block_cursor(self.site.cheap_clone())
            .await
            .map(FirehoseCursor::from)
    }

    fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError> {
        retry::forever(&self.logger, "start_subgraph_deployment", || {
            let graft_base = match self.writable.graft_pending(&self.site.deployment)? {
                Some((base_id, base_ptr)) => {
                    let src = self.store.layout(&base_id)?;
                    let deployment_entity = self.store.load_deployment(&src.site)?;
                    Some((src, base_ptr, deployment_entity))
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
        firehose_cursor: &FirehoseCursor,
    ) -> Result<(), StoreError> {
        retry::forever(&self.logger, "revert_block_operations", || {
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
        retry::forever(&self.logger, "unfail_deterministic_error", || {
            self.writable
                .unfail_deterministic_error(self.site.clone(), current_ptr, parent_ptr)
        })
    }

    fn unfail_non_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        retry::forever(&self.logger, "unfail_non_deterministic_error", || {
            self.writable
                .unfail_non_deterministic_error(self.site.clone(), current_ptr)
        })
    }

    async fn fail_subgraph(&self, error: SubgraphError) -> Result<(), StoreError> {
        retry::forever_async(&self.logger, "fail_subgraph", || {
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
        retry::forever_async(&self.logger, "supports_proof_of_indexing", || async {
            self.writable
                .supports_proof_of_indexing(self.site.clone())
                .await
        })
        .await
    }

    fn get(&self, key: &EntityKey, block: BlockNumber) -> Result<Option<Entity>, StoreError> {
        retry::forever(&self.logger, "get", || {
            self.writable.get(self.site.cheap_clone(), key, block)
        })
    }

    fn transact_block_operations(
        &self,
        block_ptr_to: &BlockPtr,
        firehose_cursor: &FirehoseCursor,
        mods: &[EntityModification],
        stopwatch: &StopwatchMetrics,
        data_sources: &[StoredDynamicDataSource],
        deterministic_errors: &[SubgraphError],
        manifest_idx_and_name: &[(u32, String)],
        processed_data_sources: &[StoredDynamicDataSource],
    ) -> Result<(), StoreError> {
        retry::forever(&self.logger, "transact_block_operations", move || {
            let event = self.writable.transact_block_operations(
                &self.logger,
                self.site.clone(),
                block_ptr_to,
                firehose_cursor,
                mods,
                stopwatch,
                data_sources,
                deterministic_errors,
                manifest_idx_and_name,
                processed_data_sources,
            )?;

            let _section = stopwatch.start_section("send_store_event");
            self.try_send_store_event(event)?;
            Ok(())
        })
    }

    fn get_many(
        &self,
        keys: BTreeSet<EntityKey>,
        block: BlockNumber,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        let mut by_type: BTreeMap<(EntityType, CausalityRegion), Vec<String>> = BTreeMap::new();
        for key in keys {
            by_type
                .entry((key.entity_type, key.causality_region))
                .or_default()
                .push(key.entity_id.into());
        }

        retry::forever(&self.logger, "get_many", || {
            self.writable
                .get_many(self.site.cheap_clone(), &by_type, block)
        })
    }

    fn get_derived(
        &self,
        key: &DerivedEntityQuery,
        block: BlockNumber,
        excluded_keys: Vec<EntityKey>,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        retry::forever(&self.logger, "get_derived", || {
            self.writable
                .get_derived(self.site.cheap_clone(), key, block, &excluded_keys)
        })
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        retry::forever_async(&self.logger, "is_deployment_synced", || async {
            self.writable
                .exists_and_synced(self.site.deployment.cheap_clone())
                .await
        })
        .await
    }

    fn unassign_subgraph(&self, site: &Site) -> Result<(), StoreError> {
        retry::forever(&self.logger, "unassign_subgraph", || {
            let pconn = self.store.primary_conn()?;
            pconn.transaction(|| -> Result<_, StoreError> {
                let changes = pconn.unassign_subgraph(site)?;
                self.store.send_store_event(&StoreEvent::new(changes))
            })
        })
    }

    async fn load_dynamic_data_sources(
        &self,
        block: BlockNumber,
        manifest_idx_and_name: Vec<(u32, String)>,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        retry::forever_async(&self.logger, "load_dynamic_data_sources", || async {
            self.writable
                .load_dynamic_data_sources(
                    self.site.cheap_clone(),
                    block,
                    manifest_idx_and_name.clone(),
                )
                .await
        })
        .await
    }

    pub(crate) async fn causality_region_curr_val(
        &self,
    ) -> Result<Option<CausalityRegion>, StoreError> {
        retry::forever_async(&self.logger, "causality_region_curr_val", || async {
            self.writable
                .causality_region_curr_val(self.site.cheap_clone())
                .await
        })
        .await
    }

    fn maybe_find_site(&self, src: DeploymentId) -> Result<Option<Arc<Site>>, StoreError> {
        match self.store.find_site(src) {
            Ok(site) => Ok(Some(site)),
            Err(StoreError::DeploymentNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        retry::forever(&self.logger, "deployment_synced", || {
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

            // Handle on_sync actions. They only apply to copies (not
            // grafts) so we make sure that the source, if it exists, has
            // the same hash as `self.site`
            if let Some(src) = self.writable.source_of_copy(&self.site)? {
                if let Some(src) = self.maybe_find_site(src)? {
                    if src.deployment == self.site.deployment {
                        let on_sync = self.writable.on_sync(&self.site)?;
                        if on_sync.activate() {
                            let pconn = self.store.primary_conn()?;
                            pconn.activate(&self.site.as_ref().into())?;
                        }
                        if on_sync.replace() {
                            self.unassign_subgraph(&src)?;
                        }
                    }
                }
            }

            self.writable.deployment_synced(&self.site.deployment)?;

            self.store.send_store_event(&event)
        })
    }

    fn shard(&self) -> &str {
        self.site.shard.as_str()
    }

    async fn health(&self) -> Result<schema::SubgraphHealth, StoreError> {
        retry::forever_async(&self.logger, "health", || async {
            self.writable.health(&self.site).await.map(Into::into)
        })
        .await
    }

    fn input_schema(&self) -> Arc<InputSchema> {
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
            Request::Stop => { /* do nothing */ }
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
        firehose_cursor: FirehoseCursor,
        mods: Vec<EntityModification>,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
        manifest_idx_and_name: Vec<(u32, String)>,
        processed_data_sources: Vec<StoredDynamicDataSource>,
    },
    RevertTo {
        store: Arc<SyncStore>,
        /// The subgraph head will be at this block pointer after the revert
        block_ptr: BlockPtr,
        firehose_cursor: FirehoseCursor,
    },
    Stop,
}

impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Write {
                block_ptr,
                mods,
                store,
                ..
            } => write!(
                f,
                "write[{}, {:p}, {} entities]",
                block_ptr.number,
                store.as_ref(),
                mods.len()
            ),
            Self::RevertTo {
                block_ptr, store, ..
            } => write!(f, "revert[{}, {:p}]", block_ptr.number, store.as_ref()),
            Self::Stop => write!(f, "stop"),
        }
    }
}

enum ExecResult {
    Continue,
    Stop,
}

impl Request {
    fn execute(&self) -> Result<ExecResult, StoreError> {
        match self {
            Request::Write {
                store,
                stopwatch,
                block_ptr: block_ptr_to,
                firehose_cursor,
                mods,
                data_sources,
                deterministic_errors,
                manifest_idx_and_name,
                processed_data_sources,
            } => store
                .transact_block_operations(
                    block_ptr_to,
                    firehose_cursor,
                    mods,
                    stopwatch,
                    data_sources,
                    deterministic_errors,
                    manifest_idx_and_name,
                    processed_data_sources,
                )
                .map(|()| ExecResult::Continue),
            Request::RevertTo {
                store,
                block_ptr,
                firehose_cursor,
            } => store
                .revert_block_operations(block_ptr.clone(), firehose_cursor)
                .map(|()| ExecResult::Continue),
            Request::Stop => Ok(ExecResult::Stop),
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
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use graph::{
        components::store::{DeploymentId, DeploymentLocator},
        prelude::lazy_static,
        util::bounded_queue::BoundedQueue,
    };

    lazy_static! {
        static ref STEPS: Mutex<HashMap<DeploymentId, Arc<BoundedQueue<()>>>> =
            Mutex::new(HashMap::new());
    }

    pub(super) async fn take_step(deployment: &DeploymentLocator) {
        let steps = STEPS.lock().unwrap().get(&deployment.id).cloned();
        if let Some(steps) = steps {
            steps.pop().await;
        }
    }

    /// Allow the writer to process `steps` requests. After calling this,
    /// the writer will only process the number of requests it is allowed to
    pub async fn allow_steps(deployment: &DeploymentLocator, steps: usize) {
        let queue = {
            let mut map = STEPS.lock().unwrap();
            map.entry(deployment.id)
                .or_insert_with(|| Arc::new(BoundedQueue::with_capacity(1_000)))
                .clone()
        };
        for _ in 0..steps {
            queue.push(()).await
        }
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reqs = self.queue.fold(vec![], |mut reqs, req| {
            reqs.push(req.clone());
            reqs
        });

        write!(f, "reqs[{} : ", self.store.site)?;
        for req in reqs {
            write!(f, " {:?}", req)?;
        }
        writeln!(f, "]")
    }
}

impl Queue {
    /// Create a new queue and spawn a task that processes write requests
    fn start(
        logger: Logger,
        store: Arc<SyncStore>,
        capacity: usize,
        registry: Arc<MetricsRegistry>,
    ) -> (Arc<Self>, JoinHandle<()>) {
        async fn start_writer(queue: Arc<Queue>, logger: Logger) {
            loop {
                #[cfg(debug_assertions)]
                test_support::take_step(&queue.store.site.as_ref().into()).await;

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
                use ExecResult::*;
                match res {
                    Ok(Ok(Continue)) => {
                        // The request has been handled. It's now safe to remove it
                        // from the queue
                        queue.queue.pop().await;
                    }
                    Ok(Ok(Stop)) => {
                        // Graceful shutdown. We also handled the request
                        // successfully
                        queue.queue.pop().await;
                        return;
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

        let handle = graph::spawn(start_writer(queue.cheap_clone(), logger));

        (queue, handle)
    }

    /// Add a write request to the queue
    async fn push(&self, req: Request) -> Result<(), StoreError> {
        self.check_err()?;
        self.queue.push(Arc::new(req)).await;
        Ok(())
    }

    /// Wait for the background writer to finish processing queued entries
    async fn flush(&self) -> Result<(), StoreError> {
        self.check_err()?;
        self.queue.wait_empty().await;
        self.check_err()
    }

    async fn stop(&self) -> Result<(), StoreError> {
        self.push(Request::Stop).await
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
                            .find(|emod| emod.entity_ref() == key)
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
                Request::RevertTo { .. } | Request::Stop => None,
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
        mut keys: BTreeSet<EntityKey>,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        // See the implementation of `get` for how we handle reverts
        let mut tracker = BlockTracker::new();

        // Get entities from entries in the queue
        let entities_in_queue = self.queue.fold(
            BTreeMap::new(),
            |mut map: BTreeMap<EntityKey, Option<Entity>>, req| {
                tracker.update(req.as_ref());
                match req.as_ref() {
                    Request::Write {
                        block_ptr, mods, ..
                    } => {
                        if tracker.visible(block_ptr) {
                            for emod in mods {
                                let key = emod.entity_ref();
                                // The key must be removed to avoid overwriting it with a stale value.
                                if let Some(key) = keys.take(key) {
                                    map.insert(key, emod.entity().cloned());
                                }
                            }
                        }
                    }
                    Request::RevertTo { .. } | Request::Stop => { /* nothing to do */ }
                }
                map
            },
        );

        // Whatever remains in `keys` needs to be gotten from the store
        let mut map = self.store.get_many(keys, tracker.query_block())?;

        // Extend the store results with the entities from the queue.
        for (key, entity) in entities_in_queue {
            if let Some(entity) = entity {
                let overwrite = map.insert(key, entity).is_some();
                assert!(!overwrite);
            }
        }

        Ok(map)
    }

    fn get_derived(
        &self,
        derived_query: &DerivedEntityQuery,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        let mut tracker = BlockTracker::new();

        // Get entities from entries in the queue
        let entities_in_queue = self.queue.fold(
            BTreeMap::new(),
            |mut map: BTreeMap<EntityKey, Option<Entity>>, req| {
                tracker.update(req.as_ref());
                match req.as_ref() {
                    Request::Write {
                        block_ptr, mods, ..
                    } => {
                        if tracker.visible(block_ptr) {
                            for emod in mods {
                                let key = emod.entity_ref();
                                // we select just the entities that match the query
                                if derived_query.entity_type == key.entity_type {
                                    if let Some(entity) = emod.entity().cloned() {
                                        if let Some(related_id) =
                                            entity.get(derived_query.entity_field.as_str())
                                        {
                                            // we check only the field agains the value
                                            if related_id.to_string()
                                                == derived_query.value.to_string()
                                            {
                                                map.insert(key.clone(), Some(entity));
                                            }
                                        }
                                    } else {
                                        // if the entity was deleted, we add here with no checks
                                        // just for removing from the query
                                        map.insert(key.clone(), emod.entity().cloned());
                                    }
                                }
                            }
                        }
                    }
                    Request::RevertTo { .. } | Request::Stop => { /* nothing to do */ }
                }
                map
            },
        );

        let excluded_keys: Vec<EntityKey> = entities_in_queue.keys().cloned().collect();

        // We filter to exclude the entities ids that we already have from the queue
        let mut items_from_database =
            self.store
                .get_derived(derived_query, tracker.query_block(), excluded_keys)?;

        // Extend the store results with the entities from the queue.
        // This overwrites any entitiy from the database with the same key from queue
        let items_from_queue: BTreeMap<EntityKey, Entity> = entities_in_queue
            .into_iter()
            .filter_map(|(key, entity)| entity.map(|entity| (key, entity)))
            .collect();
        items_from_database.extend(items_from_queue);

        Ok(items_from_database)
    }

    /// Load dynamic data sources by looking at both the queue and the store
    async fn load_dynamic_data_sources(
        &self,
        manifest_idx_and_name: Vec<(u32, String)>,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
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
                    processed_data_sources,
                    ..
                } => {
                    if tracker.visible(block_ptr) {
                        dds.extend(data_sources.clone());
                        dds.retain(|dds| !processed_data_sources.contains(dds));
                    }
                }
                Request::RevertTo { .. } | Request::Stop => { /* nothing to do */ }
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
            .load_dynamic_data_sources(tracker.query_block(), manifest_idx_and_name)
            .await?;
        dds.append(&mut queue_dds);

        Ok(dds)
    }

    fn poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }

    fn deployment_synced(&self) {
        self.stopwatch.disable()
    }
}

/// A shim to allow bypassing any pipelined store handling if need be
enum Writer {
    Sync(Arc<SyncStore>),
    Async {
        queue: Arc<Queue>,
        join_handle: JoinHandle<()>,
    },
}

impl Writer {
    fn new(
        logger: Logger,
        store: Arc<SyncStore>,
        capacity: usize,
        registry: Arc<MetricsRegistry>,
    ) -> Self {
        info!(logger, "Starting subgraph writer"; "queue_size" => capacity);
        if capacity == 0 {
            Self::Sync(store)
        } else {
            let (queue, join_handle) = Queue::start(logger, store.clone(), capacity, registry);
            Self::Async { queue, join_handle }
        }
    }

    fn check_queue_running(&self) -> Result<(), StoreError> {
        match self {
            Writer::Sync(_) => Ok(()),
            Writer::Async { join_handle, queue } => {
                // If there was an error, report that instead of a naked 'writer not running'
                queue.check_err()?;
                if join_handle.is_finished() {
                    Err(constraint_violation!(
                        "Subgraph writer for {} is not running",
                        queue.store.site
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn write(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: FirehoseCursor,
        mods: Vec<EntityModification>,
        stopwatch: &StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
        manifest_idx_and_name: Vec<(u32, String)>,
        processed_data_sources: Vec<StoredDynamicDataSource>,
    ) -> Result<(), StoreError> {
        match self {
            Writer::Sync(store) => store.transact_block_operations(
                &block_ptr_to,
                &firehose_cursor,
                &mods,
                stopwatch,
                &data_sources,
                &deterministic_errors,
                &manifest_idx_and_name,
                &processed_data_sources,
            ),
            Writer::Async { queue, .. } => {
                self.check_queue_running()?;
                let req = Request::Write {
                    store: queue.store.cheap_clone(),
                    stopwatch: queue.stopwatch.cheap_clone(),
                    block_ptr: block_ptr_to,
                    firehose_cursor,
                    mods,
                    data_sources,
                    deterministic_errors,
                    manifest_idx_and_name,
                    processed_data_sources,
                };
                queue.push(req).await
            }
        }
    }

    async fn revert(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: FirehoseCursor,
    ) -> Result<(), StoreError> {
        match self {
            Writer::Sync(store) => store.revert_block_operations(block_ptr_to, &firehose_cursor),
            Writer::Async { queue, .. } => {
                self.check_queue_running()?;
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
            Writer::Async { queue, .. } => {
                self.check_queue_running()?;
                queue.flush().await
            }
        }
    }

    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        match self {
            Writer::Sync(store) => store.get(key, BLOCK_NUMBER_MAX),
            Writer::Async { queue, .. } => queue.get(key),
        }
    }

    fn get_many(
        &self,
        keys: BTreeSet<EntityKey>,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        match self {
            Writer::Sync(store) => store.get_many(keys, BLOCK_NUMBER_MAX),
            Writer::Async { queue, .. } => queue.get_many(keys),
        }
    }

    fn get_derived(
        &self,
        key: &DerivedEntityQuery,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        match self {
            Writer::Sync(store) => store.get_derived(key, BLOCK_NUMBER_MAX, vec![]),
            Writer::Async { queue, .. } => queue.get_derived(key),
        }
    }

    async fn load_dynamic_data_sources(
        &self,
        manifest_idx_and_name: Vec<(u32, String)>,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        match self {
            Writer::Sync(store) => {
                store
                    .load_dynamic_data_sources(BLOCK_NUMBER_MAX, manifest_idx_and_name)
                    .await
            }
            Writer::Async { queue, .. } => {
                queue.load_dynamic_data_sources(manifest_idx_and_name).await
            }
        }
    }

    fn poisoned(&self) -> bool {
        match self {
            Writer::Sync(_) => false,
            Writer::Async { queue, .. } => queue.poisoned(),
        }
    }

    async fn stop(&self) -> Result<(), StoreError> {
        match self {
            Writer::Sync(_) => Ok(()),
            Writer::Async { queue, .. } => queue.stop().await,
        }
    }

    fn deployment_synced(&self) {
        match self {
            Writer::Sync(_) => {}
            Writer::Async { queue, .. } => queue.deployment_synced(),
        }
    }
}

pub struct WritableStore {
    store: Arc<SyncStore>,
    block_ptr: Mutex<Option<BlockPtr>>,
    block_cursor: Mutex<FirehoseCursor>,
    writer: Writer,
}

impl WritableStore {
    pub(crate) async fn new(
        subgraph_store: SubgraphStore,
        logger: Logger,
        site: Arc<Site>,
        registry: Arc<MetricsRegistry>,
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

    pub(crate) fn poisoned(&self) -> bool {
        self.writer.poisoned()
    }

    pub(crate) async fn stop(&self) -> Result<(), StoreError> {
        self.writer.stop().await
    }
}

impl ReadStore for WritableStore {
    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        self.writer.get(key)
    }

    fn get_many(
        &self,
        keys: BTreeSet<EntityKey>,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        self.writer.get_many(keys)
    }

    fn get_derived(
        &self,
        key: &DerivedEntityQuery,
    ) -> Result<BTreeMap<EntityKey, Entity>, StoreError> {
        self.writer.get_derived(key)
    }

    fn input_schema(&self) -> Arc<InputSchema> {
        self.store.input_schema()
    }
}

impl DeploymentCursorTracker for WritableStore {
    fn block_ptr(&self) -> Option<BlockPtr> {
        self.block_ptr.lock().unwrap().clone()
    }

    fn firehose_cursor(&self) -> FirehoseCursor {
        self.block_cursor.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl WritableStoreTrait for WritableStore {
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
        firehose_cursor: FirehoseCursor,
    ) -> Result<(), StoreError> {
        *self.block_ptr.lock().unwrap() = Some(block_ptr_to.clone());
        *self.block_cursor.lock().unwrap() = firehose_cursor.clone();

        self.writer.revert(block_ptr_to, firehose_cursor).await
    }

    async fn unfail_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        let outcome = self
            .store
            .unfail_deterministic_error(current_ptr, parent_ptr)?;

        if let UnfailOutcome::Unfailed = outcome {
            *self.block_ptr.lock().unwrap() = self.store.block_ptr().await?;
            *self.block_cursor.lock().unwrap() = self.store.block_cursor().await?;
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

    async fn transact_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: FirehoseCursor,
        mods: Vec<EntityModification>,
        stopwatch: &StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
        manifest_idx_and_name: Vec<(u32, String)>,
        processed_data_sources: Vec<StoredDynamicDataSource>,
    ) -> Result<(), StoreError> {
        self.writer
            .write(
                block_ptr_to.clone(),
                firehose_cursor.clone(),
                mods,
                stopwatch,
                data_sources,
                deterministic_errors,
                manifest_idx_and_name,
                processed_data_sources,
            )
            .await?;

        *self.block_ptr.lock().unwrap() = Some(block_ptr_to);
        *self.block_cursor.lock().unwrap() = firehose_cursor;

        Ok(())
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        self.writer.deployment_synced();
        self.store.deployment_synced()
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        self.store.is_deployment_synced().await
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        self.store.unassign_subgraph(&self.store.site)
    }

    async fn load_dynamic_data_sources(
        &self,
        manifest_idx_and_name: Vec<(u32, String)>,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.writer
            .load_dynamic_data_sources(manifest_idx_and_name)
            .await
    }

    async fn causality_region_curr_val(&self) -> Result<Option<CausalityRegion>, StoreError> {
        // It should be empty when we call this, but just in case.
        self.writer.flush().await?;
        self.store.causality_region_curr_val().await
    }

    fn shard(&self) -> &str {
        self.store.shard()
    }

    async fn health(&self) -> Result<schema::SubgraphHealth, StoreError> {
        self.store.health().await
    }

    async fn flush(&self) -> Result<(), StoreError> {
        self.writer.flush().await
    }

    async fn restart(self: Arc<Self>) -> Result<Option<Arc<dyn WritableStoreTrait>>, StoreError> {
        if self.poisoned() {
            // When the writer is poisoned, the background thread has
            // finished since `start_writer` returns whenever it encounters
            // an error. Just to make extra-sure, we log a warning if the
            // join handle indicates that the writer hasn't stopped yet.
            let logger = self.store.logger.clone();
            match &self.writer {
                Writer::Sync(_) => { /* can't happen, a sync writer never gets poisoned */ }
                Writer::Async { join_handle, queue } => {
                    let err = match queue.check_err() {
                        Ok(()) => "error missing".to_string(),
                        Err(e) => e.to_string(),
                    };
                    if !join_handle.is_finished() {
                        warn!(logger, "Writer was poisoned, but background thread didn't finish. Creating new writer regardless"; "error" => err);
                    }
                }
            }
            let store = Arc::new(self.store.store.0.clone());
            store
                .writable(logger, self.store.site.id.into())
                .await
                .map(|store| Some(store))
        } else {
            Ok(None)
        }
    }
}
