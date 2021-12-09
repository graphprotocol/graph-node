use std::sync::Mutex;
use std::time::Duration;
use std::{collections::BTreeMap, sync::Arc};

use graph::data::subgraph::schema;
use graph::prelude::{Entity, Schema, SubgraphStore as _};
use graph::{
    cheap_clone::CheapClone,
    components::store::{self, EntityType, WritableStore as WritableStoreTrait},
    data::subgraph::schema::SubgraphError,
    prelude::StoreEvent,
    prelude::{
        lazy_static, BlockPtr, DeploymentHash, EntityKey, EntityModification, Error, Logger,
        StopwatchMetrics, StoreError,
    },
    slog::{error, warn},
    util::backoff::ExponentialBackoff,
};
use store::StoredDynamicDataSource;

use crate::deployment_store::DeploymentStore;
use crate::{primary, primary::Site, relational::Layout, SubgraphStore};

lazy_static! {
    /// Whether to disable the notifications that feed GraphQL
    /// subscriptions; when the environment variable is set, no updates
    /// about entity changes will be sent to query nodes
    pub static ref SEND_SUBSCRIPTION_NOTIFICATIONS: bool = {
      std::env::var("GRAPH_DISABLE_SUBSCRIPTION_NOTIFICATIONS").ok().is_none()
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

pub(crate) struct WritableStore {
    logger: Logger,
    store: WritableSubgraphStore,
    writable: Arc<DeploymentStore>,
    site: Arc<Site>,
    input_schema: Arc<Schema>,
}

impl WritableStore {
    const BACKOFF_BASE: Duration = Duration::from_millis(100);
    const BACKOFF_CEIL: Duration = Duration::from_secs(10);

    pub(crate) fn new(
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
        if *SEND_SUBSCRIPTION_NOTIFICATIONS {
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
impl WritableStore {
    fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        self.retry("block_ptr", || self.writable.block_ptr(self.site.as_ref()))
    }

    fn block_cursor(&self) -> Result<Option<String>, StoreError> {
        self.writable.block_cursor(self.site.as_ref())
    }

    fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError> {
        self.retry("start_subgraph_deployment", || {
            let store = &self.writable;

            let graft_base = match store.graft_pending(&self.site.deployment)? {
                Some((base_id, base_ptr)) => {
                    let src = self.store.layout(&base_id)?;
                    Some((src, base_ptr))
                }
                None => None,
            };
            store.start_subgraph(logger, self.site.clone(), graft_base)?;
            self.store.primary_conn()?.copy_finished(self.site.as_ref())
        })
    }

    fn revert_block_operations(&self, block_ptr_to: BlockPtr) -> Result<(), StoreError> {
        self.retry("revert_block_operations", || {
            let event = self
                .writable
                .revert_block_operations(self.site.clone(), block_ptr_to.clone())?;
            self.try_send_store_event(event)
        })
    }

    fn unfail_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<(), StoreError> {
        self.retry("unfail_deterministic_error", || {
            self.writable
                .unfail_deterministic_error(self.site.clone(), current_ptr, parent_ptr)
        })
    }

    fn unfail_non_deterministic_error(&self, current_ptr: &BlockPtr) -> Result<(), StoreError> {
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

    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        self.retry("get", || self.writable.get(self.site.cheap_clone(), key))
    }

    fn transact_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<String>,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        assert!(
            same_subgraph(&mods, &self.site.deployment),
            "can only transact operations within one shard"
        );
        self.retry("transact_block_operations", move || {
            let event = self.writable.transact_block_operations(
                self.site.clone(),
                &block_ptr_to,
                firehose_cursor.as_deref(),
                &mods,
                stopwatch.cheap_clone(),
                &data_sources,
                &deterministic_errors,
            )?;

            let _section = stopwatch.start_section("send_store_event");
            self.try_send_store_event(event)?;
            Ok(())
        })
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        self.retry("get_many", || {
            self.writable
                .get_many(self.site.cheap_clone(), &ids_for_type)
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

    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.retry_async("load_dynamic_data_sources", || async {
            self.writable
                .load_dynamic_data_sources(self.site.deployment.clone())
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

    async fn health(&self, id: &DeploymentHash) -> Result<schema::SubgraphHealth, StoreError> {
        self.retry_async("health", || async {
            self.writable.health(id).await.map(Into::into)
        })
        .await
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.input_schema.clone()
    }
}

fn same_subgraph(mods: &Vec<EntityModification>, id: &DeploymentHash) -> bool {
    mods.iter().all(|md| &md.entity_key().subgraph_id == id)
}

#[allow(dead_code)]
pub struct WritableAgent {
    store: Arc<WritableStore>,
    block_ptr: Mutex<Option<BlockPtr>>,
    block_cursor: Mutex<Option<String>>,
}

impl WritableAgent {
    pub(crate) fn new(
        subgraph_store: SubgraphStore,
        logger: Logger,
        site: Arc<Site>,
    ) -> Result<Self, StoreError> {
        let store = Arc::new(WritableStore::new(subgraph_store, logger, site)?);
        let block_ptr = Mutex::new(store.block_ptr()?);
        let block_cursor = Mutex::new(store.block_cursor()?);
        Ok(Self {
            store,
            block_ptr,
            block_cursor,
        })
    }
}

#[allow(unused_variables)]
#[async_trait::async_trait]
impl WritableStoreTrait for WritableAgent {
    fn block_ptr(&self) -> Option<BlockPtr> {
        self.block_ptr.lock().unwrap().clone()
    }

    fn block_cursor(&self) -> Option<String> {
        self.block_cursor.lock().unwrap().clone()
    }

    fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError> {
        // TODO: Spin up a background writer thread and establish a channel
        self.store.start_subgraph_deployment(logger)
    }

    fn revert_block_operations(&self, block_ptr_to: BlockPtr) -> Result<(), StoreError> {
        *self.block_ptr.lock().unwrap() = Some(block_ptr_to.clone());
        // FIXME: What about the firehose cursor? Why doesn't that get updated?

        // TODO: If we haven't written the block yet, revert in memory. If
        // we have, revert in the database
        self.store.revert_block_operations(block_ptr_to)
    }

    fn unfail_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<(), StoreError> {
        self.store
            .unfail_deterministic_error(current_ptr, parent_ptr)
    }

    fn unfail_non_deterministic_error(&self, current_ptr: &BlockPtr) -> Result<(), StoreError> {
        self.store.unfail_non_deterministic_error(current_ptr)
    }

    async fn fail_subgraph(&self, error: SubgraphError) -> Result<(), StoreError> {
        self.store.fail_subgraph(error).await
    }

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError> {
        self.store.supports_proof_of_indexing().await
    }

    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError> {
        self.store.get(key)
    }

    fn transact_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<String>,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        self.store.transact_block_operations(
            block_ptr_to.clone(),
            firehose_cursor.clone(),
            mods,
            stopwatch,
            data_sources,
            deterministic_errors,
        )?;

        *self.block_ptr.lock().unwrap() = Some(block_ptr_to);
        *self.block_cursor.lock().unwrap() = firehose_cursor;

        Ok(())
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        self.store.get_many(ids_for_type)
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
        // TODO: Combine in-memory and stored data sources
        self.store.load_dynamic_data_sources().await
    }

    fn shard(&self) -> &str {
        self.store.shard()
    }

    async fn health(&self, id: &DeploymentHash) -> Result<schema::SubgraphHealth, StoreError> {
        self.store.health(id).await
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.store.input_schema()
    }
}
