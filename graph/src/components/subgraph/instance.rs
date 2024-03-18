use std::collections::HashMap;

use anyhow::Result;
use csv::Writer;
use futures03::future::join_all;
use object_store::{gcp::GoogleCloudStorageBuilder, path::Path, ObjectStore};
use serde::Serialize;
use url::Url;

use crate::{
    blockchain::{Blockchain, DataSourceTemplate as _},
    components::store::{DeploymentId, EntityLfuCache, ReadStore, StoredDynamicDataSource},
    data::{store::Id, subgraph::schema::SubgraphError},
    data_source::{DataSourceTemplate, DataSourceTemplateInfo},
    prelude::*,
    schema::EntityType,
    spawn,
};

#[derive(Debug, Clone)]
pub enum InstanceDSTemplate {
    Onchain(DataSourceTemplateInfo),
    Offchain(crate::data_source::offchain::DataSourceTemplate),
}

impl<C: Blockchain> From<&DataSourceTemplate<C>> for InstanceDSTemplate {
    fn from(value: &crate::data_source::DataSourceTemplate<C>) -> Self {
        match value {
            DataSourceTemplate::Onchain(ds) => Self::Onchain(ds.info()),
            DataSourceTemplate::Offchain(ds) => Self::Offchain(ds.clone()),
        }
    }
}

impl InstanceDSTemplate {
    pub fn name(&self) -> &str {
        match self {
            Self::Onchain(ds) => &ds.name,
            Self::Offchain(ds) => &ds.name,
        }
    }

    pub fn is_onchain(&self) -> bool {
        match self {
            Self::Onchain(_) => true,
            Self::Offchain(_) => false,
        }
    }

    pub fn into_onchain(self) -> Option<DataSourceTemplateInfo> {
        match self {
            Self::Onchain(ds) => Some(ds),
            Self::Offchain(_) => None,
        }
    }

    pub fn manifest_idx(&self) -> Option<u32> {
        match self {
            InstanceDSTemplate::Onchain(info) => info.manifest_idx,
            InstanceDSTemplate::Offchain(info) => Some(info.manifest_idx),
        }
    }
}

#[derive(Clone, Debug)]
pub struct InstanceDSTemplateInfo {
    pub template: InstanceDSTemplate,
    pub params: Vec<String>,
    pub context: Option<DataSourceContext>,
    pub creation_block: BlockNumber,
}

#[derive(Debug)]
pub struct BlockState {
    pub entity_cache: EntityCache,
    pub deterministic_errors: Vec<SubgraphError>,
    created_data_sources: Vec<InstanceDSTemplateInfo>,

    // Data sources to be transacted into the store.
    pub persisted_data_sources: Vec<StoredDynamicDataSource>,

    // Data sources created in the current handler.
    handler_created_data_sources: Vec<InstanceDSTemplateInfo>,

    // data source that have been processed.
    pub processed_data_sources: Vec<StoredDynamicDataSource>,

    // Marks whether a handler is currently executing.
    in_handler: bool,

    pub metrics: BlockStateMetrics,
}

#[derive(Debug)]
pub struct BlockStateMetrics {
    pub gas_counter: HashMap<CounterKey, u64>,
    pub op_counter: HashMap<CounterKey, u64>,
    pub read_bytes_counter: HashMap<CounterKey, u64>,
    pub write_bytes_counter: HashMap<CounterKey, u64>,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub enum CounterKey {
    Entity(EntityType, Id),
    String(String),
}

impl From<&str> for CounterKey {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl BlockStateMetrics {
    pub fn new() -> Self {
        BlockStateMetrics {
            read_bytes_counter: HashMap::new(),
            write_bytes_counter: HashMap::new(),
            gas_counter: HashMap::new(),
            op_counter: HashMap::new(),
        }
    }

    pub fn extend(&mut self, other: BlockStateMetrics) {
        for (key, value) in other.read_bytes_counter {
            *self.read_bytes_counter.entry(key).or_insert(0) += value;
        }

        for (key, value) in other.write_bytes_counter {
            *self.write_bytes_counter.entry(key).or_insert(0) += value;
        }

        for (key, value) in other.gas_counter {
            *self.gas_counter.entry(key).or_insert(0) += value;
        }

        for (key, value) in other.op_counter {
            *self.op_counter.entry(key).or_insert(0) += value;
        }
    }

    fn serialize_to_csv<T: Serialize, U: Serialize, I: IntoIterator<Item = T>>(
        data: I,
        column_names: U,
    ) -> Result<String> {
        let mut wtr = Writer::from_writer(vec![]);
        wtr.serialize(column_names)?;
        for record in data {
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(String::from_utf8(wtr.into_inner()?)?)
    }

    pub fn counter_to_csv(
        data: &HashMap<CounterKey, u64>,
        column_names: Vec<&str>,
    ) -> Result<String> {
        Self::serialize_to_csv(
            data.iter().map(|(key, value)| match key {
                CounterKey::Entity(typename, id) => {
                    vec![
                        typename.typename().to_string(),
                        id.to_string(),
                        value.to_string(),
                    ]
                }
                CounterKey::String(key) => vec![key.to_string(), value.to_string()],
            }),
            column_names,
        )
    }

    async fn write_csv_to_store(bucket: &str, path: &str, data: String) -> Result<()> {
        let data_bytes = data.into_bytes();

        let bucket =
            Url::parse(&bucket).map_err(|e| anyhow!("Failed to parse bucket url: {}", e))?;
        let store = GoogleCloudStorageBuilder::from_env()
            .with_url(bucket)
            .build()?;

        store.put(&Path::parse(path)?, data_bytes.into()).await?;

        Ok(())
    }

    pub fn track_gas_and_ops(&mut self, method: &str, gas_used: u64) {
        if ENV_VARS.enable_dips_metrics {
            let key = CounterKey::from(method);
            let counter = self.gas_counter.entry(key.clone()).or_insert(0);
            *counter += gas_used;

            let counter = self.op_counter.entry(key).or_insert(0);
            *counter += 1;
        }
    }

    pub fn track_entity_read(&mut self, entity_type: &EntityType, entity: &Entity) {
        if ENV_VARS.enable_dips_metrics {
            let key = CounterKey::Entity(entity_type.clone(), entity.id());
            let counter = self.read_bytes_counter.entry(key).or_insert(0);
            *counter += entity.weight() as u64;
        }
    }

    pub fn track_entity_write(&mut self, entity_type: &EntityType, entity: &Entity) {
        if ENV_VARS.enable_dips_metrics {
            let key = CounterKey::Entity(entity_type.clone(), entity.id());
            let counter = self.write_bytes_counter.entry(key).or_insert(0);
            *counter += entity.weight() as u64;
        }
    }

    pub fn track_entity_read_batch(&mut self, entity_type: &EntityType, entities: &[Entity]) {
        if ENV_VARS.enable_dips_metrics {
            for entity in entities {
                let key = CounterKey::Entity(entity_type.clone(), entity.id());
                let counter = self.read_bytes_counter.entry(key).or_insert(0);
                *counter += entity.weight() as u64;
            }
        }
    }

    pub fn track_entity_write_batch(&mut self, entity_type: &EntityType, entities: &[Entity]) {
        if ENV_VARS.enable_dips_metrics {
            for entity in entities {
                let key = CounterKey::Entity(entity_type.clone(), entity.id());
                let counter = self.write_bytes_counter.entry(key).or_insert(0);
                *counter += entity.weight() as u64;
            }
        }
    }

    pub fn flush_metrics_to_store(
        &self,
        logger: &Logger,
        block_ptr: BlockPtr,
        subgraph_id: DeploymentId,
    ) -> Result<()> {
        if !ENV_VARS.enable_dips_metrics {
            return Ok(());
        }

        let logger = logger.clone();

        let bucket = ENV_VARS
            .dips_metrics_object_store_url
            .as_deref()
            .ok_or_else(|| anyhow!("Object store URL is not set"))?;

        // Clone self and other necessary data for the async block
        let gas_counter = self.gas_counter.clone();
        let op_counter = self.op_counter.clone();
        let read_bytes_counter = self.read_bytes_counter.clone();
        let write_bytes_counter = self.write_bytes_counter.clone();

        // Spawn the async task
        spawn(async move {
            // Prepare data for uploading
            let metrics_data = vec![
                (
                    "gas",
                    Self::counter_to_csv(&gas_counter, vec!["method", "gas"]).unwrap(),
                ),
                (
                    "op",
                    Self::counter_to_csv(&op_counter, vec!["method", "count"]).unwrap(),
                ),
                (
                    "read_bytes",
                    Self::counter_to_csv(&read_bytes_counter, vec!["entity", "id", "bytes"])
                        .unwrap(),
                ),
                (
                    "write_bytes",
                    Self::counter_to_csv(&write_bytes_counter, vec!["entity", "id", "bytes"])
                        .unwrap(),
                ),
            ];

            // Convert each metrics upload into a future
            let upload_futures = metrics_data.into_iter().map(|(metric_name, data)| {
                let file_path = format!("{}/{}/{}.csv", subgraph_id, block_ptr.number, metric_name);
                let bucket_clone = bucket.to_string();
                let logger_clone = logger.clone();
                async move {
                    match Self::write_csv_to_store(&bucket_clone, &file_path, data).await {
                        Ok(_) => info!(
                            logger_clone,
                            "Uploaded {} metrics for block {}", metric_name, block_ptr.number
                        ),
                        Err(e) => error!(
                            logger_clone,
                            "Error uploading {} metrics: {}", metric_name, e
                        ),
                    }
                }
            });

            join_all(upload_futures).await;
        });

        Ok(())
    }
}

impl BlockState {
    pub fn new(store: impl ReadStore, lfu_cache: EntityLfuCache) -> Self {
        BlockState {
            entity_cache: EntityCache::with_current(Arc::new(store), lfu_cache),
            deterministic_errors: Vec::new(),
            created_data_sources: Vec::new(),
            persisted_data_sources: Vec::new(),
            handler_created_data_sources: Vec::new(),
            processed_data_sources: Vec::new(),
            in_handler: false,
            metrics: BlockStateMetrics::new(),
        }
    }
}

impl BlockState {
    pub fn extend(&mut self, other: BlockState) {
        assert!(!other.in_handler);

        let BlockState {
            entity_cache,
            deterministic_errors,
            created_data_sources,
            persisted_data_sources,
            handler_created_data_sources,
            processed_data_sources,
            in_handler,
            metrics,
        } = self;

        match in_handler {
            true => handler_created_data_sources.extend(other.created_data_sources),
            false => created_data_sources.extend(other.created_data_sources),
        }
        deterministic_errors.extend(other.deterministic_errors);
        entity_cache.extend(other.entity_cache);
        processed_data_sources.extend(other.processed_data_sources);
        persisted_data_sources.extend(other.persisted_data_sources);
        metrics.extend(other.metrics)
    }

    pub fn has_errors(&self) -> bool {
        !self.deterministic_errors.is_empty()
    }

    pub fn has_created_data_sources(&self) -> bool {
        assert!(!self.in_handler);
        !self.created_data_sources.is_empty()
    }

    pub fn has_created_on_chain_data_sources(&self) -> bool {
        assert!(!self.in_handler);
        self.created_data_sources
            .iter()
            .any(|ds| match ds.template {
                InstanceDSTemplate::Onchain(_) => true,
                _ => false,
            })
    }

    pub fn drain_created_data_sources(&mut self) -> Vec<InstanceDSTemplateInfo> {
        assert!(!self.in_handler);
        std::mem::take(&mut self.created_data_sources)
    }

    pub fn enter_handler(&mut self) {
        assert!(!self.in_handler);
        self.in_handler = true;
        self.entity_cache.enter_handler()
    }

    pub fn exit_handler(&mut self) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.created_data_sources
            .append(&mut self.handler_created_data_sources);
        self.entity_cache.exit_handler()
    }

    pub fn exit_handler_and_discard_changes_due_to_error(&mut self, e: SubgraphError) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.handler_created_data_sources.clear();
        self.entity_cache.exit_handler_and_discard_changes();
        self.deterministic_errors.push(e);
    }

    pub fn push_created_data_source(&mut self, ds: InstanceDSTemplateInfo) {
        assert!(self.in_handler);
        self.handler_created_data_sources.push(ds);
    }

    pub fn persist_data_source(&mut self, ds: StoredDynamicDataSource) {
        self.persisted_data_sources.push(ds)
    }
}
