use crate::{
    blockchain::{Blockchain, DataSourceTemplate as _},
    components::store::{EntityLfuCache, ReadStore, StoredDynamicDataSource},
    data::subgraph::schema::SubgraphError,
    data_source::{DataSourceTemplate, DataSourceTemplateInfo},
    prelude::*,
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
        } = self;

        match in_handler {
            true => handler_created_data_sources.extend(other.created_data_sources),
            false => created_data_sources.extend(other.created_data_sources),
        }
        deterministic_errors.extend(other.deterministic_errors);
        entity_cache.extend(other.entity_cache);
        processed_data_sources.extend(other.processed_data_sources);
        persisted_data_sources.extend(other.persisted_data_sources);
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
