use crate::prelude::*;
use crate::util::lfu_cache::LfuCache;
use crate::{components::store::WritableStore, data::subgraph::schema::SubgraphError};

#[derive(Clone, Debug)]
pub struct DataSourceTemplateInfo {
    pub template: DataSourceTemplate,
    pub params: Vec<String>,
    pub context: Option<DataSourceContext>,
    pub creation_block: BlockNumber,
}

#[derive(Debug)]
pub struct BlockState {
    pub entity_cache: EntityCache,
    pub deterministic_errors: Vec<SubgraphError>,
    created_data_sources: Vec<DataSourceTemplateInfo>,

    // Data sources created in the current handler.
    handler_created_data_sources: Vec<DataSourceTemplateInfo>,

    // Marks whether a handler is currently executing.
    in_handler: bool,
}

impl BlockState {
    pub fn new(
        store: Arc<dyn WritableStore>,
        lfu_cache: LfuCache<EntityKey, Option<Entity>>,
    ) -> Self {
        BlockState {
            entity_cache: EntityCache::with_current(store, lfu_cache),
            deterministic_errors: Vec::new(),
            created_data_sources: Vec::new(),
            handler_created_data_sources: Vec::new(),
            in_handler: false,
        }
    }

    pub fn extend(&mut self, other: BlockState) {
        assert!(!other.in_handler);

        let BlockState {
            entity_cache,
            deterministic_errors,
            created_data_sources,
            handler_created_data_sources,
            in_handler,
        } = self;

        match in_handler {
            true => handler_created_data_sources.extend(other.created_data_sources),
            false => created_data_sources.extend(other.created_data_sources),
        }
        deterministic_errors.extend(other.deterministic_errors);
        entity_cache.extend(other.entity_cache);
    }

    pub fn has_errors(&self) -> bool {
        !self.deterministic_errors.is_empty()
    }

    pub fn has_created_data_sources(&self) -> bool {
        assert!(!self.in_handler);
        !self.created_data_sources.is_empty()
    }

    pub fn drain_created_data_sources(&mut self) -> Vec<DataSourceTemplateInfo> {
        assert!(!self.in_handler);
        std::mem::replace(&mut self.created_data_sources, Vec::new())
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
            .extend(self.handler_created_data_sources.drain(..));
        self.entity_cache.exit_handler()
    }

    pub fn exit_handler_and_discard_changes_due_to_error(&mut self, e: SubgraphError) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.handler_created_data_sources.clear();
        self.entity_cache.exit_handler_and_discard_changes();
        self.deterministic_errors.push(e);
    }

    pub fn push_created_data_source(&mut self, ds: DataSourceTemplateInfo) {
        assert!(self.in_handler);
        self.handler_created_data_sources.push(ds);
    }
}
