use crate::{
    blockchain::Blockchain,
    components::store::{EntityKey, ReadStore, StoredDynamicDataSource},
    data::subgraph::schema::SubgraphError,
    data_source::DataSourceTemplate,
    prelude::*,
    util::lfu_cache::LfuCache,
};

#[derive(Clone, Debug)]
pub struct DataSourceTemplateInfo<C: Blockchain> {
    pub template: DataSourceTemplate<C>,
    pub params: Vec<String>,
    pub context: Option<DataSourceContext>,
    pub creation_block: BlockNumber,
}

#[derive(Debug)]
pub struct BlockState<C: Blockchain> {
    pub entity_cache: EntityCache,
    pub deterministic_errors: Vec<SubgraphError>,
    created_data_sources: Vec<DataSourceTemplateInfo<C>>,

    // Data sources created in the current handler.
    handler_created_data_sources: Vec<DataSourceTemplateInfo<C>>,

    // offchain data sources to be removed because they've been processed.
    pub offchain_to_remove: Vec<StoredDynamicDataSource>,

    // Marks whether a handler is currently executing.
    in_handler: bool,
}

impl<C: Blockchain> BlockState<C> {
    pub fn new(store: impl ReadStore, lfu_cache: LfuCache<EntityKey, Option<Entity>>) -> Self {
        BlockState {
            entity_cache: EntityCache::with_current(Arc::new(store), lfu_cache),
            deterministic_errors: Vec::new(),
            created_data_sources: Vec::new(),
            handler_created_data_sources: Vec::new(),
            offchain_to_remove: Vec::new(),
            in_handler: false,
        }
    }

    pub fn extend(&mut self, other: BlockState<C>) {
        assert!(!other.in_handler);

        let BlockState {
            entity_cache,
            deterministic_errors,
            created_data_sources,
            handler_created_data_sources,
            offchain_to_remove,
            in_handler,
        } = self;

        match in_handler {
            true => handler_created_data_sources.extend(other.created_data_sources),
            false => created_data_sources.extend(other.created_data_sources),
        }
        deterministic_errors.extend(other.deterministic_errors);
        entity_cache.extend(other.entity_cache);
        offchain_to_remove.extend(other.offchain_to_remove);
    }

    pub fn has_errors(&self) -> bool {
        !self.deterministic_errors.is_empty()
    }

    pub fn has_created_data_sources(&self) -> bool {
        assert!(!self.in_handler);
        !self.created_data_sources.is_empty()
    }

    pub fn drain_created_data_sources(&mut self) -> Vec<DataSourceTemplateInfo<C>> {
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

    pub fn push_created_data_source(&mut self, ds: DataSourceTemplateInfo<C>) {
        assert!(self.in_handler);
        self.handler_created_data_sources.push(ds);
    }
}
