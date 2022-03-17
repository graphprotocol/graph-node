use graph::{
    prelude::{Entity, EntityKey},
    util::lfu_cache::LfuCache,
};

pub struct IndexingState {
    pub entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}
