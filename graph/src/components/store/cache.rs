use anyhow::anyhow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug};
use std::sync::Arc;

use crate::blockchain::BlockPtr;
use crate::components::store::{
    self as s, Entity, EntityKey, EntityOp, EntityOperation, EntityType,
};
use crate::prelude::ENV_VARS;
use crate::util::lfu_cache::LfuCache;

/// A cache for entities from the store that provides the basic functionality
/// needed for the store interactions in the host exports. This struct tracks
/// how entities are modified, and caches all entities looked up from the
/// store. The cache makes sure that
///   (1) no entity appears in more than one operation
///   (2) only entities that will actually be changed from what they
///       are in the store are changed
pub struct EntityCache {
    /// The state of entities in the store. An entry of `None`
    /// means that the entity is not present in the store
    current: LfuCache<EntityKey, Option<Entity>>,

    /// The accumulated changes to an entity.
    updates: HashMap<EntityKey, EntityOp>,

    // Updates for a currently executing handler.
    handler_updates: HashMap<EntityKey, EntityOp>,

    // Marks whether updates should go in `handler_updates`.
    in_handler: bool,

    data_sources: Vec<s::StoredDynamicDataSource>,

    /// The store is only used to read entities.
    pub store: Arc<dyn s::WritableStore>,
}

impl Debug for EntityCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EntityCache")
            .field("current", &self.current)
            .field("updates", &self.updates)
            .finish()
    }
}

pub struct ModificationsAndCache {
    pub modifications: Vec<s::EntityModification>,
    pub data_sources: Vec<s::StoredDynamicDataSource>,
    pub entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}

impl EntityCache {
    pub fn new(store: Arc<dyn s::WritableStore>) -> Self {
        Self {
            current: LfuCache::new(),
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            data_sources: vec![],
            store,
        }
    }

    pub fn with_current(
        store: Arc<dyn s::WritableStore>,
        current: LfuCache<EntityKey, Option<Entity>>,
    ) -> EntityCache {
        EntityCache {
            current,
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            data_sources: vec![],
            store,
        }
    }

    pub(crate) fn enter_handler(&mut self) {
        assert!(!self.in_handler);
        self.in_handler = true;
    }

    pub(crate) fn exit_handler(&mut self) {
        assert!(self.in_handler);
        self.in_handler = false;

        // Apply all handler updates to the main `updates`.
        let handler_updates = Vec::from_iter(self.handler_updates.drain());
        for (key, op) in handler_updates {
            self.entity_op(key, op)
        }
    }

    pub(crate) fn exit_handler_and_discard_changes(&mut self) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.handler_updates.clear();
    }

    pub fn get(&mut self, key: &EntityKey) -> Result<Option<Entity>, s::QueryExecutionError> {
        // Get the current entity, apply any updates from `updates`, then
        // from `handler_updates`.
        let mut entity = self.current.get_entity(&*self.store, key)?;
        if let Some(op) = self.updates.get(key).cloned() {
            entity = op.apply_to(entity)
        }
        if let Some(op) = self.handler_updates.get(key).cloned() {
            entity = op.apply_to(entity)
        }
        Ok(entity)
    }

    pub fn remove(&mut self, key: EntityKey) {
        self.entity_op(key, EntityOp::Remove);
    }

    /// Store the `entity` under the given `key`. The `entity` may be only a
    /// partial entity; the cache will ensure partial updates get merged
    /// with existing data. The entity will be validated against the
    /// subgraph schema, and any errors will result in an `Err` being
    /// returned.
    pub fn set(&mut self, key: EntityKey, mut entity: Entity) -> Result<(), anyhow::Error> {
        fn check_id(key: &EntityKey, prev_id: &str) -> Result<(), anyhow::Error> {
            if prev_id != key.entity_id {
                return Err(anyhow!(
                    "Value of {} attribute 'id' conflicts with ID passed to `store.set()`: \
                {} != {}",
                    key.entity_type,
                    prev_id,
                    key.entity_id,
                ));
            } else {
                Ok(())
            }
        }

        // Set the id if there isn't one yet, and make sure that a
        // previously set id agrees with the one in the `key`
        match entity.get("id") {
            Some(s::Value::String(s)) => check_id(&key, s)?,
            Some(s::Value::Bytes(b)) => check_id(&key, &b.to_string())?,
            Some(_) => {
                // The validation will catch the type mismatch
            }
            None => {
                let value = self.store.input_schema().id_value(&key)?;
                entity.set("id", value);
            }
        }

        let is_valid = entity.validate(&self.store.input_schema(), &key).is_ok();

        self.entity_op(key.clone(), EntityOp::Update(entity));

        // The updates we were given are not valid by themselves; force a
        // lookup in the database and check again with an entity that merges
        // the existing entity with the changes
        if !is_valid {
            let entity = self.get(&key)?.ok_or_else(|| {
                anyhow!(
                    "Failed to read entity {}[{}] back from cache",
                    key.entity_type,
                    key.entity_id
                )
            })?;
            entity.validate(&self.store.input_schema(), &key)?;
        }

        Ok(())
    }

    pub fn append(&mut self, operations: Vec<EntityOperation>) {
        assert!(!self.in_handler);

        for operation in operations {
            match operation {
                EntityOperation::Set { key, data } => {
                    self.entity_op(key, EntityOp::Update(data));
                }
                EntityOperation::Remove { key } => {
                    self.entity_op(key, EntityOp::Remove);
                }
            }
        }
    }

    /// Add a dynamic data source
    pub fn add_data_source<C: s::Blockchain>(&mut self, data_source: &impl s::DataSource<C>) {
        self.data_sources
            .push(data_source.as_stored_dynamic_data_source());
    }

    fn entity_op(&mut self, key: EntityKey, op: EntityOp) {
        use std::collections::hash_map::Entry;
        let updates = match self.in_handler {
            true => &mut self.handler_updates,
            false => &mut self.updates,
        };

        match updates.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(op);
            }
            Entry::Occupied(mut entry) => entry.get_mut().accumulate(op),
        }
    }

    pub(crate) fn extend(&mut self, other: EntityCache) {
        assert!(!other.in_handler);

        self.current.extend(other.current);
        for (key, op) in other.updates {
            self.entity_op(key, op);
        }
    }

    /// Return the changes that have been made via `set` and `remove` as
    /// `EntityModification`, making sure to only produce one when a change
    /// to the current state is actually needed.
    ///
    /// Also returns the updated `LfuCache`.
    pub fn as_modifications(mut self) -> Result<ModificationsAndCache, s::QueryExecutionError> {
        assert!(!self.in_handler);

        // The first step is to make sure all entities being set are in `self.current`.
        // For each subgraph, we need a map of entity type to missing entity ids.
        let missing = self
            .updates
            .keys()
            .filter(|key| !self.current.contains_key(key));

        // For immutable types, we assume that the subgraph is well-behaved,
        // and all updated immutable entities are in fact new, and skip
        // looking them up in the store. That ultimately always leads to an
        // `Insert` modification for immutable entities; if the assumption
        // is wrong and the store already has a version of the entity from a
        // previous block, the attempt to insert will trigger a constraint
        // violation in the database, ensuring correctness
        let missing =
            missing.filter(|key| !self.store.input_schema().is_immutable(&key.entity_type));

        let mut missing_by_subgraph: BTreeMap<_, BTreeMap<&EntityType, Vec<&str>>> =
            BTreeMap::new();
        for key in missing {
            missing_by_subgraph
                .entry(&key.subgraph_id)
                .or_default()
                .entry(&key.entity_type)
                .or_default()
                .push(&key.entity_id);
        }

        for (subgraph_id, keys) in missing_by_subgraph {
            for (entity_type, entities) in self.store.get_many(keys)? {
                for entity in entities {
                    let key = EntityKey {
                        subgraph_id: subgraph_id.clone(),
                        entity_type: entity_type.clone(),
                        entity_id: entity.id().unwrap(),
                    };
                    self.current.insert(key, Some(entity));
                }
            }
        }

        let mut mods = Vec::new();
        for (key, update) in self.updates {
            use s::EntityModification::*;

            let current = self.current.remove(&key).and_then(|entity| entity);
            let modification = match (current, update) {
                // Entity was created
                (None, EntityOp::Update(updates)) | (None, EntityOp::Overwrite(updates)) => {
                    // Merging with an empty entity removes null fields.
                    let mut data = Entity::new();
                    data.merge_remove_null_fields(updates);
                    self.current.insert(key.clone(), Some(data.clone()));
                    Some(Insert { key, data })
                }
                // Entity may have been changed
                (Some(current), EntityOp::Update(updates)) => {
                    let mut data = current.clone();
                    data.merge_remove_null_fields(updates);
                    self.current.insert(key.clone(), Some(data.clone()));
                    if current != data {
                        Some(Overwrite { key, data })
                    } else {
                        None
                    }
                }
                // Entity was removed and then updated, so it will be overwritten
                (Some(current), EntityOp::Overwrite(data)) => {
                    self.current.insert(key.clone(), Some(data.clone()));
                    if current != data {
                        Some(Overwrite { key, data })
                    } else {
                        None
                    }
                }
                // Existing entity was deleted
                (Some(_), EntityOp::Remove) => {
                    self.current.insert(key.clone(), None);
                    Some(Remove { key })
                }
                // Entity was deleted, but it doesn't exist in the store
                (None, EntityOp::Remove) => None,
            };
            if let Some(modification) = modification {
                mods.push(modification)
            }
        }
        self.current.evict(ENV_VARS.mappings.entity_cache_size);

        Ok(ModificationsAndCache {
            modifications: mods,
            data_sources: self.data_sources,
            entity_lfu_cache: self.current,
        })
    }
}

impl LfuCache<EntityKey, Option<Entity>> {
    // Helper for cached lookup of an entity.
    fn get_entity(
        &mut self,
        store: &(impl s::WritableStore + ?Sized),
        key: &EntityKey,
    ) -> Result<Option<Entity>, s::QueryExecutionError> {
        match self.get(key) {
            None => {
                let mut entity = store.get(key)?;
                if let Some(entity) = &mut entity {
                    // `__typename` is for queries not for mappings.
                    entity.remove("__typename");
                }
                self.insert(key.clone(), entity.clone());
                Ok(entity)
            }
            Some(data) => Ok(data.to_owned()),
        }
    }
}

/// Represents an item retrieved from an
/// [`EthereumCallCache`](super::EthereumCallCache) implementor.
pub struct CachedEthereumCall {
    /// The BLAKE3 hash that uniquely represents this cache item. The way this
    /// hash is constructed is an implementation detail.
    pub blake3_id: Vec<u8>,

    /// Block details related to this Ethereum call.
    pub block_ptr: BlockPtr,

    /// The address to the called contract.
    pub contract_address: ethabi::Address,

    /// The encoded return value of this call.
    pub return_value: Vec<u8>,
}
