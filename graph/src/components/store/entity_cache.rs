use anyhow::anyhow;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use crate::components::store::{self as s, Entity, EntityKey, EntityOp, EntityOperation};
use crate::data::store::IntoEntityIterator;
use crate::prelude::ENV_VARS;
use crate::schema::InputSchema;
use crate::util::lfu_cache::{EvictStats, LfuCache};

use super::{DerivedEntityQuery, EntityType, LoadRelatedRequest, StoreError};

/// The scope in which the `EntityCache` should perform a `get` operation
pub enum GetScope {
    /// Get from all previously stored entities in the store
    Store,
    /// Get from the entities that have been stored during this block
    InBlock,
}

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

    /// The store is only used to read entities.
    pub store: Arc<dyn s::ReadStore>,

    pub schema: Arc<InputSchema>,
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
    pub entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
    pub evict_stats: EvictStats,
}

impl EntityCache {
    pub fn new(store: Arc<dyn s::ReadStore>) -> Self {
        Self {
            current: LfuCache::new(),
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            schema: store.input_schema(),
            store,
        }
    }

    /// Make a new entity. The entity is not part of the cache
    pub fn make_entity<I: IntoEntityIterator>(&self, iter: I) -> Result<Entity, anyhow::Error> {
        self.schema.make_entity(iter)
    }

    pub fn with_current(
        store: Arc<dyn s::ReadStore>,
        current: LfuCache<EntityKey, Option<Entity>>,
    ) -> EntityCache {
        EntityCache {
            current,
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            schema: store.input_schema(),
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

    pub fn get(&mut self, key: &EntityKey, scope: GetScope) -> Result<Option<Entity>, StoreError> {
        // Get the current entity, apply any updates from `updates`, then
        // from `handler_updates`.
        let mut entity = match scope {
            GetScope::Store => self.current.get_entity(&*self.store, key)?,
            GetScope::InBlock => None,
        };

        // Always test the cache consistency in debug mode. The test only
        // makes sense when we were actually asked to read from the store
        debug_assert!(match scope {
            GetScope::Store => entity == self.store.get(key).unwrap(),
            GetScope::InBlock => true,
        });

        if let Some(op) = self.updates.get(key).cloned() {
            entity = op.apply_to(entity).map_err(|e| key.unknown_attribute(e))?;
        }
        if let Some(op) = self.handler_updates.get(key).cloned() {
            entity = op.apply_to(entity).map_err(|e| key.unknown_attribute(e))?;
        }
        Ok(entity)
    }

    pub fn load_related(
        &mut self,
        eref: &LoadRelatedRequest,
    ) -> Result<Vec<Entity>, anyhow::Error> {
        let (base_type, field) = self.schema.get_field_related(eref)?;

        let query = DerivedEntityQuery {
            entity_type: EntityType::new(base_type.to_string()),
            entity_field: field.name.clone().into(),
            value: eref.entity_id.clone(),
            causality_region: eref.causality_region,
        };

        let entities = self.store.get_derived(&query)?;
        entities.iter().for_each(|(key, e)| {
            self.current.insert(key.clone(), Some(e.clone()));
        });
        let entities: Vec<Entity> = entities.values().cloned().collect();
        Ok(entities)
    }

    pub fn remove(&mut self, key: EntityKey) {
        self.entity_op(key, EntityOp::Remove);
    }

    /// Store the `entity` under the given `key`. The `entity` may be only a
    /// partial entity; the cache will ensure partial updates get merged
    /// with existing data. The entity will be validated against the
    /// subgraph schema, and any errors will result in an `Err` being
    /// returned.
    pub fn set(&mut self, key: EntityKey, entity: Entity) -> Result<(), anyhow::Error> {
        // check the validate for derived fields
        let is_valid = entity.validate(&self.schema, &key).is_ok();

        self.entity_op(key.clone(), EntityOp::Update(entity));

        // The updates we were given are not valid by themselves; force a
        // lookup in the database and check again with an entity that merges
        // the existing entity with the changes
        if !is_valid {
            let entity = self.get(&key, GetScope::Store)?.ok_or_else(|| {
                anyhow!(
                    "Failed to read entity {}[{}] back from cache",
                    key.entity_type,
                    key.entity_id
                )
            })?;
            entity.validate(&self.schema, &key)?;
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
    pub fn as_modifications(mut self) -> Result<ModificationsAndCache, StoreError> {
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
        let missing = missing.filter(|key| !self.schema.is_immutable(&key.entity_type));

        for (entity_key, entity) in self.store.get_many(missing.cloned().collect())? {
            self.current.insert(entity_key, Some(entity));
        }

        let mut mods = Vec::new();
        for (key, update) in self.updates {
            use s::EntityModification::*;

            let current = self.current.remove(&key).and_then(|entity| entity);
            let modification = match (current, update) {
                // Entity was created
                (None, EntityOp::Update(mut updates))
                | (None, EntityOp::Overwrite(mut updates)) => {
                    updates.remove_null_fields();
                    self.current.insert(key.clone(), Some(updates.clone()));
                    Some(Insert { key, data: updates })
                }
                // Entity may have been changed
                (Some(current), EntityOp::Update(updates)) => {
                    let mut data = current.clone();
                    data.merge_remove_null_fields(updates)
                        .map_err(|e| key.unknown_attribute(e))?;
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
        let evict_stats = self
            .current
            .evict_and_stats(ENV_VARS.mappings.entity_cache_size);

        Ok(ModificationsAndCache {
            modifications: mods,
            entity_lfu_cache: self.current,
            evict_stats,
        })
    }
}

impl LfuCache<EntityKey, Option<Entity>> {
    // Helper for cached lookup of an entity.
    fn get_entity(
        &mut self,
        store: &(impl s::ReadStore + ?Sized),
        key: &EntityKey,
    ) -> Result<Option<Entity>, s::QueryExecutionError> {
        match self.get(key) {
            None => {
                let entity = store.get(key)?;
                self.insert(key.clone(), entity.clone());
                Ok(entity)
            }
            Some(data) => Ok(data.clone()),
        }
    }
}
