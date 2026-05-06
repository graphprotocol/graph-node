use anyhow::{anyhow, bail};
use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::cheap_clone::CheapClone;
use crate::components::store::write::EntityModification;
use crate::components::store::{self as s, Entity, EntityOperation};
use crate::data::store::{EntityValidationError, Id, IdType, IntoEntityIterator};
use crate::prelude::{CacheWeight, ENV_VARS, StopwatchMetrics};
use crate::schema::{EntityKey, InputSchema};
use crate::util::intern::Error as InternError;
use crate::util::lfu_cache::{EvictStats, LfuCache};

use super::{BlockNumber, DerivedEntityQuery, LoadRelatedRequest, StoreError};

pub type EntityLfuCache = LfuCache<EntityKey, Option<Arc<Entity>>>;

// Number of VIDs that are reserved outside of the generated ones here.
// Currently none is used, but lets reserve a few more.
const RESERVED_VIDS: u32 = 100;

/// Shared generator for VID and entity ID sequences within a block.
///
/// Created once per block and shared (via `Arc`) across all `EntityCache`
/// instances that operate on the same block. This prevents VID collisions
/// when multiple isolated caches (e.g. ipfs.map callbacks, offchain
/// triggers) write entities in the same block.
#[derive(Clone, Debug)]
pub struct SeqGenerator {
    block: BlockNumber,
    vid_seq: Arc<AtomicU32>,
    id_seq: Arc<AtomicU32>,
}

impl CheapClone for SeqGenerator {
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

impl SeqGenerator {
    pub fn new(block: BlockNumber) -> Self {
        SeqGenerator {
            block,
            vid_seq: Arc::new(AtomicU32::new(RESERVED_VIDS)),
            id_seq: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Return the next VID. The VID encodes the block number in the upper
    /// 32 bits and a monotonically increasing sequence in the lower 32
    /// bits.
    pub fn vid(&self) -> i64 {
        let seq = self.vid_seq.fetch_add(1, Ordering::Relaxed);
        ((self.block as i64) << 32) + seq as i64
    }

    /// Generate the next entity ID for the given ID type. The ID encodes
    /// the block number in the upper 32 bits and a monotonically
    /// increasing sequence in the lower 32 bits.
    pub fn id(&self, id_type: IdType) -> anyhow::Result<Id> {
        let seq = self.id_seq.fetch_add(1, Ordering::Relaxed);

        id_type.generate_id(self.block, seq)
    }
}

/// The scope in which the `EntityCache` should perform a `get` operation
pub enum GetScope {
    /// Get from all previously stored entities in the store
    Store,
    /// Get from the entities that have been stored during this block
    InBlock,
}

/// A representation of entity operations that can be accumulated.
#[derive(Debug, Clone)]
enum EntityOp {
    Remove,
    Update(Entity),
    Overwrite(Entity),
}

impl EntityOp {
    fn apply_to<E: Borrow<Entity>>(
        self,
        entity: &Option<E>,
    ) -> Result<Option<Entity>, InternError> {
        use EntityOp::*;
        match (self, entity) {
            (Remove, _) => Ok(None),
            (Overwrite(new), _) | (Update(new), None) => Ok(Some(new)),
            (Update(updates), Some(entity)) => {
                let mut e = entity.borrow().clone();
                e.merge_remove_null_fields(updates)?;
                Ok(Some(e))
            }
        }
    }

    fn accumulate(&mut self, next: EntityOp) {
        use EntityOp::*;
        let update = match next {
            // Remove and Overwrite ignore the current value.
            Remove | Overwrite(_) => {
                *self = next;
                return;
            }
            Update(update) => update,
        };

        // We have an update, apply it.
        match self {
            // This is how `Overwrite` is constructed, by accumulating `Update` onto `Remove`.
            Remove => *self = Overwrite(update),
            Update(current) | Overwrite(current) => current.merge(update),
        }
    }
}

/// A cache for entities from the store that provides the basic functionality
/// needed for the store interactions in the host exports. This struct tracks
/// how entities are modified, and caches all entities looked up from the
/// store. The cache makes sure that
///   (1) no entity appears in more than one operation
///   (2) only entities that will actually be changed from what they
///       are in the store are changed
///
/// It is important for correctness that this struct is newly instantiated
/// at every block using `with_current` to seed the cache.
pub struct EntityCache {
    /// The state of entities in the store. An entry of `None`
    /// means that the entity is not present in the store
    current: LfuCache<EntityKey, Option<Arc<Entity>>>,

    /// The accumulated changes to an entity.
    updates: HashMap<EntityKey, EntityOp>,

    // Updates for a currently executing handler.
    handler_updates: HashMap<EntityKey, EntityOp>,

    // Marks whether updates should go in `handler_updates`.
    in_handler: bool,

    /// The store is only used to read entities.
    pub store: Arc<dyn s::ReadStore>,

    pub schema: InputSchema,

    /// Shared sequence generator for VIDs and entity IDs within a block.
    pub seq_gen: SeqGenerator,
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
    pub entity_lfu_cache: EntityLfuCache,
    pub evict_stats: EvictStats,
}

impl EntityCache {
    pub fn new(store: Arc<dyn s::ReadStore>, seq_gen: SeqGenerator) -> Self {
        Self {
            current: LfuCache::new(),
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            schema: store.input_schema(),
            store,
            seq_gen,
        }
    }

    /// Make a new entity. The entity is not part of the cache
    pub fn make_entity<I: IntoEntityIterator>(
        &self,
        iter: I,
    ) -> Result<Entity, EntityValidationError> {
        self.schema.make_entity(iter)
    }

    pub fn with_current(
        store: Arc<dyn s::ReadStore>,
        current: EntityLfuCache,
        seq_gen: SeqGenerator,
    ) -> EntityCache {
        EntityCache {
            current,
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            schema: store.input_schema(),
            store,
            seq_gen,
        }
    }

    pub fn seq_gen(&self) -> SeqGenerator {
        self.seq_gen.cheap_clone()
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

    pub async fn get(
        &mut self,
        key: &EntityKey,
        scope: GetScope,
    ) -> Result<Option<Arc<Entity>>, StoreError> {
        // Get the current entity, apply any updates from `updates`, then
        // from `handler_updates`.
        let mut entity: Option<Arc<Entity>> = match scope {
            GetScope::Store => {
                if !self.current.contains_key(key) {
                    let entity = self.store.get(key).await?;
                    self.current.insert(key.clone(), entity.map(Arc::new));
                }
                // Unwrap: we just inserted the entity
                self.current.get(key).unwrap().cheap_clone()
            }
            GetScope::InBlock => None,
        };

        // Always test the cache consistency in debug mode. The test only
        // makes sense when we were actually asked to read from the store.
        // We need to remove the VID as the one from the DB  might come from
        // a legacy subgraph that has VID autoincremented while this trait
        // always creates it in a new style.
        debug_assert!(match scope {
            GetScope::Store => {
                entity == self.store.get(key).await.unwrap().map(Arc::new)
            }
            GetScope::InBlock => true,
        });

        if let Some(op) = self.updates.get(key).cloned() {
            entity = op
                .apply_to(&entity)
                .map_err(|e| key.unknown_attribute(e))?
                .map(Arc::new);
        }
        if let Some(op) = self.handler_updates.get(key).cloned() {
            entity = op
                .apply_to(&entity)
                .map_err(|e| key.unknown_attribute(e))?
                .map(Arc::new);
        }
        Ok(entity)
    }

    pub async fn load_related(
        &mut self,
        eref: &LoadRelatedRequest,
    ) -> Result<Vec<Entity>, anyhow::Error> {
        let (entity_type, field) = self.schema.get_field_related(eref)?;

        let query = DerivedEntityQuery {
            entity_type,
            entity_field: field.name.clone(),
            value: eref.entity_id.clone(),
            causality_region: eref.causality_region,
        };

        // Entities that satisfied the query at the start of this block.
        let stored = self.store.get_derived(&query).await?;

        for (key, entity) in &stored {
            if !self.current.contains_key(key) {
                self.current
                    .insert(key.clone(), Some(Arc::new(entity.clone())));
            }
        }

        // Candidate set: keys that were matching at baseline, plus keys
        // any in-block write has touched whose entity_type and causality
        // region are compatible with the query. The latter catches
        // entities an in-block write has moved into the matching set or
        // created fresh in this block.
        let mut candidates: BTreeSet<&EntityKey> = stored.keys().collect();
        for key in self.updates.keys().chain(self.handler_updates.keys()) {
            if key.entity_type == query.entity_type
                && key.causality_region == query.causality_region
            {
                candidates.insert(key);
            }
        }

        let mut result = Vec::new();
        for key in candidates {
            // Resolve the entity's final in-block state by layering
            // store baseline, then self.updates, then self.handler_updates.
            // Each layer's op may mutate, replace, or remove the entity.
            let mut entity: Option<Entity> = stored.get(key).cloned();
            if let Some(op) = self.updates.get(key).cloned() {
                entity = op.apply_to(&entity).map_err(|e| key.unknown_attribute(e))?;
            }
            if let Some(op) = self.handler_updates.get(key).cloned() {
                entity = op.apply_to(&entity).map_err(|e| key.unknown_attribute(e))?;
            }

            // Include the entity only if its final state still matches the query.
            if let Some(entity) = entity
                && query.matches(key, &entity)
            {
                result.push(entity);
            }
        }

        Ok(result)
    }

    pub fn remove(&mut self, key: EntityKey) {
        self.entity_op(key, EntityOp::Remove);
    }

    /// Store the `entity` under the given `key`. The `entity` may be only a
    /// partial entity; the cache will ensure partial updates get merged
    /// with existing data. The entity will be validated against the
    /// subgraph schema, and any errors will result in an `Err` being
    /// returned.
    pub async fn set(
        &mut self,
        key: EntityKey,
        entity: Entity,
        write_capacity_remaining: Option<&mut usize>,
    ) -> Result<(), anyhow::Error> {
        // check the validate for derived fields
        let is_valid = entity.validate(&key).is_ok();

        if let Some(write_capacity_remaining) = write_capacity_remaining {
            let weight = entity.weight();
            if !self.current.contains_key(&key) && weight > *write_capacity_remaining {
                return Err(anyhow!(
                    "exceeded block write limit when writing entity `{}`",
                    key.entity_id,
                ));
            }

            *write_capacity_remaining -= weight;
        }

        let vid = self.seq_gen.vid();
        let mut entity = entity;
        let old_vid = entity.set_vid(vid).expect("the vid should be set");
        // Make sure that there was no VID previously set for this entity.
        if let Some(ovid) = old_vid {
            bail!(
                "VID: {} of entity: {} with ID: {} was already present when set in EntityCache",
                ovid,
                key.entity_type,
                entity.id()
            );
        }

        self.entity_op(key.clone(), EntityOp::Update(entity));

        // The updates we were given are not valid by themselves; force a
        // lookup in the database and check again with an entity that merges
        // the existing entity with the changes
        if !is_valid {
            let entity = self.get(&key, GetScope::Store).await?.ok_or_else(|| {
                anyhow!(
                    "Failed to read entity {}[{}] back from cache",
                    key.entity_type,
                    key.entity_id
                )
            })?;
            entity.validate(&key)?;
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
    pub async fn as_modifications(
        mut self,
        block: BlockNumber,
        stopwatch: &StopwatchMetrics,
    ) -> Result<ModificationsAndCache, StoreError> {
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
        {
            let _section = stopwatch.start_section("as_modifications_load");

            let missing = missing.filter(|key| !key.entity_type.is_immutable());

            for (entity_key, entity) in self.store.get_many(missing.cloned().collect()).await? {
                self.current.insert(entity_key, Some(Arc::new(entity)));
            }
        }

        let mut mods = Vec::new();
        for (key, update) in self.updates {
            use EntityModification::*;

            let current = self.current.remove(&key).and_then(|entity| entity);
            let modification = match (current, update) {
                // Entity was created
                (None, EntityOp::Update(mut updates))
                | (None, EntityOp::Overwrite(mut updates)) => {
                    updates.remove_null_fields();
                    let data = Arc::new(updates);
                    self.current.insert(key.clone(), Some(data.cheap_clone()));
                    Some(Insert {
                        key,
                        data,
                        block,
                        end: None,
                    })
                }
                // Entity may have been changed
                (Some(current), EntityOp::Update(updates)) => {
                    let mut data =
                        Arc::try_unwrap(current).unwrap_or_else(|arc| arc.as_ref().clone());
                    let changed = data
                        .merge_remove_null_fields(updates)
                        .map_err(|e| key.unknown_attribute(e))?;
                    let data = Arc::new(data);
                    self.current.insert(key.clone(), Some(data.cheap_clone()));
                    if changed {
                        Some(Overwrite {
                            key,
                            data,
                            block,
                            end: None,
                        })
                    } else {
                        None
                    }
                }
                // Entity was removed and then updated, so it will be overwritten
                (Some(current), EntityOp::Overwrite(data)) => {
                    let data = Arc::new(data);
                    self.current.insert(key.clone(), Some(data.cheap_clone()));
                    if current != data {
                        Some(Overwrite {
                            key,
                            data,
                            block,
                            end: None,
                        })
                    } else {
                        None
                    }
                }
                // Existing entity was deleted
                (Some(_), EntityOp::Remove) => {
                    self.current.insert(key.clone(), None);
                    Some(Remove { key, block })
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
