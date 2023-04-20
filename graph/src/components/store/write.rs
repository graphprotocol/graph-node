//! Data structures and helpers for writing subgraph changes to the store
use std::collections::HashSet;

use crate::{
    blockchain::{block_stream::FirehoseCursor, BlockPtr},
    cheap_clone::CheapClone,
    components::subgraph::Entity,
    data::subgraph::schema::SubgraphError,
    prelude::DeploymentHash,
};

use super::{
    BlockNumber, EntityKey, EntityModification, EntityType, StoreEvent, StoredDynamicDataSource,
};

/// The data for a write operation; a write operation is either an insert of
/// a new entity or the overwriting of an existing entity.
#[derive(Debug)]
pub struct EntityWrite {
    pub key: EntityKey,
    pub data: Entity,
    pub block: BlockNumber,
}

impl EntityWrite {
    pub fn new(key: EntityKey, data: Entity, block: BlockNumber) -> Self {
        Self { key, data, block }
    }
}

/// A list of entity changes grouped by the entity type
pub struct RowGroup<R> {
    pub entity_type: EntityType,
    pub rows: Vec<R>,
}

impl<R> RowGroup<R> {
    pub fn new(entity_type: EntityType) -> Self {
        Self {
            entity_type,
            rows: Vec::new(),
        }
    }

    pub fn push(&mut self, row: R) {
        self.rows.push(row)
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// A list of entity changes with one group per entity type
pub struct Sheet<R> {
    pub groups: Vec<RowGroup<R>>,
}

impl<R> Sheet<R> {
    fn new() -> Self {
        Self { groups: Vec::new() }
    }

    fn group(&self, entity_type: &EntityType) -> Option<&RowGroup<R>> {
        self.groups
            .iter()
            .find(|group| &group.entity_type == entity_type)
    }

    /// Return a mutable reference to an existing group.
    fn group_mut(&mut self, entity_type: &EntityType) -> Option<&mut RowGroup<R>> {
        self.groups
            .iter_mut()
            .find(|group| &group.entity_type == entity_type)
    }

    /// Return a mutable reference to an existing group, or create a new one
    /// if there isn't one yet and return a reference to that
    fn group_entry(&mut self, entity_type: &EntityType) -> &mut RowGroup<R> {
        let pos = self
            .groups
            .iter()
            .position(|group| &group.entity_type == entity_type);
        match pos {
            Some(pos) => &mut self.groups[pos],
            None => {
                self.groups.push(RowGroup::new(entity_type.clone()));
                // unwrap: we just pushed an entry
                self.groups.last_mut().unwrap()
            }
        }
    }

    fn entity_count(&self) -> usize {
        self.groups.iter().map(|group| group.row_count()).sum()
    }
}

// Arbitrary data grouped by block
pub struct BlockEntries<E> {
    pub entries: Vec<(BlockPtr, E)>,
}

impl<E> BlockEntries<E> {
    fn new(ptr: BlockPtr, entries: E) -> Self {
        BlockEntries {
            entries: vec![(ptr, entries)],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub type DataSources = BlockEntries<Vec<StoredDynamicDataSource>>;

pub enum EntityOp<'a> {
    Write(&'a Entity),
    Remove,
}

/// A write batch. This data structure encapsulates all the things that need
/// to be changed to persist the output of mappings up to a certain block.
/// For now, a batch will only contain changes for a single block, but will
/// eventually contain data for multiple blocks.
pub struct Batch {
    /// The last block for which this batch contains changes
    pub block_ptr: BlockPtr,
    /// The firehose cursor corresponding to `block_ptr`
    pub firehose_cursor: FirehoseCursor,
    /// New entities that need to be inserted
    pub inserts: Sheet<EntityWrite>,
    /// Existing entities that need to be modified
    pub overwrites: Sheet<EntityWrite>,
    /// Existing entities that need to be removed
    pub removes: Sheet<EntityKey>,
    /// New data sources
    pub data_sources: DataSources,
    pub deterministic_errors: Vec<SubgraphError>,
    pub offchain_to_remove: DataSources,
}

impl Batch {
    pub fn new(
        block_ptr: BlockPtr,
        firehose_cursor: FirehoseCursor,
        mods: Vec<EntityModification>,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
        offchain_to_remove: Vec<StoredDynamicDataSource>,
    ) -> Self {
        let block = block_ptr.number;

        let mut inserts = Sheet::new();
        let mut overwrites = Sheet::new();
        let mut removes = Sheet::new();

        for m in mods {
            match m {
                EntityModification::Insert { key, data } => {
                    let row = EntityWrite::new(key, data, block);
                    inserts.group_entry(&row.key.entity_type).push(row);
                }
                EntityModification::Overwrite { key, data } => {
                    let row = EntityWrite::new(key, data, block);
                    overwrites.group_entry(&row.key.entity_type).push(row);
                }
                EntityModification::Remove { key } => {
                    removes.group_entry(&key.entity_type).push(key)
                }
            }
        }

        let data_sources = DataSources::new(block_ptr.cheap_clone(), data_sources);
        let offchain_to_remove = DataSources::new(block_ptr.cheap_clone(), offchain_to_remove);
        Self {
            block_ptr,
            firehose_cursor,
            inserts,
            overwrites,
            removes,
            data_sources,
            deterministic_errors,
            offchain_to_remove,
        }
    }

    pub fn entity_count(&self) -> usize {
        self.inserts.entity_count() + self.overwrites.entity_count() + self.removes.entity_count()
    }

    /// Find out whether the latest operation for the entity with type
    /// `entity_type` and `id` is going to write that entity, i.e., insert
    /// or overwrite it, or if it is going to remove it. If no change will
    /// be made to the entity, return `None`
    pub fn last_op(&self, entity_type: &EntityType, id: &str) -> Option<EntityOp<'_>> {
        // Check if we are inserting or overwriting the entity
        if let Some((_, entity)) = self
            .writes(entity_type)
            .find(|(_, entity)| entity.id() == id)
        {
            return Some(EntityOp::Write(entity));
        }
        self.removes(entity_type)
            .find(|key| key.entity_id.as_str() == id)
            .map(|_| EntityOp::Remove)
    }

    /// Iterate over all entities that have a pending write
    pub fn writes(&self, entity_type: &EntityType) -> impl Iterator<Item = (&EntityKey, &Entity)> {
        self.inserts
            .group(entity_type)
            .into_iter()
            .map(|ew| &ew.rows)
            .flatten()
            .chain(
                self.overwrites
                    .group(entity_type)
                    .into_iter()
                    .map(|ew| &ew.rows)
                    .flatten(),
            )
            .map(|ew| (&ew.key, &ew.data))
    }

    /// Iterate over all entities that have a pending write, allowing for
    /// mutation of the entity
    pub fn writes_mut(
        &mut self,
        entity_type: &EntityType,
    ) -> impl Iterator<Item = (&EntityKey, &mut Entity)> {
        self.inserts
            .group_mut(entity_type)
            .into_iter()
            .map(|rg| &mut rg.rows)
            .flatten()
            .chain(
                self.overwrites
                    .group_mut(entity_type)
                    .into_iter()
                    .map(|rg| &mut rg.rows)
                    .flatten(),
            )
            .map(|ew| (&ew.key, &mut ew.data))
    }

    /// Iterate over all entity deletions/removals
    pub fn removes(&self, entity_type: &EntityType) -> impl Iterator<Item = &EntityKey> {
        self.removes
            .group(entity_type)
            .into_iter()
            .map(|rg| &rg.rows)
            .flatten()
    }

    pub fn new_data_sources(&self) -> impl Iterator<Item = &StoredDynamicDataSource> {
        self.data_sources
            .entries
            .iter()
            .map(|(_, ds)| ds)
            .flatten()
            .filter(|ds| {
                !self
                    .offchain_to_remove
                    .entries
                    .iter()
                    .any(|(_, entries)| entries.contains(ds))
            })
    }

    /// Generate a store event for all the changes that this batch makes
    pub fn store_event(&self, deployment: &DeploymentHash) -> StoreEvent {
        let entity_types = HashSet::from_iter(
            self.inserts
                .groups
                .iter()
                .chain(self.overwrites.groups.iter())
                .map(|group| group.entity_type.clone()),
        );
        StoreEvent::from_types(deployment, entity_types)
    }
}
