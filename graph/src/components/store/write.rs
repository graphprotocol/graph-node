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

/// Trait for something that has a block number associated with it
pub trait BlockTagged {
    fn block(&self) -> BlockNumber;
}

#[derive(Debug)]
/// The data for a write operation; a write operation is either an insert of
/// a new entity or the overwriting of an existing entity.
/// A helper for objects that are tagged with a block number
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

impl BlockTagged for EntityWrite {
    fn block(&self) -> BlockNumber {
        self.block
    }
}

pub struct EntityRef {
    pub key: EntityKey,
    pub block: BlockNumber,
}

impl EntityRef {
    pub fn new(key: EntityKey, block: BlockNumber) -> Self {
        Self { key, block }
    }
}

impl BlockTagged for EntityRef {
    fn block(&self) -> BlockNumber {
        self.block
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

impl<R: BlockTagged> RowGroup<R> {
    pub fn runs(&self) -> impl Iterator<Item = (BlockNumber, &[R])> {
        RunIterator::new(self)
    }
}

struct RunIterator<'a, R> {
    position: usize,
    rows: &'a [R],
}

impl<'a, R> RunIterator<'a, R> {
    fn new(group: &'a RowGroup<R>) -> Self {
        RunIterator {
            position: 0,
            rows: &group.rows,
        }
    }
}

impl<'a, R: BlockTagged> Iterator for RunIterator<'a, R> {
    type Item = (BlockNumber, &'a [R]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.rows.len() {
            return None;
        }
        let block = self.rows[self.position].block();
        let mut next = self.position;
        while next < self.rows.len() && self.rows[next].block() == block {
            next += 1;
        }
        let res = Some((block, &self.rows[self.position..next]));
        self.position = next;
        res
    }
}

/// A list of entity changes with one group per entity type
pub struct RowGroups<R> {
    pub groups: Vec<RowGroup<R>>,
}

impl<R> RowGroups<R> {
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

/// Data sources data grouped by block
pub struct DataSources {
    pub entries: Vec<(BlockPtr, Vec<StoredDynamicDataSource>)>,
}

impl DataSources {
    fn new(ptr: BlockPtr, entries: Vec<StoredDynamicDataSource>) -> Self {
        let entries = if entries.is_empty() {
            Vec::new()
        } else {
            vec![(ptr, entries)]
        };
        DataSources { entries }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(|(_, dss)| dss.is_empty())
    }
}

/// Indicate to code that looks up entities from the in-memory batch whether
/// the entity in question will be written or removed at the block of the
/// lookup
pub enum EntityOp<'a> {
    /// There is a new version of the entity that will be written
    Write(&'a Entity),
    /// The entity has been removed
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
    pub inserts: RowGroups<EntityWrite>,
    /// Existing entities that need to be modified
    pub overwrites: RowGroups<EntityWrite>,
    /// Existing entities that need to be removed
    pub removes: RowGroups<EntityRef>,
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

        let mut inserts = RowGroups::new();
        let mut overwrites = RowGroups::new();
        let mut removes = RowGroups::new();

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
                    let row = EntityRef::new(key, block);
                    removes.group_entry(&row.key.entity_type).push(row)
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
            .find(|eref| eref.key.entity_id.as_str() == id)
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
    pub fn removes(&self, entity_type: &EntityType) -> impl Iterator<Item = &EntityRef> {
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

#[cfg(test)]
mod test {
    use crate::components::store::{BlockNumber, EntityType};

    use super::{BlockTagged, RowGroup};

    #[derive(Debug)]
    struct Entry {
        value: usize,
        block: BlockNumber,
    }

    impl BlockTagged for Entry {
        fn block(&self) -> BlockNumber {
            self.block
        }
    }

    #[track_caller]
    fn check_runs(values: &[usize], blocks: &[BlockNumber], exp: &[(BlockNumber, &[usize])]) {
        assert_eq!(values.len(), blocks.len());

        let rows = values
            .iter()
            .zip(blocks.iter())
            .map(|(value, block)| Entry {
                value: *value,
                block: *block,
            })
            .collect();
        let group = RowGroup {
            entity_type: EntityType::new("Entry".to_string()),
            rows,
        };
        let act = group
            .runs()
            .map(|(block, entries)| {
                (
                    block,
                    entries.iter().map(|entry| entry.value).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let exp = Vec::from_iter(
            exp.into_iter()
                .map(|(block, values)| (*block, Vec::from_iter(values.iter().cloned()))),
        );
        assert_eq!(exp, act);
    }

    #[test]
    fn run_iterator() {
        type RunList<'a> = &'a [(i32, &'a [usize])];

        let exp: RunList<'_> = &[(1, &[10, 11, 12])];
        check_runs(&[10, 11, 12], &[1, 1, 1], exp);

        let exp: RunList<'_> = &[(1, &[10, 11, 12]), (2, &[20, 21])];
        check_runs(&[10, 11, 12, 20, 21], &[1, 1, 1, 2, 2], exp);

        let exp: RunList<'_> = &[(1, &[10]), (2, &[20]), (1, &[11])];
        check_runs(&[10, 20, 11], &[1, 2, 1], exp);
    }
}
