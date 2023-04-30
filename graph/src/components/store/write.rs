//! Data structures and helpers for writing subgraph changes to the store
use std::collections::{HashMap, HashSet};

use crate::{
    blockchain::{block_stream::FirehoseCursor, BlockPtr},
    cheap_clone::CheapClone,
    components::subgraph::Entity,
    constraint_violation,
    data::{subgraph::schema::SubgraphError, value::Word},
    data_source::CausalityRegion,
    prelude::DeploymentHash,
    util::cache_weight::CacheWeight,
};

use super::{
    BlockNumber, EntityKey, EntityModification, EntityType, StoreError, StoreEvent,
    StoredDynamicDataSource,
};

/// A data structure similar to `EntityModification`, but tagged with a
/// block. We might eventually replace `EntityModification` with this, but
/// until the dust settles, we'll keep them separate.
///
/// This is geared towards how we persist entity changes: there are only
/// ever two operations we perform on them, clamping the range of an
/// existing entity version, and writing a new entity version.
///
/// The difference between `Insert` and `Overwrite` is that `Overwrite`
/// requires that we clamp an existing prior version of the entity at
/// `block`. We only ever get an `Overwrite` if such a version actually
/// exists. `Insert` simply inserts a new row into the underlying table,
/// assuming that there is no need to fix up any prior version.
#[derive(Debug)]
pub enum EntityMod {
    /// Insert the entity
    Insert {
        key: EntityKey,
        data: Entity,
        block: BlockNumber,
        end: Option<BlockNumber>,
    },
    /// Update the entity by overwriting it
    Overwrite {
        key: EntityKey,
        data: Entity,
        block: BlockNumber,
        end: Option<BlockNumber>,
    },
    /// Remove the entity
    Remove { key: EntityKey, block: BlockNumber },
}

/// A helper struct for passing entity writes to the outside world, viz. the
/// SQL query generation that inserts rows
pub struct EntityWrite<'a> {
    pub id: &'a Word,
    pub entity: &'a Entity,
    pub causality_region: CausalityRegion,
    pub block: BlockNumber,
    // The end of the block range for which this write is valid. The value
    // of `end` itself is not included in the range
    pub end: Option<BlockNumber>,
}

impl<'a> TryFrom<&'a EntityMod> for EntityWrite<'a> {
    type Error = ();

    fn try_from(emod: &'a EntityMod) -> Result<Self, Self::Error> {
        match emod {
            EntityMod::Insert {
                key,
                data,
                block,
                end,
            }
            | EntityMod::Overwrite {
                key,
                data,
                block,
                end,
            } => Ok(EntityWrite {
                id: &key.entity_id,
                entity: data,
                causality_region: key.causality_region,
                block: *block,
                end: *end,
            }),

            EntityMod::Remove { .. } => Err(()),
        }
    }
}

impl EntityMod {
    fn new(m: EntityModification, block: BlockNumber) -> Self {
        match m {
            EntityModification::Insert { key, data } => Self::Insert {
                key,
                data,
                block,
                end: None,
            },
            EntityModification::Overwrite { key, data } => Self::Overwrite {
                key,
                data,
                block,
                end: None,
            },
            EntityModification::Remove { key } => Self::Remove { key, block },
        }
    }

    #[cfg(debug_assertions)]
    pub fn new_test(m: EntityModification, block: BlockNumber) -> Self {
        Self::new(m, block)
    }

    pub fn id(&self) -> &Word {
        match self {
            EntityMod::Insert { key, .. }
            | EntityMod::Overwrite { key, .. }
            | EntityMod::Remove { key, .. } => &key.entity_id,
        }
    }

    fn block(&self) -> BlockNumber {
        match self {
            EntityMod::Insert { block, .. }
            | EntityMod::Overwrite { block, .. }
            | EntityMod::Remove { block, .. } => *block,
        }
    }

    /// Return `true` if `self` requires a write operation, i.e.,insert of a
    /// new row, for either a new or an existing entity
    fn is_write(&self) -> bool {
        match self {
            EntityMod::Insert { .. } | EntityMod::Overwrite { .. } => true,
            EntityMod::Remove { .. } => false,
        }
    }

    /// Return the details of the write if `self` is a write operation for a
    /// new or an existing entity
    fn as_write(&self) -> Option<EntityWrite> {
        EntityWrite::try_from(self).ok()
    }

    /// Return `true` if `self` requires clamping of an existing version
    fn is_clamp(&self) -> bool {
        match self {
            EntityMod::Insert { .. } => false,
            EntityMod::Overwrite { .. } | EntityMod::Remove { .. } => true,
        }
    }

    pub fn creates_entity(&self) -> bool {
        match self {
            EntityMod::Insert { .. } => true,
            EntityMod::Overwrite { .. } | EntityMod::Remove { .. } => false,
        }
    }

    fn clamp(&mut self, block: BlockNumber) -> Result<(), StoreError> {
        use EntityMod::*;

        match self {
            Insert { end, .. } | Overwrite { end, .. } => {
                if end.is_some() {
                    return Err(constraint_violation!("can not clamp again {:?}", self));
                }
                *end = Some(block);
            }
            Remove { .. } => {
                return Err(constraint_violation!(
                    "can not clamp block range for removal of {:?}",
                    self
                ))
            }
        }
        Ok(())
    }

    /// Turn an `Overwrite` into an `Insert`, return an error if this is a `Remove`
    fn as_insert(self, entity_type: &EntityType) -> Result<Self, StoreError> {
        use EntityMod::*;

        match self {
            Insert { .. } => Ok(self),
            Overwrite {
                key,
                data,
                block,
                end,
            } => Ok(Insert {
                key,
                data,
                block,
                end,
            }),
            Remove { key, .. } => {
                return Err(constraint_violation!(
                    "a remove for {}[{}] can not be converted into an insert",
                    entity_type,
                    key.entity_id
                ))
            }
        }
    }
}

/// A list of entity changes grouped by the entity type
#[derive(Debug)]
pub struct RowGroup {
    pub entity_type: EntityType,
    /// All changes for this entity type, ordered by block; i.e., if `i < j`
    /// then `rows[i].block() <= rows[j].block()`. Several methods on this
    /// struct rely on the fact that this ordering is observed.
    rows: Vec<EntityMod>,

    /// Map the `key.entity_id` of all entries in `rows` to the index with
    /// the most recent entry for that id to speed up lookups
    last_mod: HashMap<Word, usize>,
}

impl RowGroup {
    pub fn new(entity_type: EntityType) -> Self {
        Self {
            entity_type,
            rows: Vec::new(),
            last_mod: HashMap::new(),
        }
    }

    pub fn push(&mut self, emod: EntityModification, block: BlockNumber) -> Result<(), StoreError> {
        debug_assert!(self
            .rows
            .last()
            .map(|emod| emod.block() <= block)
            .unwrap_or(true));
        let row = EntityMod::new(emod, block);
        self.append_row(row)
    }

    fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Iterate over all changes that need clamping of the block range of an
    /// existing entity version
    pub fn clamps_by_block(&self) -> impl Iterator<Item = (BlockNumber, &[EntityMod])> {
        ClampsByBlockIterator::new(self)
    }

    /// Iterate over all changes that require writing a new entity version
    pub fn writes(&self) -> impl Iterator<Item = &EntityMod> {
        self.rows.iter().filter(|row| row.is_write())
    }

    /// Return an iterator over all writes in chunks. The returned
    /// `WriteChunker` is an iterator that produces `WriteChunk`s, which are
    /// the iterators over the writes. Each `WriteChunk` has `chunk_size`
    /// elements, except for the last one which might have fewer
    pub fn write_chunks<'a>(&'a self, chunk_size: usize) -> WriteChunker<'a> {
        WriteChunker::new(self, chunk_size)
    }

    pub fn has_clamps(&self) -> bool {
        self.rows.iter().any(|row| row.is_clamp())
    }

    pub fn last_op(&self, key: &EntityKey) -> Option<EntityOp<'_>> {
        let idx = *self.last_mod.get(&key.entity_id)?;
        self.rows.get(idx).map(EntityOp::from)
    }

    pub fn effective_ops(&self) -> impl Iterator<Item = EntityOp<'_>> {
        let mut seen = HashSet::new();
        self.rows
            .iter()
            .rev()
            .filter(move |emod| seen.insert(emod.id()))
            .map(EntityOp::from)
    }

    /// Find the most recent entry for `id`
    fn prev_row_mut(&mut self, id: &Word) -> Option<&mut EntityMod> {
        let idx = *self.last_mod.get(id)?;
        self.rows.get_mut(idx)
    }

    /// Append `row` to `self.rows` by combining it with a previously
    /// existing row, if that is possible
    fn append_row(&mut self, row: EntityMod) -> Result<(), StoreError> {
        if let Some(prev_row) = self.prev_row_mut(row.id()) {
            use EntityMod::*;

            if row.block() <= prev_row.block() {
                return Err(constraint_violation!(
                    "can not append operations that go backwards from {:?} to {:?}",
                    prev_row,
                    row
                ));
            }

            if row.id() != prev_row.id() {
                return Err(constraint_violation!(
                    "last_mod map is corrupted: got id {} looking up id {}",
                    prev_row.id(),
                    row.id()
                ));
            }

            // The heart of the matter: depending on what `row` is, clamp
            // `prev_row` and either ignore `row` since it is not needed, or
            // turn it into an `Insert`, which also does not require
            // clamping an old version
            match (&*prev_row, &row) {
                (Insert { .. }, Insert { .. })
                | (Overwrite { .. }, Insert { .. })
                | (Remove { .. }, Overwrite { .. })
                | (Remove { .. }, Remove { .. }) => {
                    return Err(constraint_violation!(
                        "impossible combination of entity operations: {:?} and then {:?}",
                        prev_row,
                        row
                    ))
                }
                (Insert { .. }, Overwrite { block, .. })
                | (Overwrite { .. }, Overwrite { block, .. }) => {
                    prev_row.clamp(*block)?;
                    let row = row.as_insert(&self.entity_type)?;
                    self.last_mod.insert(row.id().clone(), self.rows.len());
                    self.rows.push(row);
                }
                (Insert { .. }, Remove { block, .. })
                | (Overwrite { .. }, Remove { block, .. }) => {
                    prev_row.clamp(*block)?;
                }
                (Remove { .. }, Insert { .. }) => { /* nothing to do */ }
            }
        } else {
            self.last_mod.insert(row.id().clone(), self.rows.len());
            self.rows.push(row);
        }
        Ok(())
    }

    fn append(&mut self, group: RowGroup) -> Result<(), StoreError> {
        if self.entity_type != group.entity_type {
            return Err(constraint_violation!(
                "Can not append a row group for {} to a row group for {}",
                group.entity_type,
                self.entity_type
            ));
        }

        for row in group.rows {
            self.append_row(row)?;
        }

        Ok(())
    }

    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.rows.iter().map(|emod| emod.id().as_str())
    }
}

struct ClampsByBlockIterator<'a> {
    position: usize,
    rows: &'a [EntityMod],
}

impl<'a> ClampsByBlockIterator<'a> {
    fn new(group: &'a RowGroup) -> Self {
        ClampsByBlockIterator {
            position: 0,
            rows: &group.rows,
        }
    }
}

impl<'a> Iterator for ClampsByBlockIterator<'a> {
    type Item = (BlockNumber, &'a [EntityMod]);

    fn next(&mut self) -> Option<Self::Item> {
        // Make sure we start on a clamp
        while self.position < self.rows.len() && !self.rows[self.position].is_clamp() {
            self.position += 1;
        }
        if self.position >= self.rows.len() {
            return None;
        }
        let block = self.rows[self.position].block();
        let mut next = self.position;
        // Collect consecutive clamps
        while next < self.rows.len()
            && self.rows[next].block() == block
            && self.rows[next].is_clamp()
        {
            next += 1;
        }
        let res = Some((block, &self.rows[self.position..next]));
        self.position = next;
        res
    }
}

/// A list of entity changes with one group per entity type
pub struct Sheet {
    pub groups: Vec<RowGroup>,
}

impl Sheet {
    fn new() -> Self {
        Self { groups: Vec::new() }
    }

    fn group(&self, entity_type: &EntityType) -> Option<&RowGroup> {
        self.groups
            .iter()
            .find(|group| &group.entity_type == entity_type)
    }

    /// Return a mutable reference to an existing group, or create a new one
    /// if there isn't one yet and return a reference to that
    fn group_entry(&mut self, entity_type: &EntityType) -> &mut RowGroup {
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

    fn append(&mut self, other: Sheet) -> Result<(), StoreError> {
        for group in other.groups {
            self.group_entry(&group.entity_type).append(group)?;
        }
        Ok(())
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

    fn append(&mut self, mut other: BlockEntries<E>) {
        self.entries.append(&mut other.entries);
    }
}

pub type DataSources = BlockEntries<Vec<StoredDynamicDataSource>>;

pub enum EntityOp<'a> {
    Write {
        key: &'a EntityKey,
        entity: &'a Entity,
    },
    Remove {
        key: &'a EntityKey,
    },
}

impl<'a> From<&'a EntityMod> for EntityOp<'a> {
    fn from(emod: &'a EntityMod) -> Self {
        match emod {
            EntityMod::Insert { data, key, .. } | EntityMod::Overwrite { data, key, .. } => {
                EntityOp::Write { key, entity: data }
            }
            EntityMod::Remove { key, .. } => EntityOp::Remove { key },
        }
    }
}

/// A write batch. This data structure encapsulates all the things that need
/// to be changed to persist the output of mappings up to a certain block.
/// For now, a batch will only contain changes for a single block, but will
/// eventually contain data for multiple blocks.
pub struct Batch {
    /// The last block for which this batch contains changes
    pub block_ptr: BlockPtr,
    /// The first block for which this batch contains changes
    pub first_block: BlockNumber,
    /// The firehose cursor corresponding to `block_ptr`
    pub firehose_cursor: FirehoseCursor,
    mods: Sheet,
    /// New data sources
    pub data_sources: DataSources,
    pub deterministic_errors: Vec<SubgraphError>,
    pub offchain_to_remove: DataSources,
}

impl Batch {
    pub fn new(
        block_ptr: BlockPtr,
        firehose_cursor: FirehoseCursor,
        mut raw_mods: Vec<EntityModification>,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
        offchain_to_remove: Vec<StoredDynamicDataSource>,
    ) -> Result<Self, StoreError> {
        let block = block_ptr.number;

        // Sort the modifications such that writes and clamps are
        // consecutive. It's not needed for correctness but helps with some
        // of the iterations, especially when we iterate with
        // `clamps_by_block` so we get only one run for each block
        raw_mods.sort_unstable_by_key(|emod| match emod {
            EntityModification::Insert { .. } => 2,
            EntityModification::Overwrite { .. } => 1,
            EntityModification::Remove { .. } => 0,
        });

        let mut mods = Sheet::new();

        for m in raw_mods {
            mods.group_entry(&m.entity_ref().entity_type)
                .push(m, block)?;
        }

        let data_sources = DataSources::new(block_ptr.cheap_clone(), data_sources);
        let offchain_to_remove = DataSources::new(block_ptr.cheap_clone(), offchain_to_remove);
        let first_block = block_ptr.number;
        Ok(Self {
            block_ptr,
            first_block,
            firehose_cursor,
            mods,
            data_sources,
            deterministic_errors,
            offchain_to_remove,
        })
    }

    /// Append `batch` to `self` so that writing `self` afterwards has the
    /// same effect as writing `self` first and then `batch` in separate
    /// transactions
    pub fn append(&mut self, mut batch: Batch) -> Result<(), StoreError> {
        if batch.block_ptr.number <= self.block_ptr.number {
            return Err(constraint_violation!("Batches must go forward. Can't append a batch with block pointer {} to one with block pointer {}", batch.block_ptr, self.block_ptr));
        }

        self.block_ptr = batch.block_ptr;
        self.firehose_cursor = batch.firehose_cursor;
        self.mods.append(batch.mods)?;
        self.data_sources.append(batch.data_sources);
        self.deterministic_errors
            .append(&mut batch.deterministic_errors);
        self.offchain_to_remove.append(batch.offchain_to_remove);
        Ok(())
    }

    pub fn entity_count(&self) -> usize {
        self.mods.entity_count()
    }

    /// Find out whether the latest operation for the entity with type
    /// `entity_type` and `id` is going to write that entity, i.e., insert
    /// or overwrite it, or if it is going to remove it. If no change will
    /// be made to the entity, return `None`
    pub fn last_op(&self, key: &EntityKey) -> Option<EntityOp<'_>> {
        self.mods.group(&key.entity_type)?.last_op(key)
    }

    pub fn effective_ops(&self, entity_type: &EntityType) -> impl Iterator<Item = EntityOp> {
        self.mods
            .group(entity_type)
            .map(|group| group.effective_ops())
            .into_iter()
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
            self.mods
                .groups
                .iter()
                .map(|group| group.entity_type.clone()),
        );
        StoreEvent::from_types(deployment, entity_types)
    }

    pub fn groups<'a>(&'a self) -> impl Iterator<Item = &'a RowGroup> {
        self.mods.groups.iter()
    }
}

impl CacheWeight for Batch {
    fn indirect_weight(&self) -> usize {
        self.mods.indirect_weight()
    }
}

impl CacheWeight for Sheet {
    fn indirect_weight(&self) -> usize {
        self.groups.indirect_weight()
    }
}

impl CacheWeight for RowGroup {
    fn indirect_weight(&self) -> usize {
        self.rows.indirect_weight()
    }
}

impl CacheWeight for EntityMod {
    fn indirect_weight(&self) -> usize {
        match self {
            EntityMod::Insert { key, data, .. } | EntityMod::Overwrite { key, data, .. } => {
                key.indirect_weight() + data.indirect_weight()
            }
            EntityMod::Remove { key, .. } => key.indirect_weight(),
        }
    }
}

pub struct WriteChunker<'a> {
    group: &'a RowGroup,
    chunk_size: usize,
    position: usize,
}

impl<'a> WriteChunker<'a> {
    fn new(group: &'a RowGroup, chunk_size: usize) -> Self {
        Self {
            group,
            chunk_size,
            position: 0,
        }
    }
}

impl<'a> Iterator for WriteChunker<'a> {
    type Item = WriteChunk<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // Produce a chunk according to the current `self.position`
        let res = if self.position < self.group.rows.len() {
            Some(WriteChunk {
                group: self.group,
                chunk_size: self.chunk_size,
                position: self.position,
            })
        } else {
            None
        };

        // Advance `self.position` to the start of the next chunk
        let mut count = 0;
        while count < self.chunk_size && self.position < self.group.rows.len() {
            if self.group.rows[self.position].is_write() {
                count += 1;
            }
            self.position += 1;
        }

        res
    }
}

#[derive(Debug)]
pub struct WriteChunk<'a> {
    group: &'a RowGroup,
    chunk_size: usize,
    position: usize,
}

impl<'a> WriteChunk<'a> {
    pub fn is_empty(&'a self) -> bool {
        self.iter().next().is_none()
    }

    pub fn iter(&self) -> WriteChunkIter<'a> {
        WriteChunkIter {
            group: self.group,
            chunk_size: self.chunk_size,
            position: self.position,
            count: 0,
        }
    }
}

impl<'a> IntoIterator for &WriteChunk<'a> {
    type Item = EntityWrite<'a>;

    type IntoIter = WriteChunkIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        WriteChunkIter {
            group: self.group,
            chunk_size: self.chunk_size,
            position: self.position,
            count: 0,
        }
    }
}

pub struct WriteChunkIter<'a> {
    group: &'a RowGroup,
    chunk_size: usize,
    position: usize,
    count: usize,
}

impl<'a> Iterator for WriteChunkIter<'a> {
    type Item = EntityWrite<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.count < self.chunk_size && self.position < self.group.rows.len() {
            let insert = self.group.rows[self.position].as_write();
            self.position += 1;
            if insert.is_some() {
                self.count += 1;
                return insert;
            }
        }
        return None;
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::components::store::{write::EntityMod, BlockNumber, EntityKey, EntityType};

    use super::RowGroup;

    #[track_caller]
    fn check_runs(values: &[usize], blocks: &[BlockNumber], exp: &[(BlockNumber, &[usize])]) {
        assert_eq!(values.len(), blocks.len());

        let rows: Vec<_> = values
            .iter()
            .zip(blocks.iter())
            .map(|(value, block)| EntityMod::Remove {
                key: EntityKey::data("RowGroup".to_string(), value.to_string()),
                block: *block,
            })
            .collect();
        let last_mod = rows
            .iter()
            .enumerate()
            .fold(HashMap::new(), |mut map, (idx, emod)| {
                map.insert(emod.id().clone(), idx);
                map
            });

        let group = RowGroup {
            entity_type: EntityType::new("Entry".to_string()),
            rows,
            last_mod,
        };
        let act = group
            .clamps_by_block()
            .map(|(block, entries)| {
                (
                    block,
                    entries
                        .iter()
                        .map(|entry| entry.id().parse().unwrap())
                        .collect::<Vec<_>>(),
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
