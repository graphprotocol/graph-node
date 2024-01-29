use std::sync::Arc;

use crate::blockchain::BlockHash;
use crate::components::store::SubgraphSegment;
use crate::tokio::sync::RwLock;
use crate::{blockchain::BlockPtr, prelude::BlockNumber};
use anyhow::{Error, Result};
use async_trait::async_trait;
use borsh::{BorshDeserialize, BorshSerialize};
use sled::{Db, Tree};
use thiserror::Error;

use crate::indexer::{BlockSender, EncodedTriggers, IndexerStore, State, StateDelta};
pub const DB_NAME: &str = "/media/data/sled_indexer_db";
pub const STATE_SNAPSHOT_FREQUENCY: u32 = 1000;
pub const TRIGGER_PREFIX: &str = "trigger_";

#[derive(BorshSerialize, BorshDeserialize)]
struct Value {
    block_hash: Box<[u8]>,
    data: Box<[u8]>,
}

/// How frequently do we want state to be fully stored.
/// Never will prevent snapshots, this likely means state is not being used.
#[derive(Clone)]
pub enum StateSnapshotFrequency {
    Never,
    Blocks(u32),
}

impl Default for StateSnapshotFrequency {
    fn default() -> Self {
        Self::Blocks(STATE_SNAPSHOT_FREQUENCY)
    }
}

#[derive(Debug, Error)]
pub enum SledStoreError {
    #[error("A last stable block is required for this operation")]
    LastStableBlockRequired,
    #[error("sled returned an error: {0}")]
    SledError(#[from] sled::Error),
}

struct StoreInner {
    last_state_snapshot: Option<BlockNumber>,
}

#[derive(Clone)]
pub struct SledIndexerStore {
    tree: Tree,
    // Keeping interior mutability for now because of there is a chance we need to share the store
    // and the IndexingContext would definitely be shared
    inner: Arc<RwLock<StoreInner>>,
    snapshot_frequency: StateSnapshotFrequency,
}

impl SledIndexerStore {
    pub fn new(
        db: Arc<Db>,
        tree_name: &str,
        snapshot_frequency: StateSnapshotFrequency,
    ) -> Result<Self> {
        let tree = db.open_tree(tree_name).map_err(SledStoreError::from)?;
        let last_state_snapshot = tree
            .get(Self::latest_snapshot_key())
            .map_err(SledStoreError::from)?
            .map(|v| BlockNumber::from_le_bytes(v.to_vec().try_into().unwrap()));

        let inner = Arc::new(RwLock::new(StoreInner {
            last_state_snapshot,
        }));

        Ok(Self {
            tree,
            inner,
            snapshot_frequency,
        })
    }

    pub fn state_delta_key(bn: BlockNumber) -> String {
        format!("state_delta_{}", bn)
    }
    pub fn trigger_key(bn: BlockNumber) -> String {
        format!("{}{}", TRIGGER_PREFIX, bn)
    }
    pub fn snapshot_key(bn: BlockNumber) -> String {
        format!("state_snapshot_{}", bn)
    }
    pub fn latest_snapshot_key() -> String {
        "latest_snapshot".to_string()
    }

    pub fn last_stable_block_key() -> String {
        "last_stable_block".to_string()
    }

    async fn should_snapshot(&self, bn: BlockNumber) -> bool {
        use StateSnapshotFrequency::*;

        let freq = match self.snapshot_frequency {
            Never => return false,
            Blocks(blocks) => blocks,
        };

        bn - self.inner.read().await.last_state_snapshot.unwrap_or(0) > freq.try_into().unwrap()
    }
}

#[async_trait]
impl IndexerStore for SledIndexerStore {
    async fn get_segments(&self) -> Result<Vec<SubgraphSegment>> {
        unimplemented!();
    }
    async fn set_segments(&self, _segments: Vec<(i32, i32)>) -> Result<Vec<SubgraphSegment>> {
        unimplemented!();
    }
    async fn get(&self, bn: BlockNumber, _s: SubgraphSegment) -> Result<Option<EncodedTriggers>> {
        let res = match self
            .tree
            .get(bn.to_string())
            .map_err(SledStoreError::from)?
        {
            None => None,
            Some(v) => Value::try_from_slice(v.as_ref())
                .map(|v| EncodedTriggers(v.data))
                .map(Some)?,
        };

        Ok(res)
    }
    async fn set_last_stable_block(&self, _: SubgraphSegment, bn: BlockNumber) -> Result<()> {
        self.tree
            .insert(
                SledIndexerStore::last_stable_block_key().as_str(),
                bn.to_le_bytes().to_vec(),
            )
            .map_err(Error::from)?;

        self.tree
            .flush_async()
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    async fn set(
        &self,
        ptr: BlockPtr,
        _s: &SubgraphSegment,
        state: &State,
        triggers: EncodedTriggers,
    ) -> Result<()> {
        let BlockPtr { hash, number: bn } = ptr;
        let should_snapshot = self.should_snapshot(bn).await;

        let v = Value {
            block_hash: hash.0,
            data: triggers.0,
        };
        let v = borsh::to_vec(&v)?;
        self.tree.transaction(move |tx_db| {
            use sled::transaction::ConflictableTransactionError::*;

            tx_db
                .insert(
                    SledIndexerStore::state_delta_key(bn).as_str(),
                    borsh::to_vec(&state.delta()).unwrap(),
                )
                .map_err(Abort)?;

            tx_db
                .insert(SledIndexerStore::trigger_key(bn).as_str(), &*v)
                .map_err(Abort)?;

            if should_snapshot {
                tx_db
                    .insert(
                        SledIndexerStore::snapshot_key(bn).as_str(),
                        borsh::to_vec(&state).unwrap(),
                    )
                    .map_err(Abort)?;

                tx_db
                    .insert(
                        SledIndexerStore::latest_snapshot_key().as_str(),
                        bn.to_le_bytes().to_vec(),
                    )
                    .map_err(Abort)?;
            }

            Ok(())
        })?;

        // This should always be a NOOP is state is not in use.
        if should_snapshot {
            self.inner.write().await.last_state_snapshot = Some(bn);
        }

        Ok(())
    }

    async fn get_state(&self, bn: BlockNumber) -> Result<State> {
        let block = match self.inner.read().await.last_state_snapshot {
            None => return Ok(State::default()),
            Some(block) => block,
        };

        let snapshot = self
            .tree
            .get(SledIndexerStore::snapshot_key(block))
            .map_err(SledStoreError::from)?
            .expect("last_state_snapshot doesn't match the marker");
        let mut state = State::try_from_slice(&snapshot.as_ref()).unwrap();

        self.tree
            .range(
                SledIndexerStore::state_delta_key(block + 1)..SledIndexerStore::state_delta_key(bn),
            )
            .collect::<Result<Vec<(_, _)>, _>>()?
            .iter()
            .for_each(|(_, v)| {
                let delta = StateDelta::try_from_slice(v.as_ref()).unwrap();

                state.apply(delta);
            });

        Ok(state)
    }
    async fn get_last_stable_block(&self) -> Result<Option<BlockNumber>> {
        let b = self
            .tree
            .get(Self::last_stable_block_key())?
            .map(|ivec| i32::from_le_bytes(ivec.as_ref().try_into().unwrap()));

        Ok(b)
    }

    async fn stream_from(&self, bn: BlockNumber, sender: BlockSender) -> Result<()> {
        let last = self
            .get_last_stable_block()
            .await?
            .ok_or(SledStoreError::LastStableBlockRequired)?;

        let mut iter = self
            .tree
            .range(Self::trigger_key(bn)..Self::trigger_key(last));

        loop {
            let next_trigger = iter.next();
            let bs: (BlockPtr, EncodedTriggers) = match next_trigger {
                Some(Ok((key, value))) => {
                    let block =
                        key.subslice(TRIGGER_PREFIX.len(), key.len() - TRIGGER_PREFIX.len());
                    let block: i32 = String::from_utf8_lossy(block.as_ref()).parse().unwrap();

                    let value = Value::try_from_slice(value.as_ref())?;
                    let trigger = EncodedTriggers(value.data);
                    let block = BlockPtr {
                        hash: BlockHash(value.block_hash),
                        number: block,
                    };

                    (block, trigger)
                }
                None => break,
                _ => unreachable!(),
            };

            match sender.send(bs).await {
                Ok(()) => {}
                Err(_) => {
                    println!("sender dropped, stream ending");
                    break;
                }
            }
        }

        Ok(())
    }
}
