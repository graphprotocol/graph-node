use web3::types::{Address, H256};

use super::*;
use crate::components::server::index_node::VersionInfo;
use crate::components::transaction_receipt;
use crate::data::subgraph::status;
use crate::data::value::Word;
use crate::data::{query::QueryTarget, subgraph::schema::*};

pub trait SubscriptionManager: Send + Sync + 'static {
    /// Subscribe to changes for specific subgraphs and entities.
    ///
    /// Returns a stream of store events that match the input arguments.
    fn subscribe(&self, entities: BTreeSet<SubscriptionFilter>) -> StoreEventStreamBox;

    /// If the payload is not required, use for a more efficient subscription mechanism backed by a watcher.
    fn subscribe_no_payload(&self, entities: BTreeSet<SubscriptionFilter>) -> UnitStream;
}

/// Subgraph forking is the process of lazily fetching entities
/// from another subgraph's store (usually a remote one).
pub trait SubgraphFork: Send + Sync + 'static {
    fn fetch(&self, entity_type: String, id: String) -> Result<Option<Entity>, StoreError>;
}

/// A special trait to handle looking up ENS names from special rainbow
/// tables that need to be manually loaded into the system
pub trait EnsLookup: Send + Sync + 'static {
    /// Find the reverse of keccak256 for `hash` through looking it up in the
    /// rainbow table.
    fn find_name(&self, hash: &str) -> Result<Option<String>, StoreError>;
}

/// An entry point for all operations that require access to the node's storage
/// layer. It provides access to a [`BlockStore`] and a [`SubgraphStore`].
pub trait Store: Clone + StatusStore + Send + Sync + 'static {
    /// The [`BlockStore`] implementor used by this [`Store`].
    type BlockStore: BlockStore;

    /// The [`SubgraphStore`] implementor used by this [`Store`].
    type SubgraphStore: SubgraphStore;

    fn block_store(&self) -> Arc<Self::BlockStore>;

    fn subgraph_store(&self) -> Arc<Self::SubgraphStore>;
}

/// Common trait for store implementations.
#[async_trait]
pub trait SubgraphStore: Send + Sync + 'static {
    fn ens_lookup(&self) -> Arc<dyn EnsLookup>;

    /// Check if the store is accepting queries for the specified subgraph.
    /// May return true even if the specified subgraph is not currently assigned to an indexing
    /// node, as the store will still accept queries.
    fn is_deployed(&self, id: &DeploymentHash) -> Result<bool, StoreError>;

    /// Create a new deployment for the subgraph `name`. If the deployment
    /// already exists (as identified by the `schema.id`), reuse that, otherwise
    /// create a new deployment, and point the current or pending version of
    /// `name` at it, depending on the `mode`
    fn create_subgraph_deployment(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: DeploymentCreate,
        node_id: NodeId,
        network: String,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<DeploymentLocator, StoreError>;

    /// Create a new subgraph with the given name. If one already exists, use
    /// the existing one. Return the `id` of the newly created or existing
    /// subgraph
    fn create_subgraph(&self, name: SubgraphName) -> Result<String, StoreError>;

    /// Remove a subgraph and all its versions; if deployments that were used
    /// by this subgraph do not need to be indexed anymore, also remove
    /// their assignment, but keep the deployments themselves around
    fn remove_subgraph(&self, name: SubgraphName) -> Result<(), StoreError>;

    /// Assign the subgraph with `id` to the node `node_id`. If there is no
    /// assignment for the given deployment, report an error.
    fn reassign_subgraph(
        &self,
        deployment: &DeploymentLocator,
        node_id: &NodeId,
    ) -> Result<(), StoreError>;

    fn assigned_node(&self, deployment: &DeploymentLocator) -> Result<Option<NodeId>, StoreError>;

    fn assignments(&self, node: &NodeId) -> Result<Vec<DeploymentLocator>, StoreError>;

    /// Return `true` if a subgraph `name` exists, regardless of whether the
    /// subgraph has any deployments attached to it
    fn subgraph_exists(&self, name: &SubgraphName) -> Result<bool, StoreError>;

    /// Returns a collection of all [`EntityModification`] items in relation to
    /// the given [`BlockNumber`]. No distinction is made between inserts and
    /// updates, which may be returned as either [`EntityModification::Insert`]
    /// or [`EntityModification::Overwrite`].
    fn entity_changes_in_block(
        &self,
        subgraph_id: &DeploymentHash,
        block_number: BlockNumber,
    ) -> Result<Vec<EntityOperation>, StoreError>;

    /// Return the GraphQL schema supplied by the user
    fn input_schema(&self, subgraph_id: &DeploymentHash) -> Result<Arc<Schema>, StoreError>;

    /// Return the GraphQL schema that was derived from the user's schema by
    /// adding a root query type etc. to it
    fn api_schema(&self, subgraph_id: &DeploymentHash) -> Result<Arc<ApiSchema>, StoreError>;

    /// Return a `SubgraphFork`, derived from the user's `debug-fork` deployment argument,
    /// that is used for debugging purposes only.
    fn debug_fork(
        &self,
        subgraph_id: &DeploymentHash,
        logger: Logger,
    ) -> Result<Option<Arc<dyn SubgraphFork>>, StoreError>;

    /// Return a `WritableStore` that is used for indexing subgraphs. Only
    /// code that is part of indexing a subgraph should ever use this. The
    /// `logger` will be used to log important messages related to the
    /// subgraph
    async fn writable(
        self: Arc<Self>,
        logger: Logger,
        deployment: DeploymentId,
    ) -> Result<Arc<dyn WritableStore>, StoreError>;

    /// Return the minimum block pointer of all deployments with this `id`
    /// that we would use to query or copy from; in particular, this will
    /// ignore any instances of this deployment that are in the process of
    /// being set up
    async fn least_block_ptr(&self, id: &DeploymentHash) -> Result<Option<BlockPtr>, StoreError>;

    async fn is_healthy(&self, id: &DeploymentHash) -> Result<bool, StoreError>;

    /// Find the deployment locators for the subgraph with the given hash
    fn locators(&self, hash: &str) -> Result<Vec<DeploymentLocator>, StoreError>;
}

/// A view of the store for indexing. All indexing-related operations need
/// to go through this trait. Methods in this trait will never return a
/// `StoreError::DatabaseUnavailable`. Instead, they will retry the
/// operation indefinitely until it succeeds.
#[async_trait]
pub trait WritableStore: Send + Sync + 'static {
    /// Get a pointer to the most recently processed block in the subgraph.
    async fn block_ptr(&self) -> Option<BlockPtr>;

    /// Returns the Firehose `cursor` this deployment is currently at in the block stream of events. This
    /// is used when re-connecting a Firehose stream to start back exactly where we left off.
    async fn block_cursor(&self) -> Option<String>;

    /// Deletes the current Firehose `cursor` this deployment is currently at.
    async fn delete_block_cursor(&self) -> Result<(), StoreError>;

    /// Start an existing subgraph deployment.
    async fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError>;

    /// Revert the entity changes from a single block atomically in the store, and update the
    /// subgraph block pointer to `block_ptr_to`.
    ///
    /// `block_ptr_to` must point to the parent block of the subgraph block pointer.
    async fn revert_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<&str>,
    ) -> Result<(), StoreError>;

    /// If a deterministic error happened, this function reverts the block operations from the
    /// current block to the previous block.
    fn unfail_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError>;

    /// If a non-deterministic error happened and the current deployment head is past the error
    /// block range, this function unfails the subgraph and deletes the error.
    fn unfail_non_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError>;

    /// Set subgraph status to failed with the given error as the cause.
    async fn fail_subgraph(&self, error: SubgraphError) -> Result<(), StoreError>;

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError>;

    /// Looks up an entity using the given store key at the latest block.
    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError>;

    /// Transact the entity changes from a single block atomically into the store, and update the
    /// subgraph block pointer to `block_ptr_to`, and update the firehose cursor to `firehose_cursor`
    ///
    /// `block_ptr_to` must point to a child block of the current subgraph block pointer.
    async fn transact_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<String>,
        mods: Vec<EntityModification>,
        stopwatch: &StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError>;

    /// Look up multiple entities as of the latest block. Returns a map of
    /// entities by type.
    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError>;

    /// The deployment `id` finished syncing, mark it as synced in the database
    /// and promote it to the current version in the subgraphs where it was the
    /// pending version so far
    fn deployment_synced(&self) -> Result<(), StoreError>;

    /// Return true if the deployment with the given id is fully synced,
    /// and return false otherwise. Errors from the store are passed back up
    async fn is_deployment_synced(&self) -> Result<bool, StoreError>;

    fn unassign_subgraph(&self) -> Result<(), StoreError>;

    /// Load the dynamic data sources for the given deployment
    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError>;

    /// Report the name of the shard in which the subgraph is stored. This
    /// should only be used for reporting and monitoring
    fn shard(&self) -> &str;

    async fn health(&self) -> Result<SubgraphHealth, StoreError>;

    fn input_schema(&self) -> Arc<Schema>;

    /// Wait for the background writer to finish processing its queue
    async fn flush(&self) -> Result<(), StoreError>;
}

#[async_trait]
pub trait QueryStoreManager: Send + Sync + 'static {
    /// Get a new `QueryStore`. A `QueryStore` is tied to a DB replica, so if Graph Node is
    /// configured to use secondary DB servers the queries will be distributed between servers.
    ///
    /// The query store is specific to a deployment, and `id` must indicate
    /// which deployment will be queried. It is not possible to use the id of the
    /// metadata subgraph, though the resulting store can be used to query
    /// metadata about the deployment `id` (but not metadata about other deployments).
    ///
    /// If `for_subscription` is true, the main replica will always be used.
    async fn query_store(
        &self,
        target: QueryTarget,
        for_subscription: bool,
    ) -> Result<Arc<dyn QueryStore + Send + Sync>, QueryExecutionError>;
}

pub trait BlockStore: Send + Sync + 'static {
    type ChainStore: ChainStore;

    fn chain_store(&self, network: &str) -> Option<Arc<Self::ChainStore>>;
}

/// Common trait for blockchain store implementations.
#[async_trait]
pub trait ChainStore: Send + Sync + 'static {
    /// Get a pointer to this blockchain's genesis block.
    fn genesis_block_ptr(&self) -> Result<BlockPtr, Error>;

    /// Insert a block into the store (or update if they are already present).
    async fn upsert_block(&self, block: Arc<dyn Block>) -> Result<(), Error>;

    fn upsert_light_blocks(&self, blocks: &[&dyn Block]) -> Result<(), Error>;

    /// Try to update the head block pointer to the block with the highest block number.
    ///
    /// Only updates pointer if there is a block with a higher block number than the current head
    /// block, and the `ancestor_count` most recent ancestors of that block are in the store.
    /// Note that this means if the Ethereum node returns a different "latest block" with a
    /// different hash but same number, we do not update the chain head pointer.
    /// This situation can happen on e.g. Infura where requests are load balanced across many
    /// Ethereum nodes, in which case it's better not to continuously revert and reapply the latest
    /// blocks.
    ///
    /// If the pointer was updated, returns `Ok(vec![])`, and fires a HeadUpdateEvent.
    ///
    /// If no block has a number higher than the current head block, returns `Ok(vec![])`.
    ///
    /// If the candidate new head block had one or more missing ancestors, returns
    /// `Ok(missing_blocks)`, where `missing_blocks` is a nonexhaustive list of missing blocks.
    async fn attempt_chain_head_update(
        self: Arc<Self>,
        ancestor_count: BlockNumber,
    ) -> Result<Option<H256>, Error>;

    /// Get the current head block pointer for this chain.
    /// Any changes to the head block pointer will be to a block with a larger block number, never
    /// to a block with a smaller or equal block number.
    ///
    /// The head block pointer will be None on initial set up.
    async fn chain_head_ptr(self: Arc<Self>) -> Result<Option<BlockPtr>, Error>;

    /// In-memory time cached version of `chain_head_ptr`.
    async fn cached_head_ptr(self: Arc<Self>) -> Result<Option<BlockPtr>, Error>;

    /// Get the current head block cursor for this chain.
    ///
    /// The head block cursor will be None on initial set up.
    fn chain_head_cursor(&self) -> Result<Option<String>, Error>;

    /// This method does actually three operations:
    /// - Upserts received block into blocks table
    /// - Update chain head block into networks table
    /// - Update chain head cursor into networks table
    async fn set_chain_head(
        self: Arc<Self>,
        block: Arc<dyn Block>,
        cursor: String,
    ) -> Result<(), Error>;

    /// Returns the blocks present in the store.
    fn blocks(&self, hashes: &[BlockHash]) -> Result<Vec<serde_json::Value>, Error>;

    /// Get the `offset`th ancestor of `block_hash`, where offset=0 means the block matching
    /// `block_hash` and offset=1 means its parent. Returns None if unable to complete due to
    /// missing blocks in the chain store.
    ///
    /// Returns an error if the offset would reach past the genesis block.
    async fn ancestor_block(
        self: Arc<Self>,
        block_ptr: BlockPtr,
        offset: BlockNumber,
    ) -> Result<Option<serde_json::Value>, Error>;

    /// Remove old blocks from the cache we maintain in the database and
    /// return a pair containing the number of the oldest block retained
    /// and the number of blocks deleted.
    /// We will never remove blocks that are within `ancestor_count` of
    /// the chain head.
    fn cleanup_cached_blocks(
        &self,
        ancestor_count: BlockNumber,
    ) -> Result<Option<(BlockNumber, usize)>, Error>;

    /// Return the hashes of all blocks with the given number
    fn block_hashes_by_block_number(&self, number: BlockNumber) -> Result<Vec<BlockHash>, Error>;

    /// Confirm that block number `number` has hash `hash` and that the store
    /// may purge any other blocks with that number
    fn confirm_block_hash(&self, number: BlockNumber, hash: &BlockHash) -> Result<usize, Error>;

    /// Find the block with `block_hash` and return the network name and number
    fn block_number(&self, hash: &BlockHash) -> Result<Option<(String, BlockNumber)>, StoreError>;

    /// Tries to retrieve all transactions receipts for a given block.
    async fn transaction_receipts_in_block(
        &self,
        block_ptr: &H256,
    ) -> Result<Vec<transaction_receipt::LightTransactionReceipt>, StoreError>;
}

pub trait EthereumCallCache: Send + Sync + 'static {
    /// Returns the return value of the provided Ethereum call, if present in
    /// the cache.
    fn get_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: BlockPtr,
    ) -> Result<Option<Vec<u8>>, Error>;

    /// Returns all cached calls for a given `block`. This method does *not*
    /// update the last access time of the returned cached calls.
    fn get_calls_in_block(&self, block: BlockPtr) -> Result<Vec<CachedEthereumCall>, Error>;

    /// Stores the provided Ethereum call in the cache.
    fn set_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: BlockPtr,
        return_value: &[u8],
    ) -> Result<(), Error>;
}

/// Store operations used when serving queries for a specific deployment
#[async_trait]
pub trait QueryStore: Send + Sync {
    fn find_query_values(
        &self,
        query: EntityQuery,
    ) -> Result<Vec<BTreeMap<Word, r::Value>>, QueryExecutionError>;

    async fn is_deployment_synced(&self) -> Result<bool, Error>;

    async fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError>;

    fn block_number(&self, block_hash: &BlockHash) -> Result<Option<BlockNumber>, StoreError>;

    fn wait_stats(&self) -> Result<PoolWaitStats, StoreError>;

    /// If `block` is `None`, assumes the latest block.
    async fn has_non_fatal_errors(&self, block: Option<BlockNumber>) -> Result<bool, StoreError>;

    /// Find the current state for the subgraph deployment `id` and
    /// return details about it needed for executing queries
    async fn deployment_state(&self) -> Result<DeploymentState, QueryExecutionError>;

    fn api_schema(&self) -> Result<Arc<ApiSchema>, QueryExecutionError>;

    fn network_name(&self) -> &str;

    /// A permit should be acquired before starting query execution.
    async fn query_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, StoreError>;
}

/// A view of the store that can provide information about the indexing status
/// of any subgraph and any deployment
#[async_trait]
pub trait StatusStore: Send + Sync + 'static {
    /// A permit should be acquired before starting query execution.
    async fn query_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, StoreError>;

    fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError>;

    /// Support for the explorer-specific API
    fn version_info(&self, version_id: &str) -> Result<VersionInfo, StoreError>;

    /// Support for the explorer-specific API; note that `subgraph_id` must be
    /// the id of an entry in `subgraphs.subgraph`, not that of a deployment.
    /// The return values are the ids of the `subgraphs.subgraph_version` for
    /// the current and pending versions of the subgraph
    fn versions_for_subgraph_id(
        &self,
        subgraph_id: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError>;

    /// Support for the explorer-specific API. Returns a vector of (name, version) of all
    /// subgraphs for a given deployment hash.
    fn subgraphs_for_deployment_hash(
        &self,
        deployment_hash: &str,
    ) -> Result<Vec<(String, String)>, StoreError>;

    /// A value of None indicates that the table is not available. Re-deploying
    /// the subgraph fixes this. It is undesirable to force everything to
    /// re-sync from scratch, so existing deployments will continue without a
    /// Proof of Indexing. Once all subgraphs have been re-deployed the Option
    /// can be removed.
    async fn get_proof_of_indexing(
        &self,
        subgraph_id: &DeploymentHash,
        indexer: &Option<Address>,
        block: BlockPtr,
    ) -> Result<Option<[u8; 32]>, StoreError>;

    /// Like `get_proof_of_indexing` but returns a Proof of Indexing signed by
    /// address `0x00...0`, which allows it to be shared in public without
    /// revealing the indexers _real_ Proof of Indexing.
    async fn get_public_proof_of_indexing(
        &self,
        subgraph_id: &DeploymentHash,
        block_number: BlockNumber,
    ) -> Result<Option<(PartialBlockPtr, [u8; 32])>, StoreError>;
}
