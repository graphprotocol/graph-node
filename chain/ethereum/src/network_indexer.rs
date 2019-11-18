use futures::prelude::Future;
use futures::{
    future,
    future::{loop_fn, FutureResult, Loop},
};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::timer::Delay;

use graph::data::subgraph::schema::*;
use graph::prelude::{NetworkIndexer as NetworkIndexerTrait, *};

use web3::types::{Block, TransactionReceipt, H160, H256};

const NETWORK_INDEXER_VERSION: u32 = 0;

/// Helper type to bundle blocks and their uncles together.
struct BlockWithUncles {
    block: EthereumBlock,
    uncles: Vec<Option<Block<H256>>>,
}

impl BlockWithUncles {
    fn inner(&self) -> &LightEthereumBlock {
        &self.block.block
    }

    fn _transaction_receipts(&self) -> &Vec<TransactionReceipt> {
        &self.block.transaction_receipts
    }
}

/// Helper traits to work with network entities.
trait ToEntityId {
    fn to_entity_id(&self) -> String;
}
trait ToEntityKey {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey;
}
trait ToEntity {
    fn to_entity(&self) -> Result<Entity, Error>;
}

pub struct NetworkIndexer<S> {
    subgraph_name: SubgraphName,
    subgraph_id: SubgraphDeploymentId,
    logger: Logger,
    store: Arc<S>,
    adapter: Arc<dyn EthereumAdapter>,
}

/// Internal result type used to thread (NetworkIndexer, EntityCache) through
/// the chain of futures when indexing network data.
type NetworkIndexerResult<S> =
    Box<dyn Future<Item = (NetworkIndexer<S>, EntityCache), Error = Error> + Send>;

impl<S> NetworkIndexer<S>
where
    S: Store + ChainStore,
{
    pub fn new(
        subgraph_name: String,
        store: Arc<S>,
        adapter: Arc<dyn EthereumAdapter>,
        logger_factory: &LoggerFactory,
    ) -> Self {
        // Create a subgraph name and ID (base58 encoded version of the name)
        let id_str = format!(
            "{}_v{}",
            subgraph_name.replace("/", "_"),
            NETWORK_INDEXER_VERSION
        );
        let subgraph_id =
            SubgraphDeploymentId::new(dbg!(id_str)).expect("valid network subgraph ID");
        let subgraph_name = SubgraphName::new(subgraph_name).expect("valid network subgraph name");

        let logger = logger_factory.component_logger(
            "NetworkIndexer",
            Some(ComponentLoggerConfig {
                elastic: Some(ElasticComponentLoggerConfig {
                    index: String::from("ethereum-network-indexer"),
                }),
            }),
        );

        let logger = logger.new(o!(
          "subgraph_name" => subgraph_name.to_string(),
          "subgraph" => subgraph_id.to_string(),
        ));

        Self {
            subgraph_name,
            subgraph_id,
            logger,
            store,
            adapter,
        }
    }

    fn check_subgraph_exists(&self) -> impl Future<Item = bool, Error = Error> {
        future::result(
            self.store
                .get(SubgraphDeploymentEntity::key(self.subgraph_id.clone()))
                .map_err(|e| e.into())
                .map(|entity| entity.map_or(false, |_| true)),
        )
    }

    fn create_subgraph(self) -> FutureResult<Self, Error> {
        let mut ops = vec![];

        // Ensure the subgraph itself doesn't already exist
        ops.push(MetadataOperation::AbortUnless {
            description: "Subgraph entity should not exist".to_owned(),
            query: SubgraphEntity::query().filter(EntityFilter::new_equal(
                "name",
                self.subgraph_name.to_string(),
            )),
            entity_ids: vec![],
        });

        // Create the subgraph entity (e.g. `ethereum/mainnet`)
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let subgraph_entity_id = generate_entity_id();
        ops.extend(
            SubgraphEntity::new(self.subgraph_name.clone(), None, None, created_at)
                .write_operations(&subgraph_entity_id)
                .into_iter()
                .map(|op| op.into()),
        );

        // Ensure the subgraph version doesn't already exist
        ops.push(MetadataOperation::AbortUnless {
            description: "Subgraph version should not exist".to_owned(),
            query: SubgraphVersionEntity::query()
                .filter(EntityFilter::new_equal("id", self.subgraph_id.to_string())),
            entity_ids: vec![],
        });

        // Create a subgraph version entity; we're using the same ID for
        // version and deployment to make clear they belong together
        let version_entity_id = self.subgraph_id.to_string();
        ops.extend(
            SubgraphVersionEntity::new(
                subgraph_entity_id.clone(),
                self.subgraph_id.clone(),
                created_at,
            )
            .write_operations(&version_entity_id)
            .into_iter()
            .map(|op| op.into()),
        );

        // Immediately make this version the current one
        ops.extend(SubgraphEntity::update_pending_version_operations(
            &subgraph_entity_id,
            None,
        ));
        ops.extend(SubgraphEntity::update_current_version_operations(
            &subgraph_entity_id,
            Some(version_entity_id),
        ));

        // Ensure the deployment doesn't already exist
        ops.push(MetadataOperation::AbortUnless {
            description: "Subgraph deployment entity must not exist".to_owned(),
            query: SubgraphDeploymentEntity::query()
                .filter(EntityFilter::new_equal("id", self.subgraph_id.to_string())),
            entity_ids: vec![],
        });

        // Create a fake manifest
        let manifest = SubgraphManifest {
            id: self.subgraph_id.clone(),
            location: self.subgraph_name.to_string(),
            spec_version: String::from("0.0.1"),
            description: None,
            repository: None,
            schema: Schema::parse(include_str!("./ethereum.graphql"), self.subgraph_id.clone())
                .expect("valid Ethereum network subgraph schema"),
            data_sources: vec![],
            templates: vec![],
        };

        // Create deployment entity
        let chain_head_block = match self.store.chain_head_ptr() {
            Ok(block_ptr) => block_ptr,
            Err(e) => return future::err(e.into()),
        };
        ops.extend(
            SubgraphDeploymentEntity::new(&manifest, false, false, None, chain_head_block)
                .create_operations(&manifest.id),
        );

        // Create a deployment assignment entity
        ops.extend(
            SubgraphDeploymentAssignmentEntity::new(NodeId::new("__builtin").unwrap())
                .write_operations(&self.subgraph_id)
                .into_iter()
                .map(|op| op.into()),
        );

        match self.store.create_subgraph_deployment(&manifest.schema, ops) {
            Ok(_) => {
                debug!(self.logger, "Created Ethereum network subgraph");
                future::ok(self)
            }
            Err(e) => future::err(e.into()),
        }
    }

    fn ensure_subgraph_exists(self) -> impl Future<Item = Self, Error = Error> {
        debug!(self.logger, "Ensure that the network subgraph exists");

        // Do nothing if the deployment already exists
        self.check_subgraph_exists().and_then(|subgraph_exists| {
            if subgraph_exists {
                debug!(self.logger, "Network subgraph deployment already exists");
                future::ok(self)
            } else {
                debug!(
                    self.logger,
                    "Network subgraph deployment needs to be created"
                );
                self.create_subgraph()
            }
        })
    }

    fn fetch_block_and_uncles(
        logger: Logger,
        adapter: Arc<dyn EthereumAdapter>,
        block_number: u64,
    ) -> impl Future<Item = BlockWithUncles, Error = Error> {
        let logger_for_block = logger.clone();
        let adapter_for_block = adapter.clone();

        let logger_for_full_block = logger.clone();
        let adapter_for_full_block = adapter.clone();

        let logger_for_uncles = logger.clone();
        let adapter_for_uncles = adapter.clone();

        adapter
            .block_hash_by_block_number(&logger, block_number)
            .and_then(move |hash| {
                let hash = hash.expect("no block hash returned for block number");
                adapter_for_block.block_by_hash(&logger_for_block, hash)
            })
            .from_err()
            .and_then(move |block| {
                let block = block.expect("no block returned for hash");
                adapter_for_full_block
                    .load_full_block(&logger_for_full_block, block)
                    .from_err()
            })
            .and_then(move |block| {
                adapter_for_uncles
                    .uncles(&logger_for_uncles, &block.block)
                    .and_then(move |uncles| future::ok(BlockWithUncles { block, uncles }))
            })
    }

    fn index_next_blocks(self) -> impl Future<Item = Self, Error = Error> {
        let logger_for_head_comparison = self.logger.clone();

        let logger_for_fetching = self.logger.clone();
        let adapter_for_fetching = self.adapter.clone();

        let subgraph_id_for_subgraph_head = self.subgraph_id.clone();
        let store_for_subgraph_head = self.store.clone();

        // Poll the latest chain head from the network
        self.adapter
            .clone()
            .latest_block(&self.logger)
            .from_err()
            // Log chain head block and fail immediately if it is invalid (i.e.
            // is missing a block number and/or hash)
            .and_then(move |chain_head| {
                if chain_head.number.is_none() || chain_head.hash.is_none() {
                    future::err(format_err!(
                        "chain head block is missing a block number and hash"
                    ))
                } else {
                    future::ok(chain_head)
                }
            })
            // Identify the block the Ethereum network subgraph is on right now
            .and_then(move |chain_head| {
                store_for_subgraph_head
                    .clone()
                    .block_ptr(subgraph_id_for_subgraph_head.clone())
                    .map(|subgraph_head| (chain_head, subgraph_head))
            })
            // Log the this block
            .and_then(move |(chain_head, subgraph_head)| {
                debug!(
                    logger_for_head_comparison,
                    "Checking chain and subgraph head blocks";
                    "subgraph" => format!(
                        "({}, {})",
                        subgraph_head.map_or("none".into(), |ptr| format!("{}", ptr.number)),
                        subgraph_head.map_or("none".into(), |ptr| format!("{:?}", ptr.hash))
                    ),
                    "chain" => format!(
                        "({}, {:?})",
                        chain_head.number.unwrap(),
                        chain_head.hash.unwrap()
                    ),
                );

                future::ok((chain_head, subgraph_head))
            })
            .and_then(move |(chain_head, latest_block)| {
                // If we're already on the chain head, try again in 0.5s
                if Some((&chain_head).into()) == latest_block {
                    return Box::new(
                        Delay::new(Instant::now() + Duration::from_millis(500))
                            .from_err()
                            .and_then(|_| future::ok(self)),
                    ) as Box<dyn Future<Item = _, Error = _> + Send>;
                }

                // This is safe to do now; if the chain head block had no number
                // we would've failed earlier already
                let chain_head_number = chain_head.number.unwrap().as_u64();

                // Calculate the number of blocks to ingest;
                // fetch no more than 10000 blocks at a time
                let remaining_blocks =
                    chain_head_number - latest_block.map_or(0u64, |ptr| ptr.number);
                let blocks_to_ingest = remaining_blocks.min(10000);
                let block_range = latest_block.map_or(0u64, |ptr| ptr.number + 1)
                    ..(latest_block.map_or(0u64, |ptr| ptr.number + 1) + blocks_to_ingest);

                debug!(
                    logger_for_fetching,
                    "Fetching {} of {} remaining blocks ({:?})",
                    blocks_to_ingest,
                    remaining_blocks,
                    block_range,
                );

                Box::new(
                    futures::stream::iter_ok::<_, Error>(block_range.map(move |block_number| {
                        Self::fetch_block_and_uncles(
                            logger_for_fetching.clone(),
                            adapter_for_fetching.clone(),
                            block_number,
                        )
                    }))
                    .buffered(50)
                    .fold(self, move |indexer, block| indexer.index_block(block)),
                )
            })
    }

    fn index_block(self, block: BlockWithUncles) -> impl Future<Item = Self, Error = Error> {
        let hash = block.inner().hash.clone().unwrap();
        let number = block.inner().number.clone().unwrap();

        let logger = self.logger.new(o!(
            "block_hash" => format!("{:?}", hash),
            "block_number" => format!("{}", number),
        ));

        debug!(logger, "Index block");

        let block = Arc::new(block);
        let block_for_store = block.clone();

        // Add the block entity
        Box::new(
            self.set_entity(EntityCache::new(), block.as_ref())
                .and_then(move |(indexer, cache)| {
                    // Transact entity operations into the store
                    let modifications = match cache.as_modifications(indexer.store.as_ref()) {
                        Ok(mods) => mods,
                        Err(e) => return future::err(e.into()),
                    };
                    future::result(
                        indexer
                            .store
                            .transact_block_operations(
                                indexer.subgraph_id.clone(),
                                EthereumBlockPointer::from(&block_for_store.block),
                                modifications,
                            )
                            .map_err(|e| e.into())
                            .map(|_| indexer),
                    )
                }),
        )
    }

    fn set_entity(
        self,
        mut cache: EntityCache,
        value: impl ToEntity + ToEntityKey,
    ) -> NetworkIndexerResult<S> {
        cache.set(
            value.to_entity_key(self.subgraph_id.clone()),
            match value.to_entity() {
                Ok(entity) => entity,
                Err(e) => return Box::new(future::err(e.into())),
            },
        );
        Box::new(future::ok((self, cache)))
    }
}

impl<S> NetworkIndexerTrait for NetworkIndexer<S>
where
    S: Store + ChainStore,
{
    fn into_polling_stream(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        info!(self.logger, "Start network indexer");

        let logger_for_err = self.logger.clone();

        Box::new(
            self.ensure_subgraph_exists()
                .and_then(|indexer| {
                    loop_fn(indexer, |indexer| {
                        indexer
                            .index_next_blocks()
                            .map(|indexer| Loop::Continue(indexer))
                    })
                })
                .map_err(move |e| {
                    error!(
                      logger_for_err,
                      "Failed to index Ethereum network";
                      "error" => format!("{}", e)
                    )
                }),
        )
    }
}

impl ToEntityId for H160 {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self)
    }
}

impl ToEntityId for H256 {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self)
    }
}

impl ToEntityId for BlockWithUncles {
    fn to_entity_id(&self) -> String {
        (*self).block.block.hash.unwrap().to_entity_id()
    }
}

impl ToEntityKey for &BlockWithUncles {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Block".into(),
            entity_id: format!("{:x}", (*self).block.block.hash.unwrap()),
        }
    }
}

impl ToEntity for &BlockWithUncles {
    fn to_entity(&self) -> Result<Entity, Error> {
        let inner = self.inner();

        Ok(Entity::from(vec![
            ("id", format!("{:x}", inner.hash.unwrap()).into()),
            ("number", inner.number.unwrap().into()),
            ("hash", inner.hash.unwrap().into()),
            ("parent", inner.parent_hash.to_entity_id().into()),
            (
                "nonce",
                inner.nonce.map_or(Value::Null, |nonce| nonce.into()),
            ),
            ("transactionsRoot", inner.transactions_root.into()),
            ("transactionCount", (inner.transactions.len() as i32).into()),
            ("stateRoot", inner.state_root.into()),
            ("receiptsRoot", inner.receipts_root.into()),
            ("extraData", inner.extra_data.clone().into()),
            ("gasLimit", inner.gas_limit.into()),
            ("gasUsed", inner.gas_used.into()),
            ("timestamp", inner.timestamp.into()),
            ("logsBloom", inner.logs_bloom.into()),
            ("mixHash", inner.mix_hash.into()),
            ("difficulty", inner.difficulty.into()),
            ("totalDifficulty", inner.total_difficulty.into()),
            ("ommerCount", (self.uncles.len() as i32).into()),
            ("ommerHash", inner.uncles_hash.into()),
            // ("author", inner.author.to_entity_id().into()),
            // ("ommers", ...)
            ("size", inner.size.into()),
            ("sealFields", inner.seal_fields.clone().into()),
        ] as Vec<(_, Value)>))
    }
}
