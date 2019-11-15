use futures::future::{loop_fn, Loop};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Shr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::timer::Delay;

use bs58;
use graph::data::subgraph::schema::{
    generate_entity_id, SubgraphDeploymentAssignmentEntity, SubgraphEntity, SubgraphVersionEntity,
};
use graph::prelude::*;
use web3::types::{Block, Bytes, Log, Transaction, TransactionReceipt, H160, H256, U128, U256};

/// Helper type to bundle blocks and their uncles together.
struct BlockWithUncles {
    block: EthereumBlock,
    uncles: Vec<Option<Block<H256>>>,
}

/// Internal result type used to thread (NetworkIngestor, EntityCache) through
/// the chain of futures when ingesting a block.
type NetworkIngestorResult<S> =
    Box<dyn Future<Item = (NetworkIngestor<S>, EntityCache), Error = Error> + Send>;

/// An Ethereum account with a balance, as defined in network config files like
/// `mainnet.json` or `ropsten.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Account {
    balance: Option<U256>,
}

/// Ethash parameters.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EthashParams {
    block_reward: BTreeMap<U128, U256>,
}

/// Ethash configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EthashConfig {
    params: EthashParams,
}

/// Ethereum engine configuration..
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EngineConfig {
    #[serde(rename = "Ethash")]
    ethash: EthashConfig,
}

/// A network configuration as defined in files like `mainnet.json` or
/// `ropsten.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct NetworkConfig {
    accounts: BTreeMap<H160, Account>,
    engine: EngineConfig,
}

lazy_static! {
    // Load Ethereum network configurations for different networks.
    // These were retrieved from https://wiki.parity.io/Chain-specification.html.
    static ref NETWORK_CONFIGS: BTreeMap<&'static str, NetworkConfig> = BTreeMap::from_iter(
        vec![(
            "mainnet",
            serde_json::from_str(include_str!("./networks/mainnet.json"))
                .expect("failed to parse network config from mainnet.json"),
        )].into_iter(),
    );

    static ref WEI_TO_ETHER_FACTOR: BigDecimal =
        serde_json::from_str("\"0.000000000000000001\"")
        .expect("failed to parse BigDecimal");
}

fn wei_to_ether(n: &U256) -> BigDecimal {
    BigInt::from_unsigned_u256(n).to_big_decimal(0.into()) * &*WEI_TO_ETHER_FACTOR
}

/// Network ingestor.
pub struct NetworkIngestor<S>
where
    S: Store + ChainStore + NetworkStore,
{
    store: Arc<S>,
    eth_adapter: Arc<dyn EthereumAdapter>,
    network_name: String,
    subgraph_name: SubgraphName,
    subgraph_id: SubgraphDeploymentId,
    logger: Logger,
    network_config: NetworkConfig,
}

impl<S> NetworkIngestor<S>
where
    S: Store + ChainStore + NetworkStore,
{
    pub fn new(
        store: Arc<S>,
        eth_adapter: Arc<dyn EthereumAdapter>,
        network_name: String,
        logger: Logger,
        elastic_config: Option<ElasticLoggingConfig>,
    ) -> Self {
        let subgraph_name = SubgraphName::new(format!("ethereum/{}", network_name))
            .expect("invalid network subgraph name");

        let network_config = match NETWORK_CONFIGS.get(&network_name.as_str()) {
            Some(config) => config.clone(),
            None => panic!("genesis block config not found for network"),
        };

        let subgraph_id = SubgraphDeploymentId::new(
            bs58::encode(subgraph_name.to_string().as_str()).into_string(),
        )
        .expect("invalid network subgraph ID");

        let term_logger = logger.new(o!(
            "component" => "NetworkIngestor",
            "network_name" => network_name.clone(),
            "subgraph_name" => subgraph_name.to_string(),
            "subgraph_id" => subgraph_id.to_string(),
        ));
        let logger = elastic_config
            .clone()
            .map_or(term_logger.clone(), |elastic_config| {
                split_logger(
                    term_logger.clone(),
                    elastic_logger(
                        ElasticDrainConfig {
                            general: elastic_config,
                            index: String::from("ethereum-network-ingestor-logs"),
                            document_type: String::from("log"),
                            custom_id_key: String::from("network"),
                            custom_id_value: network_name.clone(),
                            flush_interval: Duration::from_secs(5),
                        },
                        term_logger,
                    ),
                )
            });

        Self {
            store,
            eth_adapter,
            network_name,
            subgraph_name,
            subgraph_id,
            logger,
            network_config,
        }
    }

    pub fn into_polling_stream(self) -> impl Future<Item = (), Error = ()> {
        let logger = self.logger.clone();
        let logger_for_schema_error = self.logger.clone();

        info!(logger, "Starting Ethereum subgraph ingestor");

        // Ensure that the ethereum subgraph exists in the store
        self.ensure_ethereum_subgraph()
            .map_err(move |e| {
                crit!(
                    logger_for_schema_error,
                    "Failed to ensure Ethereum subgraph exists";
                    "error" => format!("{}", e),
                );
            })
            .and_then(|ingestor| {
                loop_fn(ingestor, |ingestor| {
                    ingestor
                        .ingest_next_blocks()
                        .map(|ingestor| Loop::Continue(ingestor))
                })
                .map(|_: Self| ())
                .map_err(move |e: Error| {
                    crit!(
                        logger,
                        "Failed to index Ethereum subraph";
                        "error" => format!("{}", e)
                    )
                })
            })
    }

    fn ensure_ethereum_subgraph(self) -> impl Future<Item = Self, Error = Error> {
        let subgraph_query = SubgraphEntity::query().filter(EntityFilter::new_equal(
            "name",
            self.subgraph_name.to_string(),
        ));

        debug!(
            self.logger,
            "Ensure that the Ethereum network subgraph exists"
        );

        // Do nothing if the subgraph already exists
        match self.store.find_one(subgraph_query.clone()) {
            Err(e) => return future::err(e.into()),
            Ok(Some(_)) => {
                debug!(self.logger, "Ethereum network subgraph already exists");
                return future::ok(self);
            }
            _ => (),
        };

        let mut ops = vec![];

        ops.push(MetadataOperation::AbortUnless {
            description: "Subgraph entity should not exist".to_owned(),
            query: subgraph_query,
            entity_ids: vec![],
        });

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

        // Create version entity
        let version_entity_id = generate_entity_id();
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

        ops.extend(SubgraphEntity::update_pending_version_operations(
            &subgraph_entity_id,
            None,
        ));

        ops.extend(SubgraphEntity::update_current_version_operations(
            &subgraph_entity_id,
            Some(version_entity_id),
        ));

        ops.push(MetadataOperation::AbortUnless {
            description: "Subgraph deployment entity must not exist".to_owned(),
            query: SubgraphDeploymentEntity::query()
                .filter(EntityFilter::new_equal("id", self.subgraph_id.to_string())),
            entity_ids: vec![],
        });

        // Create a "fake" manifest
        let manifest = SubgraphManifest {
            id: self.subgraph_id.clone(),
            location: self.subgraph_name.to_string(),
            spec_version: String::from("0.0.1"),
            description: None,
            repository: None,
            schema: Schema::parse(include_str!("./ethereum.graphql"), self.subgraph_id.clone())
                .expect("invalid Ethereum network subgraph schema"),
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
            SubgraphDeploymentAssignmentEntity::new(
                NodeId::new("__builtin").expect("invalid node ID: __builtin"),
            )
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

    fn ingest_next_blocks(self) -> impl Future<Item = Self, Error = Error> {
        let store_for_latest_block = self.store.clone();
        let subgraph_id_for_latest_block = self.subgraph_id.clone();

        let logger_for_latest_block = self.logger.clone();
        let logger_for_block_comparison = self.logger.clone();
        let logger_for_ingesting = self.logger.clone();
        let logger_for_fetching_hash = self.logger.clone();

        let eth_adapter_for_fetching_hash = self.eth_adapter.clone();

        // TODO:
        // - Handle block reorgs
        // - Incorporate mining rewards according to
        //   https://github.com/ethereum/wiki/wiki/Mining#mining-rewards
        // - Try to fetch block from the `ethereum_blocks` table first, only
        //   fall back to getting it from Ethereum if we don't have it
        // - Support truncated subgraph support where we only keep the most recent
        //   50 blocks or so around (like we do with the block ingestor)

        // Poll the latest chain head block from Ethereum
        self.eth_adapter
            .clone()
            .latest_block(&self.logger)
            .from_err()
            // Log chain head block and fail immediately if it is invalid (i.e.
            // is missing a block number and/or hash)
            .and_then(move |chain_head| {
                debug!(
                    logger_for_latest_block,
                    "Chain head";
                    "block_number" => format!("{:?}", chain_head.number),
                    "block_hash" => format!("{:?}", chain_head.hash),
                );

                if chain_head.number.is_none() || chain_head.hash.is_none() {
                    future::err(format_err!(
                        "chain head block has no block number and/or hash"
                    ))
                } else {
                    future::ok(chain_head)
                }
            })
            // Identify the block the Ethereum subgraph is on right now
            .and_then(move |chain_head| {
                store_for_latest_block
                    .clone()
                    .block_ptr(subgraph_id_for_latest_block.clone())
                    .map(|latest_block| (chain_head, latest_block))
            })
            // Log the this block
            .and_then(move |(chain_head, latest_block)| {
                debug!(
                    logger_for_block_comparison,
                    "Latest block";
                    "block_number" => latest_block.map_or(
                        String::from("None"),
                        |ptr| format!("{:?}", ptr.number)
                    ),
                    "block_hash" => latest_block.map_or(
                        String::from("None"),
                        |ptr| format!("{:?}", ptr.hash)
                    ),
                );

                future::ok((chain_head, latest_block))
            })
            // If the chain head and the latest block we have are on different
            // forks of the chain, roll back to the most recent common ancestor;
            // this then allows us to move forward towards the new chain head
            // .and_then(move |(chain_head, latest_block)| {
            //   Pseudo-algorithm:
            //   1. Find the most recent ancestor of the chain head block that
            //      is included in the subgraph (optimistically fetch ranges of blocks)
            //   2. If this isn't the `latest_block`:
            //      a. Revert this subgraph to this ancestor
            //      b. Use this ancestor as the `latest_block` for the futures below
            // }
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
                    logger_for_ingesting,
                    "Fetching {} of {} remaining blocks ({:?})",
                    blocks_to_ingest,
                    remaining_blocks,
                    block_range,
                );

                Box::new(
                    futures::stream::iter_ok::<_, Error>(block_range.map(move |block_number| {
                        let eth_adapter_for_fetching_block = eth_adapter_for_fetching_hash.clone();
                        let eth_adapter_for_full_block = eth_adapter_for_fetching_hash.clone();
                        let eth_adapter_for_uncles = eth_adapter_for_fetching_hash.clone();

                        let logger_for_fetching_block = logger_for_fetching_hash.clone();
                        let logger_for_full_block = logger_for_fetching_hash.clone();
                        let logger_for_uncles = logger_for_fetching_hash.clone();

                        eth_adapter_for_fetching_hash
                            .block_hash_by_block_number(&logger_for_fetching_hash, block_number)
                            .and_then(move |hash| {
                                let hash = hash.expect("no block hash returned for block number");
                                eth_adapter_for_fetching_block
                                    .block_by_hash(&logger_for_fetching_block, hash)
                            })
                            .from_err()
                            .and_then(move |block| {
                                let block = block.expect("no block returned for hash");
                                eth_adapter_for_full_block
                                    .load_full_block(&logger_for_full_block, block)
                                    .from_err()
                            })
                            .and_then(move |block| {
                                eth_adapter_for_uncles
                                    .uncles(
                                        &logger_for_uncles,
                                        block.block.hash.clone().unwrap(),
                                        block.block.uncles.len(),
                                    )
                                    .and_then(move |uncles| {
                                        future::ok(BlockWithUncles { block, uncles })
                                    })
                            })
                    }))
                    .from_err()
                    .buffered(50)
                    .fold(self, move |ingestor, block| {
                        ingestor
                            .ingest_block(block)
                            .and_then(move |ingestor| future::ok(ingestor))
                    }),
                ) as Box<dyn Future<Item = _, Error = _> + Send>
            })
    }

    fn ingest_block(self, mut block: BlockWithUncles) -> impl Future<Item = Self, Error = Error> {
        let hash = block.block.block.hash.clone().expect("block has no hash");
        let number = block
            .block
            .block
            .number
            .clone()
            .expect("block has no number");

        let logger = self.logger.new(o!(
            "block_hash" => format!("{:x}", hash),
            "block_number" => format!("{}", number),
        ));

        debug!(logger, "Ingest block");

        let logger_for_txes = logger.clone();
        let logger_for_mining_reward = logger.clone();

        // Create genesis block transactions from the chain config file,
        // because the initial account balances defined in in that file
        // are not represented as transactions in the genesis block
        if number.is_zero() {
            block.block.block.transactions =
                match self.create_genesis_transactions(&block.block.block) {
                    Ok(txes) => txes,
                    Err(e) => {
                        return Box::new(future::done(Err(e)))
                            as Box<dyn Future<Item = Self, Error = Error> + Send>
                    }
                }
        }

        let block = Arc::new(block);
        let block_for_mining_reward = block.clone();
        let block_for_ops = block.clone();

        let subgraph_id_for_block = self.subgraph_id.clone();

        // Add the block entity
        Box::new(
            self.set_entity(
                EntityCache::new(),
                EntityKey {
                    subgraph_id: subgraph_id_for_block,
                    entity_type: "Block".into(),
                    entity_id: block_id(&hash),
                },
                block.block.clone(),
            )
            .and_then(move |(ingestor, cache)| {
                ingestor.apply_mining_rewards(
                    logger_for_mining_reward,
                    cache,
                    block_for_mining_reward,
                )
            })
            .and_then(move |(ingestor, cache)| {
                // Create block transactions
                // TODO: Optimize away cloning all the transactions
                ingestor.ingest_transactions(
                    logger_for_txes,
                    cache,
                    block.clone(),
                    block.block.block.transactions.clone(),
                )
            })
            .and_then(move |(ingestor, cache)| {
                // Transact entity operations into the store
                let modifications = match cache.as_modifications(ingestor.store.as_ref()) {
                    Ok(mods) => mods,
                    Err(e) => return future::err(e.into()),
                };
                future::result(
                    ingestor
                        .store
                        .transact_block_operations(
                            ingestor.subgraph_id.clone(),
                            EthereumBlockPointer::from(&block_for_ops.block),
                            modifications,
                        )
                        .map_err(|e| e.into())
                        .map(|_| ingestor),
                )
            }),
        )
    }

    fn create_genesis_transactions(
        &self,
        block: &Block<Transaction>,
    ) -> Result<Vec<Transaction>, Error> {
        // The genesis block transactions come from the genesis block config
        // rather than being real transactions in the block on chain.

        Ok(self
            .network_config
            .accounts
            .iter()
            .filter(|(_, account)| account.balance.is_some())
            .enumerate()
            .map(|(index, (address, account))| Transaction {
                hash: H256::from(address.clone()),
                nonce: index.into(),
                block_hash: block.hash.clone(),
                block_number: block.number.clone().map(Into::into),
                transaction_index: Some(index.into()),
                from: H160::zero(),
                to: Some(address.clone()),
                value: account.balance.unwrap().clone(),
                gas_price: U256::zero(),
                gas: U256::zero(),
                input: Bytes::default(),
            })
            .collect())
    }

    fn apply_mining_rewards(
        self,
        logger: Logger,
        cache: EntityCache,
        block: Arc<BlockWithUncles>,
    ) -> NetworkIngestorResult<S> {
        let logger_for_uncles = logger.clone();

        // Block rewards based on
        // https://github.com/paritytech/parity-ethereum/blob/9c8b7c23d173f3a768550f956d0e0f34a9a74ae2/ethcore/engines/ethash/src/lib.rs#L250-L309
        // but without ECIP1017 eras support.

        let block_number = block.block.block.number.unwrap();
        let num_uncles = block.block.block.uncles.len();

        // Identify static mining reward for the current block number
        // FIXME: ethash is valid for mainnet but not for POA networks like kovan.
        let static_block_reward = self
            .network_config
            .engine
            .ethash
            .params
            .block_reward
            .range(..=block.block.block.number.expect("block has no number"))
            .last()
            .expect("no block reward defined for block")
            .1
            .clone();

        // Mining reward: reward + 1/32 * reward * num_uncles
        let mining_reward =
            static_block_reward + static_block_reward.shr(5) * U256::from(num_uncles);

        // Transaction fees
        let transaction_fees = block
            .block
            .block
            .transactions
            .iter()
            .map(|tx| tx.gas * tx.gas_price)
            .fold(U256::from(0), |sum, n| sum + n);

        // Apply the mining reward
        Box::new(
            self.update_account_balance(
                "block-mining",
                logger.clone(),
                cache,
                (block.block.block.author.clone(), "Account"),
                move |balance| balance + mining_reward + transaction_fees,
            )
            .and_then(move |(ingestor, cache)| {
                let uncle_numbers_and_miners: Vec<_> = block
                    .uncles
                    .iter()
                    .filter_map(|uncle| match uncle {
                        Some(uncle) => Some((
                            uncle.number.expect("uncle without number"),
                            uncle.author.clone(),
                        )),
                        None => None,
                    })
                    .collect();

                futures::stream::iter_ok(uncle_numbers_and_miners).fold(
                    (ingestor, cache),
                    move |(ingestor, cache), (uncle_number, uncle_miner)| {
                        ingestor.update_account_balance(
                            "uncle-mining",
                            logger_for_uncles.clone(),
                            cache,
                            (uncle_miner, "Account"),
                            move |balance| {
                                // Uncle reward: reward * (8 + uncle_number - block_number) / 8
                                balance
                                    + (static_block_reward
                                        * U256::from(U128::from(8) + uncle_number - block_number))
                                    .shr(3)
                            },
                        )
                    },
                )
            }),
        )
    }

    fn ingest_transactions(
        self,
        logger: Logger,
        cache: EntityCache,
        block: Arc<BlockWithUncles>,
        txes: Vec<Transaction>,
    ) -> NetworkIngestorResult<S> {
        // Extract block information
        let block_number = block.block.block.number.unwrap();

        // Transactions w/o receipts are only acceptable in the genesis
        // block, because we need the `gas_used` field of the receipt to
        // subtract the transaction fee (gas_used * gas_price) from the
        // sender's balance
        if block_number > 0.into()
            && txes.iter().any(|tx| {
                block
                    .block
                    .transaction_receipts
                    .iter()
                    .find(|receipt| receipt.transaction_hash == tx.hash)
                    .is_none()
            })
        {
            return Box::new(future::err(format_err!("no receipt for transaction")));
        }

        Box::new(futures::stream::iter_ok(txes.into_iter()).fold(
            (self, cache),
            move |(ingestor, cache), tx| {
                let logger = logger.new(o!("transaction_hash" => format!("{:x}", tx.hash)));
                let logger_for_receipt = logger.clone();
                let logger_for_sender = logger.clone();
                let logger_for_recipient = logger.clone();

                let subgraph_id = ingestor.subgraph_id.clone();

                debug!(logger, "Ingest transaction");

                // Obtain receipt if there is one
                let tx_receipt = block
                    .block
                    .transaction_receipts
                    .iter()
                    .find(|receipt| receipt.transaction_hash == tx.hash)
                    .cloned();

                let from = tx.from.clone();
                let to = match (
                    tx.to.clone().map(|address| (address, "Account")),
                    &tx_receipt,
                ) {
                    (Some((address, entity_type)), _) => (address, entity_type),
                    (None, Some(receipt)) => receipt
                        .contract_address
                        .clone()
                        .map(|address| (address, "Contract"))
                        .expect("`to` and `contract_address` are both missing"),
                    (None, None) => panic!("`to` and `receipt` are both missing"),
                };

                // Copy transaction amount for sender and recipient balances
                let amount_sender = tx.value.clone();
                let amount_recipient = tx.value.clone();

                // Calculate transaction fee; we can safely unwrap the `gas_used`
                // here, since we're not talking to an Ethereum light client
                //
                // If there is no receipt, we assume the transaction free is 0.
                // We already validate receipts on transactions for non-genesis
                // blocks above.
                let transaction_fee = match tx_receipt {
                    None => 0.into(),
                    Some(ref receipt) => receipt.gas_used.unwrap() * tx.gas_price,
                };

                // Add transaction entity
                ingestor
                    .set_entity(
                        cache,
                        EntityKey {
                            subgraph_id,
                            entity_type: "Transaction".into(),
                            entity_id: transaction_id(&tx.hash),
                        },
                        &tx,
                    )
                    // Add transaction receipt and log entities
                    .and_then(move |(ingestor, cache)| {
                        if let Some(receipt) = tx_receipt {
                            debug!(
                                logger_for_receipt,
                                "Ingest transaction receipt";
                                "entity_id" => receipt_id(&receipt.transaction_hash)
                            );
                            ingestor.ingest_transaction_receipt(cache, receipt)
                        } else {
                            Box::new(future::ok((ingestor, cache)))
                        }
                    })
                    // Update sender balance
                    .and_then(move |(ingestor, cache)| {
                        ingestor.update_account_balance(
                            "tx-sender",
                            logger_for_sender,
                            cache,
                            (from.clone(), "Account"),
                            |balance| {
                                if from == H160::zero() {
                                    balance
                                } else {
                                    balance - amount_sender - transaction_fee
                                }
                            },
                        )
                    })
                    // Update recipient balance
                    .and_then(move |(ingestor, cache)| {
                        ingestor.update_account_balance(
                            "tx-recipient",
                            logger_for_recipient,
                            cache,
                            to.clone(),
                            |balance| balance + amount_recipient,
                        )
                    })
            },
        ))
    }

    fn ingest_transaction_receipt(
        self,
        cache: EntityCache,
        receipt: TransactionReceipt,
    ) -> NetworkIngestorResult<S> {
        let subgraph_id = self.subgraph_id.clone();
        let subgraph_id_for_logs = subgraph_id.clone();

        Box::new(
            self.set_entity(
                cache,
                EntityKey {
                    subgraph_id,
                    entity_type: "TransactionReceipt".into(),
                    entity_id: receipt_id(&receipt.transaction_hash),
                },
                &receipt,
            )
            .and_then(|(ingestor, cache)| {
                futures::stream::iter_ok(receipt.logs.into_iter()).fold(
                    (ingestor, cache),
                    move |(ingestor, cache), log| {
                        ingestor.set_entity(
                            cache,
                            EntityKey {
                                subgraph_id: subgraph_id_for_logs.clone(),
                                entity_type: "Log".into(),
                                entity_id: log_id(
                                    &log.transaction_hash.unwrap(),
                                    &log.transaction_log_index.unwrap(),
                                ),
                            },
                            log,
                        )
                    },
                )
            }),
        )
    }

    fn set_entity(
        self,
        mut cache: EntityCache,
        key: EntityKey,
        value: impl ToEntity,
    ) -> NetworkIngestorResult<S> {
        cache.set(
            key,
            match value.to_entity() {
                Ok(entity) => entity,
                Err(e) => return Box::new(future::err(e.into())),
            },
        );
        Box::new(future::ok((self, cache)))
    }

    fn lookup_contract_or_account(
        store: Arc<S>,
        subgraph_id: &SubgraphDeploymentId,
        cache: &mut EntityCache,
        address: &H160,
    ) -> Option<(EntityKey, Entity)>
    where
        S: Store,
    {
        // Try to look the address up as a contract first
        let key = EntityKey {
            subgraph_id: subgraph_id.clone(),
            entity_type: "Contract".into(),
            entity_id: address_id(address),
        };

        if let Some(entity) = cache.get(store.as_ref(), &key).unwrap() {
            return Some((key, entity));
        }

        // Try to look the address up as a regular account
        let key = EntityKey {
            subgraph_id: subgraph_id.clone(),
            entity_type: "Account".into(),
            entity_id: address_id(address),
        };

        if let Some(entity) = cache.get(store.as_ref(), &key).unwrap() {
            return Some((key, entity));
        }

        None
    }

    fn update_account_balance<F>(
        self,
        reason: &'static str,
        logger: Logger,
        mut cache: EntityCache,
        account: (H160, &'static str),
        updater: F,
    ) -> NetworkIngestorResult<S>
    where
        F: FnOnce(U256) -> U256,
    {
        let address = account.0;
        let entity_type = account.1;
        let account_id = address_id(&address);

        // Fetch account entity
        let (key, mut entity) = Self::lookup_contract_or_account(
            self.store.clone(),
            &self.subgraph_id,
            &mut cache,
            &address,
        )
        .unwrap_or_else(|| {
            (
                EntityKey {
                    subgraph_id: self.subgraph_id.clone(),
                    entity_type: entity_type.clone().into(),
                    entity_id: account_id.clone(),
                },
                Entity::from(vec![
                    ("id", account_id.clone().into()),
                    ("address", address.into()),
                    ("balance", U256::zero().into()),
                ]),
            )
        });

        // Obtain miner balance from entity
        let old_balance: U256 = entity
            .get_value("balance")
            .expect("failed to obtain account balance")
            .expect("account balance missing");

        trace!(
            logger,
            "Update account balance";
            "account" => &account_id,
            "type" => entity_type,
            "reason" => reason,
            "balance_before" => format!("{}", &old_balance),
        );

        let new_balance = updater(old_balance);

        entity.set("balance", new_balance);

        trace!(
            logger,
            "Updated account balance";
            "account" => account_id,
            "type" => entity_type,
            "reason" => reason,
            "balance_before" => format!("{}", &old_balance),
            "balance_after" => format!("{}", &new_balance),
        );

        cache.set(key, entity);

        Box::new(future::ok((self, cache)))
    }
}

trait ToEntity {
    fn to_entity(&self) -> Result<Entity, Error>;
}

fn block_id(hash: &H256) -> String {
    format!("{:x}", hash)
}

fn transaction_id(hash: &H256) -> String {
    format!("{:x}", hash)
}

fn log_id(transaction_hash: &H256, index: &U256) -> String {
    format!("{}/logs/{}", transaction_hash, index)
}

fn address_id(address: &H160) -> String {
    format!("{:x}", address)
}

fn receipt_id(hash: &H256) -> String {
    format!("{}/receipt", transaction_id(hash))
}

impl ToEntity for EthereumBlock {
    fn to_entity(&self) -> Result<Entity, Error> {
        Ok(Entity::from(vec![
            ("id", block_id(&self.block.hash.unwrap()).into()),
            ("hash", self.block.hash.unwrap().into()),
            ("number", self.block.number.unwrap().as_u64().into()),
            ("timestamp", self.block.timestamp.into()),
            ("author", address_id(&self.block.author).into()),
            (
                "nonce",
                self.block.nonce.map_or(Value::Null, |nonce| nonce.into()),
            ),
            ("parent", self.block.parent_hash.into()),
            ("unclesHash", self.block.uncles_hash.into()),
            ("stateRoot", self.block.state_root.into()),
            ("transactionsRoot", self.block.transactions_root.into()),
            ("receiptsRoot", self.block.receipts_root.into()),
            ("gasUsed", self.block.gas_used.into()),
            ("gasLimit", self.block.gas_limit.into()),
            ("extraData", self.block.extra_data.clone().into()),
            ("difficulty", self.block.difficulty.into()),
            ("totalDifficulty", self.block.total_difficulty.into()),
            ("sealFields", self.block.seal_fields.clone().into()),
            ("size", self.block.size.into()),
            ("mixHash", self.block.mix_hash.into()),
        ] as Vec<(_, Value)>))
    }
}

impl ToEntity for Arc<EthereumBlock> {
    fn to_entity(&self) -> Result<Entity, Error> {
        self.as_ref().to_entity()
    }
}

impl ToEntity for &Transaction {
    fn to_entity(&self) -> Result<Entity, Error> {
        Ok(Entity::from(vec![
            ("id", transaction_id(&self.hash).into()),
            ("hash", self.hash.into()),
            ("block", block_id(&self.block_hash.unwrap()).into()),
            ("transactionIndex", self.transaction_index.into()),
            ("nonce", self.nonce.into()),
            ("from", address_id(&self.from).into()),
            (
                "to",
                self.to.map_or(Value::Null, |to| address_id(&to).into()),
            ),
            ("value", self.value.into()),
            // ("input", self.input.clone().into()),
            ("gasPrice", self.gas_price.into()),
            ("gas", self.gas.into()),
        ]))
    }
}

impl ToEntity for &TransactionReceipt {
    fn to_entity(&self) -> Result<Entity, Error> {
        Ok(Entity::from(vec![
            ("id", receipt_id(&self.transaction_hash).into()),
            ("transaction", transaction_id(&self.transaction_hash).into()),
            ("block", block_id(&self.block_hash.unwrap()).into()),
            ("cumulativeGasUsed", self.cumulative_gas_used.into()),
            (
                "gasUsed",
                self.gas_used
                    .map_or(Value::Null, |gas_used| gas_used.into()),
            ),
            (
                "contract",
                self.contract_address
                    .map_or(Value::Null, |to| address_id(&to).into()),
            ),
            ("logsBloom", self.logs_bloom.into()),
            (
                "status",
                self.status
                    .map(|status| match status.as_u64() {
                        1 => String::from("success"),
                        _ => String::from("failure"),
                    })
                    .into(),
            ),
        ]))
    }
}

impl ToEntity for Log {
    fn to_entity(&self) -> Result<Entity, Error> {
        Ok(Entity::from(vec![
            (
                "id",
                log_id(
                    &self.transaction_hash.unwrap(),
                    &self.transaction_log_index.unwrap(),
                )
                .into(),
            ),
            ("transaction", self.transaction_hash.into()),
            ("block", self.block_hash.unwrap().into()),
            (
                "receipt",
                receipt_id(&self.transaction_hash.unwrap()).into(),
            ),
            ("logIndex", self.log_index.into()),
            ("transactionLogIndex", self.transaction_log_index.into()),
            ("contract", address_id(&self.address).into()),
            ("topics", self.topics.clone().into()),
            ("data", self.data.clone().into()),
            ("logType", self.log_type.clone().into()),
            ("removed", self.removed.into()),
        ]))
    }
}
