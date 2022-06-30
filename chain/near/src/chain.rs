use graph::blockchain::BlockchainKind;
use graph::cheap_clone::CheapClone;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints};
use graph::prelude::{MetricsRegistry, TryFutureExt};
use graph::{
    anyhow,
    anyhow::Result,
    blockchain::{
        block_stream::{
            BlockStreamEvent, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        BlockHash, BlockPtr, Blockchain, IngestorError, RuntimeAdapter as RuntimeAdapterTrait,
    },
    components::store::DeploymentLocator,
    firehose::{self as firehose, ForkStep},
    prelude::{async_trait, o, BlockNumber, ChainStore, Error, Logger, LoggerFactory},
};
use prost::Message;
use std::sync::Arc;

use crate::adapter::TriggerFilter;
use crate::capabilities::NodeCapabilities;
use crate::data_source::{DataSourceTemplate, UnresolvedDataSourceTemplate};
use crate::runtime::RuntimeAdapter;
use crate::trigger::{self, NearTrigger};
use crate::{
    codec,
    data_source::{DataSource, UnresolvedDataSource},
};
use graph::blockchain::block_stream::{BlockStream, BlockStreamBuilder};

pub struct NearStreamBuilder {}

#[async_trait]
impl BlockStreamBuilder<Chain> for NearStreamBuilder {
    fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: Option<String>,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<<Chain as Blockchain>::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let adapter = chain
            .triggers_adapter(&deployment, &NodeCapabilities {}, unified_api_version)
            .expect(&format!("no adapter for network {}", chain.name,));

        let firehose_endpoint = match chain.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available")),
        };

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "FirehoseBlockStream"));

        let firehose_mapper = Arc::new(FirehoseMapper {
            endpoint: firehose_endpoint.cheap_clone(),
        });

        Ok(Box::new(FirehoseBlockStream::new(
            deployment.hash,
            firehose_endpoint,
            subgraph_current_block,
            block_cursor,
            firehose_mapper,
            adapter,
            filter,
            start_blocks,
            logger,
            chain.metrics_registry.clone(),
        )))
    }

    async fn build_polling(
        &self,
        _chain: Arc<Chain>,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<<Chain as Blockchain>::TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        todo!()
    }
}

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    firehose_endpoints: Arc<FirehoseEndpoints>,
    chain_store: Arc<dyn ChainStore>,
    metrics_registry: Arc<dyn MetricsRegistry>,
    block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: near")
    }
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: String,
        chain_store: Arc<dyn ChainStore>,
        firehose_endpoints: FirehoseEndpoints,
        metrics_registry: Arc<dyn MetricsRegistry>,
        block_stream_builder: Arc<dyn BlockStreamBuilder<Self>>,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            firehose_endpoints: Arc::new(firehose_endpoints),
            chain_store,
            metrics_registry,
            block_stream_builder,
        }
    }
}

#[async_trait]
impl Blockchain for Chain {
    const KIND: BlockchainKind = BlockchainKind::Near;

    type Block = codec::Block;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggerData = crate::trigger::NearTrigger;

    type MappingTrigger = crate::trigger::NearTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = crate::capabilities::NodeCapabilities;

    fn triggers_adapter(
        &self,
        _loc: &DeploymentLocator,
        _capabilities: &Self::NodeCapabilities,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapterTrait<Self>>, Error> {
        let adapter = TriggersAdapter {};
        Ok(Arc::new(adapter))
    }

    async fn new_firehose_block_stream(
        &self,
        deployment: DeploymentLocator,
        block_cursor: Option<String>,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        self.block_stream_builder.build_firehose(
            self,
            deployment,
            block_cursor,
            start_blocks,
            subgraph_current_block,
            filter,
            unified_api_version,
        )
    }

    async fn new_polling_block_stream(
        &self,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<Self::TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        panic!("NEAR does not support polling block stream")
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available").into()),
        };

        firehose_endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
            .map_err(Into::into)
            .await
    }

    fn runtime_adapter(&self) -> Arc<dyn RuntimeAdapterTrait<Self>> {
        Arc::new(RuntimeAdapter {})
    }

    fn is_firehose_supported(&self) -> bool {
        true
    }
}

pub struct TriggersAdapter {}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        block: codec::Block,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        // TODO: Find the best place to introduce an `Arc` and avoid this clone.
        let shared_block = Arc::new(block.clone());

        let TriggerFilter {
            block_filter,
            receipt_filter,
        } = filter;

        // Filter non-successful or non-action receipts.
        let receipts = block.shards.iter().flat_map(|shard| {
            shard
                .receipt_execution_outcomes
                .iter()
                .filter_map(|outcome| {
                    if !outcome
                        .execution_outcome
                        .as_ref()?
                        .outcome
                        .as_ref()?
                        .status
                        .as_ref()?
                        .is_success()
                    {
                        return None;
                    }
                    if !matches!(
                        outcome.receipt.as_ref()?.receipt,
                        Some(codec::receipt::Receipt::Action(_))
                    ) {
                        return None;
                    }

                    let receipt = outcome.receipt.as_ref()?.clone();
                    if !receipt_filter.matches(&receipt.receiver_id) {
                        return None;
                    }

                    Some(trigger::ReceiptWithOutcome {
                        outcome: outcome.execution_outcome.as_ref()?.clone(),
                        receipt,
                        block: shared_block.cheap_clone(),
                    })
                })
        });

        let mut trigger_data: Vec<_> = receipts
            .map(|r| NearTrigger::Receipt(Arc::new(r)))
            .collect();

        if block_filter.trigger_every_block {
            trigger_data.push(NearTrigger::Block(shared_block.cheap_clone()));
        }

        Ok(BlockWithTriggers::new(block, trigger_data))
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        panic!("Should never be called since not used by FirehoseBlockStream")
    }

    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<codec::Block>, Error> {
        panic!("Should never be called since FirehoseBlockStream cannot resolve it")
    }

    /// Panics if `block` is genesis.
    /// But that's ok since this is only called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        // FIXME (NEAR):  Might not be necessary for NEAR support for now
        Ok(Some(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: block.number.saturating_sub(1),
        }))
    }
}

pub struct FirehoseMapper {
    endpoint: Arc<FirehoseEndpoint>,
}

#[async_trait]
impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    async fn to_block_stream_event(
        &self,
        logger: &Logger,
        response: &firehose::Response,
        adapter: &Arc<dyn TriggersAdapterTrait<Chain>>,
        filter: &TriggerFilter,
    ) -> Result<BlockStreamEvent<Chain>, FirehoseError> {
        let step = ForkStep::from_i32(response.step).unwrap_or_else(|| {
            panic!(
                "unknown step i32 value {}, maybe you forgot update & re-regenerate the protobuf definitions?",
                response.step
            )
        });

        let any_block = response
            .block
            .as_ref()
            .expect("block payload information should always be present");

        // Right now, this is done in all cases but in reality, with how the BlockStreamEvent::Revert
        // is defined right now, only block hash and block number is necessary. However, this information
        // is not part of the actual bstream::BlockResponseV2 payload. As such, we need to decode the full
        // block which is useless.
        //
        // Check about adding basic information about the block in the bstream::BlockResponseV2 or maybe
        // define a slimmed down stuct that would decode only a few fields and ignore all the rest.
        let block = codec::Block::decode(any_block.value.as_ref())?;

        use ForkStep::*;
        match step {
            StepNew => Ok(BlockStreamEvent::ProcessBlock(
                adapter.triggers_in_block(logger, block, filter).await?,
                Some(response.cursor.clone()),
            )),

            StepUndo => {
                let parent_ptr = block
                    .header()
                    .parent_ptr()
                    .expect("Genesis block should never be reverted");

                Ok(BlockStreamEvent::Revert(
                    parent_ptr,
                    Some(response.cursor.clone()),
                ))
            }

            StepIrreversible => {
                panic!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            StepUnknown => {
                panic!("unknown step should not happen in the Firehose response")
            }
        }
    }

    async fn block_ptr_for_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, Error> {
        self.endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, number)
            .await
    }

    async fn final_block_ptr_for(
        &self,
        logger: &Logger,
        block: &codec::Block,
    ) -> Result<BlockPtr, Error> {
        let final_block_number = block.header().last_final_block_height as BlockNumber;

        self.endpoint
            .block_ptr_for_number::<codec::HeaderOnlyBlock>(logger, final_block_number)
            .await
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::Arc, vec};

    use graph::{
        blockchain::{block_stream::BlockWithTriggers, DataSource as _, TriggersAdapter as _},
        prelude::{tokio, Link},
        semver::Version,
        slog::{self, o, Logger},
    };

    use crate::{
        adapter::{NearReceiptFilter, TriggerFilter},
        codec::{
            self, execution_outcome,
            receipt::{self},
            Block, BlockHeader, DataReceiver, ExecutionOutcome, ExecutionOutcomeWithId,
            IndexerExecutionOutcomeWithReceipt, IndexerShard, ReceiptAction,
            SuccessValueExecutionStatus,
        },
        data_source::{DataSource, Mapping, PartialAccounts, ReceiptHandler, NEAR_KIND},
        trigger::{NearTrigger, ReceiptWithOutcome},
        Chain,
    };

    use super::TriggersAdapter;

    #[test]
    fn validate_empty() {
        let ds = new_data_source(None, None);
        let errs = ds.validate();
        assert_eq!(errs.len(), 1, "{:?}", ds);
        assert_eq!(errs[0].to_string(), "subgraph source address is required");
    }

    #[test]
    fn validate_empty_account_none_partial() {
        let ds = new_data_source(None, Some(PartialAccounts::default()));
        let errs = ds.validate();
        assert_eq!(errs.len(), 1, "{:?}", ds);
        assert_eq!(errs[0].to_string(), "subgraph source address is required");
    }

    #[test]
    fn validate_empty_account() {
        let ds = new_data_source(
            None,
            Some(PartialAccounts {
                prefixes: vec![],
                suffixes: vec!["x.near".to_string()],
            }),
        );
        let errs = ds.validate();
        assert_eq!(errs.len(), 0, "{:?}", ds);
    }

    #[test]
    fn validate_empty_prefix_and_suffix_values() {
        let ds = new_data_source(
            None,
            Some(PartialAccounts {
                prefixes: vec!["".to_string()],
                suffixes: vec!["".to_string()],
            }),
        );
        let errs: Vec<String> = ds
            .validate()
            .into_iter()
            .map(|err| err.to_string())
            .collect();
        assert_eq!(errs.len(), 2, "{:?}", ds);

        let expected_errors = vec![
            "partial account prefixes can't have empty values".to_string(),
            "partial account suffixes can't have empty values".to_string(),
        ];
        assert_eq!(
            true,
            expected_errors.iter().all(|err| errs.contains(err)),
            "{:?}",
            errs
        );
    }

    #[test]
    fn validate_empty_partials() {
        let ds = new_data_source(Some("x.near".to_string()), None);
        let errs = ds.validate();
        assert_eq!(errs.len(), 0, "{:?}", ds);
    }

    #[test]
    fn receipt_filter_from_ds() {
        struct Case {
            name: String,
            account: Option<String>,
            partial_accounts: Option<PartialAccounts>,
            expected: HashSet<(Option<String>, Option<String>)>,
        }

        let cases = vec![
            Case {
                name: "2 prefix && 1 suffix".into(),
                account: None,
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec!["a".to_string(), "b".to_string()],
                    suffixes: vec!["d".to_string()],
                }),
                expected: HashSet::from_iter(vec![
                    (Some("a".to_string()), Some("d".to_string())),
                    (Some("b".to_string()), Some("d".to_string())),
                ]),
            },
            Case {
                name: "1 prefix && 2 suffix".into(),
                account: None,
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec!["a".to_string()],
                    suffixes: vec!["c".to_string(), "d".to_string()],
                }),
                expected: HashSet::from_iter(vec![
                    (Some("a".to_string()), Some("c".to_string())),
                    (Some("a".to_string()), Some("d".to_string())),
                ]),
            },
            Case {
                name: "no prefix".into(),
                account: None,
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec![],
                    suffixes: vec!["c".to_string(), "d".to_string()],
                }),
                expected: HashSet::from_iter(vec![
                    (None, Some("c".to_string())),
                    (None, Some("d".to_string())),
                ]),
            },
            Case {
                name: "no suffix".into(),
                account: None,
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec!["a".to_string(), "b".to_string()],
                    suffixes: vec![],
                }),
                expected: HashSet::from_iter(vec![
                    (Some("a".to_string()), None),
                    (Some("b".to_string()), None),
                ]),
            },
        ];

        for case in cases.into_iter() {
            let ds1 = new_data_source(case.account, None);
            let ds2 = new_data_source(None, case.partial_accounts);

            let receipt = NearReceiptFilter::from_data_sources(vec![&ds1, &ds2]);
            assert_eq!(
                receipt.partial_accounts.len(),
                case.expected.len(),
                "name: {}\npartial_accounts: {:?}",
                case.name,
                receipt.partial_accounts,
            );
            assert_eq!(
                true,
                case.expected
                    .iter()
                    .all(|x| receipt.partial_accounts.contains(&x)),
                "name: {}\npartial_accounts: {:?}",
                case.name,
                receipt.partial_accounts,
            );
        }
    }

    #[test]
    fn data_source_match_and_decode() {
        struct Request {
            account: String,
            matches: bool,
        }
        struct Case {
            name: String,
            account: Option<String>,
            partial_accounts: Option<PartialAccounts>,
            expected: Vec<Request>,
        }

        let cases = vec![
            Case {
                name: "2 prefix && 1 suffix".into(),
                account: None,
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec!["a".to_string(), "b".to_string()],
                    suffixes: vec!["d".to_string()],
                }),
                expected: vec![
                    Request {
                        account: "ssssssd".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "asasdasdas".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "asd".to_string(),
                        matches: true,
                    },
                    Request {
                        account: "bsd".to_string(),
                        matches: true,
                    },
                ],
            },
            Case {
                name: "1 prefix && 2 suffix".into(),
                account: None,
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec!["a".to_string()],
                    suffixes: vec!["c".to_string(), "d".to_string()],
                }),
                expected: vec![
                    Request {
                        account: "ssssssd".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "asasdasdas".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "asdc".to_string(),
                        matches: true,
                    },
                    Request {
                        account: "absd".to_string(),
                        matches: true,
                    },
                ],
            },
            Case {
                name: "no prefix with exact match".into(),
                account: Some("bsda".to_string()),
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec![],
                    suffixes: vec!["c".to_string(), "d".to_string()],
                }),
                expected: vec![
                    Request {
                        account: "ssssss".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "asasdasdas".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "asdasdasdasdc".to_string(),
                        matches: true,
                    },
                    Request {
                        account: "bsd".to_string(),
                        matches: true,
                    },
                    Request {
                        account: "bsda".to_string(),
                        matches: true,
                    },
                ],
            },
            Case {
                name: "no suffix with exact match".into(),
                account: Some("zbsd".to_string()),
                partial_accounts: Some(PartialAccounts {
                    prefixes: vec!["a".to_string(), "b".to_string()],
                    suffixes: vec![],
                }),
                expected: vec![
                    Request {
                        account: "ssssssd".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "zasdasdas".to_string(),
                        matches: false,
                    },
                    Request {
                        account: "asa".to_string(),
                        matches: true,
                    },
                    Request {
                        account: "bsb".to_string(),
                        matches: true,
                    },
                    Request {
                        account: "zbsd".to_string(),
                        matches: true,
                    },
                ],
            },
        ];

        let logger = Logger::root(slog::Discard, o!());
        for case in cases.into_iter() {
            let ds = new_data_source(case.account, case.partial_accounts);
            let filter = NearReceiptFilter::from_data_sources(vec![&ds]);

            for req in case.expected {
                let res = filter.matches(&req.account);
                assert_eq!(
                    res, req.matches,
                    "name: {} request:{} failed",
                    case.name, req.account
                );

                let block = Arc::new(new_success_block(11, &req.account));
                let receipt = Arc::new(new_receipt_with_outcome(&req.account, block.clone()));
                let res = ds
                    .match_and_decode(&NearTrigger::Receipt(receipt.clone()), &block, &logger)
                    .expect("unable to process block");
                assert_eq!(
                    req.matches,
                    res.is_some(),
                    "case name: {} req: {}",
                    case.name,
                    req.account
                );
            }
        }
    }

    #[tokio::test]
    async fn test_trigger_filter_empty() {
        let account1: String = "account1".into();

        let adapter = TriggersAdapter {};

        let logger = Logger::root(slog::Discard, o!());
        let block1 = new_success_block(1, &account1);

        let filter = TriggerFilter::default();

        let block_with_triggers: BlockWithTriggers<Chain> = adapter
            .triggers_in_block(&logger, block1, &filter)
            .await
            .expect("failed to execute triggers_in_block");
        assert_eq!(block_with_triggers.trigger_count(), 0);
    }

    #[tokio::test]
    async fn test_trigger_filter_every_block() {
        let account1: String = "account1".into();

        let adapter = TriggersAdapter {};

        let logger = Logger::root(slog::Discard, o!());
        let block1 = new_success_block(1, &account1);

        let filter = TriggerFilter {
            block_filter: crate::adapter::NearBlockFilter {
                trigger_every_block: true,
            },
            ..Default::default()
        };

        let block_with_triggers: BlockWithTriggers<Chain> = adapter
            .triggers_in_block(&logger, block1, &filter)
            .await
            .expect("failed to execute triggers_in_block");
        assert_eq!(block_with_triggers.trigger_count(), 1);

        let height: Vec<u64> = heights_from_triggers(&block_with_triggers);
        assert_eq!(height, vec![1]);
    }

    #[tokio::test]
    async fn test_trigger_filter_every_receipt() {
        let account1: String = "account1".into();

        let adapter = TriggersAdapter {};

        let logger = Logger::root(slog::Discard, o!());
        let block1 = new_success_block(1, &account1);

        let filter = TriggerFilter {
            receipt_filter: NearReceiptFilter {
                accounts: HashSet::from_iter(vec![account1]),
                partial_accounts: HashSet::new(),
            },
            ..Default::default()
        };

        let block_with_triggers: BlockWithTriggers<Chain> = adapter
            .triggers_in_block(&logger, block1, &filter)
            .await
            .expect("failed to execute triggers_in_block");
        assert_eq!(block_with_triggers.trigger_count(), 1);

        let height: Vec<u64> = heights_from_triggers(&block_with_triggers);
        assert_eq!(height.len(), 0);
    }

    fn heights_from_triggers(block: &BlockWithTriggers<Chain>) -> Vec<u64> {
        block
            .trigger_data
            .clone()
            .into_iter()
            .filter_map(|x| match x {
                crate::trigger::NearTrigger::Block(b) => b.header.clone().map(|x| x.height),
                _ => None,
            })
            .collect()
    }

    fn new_success_block(height: u64, receiver_id: &String) -> codec::Block {
        codec::Block {
            header: Some(BlockHeader {
                height,
                ..Default::default()
            }),
            shards: vec![IndexerShard {
                receipt_execution_outcomes: vec![IndexerExecutionOutcomeWithReceipt {
                    receipt: Some(crate::codec::Receipt {
                        receipt: Some(receipt::Receipt::Action(ReceiptAction {
                            output_data_receivers: vec![DataReceiver {
                                receiver_id: receiver_id.clone(),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        receiver_id: receiver_id.clone(),
                        ..Default::default()
                    }),
                    execution_outcome: Some(ExecutionOutcomeWithId {
                        outcome: Some(ExecutionOutcome {
                            status: Some(execution_outcome::Status::SuccessValue(
                                SuccessValueExecutionStatus::default(),
                            )),

                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn new_data_source(
        account: Option<String>,
        partial_accounts: Option<PartialAccounts>,
    ) -> DataSource {
        DataSource {
            kind: NEAR_KIND.to_string(),
            network: None,
            name: "asd".to_string(),
            source: crate::data_source::Source {
                account,
                start_block: 10,
                accounts: partial_accounts,
            },
            mapping: Mapping {
                api_version: Version::parse("1.0.0").expect("unable to parse version"),
                language: "".to_string(),
                entities: vec![],
                block_handlers: vec![],
                receipt_handlers: vec![ReceiptHandler {
                    handler: "asdsa".to_string(),
                }],
                runtime: Arc::new(vec![]),
                link: Link::default(),
            },
            context: Arc::new(None),
            creation_block: None,
        }
    }

    fn new_receipt_with_outcome(receiver_id: &String, block: Arc<Block>) -> ReceiptWithOutcome {
        ReceiptWithOutcome {
            outcome: ExecutionOutcomeWithId {
                outcome: Some(ExecutionOutcome {
                    status: Some(execution_outcome::Status::SuccessValue(
                        SuccessValueExecutionStatus::default(),
                    )),

                    ..Default::default()
                }),
                ..Default::default()
            },
            receipt: codec::Receipt {
                receipt: Some(receipt::Receipt::Action(ReceiptAction {
                    output_data_receivers: vec![DataReceiver {
                        receiver_id: receiver_id.clone(),
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                receiver_id: receiver_id.clone(),
                ..Default::default()
            },
            block,
        }
    }
}
