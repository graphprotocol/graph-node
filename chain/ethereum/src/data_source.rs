use anyhow::{anyhow, Error};
use anyhow::{ensure, Context};
use graph::blockchain::{BlockPtr, TriggerWithHandler};
use graph::components::metrics::subgraph::SubgraphInstanceMetrics;
use graph::components::store::{EthereumCallCache, StoredDynamicDataSource};
use graph::components::subgraph::{HostMetrics, InstanceDSTemplateInfo, MappingError};
use graph::components::trigger_processor::RunnableTriggers;
use graph::data::value::Word;
use graph::data_source::CausalityRegion;
use graph::env::ENV_VARS;
use graph::futures03::future::try_join;
use graph::futures03::stream::FuturesOrdered;
use graph::futures03::TryStreamExt;
use graph::prelude::ethabi::ethereum_types::H160;
use graph::prelude::ethabi::{StateMutability, Token};
use graph::prelude::lazy_static;
use graph::prelude::regex::Regex;
use graph::prelude::{Link, SubgraphManifestValidationError};
use graph::slog::{debug, error, o, trace};
use itertools::Itertools;
use serde::de;
use serde::de::Error as ErrorD;
use serde::{Deserialize, Deserializer};
use std::collections::HashSet;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tiny_keccak::{keccak256, Keccak};

use graph::{
    blockchain::{self, Blockchain},
    derive::CheapClone,
    prelude::{
        async_trait,
        ethabi::{Address, Contract, Event, Function, LogParam, ParamType, RawLog},
        serde_json, warn,
        web3::types::{Log, Transaction, H256},
        BlockNumber, CheapClone, EthereumCall, LightEthereumBlock, LightEthereumBlockExt,
        LinkResolver, Logger,
    },
};

use graph::data::subgraph::{
    calls_host_fn, DataSourceContext, Source, MIN_SPEC_VERSION, SPEC_VERSION_0_0_8,
    SPEC_VERSION_1_2_0,
};

use crate::adapter::EthereumAdapter as _;
use crate::chain::Chain;
use crate::network::EthereumNetworkAdapters;
use crate::trigger::{EthereumBlockTriggerType, EthereumTrigger, MappingTrigger};
use crate::{ContractCall, NodeCapabilities};

// The recommended kind is `ethereum`, `ethereum/contract` is accepted for backwards compatibility.
const ETHEREUM_KINDS: &[&str] = &["ethereum/contract", "ethereum"];
const EVENT_HANDLER_KIND: &str = "event";
const CALL_HANDLER_KIND: &str = "call";
const BLOCK_HANDLER_KIND: &str = "block";

/// Runtime representation of a data source.
// Note: Not great for memory usage that this needs to be `Clone`, considering how there may be tens
// of thousands of data sources in memory at once.
#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub manifest_idx: u32,
    pub address: Option<Address>,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
    pub contract_abi: Arc<MappingABI>,
}

impl blockchain::DataSource<Chain> for DataSource {
    fn from_template_info(
        info: InstanceDSTemplateInfo,
        ds_template: &graph::data_source::DataSourceTemplate<Chain>,
    ) -> Result<Self, Error> {
        // Note: There clearly is duplication between the data in `ds_template and the `template`
        // field here. Both represent a template definition, would be good to unify them.
        let InstanceDSTemplateInfo {
            template: _,
            params,
            context,
            creation_block,
        } = info;

        let template = ds_template.as_onchain().ok_or(anyhow!(
            "Cannot create onchain data source from offchain template"
        ))?;

        // Obtain the address from the parameters
        let string = params
            .get(0)
            .with_context(|| {
                format!(
                    "Failed to create data source from template `{}`: address parameter is missing",
                    template.name
                )
            })?
            .trim_start_matches("0x");

        let address = Address::from_str(string).with_context(|| {
            format!(
                "Failed to create data source from template `{}`, invalid address provided",
                template.name
            )
        })?;

        let contract_abi = template
            .mapping
            .find_abi(&template.source.abi)
            .with_context(|| format!("template `{}`", template.name))?;

        Ok(DataSource {
            kind: template.kind.clone(),
            network: template.network.clone(),
            name: template.name.clone(),
            manifest_idx: template.manifest_idx,
            address: Some(address),
            start_block: creation_block,
            end_block: None,
            mapping: template.mapping.clone(),
            context: Arc::new(context),
            creation_block: Some(creation_block),
            contract_abi,
        })
    }

    fn address(&self) -> Option<&[u8]> {
        self.address.as_ref().map(|x| x.as_bytes())
    }

    fn has_declared_calls(&self) -> bool {
        self.mapping
            .event_handlers
            .iter()
            .any(|handler| !handler.calls.decls.is_empty())
    }

    fn handler_kinds(&self) -> HashSet<&str> {
        let mut kinds = HashSet::new();

        let Mapping {
            event_handlers,
            call_handlers,
            block_handlers,
            ..
        } = &self.mapping;

        if !event_handlers.is_empty() {
            kinds.insert(EVENT_HANDLER_KIND);
        }
        if !call_handlers.is_empty() {
            kinds.insert(CALL_HANDLER_KIND);
        }
        for handler in block_handlers.iter() {
            kinds.insert(handler.kind());
        }

        kinds
    }

    fn start_block(&self) -> BlockNumber {
        self.start_block
    }

    fn end_block(&self) -> Option<BlockNumber> {
        self.end_block
    }

    fn match_and_decode(
        &self,
        trigger: &<Chain as Blockchain>::TriggerData,
        block: &Arc<<Chain as Blockchain>::Block>,
        logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<Chain>>, Error> {
        let block = block.light_block();
        self.match_and_decode(trigger, block, logger)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn network(&self) -> Option<&str> {
        self.network.as_deref()
    }

    fn context(&self) -> Arc<Option<DataSourceContext>> {
        self.context.cheap_clone()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        self.creation_block
    }

    fn is_duplicate_of(&self, other: &Self) -> bool {
        let DataSource {
            kind,
            network,
            name,
            manifest_idx,
            address,
            mapping,
            context,
            // The creation block is ignored for detection duplicate data sources.
            // Contract ABI equality is implicit in `mapping.abis` equality.
            creation_block: _,
            contract_abi: _,
            start_block: _,
            end_block: _,
        } = self;

        // mapping_request_sender, host_metrics, and (most of) host_exports are operational structs
        // used at runtime but not needed to define uniqueness; each runtime host should be for a
        // unique data source.
        kind == &other.kind
            && network == &other.network
            && name == &other.name
            && manifest_idx == &other.manifest_idx
            && address == &other.address
            && mapping.abis == other.mapping.abis
            && mapping.event_handlers == other.mapping.event_handlers
            && mapping.call_handlers == other.mapping.call_handlers
            && mapping.block_handlers == other.mapping.block_handlers
            && context == &other.context
    }

    fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        let param = self.address.map(|addr| addr.0.into());
        StoredDynamicDataSource {
            manifest_idx: self.manifest_idx,
            param,
            context: self
                .context
                .as_ref()
                .as_ref()
                .map(|ctx| serde_json::to_value(ctx).unwrap()),
            creation_block: self.creation_block,
            done_at: None,
            causality_region: CausalityRegion::ONCHAIN,
        }
    }

    fn from_stored_dynamic_data_source(
        template: &DataSourceTemplate,
        stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        let StoredDynamicDataSource {
            manifest_idx,
            param,
            context,
            creation_block,
            done_at,
            causality_region,
        } = stored;

        ensure!(
            causality_region == CausalityRegion::ONCHAIN,
            "stored ethereum data source has causality region {}, expected root",
            causality_region
        );
        ensure!(done_at.is_none(), "onchain data sources are never done");

        let context = context.map(serde_json::from_value).transpose()?;

        let contract_abi = template.mapping.find_abi(&template.source.abi)?;

        let address = param.map(|x| H160::from_slice(&x));
        Ok(DataSource {
            kind: template.kind.to_string(),
            network: template.network.as_ref().map(|s| s.to_string()),
            name: template.name.clone(),
            manifest_idx,
            address,
            start_block: creation_block.unwrap_or(0),
            end_block: None,
            mapping: template.mapping.clone(),
            context: Arc::new(context),
            creation_block,
            contract_abi,
        })
    }

    fn validate(&self, spec_version: &semver::Version) -> Vec<Error> {
        let mut errors = vec![];

        if !ETHEREUM_KINDS.contains(&self.kind.as_str()) {
            errors.push(anyhow!(
                "data source has invalid `kind`, expected `ethereum` but found {}",
                self.kind
            ))
        }

        // Validate that there is a `source` address if there are call or block handlers
        let no_source_address = self.address().is_none();
        let has_call_handlers = !self.mapping.call_handlers.is_empty();
        let has_block_handlers = !self.mapping.block_handlers.is_empty();
        if no_source_address && (has_call_handlers || has_block_handlers) {
            errors.push(SubgraphManifestValidationError::SourceAddressRequired.into());
        };

        // Ensure that there is at most one instance of each type of block handler
        // and that a combination of a non-filtered block handler and a filtered block handler is not allowed.

        let mut non_filtered_block_handler_count = 0;
        let mut call_filtered_block_handler_count = 0;
        let mut polling_filtered_block_handler_count = 0;
        let mut initialization_handler_count = 0;
        self.mapping
            .block_handlers
            .iter()
            .for_each(|block_handler| {
                match block_handler.filter {
                    None => non_filtered_block_handler_count += 1,
                    Some(ref filter) => match filter {
                        BlockHandlerFilter::Call => call_filtered_block_handler_count += 1,
                        BlockHandlerFilter::Once => initialization_handler_count += 1,
                        BlockHandlerFilter::Polling { every: _ } => {
                            polling_filtered_block_handler_count += 1
                        }
                    },
                };
            });

        let has_non_filtered_block_handler = non_filtered_block_handler_count > 0;
        // If there is a non-filtered block handler, we need to check if there are any
        // filtered block handlers except for the ones with call filter
        // If there are, we do not allow that combination
        let has_restricted_filtered_and_non_filtered_combination = has_non_filtered_block_handler
            && (polling_filtered_block_handler_count > 0 || initialization_handler_count > 0);

        if has_restricted_filtered_and_non_filtered_combination {
            errors.push(anyhow!(
                "data source has a combination of filtered and non-filtered block handlers that is not allowed"
            ));
        }

        // Check the number of handlers for each type
        // If there is more than one of any type, we have too many handlers
        let has_too_many = non_filtered_block_handler_count > 1
            || call_filtered_block_handler_count > 1
            || initialization_handler_count > 1
            || polling_filtered_block_handler_count > 1;

        if has_too_many {
            errors.push(anyhow!("data source has duplicated block handlers"));
        }

        // Validate that event handlers don't require receipts for API versions lower than 0.0.7
        let api_version = self.api_version();
        if api_version < semver::Version::new(0, 0, 7) {
            for event_handler in &self.mapping.event_handlers {
                if event_handler.receipt {
                    errors.push(anyhow!(
                        "data source has event handlers that require transaction receipts, but this \
                         is only supported for apiVersion >= 0.0.7"
                    ));
                    break;
                }
            }
        }

        if spec_version < &SPEC_VERSION_1_2_0 {
            for handler in &self.mapping.event_handlers {
                if !handler.calls.decls.is_empty() {
                    errors.push(anyhow!(
                        "handler {}: declaring eth calls on handlers is only supported for specVersion >= 1.2.0", handler.event
                    ));
                    break;
                }
            }
        }

        for handler in &self.mapping.event_handlers {
            for call in handler.calls.decls.as_ref() {
                match self.mapping.find_abi(&call.expr.abi) {
                    // TODO: Handle overloaded functions by passing a signature
                    Ok(abi) => match abi.function(&call.expr.abi, &call.expr.func, None) {
                        Ok(_) => {}
                        Err(e) => {
                            errors.push(e);
                        }
                    },
                    Err(e) => {
                        errors.push(e);
                    }
                }
            }
        }
        errors
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn min_spec_version(&self) -> semver::Version {
        let mut min_version = MIN_SPEC_VERSION;

        for handler in &self.mapping.block_handlers {
            match handler.filter {
                Some(BlockHandlerFilter::Polling { every: _ }) | Some(BlockHandlerFilter::Once) => {
                    min_version = std::cmp::max(min_version, SPEC_VERSION_0_0_8);
                }
                _ => {}
            }
        }

        for handler in &self.mapping.event_handlers {
            if handler.has_additional_topics() {
                min_version = std::cmp::max(min_version, SPEC_VERSION_1_2_0);
            }
        }

        min_version
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        Some(self.mapping.runtime.cheap_clone())
    }
}

impl DataSource {
    fn from_manifest(
        kind: String,
        network: Option<String>,
        name: String,
        source: Source,
        mapping: Mapping,
        context: Option<DataSourceContext>,
        manifest_idx: u32,
    ) -> Result<Self, Error> {
        // Data sources in the manifest are created "before genesis" so they have no creation block.
        let creation_block = None;
        let contract_abi = mapping
            .find_abi(&source.abi)
            .with_context(|| format!("data source `{}`", name))?;

        Ok(DataSource {
            kind,
            network,
            name,
            manifest_idx,
            address: source.address,
            start_block: source.start_block,
            end_block: source.end_block,
            mapping,
            context: Arc::new(context),
            creation_block,
            contract_abi,
        })
    }

    fn handlers_for_log(&self, log: &Log) -> Vec<MappingEventHandler> {
        self.mapping
            .event_handlers
            .iter()
            .filter(|handler| handler.matches(&log))
            .cloned()
            .collect::<Vec<_>>()
    }

    fn handler_for_call(&self, call: &EthereumCall) -> Result<Option<&MappingCallHandler>, Error> {
        // First four bytes of the input for the call are the first four
        // bytes of hash of the function signature
        ensure!(
            call.input.0.len() >= 4,
            "Ethereum call has input with less than 4 bytes"
        );

        let target_method_id = &call.input.0[..4];

        Ok(self.mapping.call_handlers.iter().find(move |handler| {
            let fhash = keccak256(handler.function.as_bytes());
            let actual_method_id = [fhash[0], fhash[1], fhash[2], fhash[3]];
            target_method_id == actual_method_id
        }))
    }

    fn handler_for_block(
        &self,
        trigger_type: &EthereumBlockTriggerType,
        block: BlockNumber,
    ) -> Option<&MappingBlockHandler> {
        match trigger_type {
            // Start matches only initialization handlers with a `once` filter
            EthereumBlockTriggerType::Start => {
                self.mapping
                    .block_handlers
                    .iter()
                    .find(move |handler| match handler.filter {
                        Some(BlockHandlerFilter::Once) => block == self.start_block,
                        _ => false,
                    })
            }
            // End matches all handlers without a filter or with a `polling` filter
            EthereumBlockTriggerType::End => {
                self.mapping
                    .block_handlers
                    .iter()
                    .find(move |handler| match handler.filter {
                        Some(BlockHandlerFilter::Polling { every }) => {
                            let start_block = self.start_block;
                            let should_trigger = (block - start_block) % every.get() as i32 == 0;
                            should_trigger
                        }
                        None => true,
                        _ => false,
                    })
            }
            EthereumBlockTriggerType::WithCallTo(_address) => self
                .mapping
                .block_handlers
                .iter()
                .find(move |handler| handler.filter == Some(BlockHandlerFilter::Call)),
        }
    }

    /// Returns the contract event with the given signature, if it exists. A an event from the ABI
    /// will be matched if:
    /// 1. An event signature is equal to `signature`.
    /// 2. There are no equal matches, but there is exactly one event that equals `signature` if all
    ///    `indexed` modifiers are removed from the parameters.
    fn contract_event_with_signature(&self, signature: &str) -> Option<&Event> {
        // Returns an `Event(uint256,address)` signature for an event, without `indexed` hints.
        fn ambiguous_event_signature(event: &Event) -> String {
            format!(
                "{}({})",
                event.name,
                event
                    .inputs
                    .iter()
                    .map(|input| event_param_type_signature(&input.kind))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }

        // Returns an `Event(indexed uint256,address)` type signature for an event.
        fn event_signature(event: &Event) -> String {
            format!(
                "{}({})",
                event.name,
                event
                    .inputs
                    .iter()
                    .map(|input| format!(
                        "{}{}",
                        if input.indexed { "indexed " } else { "" },
                        event_param_type_signature(&input.kind)
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }

        // Returns the signature of an event parameter type (e.g. `uint256`).
        fn event_param_type_signature(kind: &ParamType) -> String {
            use ParamType::*;

            match kind {
                Address => "address".into(),
                Bytes => "bytes".into(),
                Int(size) => format!("int{}", size),
                Uint(size) => format!("uint{}", size),
                Bool => "bool".into(),
                String => "string".into(),
                Array(inner) => format!("{}[]", event_param_type_signature(inner)),
                FixedBytes(size) => format!("bytes{}", size),
                FixedArray(inner, size) => {
                    format!("{}[{}]", event_param_type_signature(inner), size)
                }
                Tuple(components) => format!(
                    "({})",
                    components
                        .iter()
                        .map(event_param_type_signature)
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            }
        }

        self.contract_abi
            .contract
            .events()
            .find(|event| event_signature(event) == signature)
            .or_else(|| {
                // Fallback for subgraphs that don't use `indexed` in event signatures yet:
                //
                // If there is only one event variant with this name and if its signature
                // without `indexed` matches the event signature from the manifest, we
                // can safely assume that the event is a match, we don't need to force
                // the subgraph to add `indexed`.

                // Extract the event name; if there is no '(' in the signature,
                // `event_name` will be empty and not match any events, so that's ok
                let parens = signature.find('(').unwrap_or(0);
                let event_name = &signature[0..parens];

                let matching_events = self
                    .contract_abi
                    .contract
                    .events()
                    .filter(|event| event.name == event_name)
                    .collect::<Vec<_>>();

                // Only match the event signature without `indexed` if there is
                // only a single event variant
                if matching_events.len() == 1
                    && ambiguous_event_signature(matching_events[0]) == signature
                {
                    Some(matching_events[0])
                } else {
                    // More than one event variant or the signature
                    // still doesn't match, even if we ignore `indexed` hints
                    None
                }
            })
    }

    fn contract_function_with_signature(&self, target_signature: &str) -> Option<&Function> {
        self.contract_abi
            .contract
            .functions()
            .filter(|function| match function.state_mutability {
                StateMutability::Payable | StateMutability::NonPayable => true,
                StateMutability::Pure | StateMutability::View => false,
            })
            .find(|function| {
                // Construct the argument function signature:
                // `address,uint256,bool`
                let mut arguments = function
                    .inputs
                    .iter()
                    .map(|input| format!("{}", input.kind))
                    .collect::<Vec<String>>()
                    .join(",");
                // `address,uint256,bool)
                arguments.push(')');
                // `operation(address,uint256,bool)`
                let actual_signature = vec![function.name.clone(), arguments].join("(");
                target_signature == actual_signature
            })
    }

    fn matches_trigger_address(&self, trigger: &EthereumTrigger) -> bool {
        let Some(ds_address) = self.address else {
            // 'wildcard' data sources match any trigger address.
            return true;
        };

        let Some(trigger_address) = trigger.address() else {
            return true;
        };

        ds_address == *trigger_address
    }

    /// Checks if `trigger` matches this data source, and if so decodes it into a `MappingTrigger`.
    /// A return of `Ok(None)` mean the trigger does not match.
    fn match_and_decode(
        &self,
        trigger: &EthereumTrigger,
        block: &Arc<LightEthereumBlock>,
        logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<Chain>>, Error> {
        if !self.matches_trigger_address(trigger) {
            return Ok(None);
        }

        if self.start_block > block.number() {
            return Ok(None);
        }

        match trigger {
            EthereumTrigger::Block(_, trigger_type) => {
                let handler = match self.handler_for_block(trigger_type, block.number()) {
                    Some(handler) => handler,
                    None => return Ok(None),
                };
                Ok(Some(TriggerWithHandler::<Chain>::new(
                    MappingTrigger::Block {
                        block: block.cheap_clone(),
                    },
                    handler.handler.clone(),
                    block.block_ptr(),
                    block.timestamp(),
                )))
            }
            EthereumTrigger::Log(log_ref) => {
                let log = Arc::new(log_ref.log().clone());
                let receipt = log_ref.receipt();
                let potential_handlers = self.handlers_for_log(&log);

                // Map event handlers to (event handler, event ABI) pairs; fail if there are
                // handlers that don't exist in the contract ABI
                let valid_handlers = potential_handlers
                    .into_iter()
                    .map(|event_handler| {
                        // Identify the event ABI in the contract
                        let event_abi = self
                            .contract_event_with_signature(event_handler.event.as_str())
                            .with_context(|| {
                                anyhow!(
                                    "Event with the signature \"{}\" not found in \
                                            contract \"{}\" of data source \"{}\"",
                                    event_handler.event,
                                    self.contract_abi.name,
                                    self.name,
                                )
                            })?;
                        Ok((event_handler, event_abi))
                    })
                    .collect::<Result<Vec<_>, anyhow::Error>>()?;

                // Filter out handlers whose corresponding event ABIs cannot decode the
                // params (this is common for overloaded events that have the same topic0
                // but have indexed vs. non-indexed params that are encoded differently).
                //
                // Map (handler, event ABI) pairs to (handler, decoded params) pairs.
                let mut matching_handlers = valid_handlers
                    .into_iter()
                    .filter_map(|(event_handler, event_abi)| {
                        event_abi
                            .parse_log(RawLog {
                                topics: log.topics.clone(),
                                data: log.data.clone().0,
                            })
                            .map(|log| log.params)
                            .map_err(|e| {
                                trace!(
                                    logger,
                                    "Skipping handler because the event parameters do not \
                                    match the event signature. This is typically the case \
                                    when parameters are indexed in the event but not in the \
                                    signature or the other way around";
                                    "handler" => &event_handler.handler,
                                    "event" => &event_handler.event,
                                    "error" => format!("{}", e),
                                );
                            })
                            .ok()
                            .map(|params| (event_handler, params))
                    })
                    .collect::<Vec<_>>();

                if matching_handlers.is_empty() {
                    return Ok(None);
                }

                // Process the event with the matching handler
                let (event_handler, params) = matching_handlers.pop().unwrap();

                ensure!(
                    matching_handlers.is_empty(),
                    format!(
                        "Multiple handlers defined for event `{}`, only one is supported",
                        &event_handler.event
                    )
                );

                // Special case: In Celo, there are Epoch Rewards events, which do not have an
                // associated transaction and instead have `transaction_hash == block.hash`,
                // in which case we pass a dummy transaction to the mappings.
                // See also ca0edc58-0ec5-4c89-a7dd-2241797f5e50.
                // There is another special case in zkSync-era, where the transaction hash in this case would be zero
                // See https://docs.zksync.io/zk-stack/concepts/blocks.html#fictive-l2-block-finalizing-the-batch
                let transaction = if log.transaction_hash == block.hash
                    || log.transaction_hash == Some(H256::zero())
                {
                    Transaction {
                        hash: log.transaction_hash.unwrap(),
                        block_hash: block.hash,
                        block_number: block.number,
                        transaction_index: log.transaction_index,
                        from: Some(H160::zero()),
                        ..Transaction::default()
                    }
                } else {
                    // This is the general case where the log's transaction hash does not match the block's hash
                    // and is not a special zero hash, implying a real transaction associated with this log.
                    block
                        .transaction_for_log(&log)
                        .context("Found no transaction for event")?
                };

                let logging_extras = Arc::new(o! {
                    "signature" => event_handler.event.to_string(),
                    "address" => format!("{}", &log.address),
                    "transaction" => format!("{}", &transaction.hash),
                });
                let handler = event_handler.handler.clone();
                let calls = DeclaredCall::new(&self.mapping, &event_handler, &log, &params)?;
                Ok(Some(TriggerWithHandler::<Chain>::new_with_logging_extras(
                    MappingTrigger::Log {
                        block: block.cheap_clone(),
                        transaction: Arc::new(transaction),
                        log,
                        params,
                        receipt: receipt.map(|r| r.cheap_clone()),
                        calls,
                    },
                    handler,
                    block.block_ptr(),
                    block.timestamp(),
                    logging_extras,
                )))
            }
            EthereumTrigger::Call(call) => {
                // Identify the call handler for this call
                let handler = match self.handler_for_call(call)? {
                    Some(handler) => handler,
                    None => return Ok(None),
                };

                // Identify the function ABI in the contract
                let function_abi = self
                    .contract_function_with_signature(handler.function.as_str())
                    .with_context(|| {
                        anyhow!(
                            "Function with the signature \"{}\" not found in \
                    contract \"{}\" of data source \"{}\"",
                            handler.function,
                            self.contract_abi.name,
                            self.name
                        )
                    })?;

                // Parse the inputs
                //
                // Take the input for the call, chop off the first 4 bytes, then call
                // `function.decode_input` to get a vector of `Token`s. Match the `Token`s
                // with the `Param`s in `function.inputs` to create a `Vec<LogParam>`.
                let tokens = match function_abi.decode_input(&call.input.0[4..]).with_context(
                    || {
                        format!(
                            "Generating function inputs for the call {:?} failed, raw input: {}",
                            &function_abi,
                            hex::encode(&call.input.0)
                        )
                    },
                ) {
                    Ok(val) => val,
                    // See also 280b0108-a96e-4738-bb37-60ce11eeb5bf
                    Err(err) => {
                        warn!(logger, "Failed parsing inputs, skipping"; "error" => &err.to_string());
                        return Ok(None);
                    }
                };

                ensure!(
                    tokens.len() == function_abi.inputs.len(),
                    "Number of arguments in call does not match \
                    number of inputs in function signature."
                );

                let inputs = tokens
                    .into_iter()
                    .enumerate()
                    .map(|(i, token)| LogParam {
                        name: function_abi.inputs[i].name.clone(),
                        value: token,
                    })
                    .collect::<Vec<_>>();

                // Parse the outputs
                //
                // Take the output for the call, then call `function.decode_output` to
                // get a vector of `Token`s. Match the `Token`s with the `Param`s in
                // `function.outputs` to create a `Vec<LogParam>`.
                let tokens = function_abi
                    .decode_output(&call.output.0)
                    .with_context(|| {
                        format!(
                            "Decoding function outputs for the call {:?} failed, raw output: {}",
                            &function_abi,
                            hex::encode(&call.output.0)
                        )
                    })?;

                ensure!(
                    tokens.len() == function_abi.outputs.len(),
                    "Number of parameters in the call output does not match \
                        number of outputs in the function signature."
                );

                let outputs = tokens
                    .into_iter()
                    .enumerate()
                    .map(|(i, token)| LogParam {
                        name: function_abi.outputs[i].name.clone(),
                        value: token,
                    })
                    .collect::<Vec<_>>();

                let transaction = Arc::new(
                    block
                        .transaction_for_call(call)
                        .context("Found no transaction for call")?,
                );
                let logging_extras = Arc::new(o! {
                    "function" => handler.function.to_string(),
                    "to" => format!("{}", &call.to),
                    "transaction" => format!("{}", &transaction.hash),
                });
                Ok(Some(TriggerWithHandler::<Chain>::new_with_logging_extras(
                    MappingTrigger::Call {
                        block: block.cheap_clone(),
                        transaction,
                        call: call.cheap_clone(),
                        inputs,
                        outputs,
                    },
                    handler.handler.clone(),
                    block.block_ptr(),
                    block.timestamp(),
                    logging_extras,
                )))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeclaredCall {
    /// The user-supplied label from the manifest
    label: String,
    contract_name: String,
    address: Address,
    function: Function,
    args: Vec<Token>,
}

impl DeclaredCall {
    fn new(
        mapping: &Mapping,
        handler: &MappingEventHandler,
        log: &Log,
        params: &[LogParam],
    ) -> Result<Vec<DeclaredCall>, anyhow::Error> {
        let mut calls = Vec::new();
        for decl in handler.calls.decls.iter() {
            let contract_name = decl.expr.abi.to_string();
            let function_name = decl.expr.func.as_str();
            // Obtain the path to the contract ABI
            let abi = mapping.find_abi(&contract_name)?;
            // TODO: Handle overloaded functions
            let function = {
                // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
                // functions this always picks the same overloaded variant, which is incorrect
                // and may lead to encoding/decoding errors
                abi.contract.function(function_name).with_context(|| {
                    format!(
                        "Unknown function \"{}::{}\" called from WASM runtime",
                        contract_name, function_name
                    )
                })?
            };

            let address = decl.address(log, params)?;
            let args = decl.args(log, params)?;

            let call = DeclaredCall {
                label: decl.label.clone(),
                contract_name,
                address,
                function: function.clone(),
                args,
            };
            calls.push(call);
        }

        Ok(calls)
    }

    fn as_eth_call(self, block_ptr: BlockPtr, gas: Option<u32>) -> (ContractCall, String) {
        (
            ContractCall {
                contract_name: self.contract_name,
                address: self.address,
                block_ptr,
                function: self.function,
                args: self.args,
                gas,
            },
            self.label,
        )
    }
}

pub struct DecoderHook {
    eth_adapters: Arc<EthereumNetworkAdapters>,
    call_cache: Arc<dyn EthereumCallCache>,
    eth_call_gas: Option<u32>,
}

impl DecoderHook {
    pub fn new(
        eth_adapters: Arc<EthereumNetworkAdapters>,
        call_cache: Arc<dyn EthereumCallCache>,
        eth_call_gas: Option<u32>,
    ) -> Self {
        Self {
            eth_adapters,
            call_cache,
            eth_call_gas,
        }
    }
}

impl DecoderHook {
    /// Perform a batch of eth_calls, observing the execution time of each
    /// call. Returns a list of the call labels for which we received a
    /// `None` response, indicating a revert
    async fn eth_calls(
        &self,
        logger: &Logger,
        block_ptr: &BlockPtr,
        calls_and_metrics: Vec<(Arc<HostMetrics>, DeclaredCall)>,
    ) -> Result<Vec<String>, MappingError> {
        // This check is not just to speed things up, but is also needed to
        // make sure the runner tests don't fail; they don't have declared
        // eth calls, but without this check we try to get an eth adapter
        // even when there are no calls, which fails in the runner test
        // setup
        if calls_and_metrics.is_empty() {
            return Ok(vec![]);
        }

        let start = Instant::now();

        let (metrics, calls): (Vec<_>, Vec<_>) = calls_and_metrics.into_iter().unzip();

        let (calls, labels): (Vec<_>, Vec<_>) = calls
            .into_iter()
            .map(|call| call.as_eth_call(block_ptr.clone(), self.eth_call_gas))
            .unzip();

        let eth_adapter = self.eth_adapters.call_or_cheapest(Some(&NodeCapabilities {
            archive: true,
            traces: false,
        }))?;

        let call_refs = calls.iter().collect::<Vec<_>>();
        let results = eth_adapter
            .contract_calls(logger, &call_refs, self.call_cache.cheap_clone())
            .await
            .map_err(|e| {
                // An error happened, everybody gets charged
                let elapsed = start.elapsed().as_secs_f64() / call_refs.len() as f64;
                for (metrics, call) in metrics.iter().zip(call_refs) {
                    metrics.observe_eth_call_execution_time(
                        elapsed,
                        &call.contract_name,
                        &call.function.name,
                    );
                }
                MappingError::from(e)
            })?;

        // We don't have time measurements for each call (though that would be nice)
        // Use the average time of all calls that we want to observe as the time for
        // each call
        let to_observe = results.iter().map(|(_, source)| source.observe()).count() as f64;
        let elapsed = start.elapsed().as_secs_f64() / to_observe;

        results
            .iter()
            .zip(metrics)
            .zip(calls)
            .for_each(|(((_, source), metrics), call)| {
                if source.observe() {
                    metrics.observe_eth_call_execution_time(
                        elapsed,
                        &call.contract_name,
                        &call.function.name,
                    );
                }
            });

        let labels = results
            .iter()
            .zip(labels)
            .filter_map(|((res, _), label)| if res.is_none() { Some(label) } else { None })
            .map(|s| s.to_string())
            .collect();
        Ok(labels)
    }
}

#[async_trait]
impl blockchain::DecoderHook<Chain> for DecoderHook {
    async fn after_decode<'a>(
        &self,
        logger: &Logger,
        block_ptr: &BlockPtr,
        runnables: Vec<RunnableTriggers<'a, Chain>>,
        metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<Vec<RunnableTriggers<'a, Chain>>, MappingError> {
        /// Log information about failed eth calls. 'Failure' here simply
        /// means that the call was reverted; outright errors lead to a real
        /// error. For reverted calls, `self.eth_calls` returns the label
        /// from the manifest for that call.
        ///
        /// One reason why declared calls can fail is if they are attached
        /// to the wrong handler, or if arguments are specified incorrectly.
        /// Calls that revert every once in a while might be ok and what the
        /// user intended, but we want to clearly log so that users can spot
        /// mistakes in their manifest, which will lead to unnecessary eth
        /// calls
        fn log_results(
            logger: &Logger,
            failures: &[String],
            calls_count: usize,
            trigger_count: usize,
            elapsed: Duration,
        ) {
            let fail_count = failures.len();

            if fail_count > 0 {
                let mut counts: Vec<_> = failures.iter().counts().into_iter().collect();
                counts.sort_by_key(|(label, _)| *label);
                let counts = counts
                    .into_iter()
                    .map(|(label, count)| {
                        let times = if count == 1 { "time" } else { "times" };
                        format!("{label} ({count} {times})")
                    })
                    .join(", ");
                error!(logger, "Declared calls failed";
                  "triggers" => trigger_count,
                  "calls_count" => calls_count,
                  "fail_count" => fail_count,
                  "calls_ms" => elapsed.as_millis(),
                  "failures" => format!("[{}]", counts));
            } else {
                debug!(logger, "Declared calls";
                  "triggers" => trigger_count,
                  "calls_count" => calls_count,
                  "calls_ms" => elapsed.as_millis());
            }
        }

        if ENV_VARS.mappings.disable_declared_calls {
            return Ok(runnables);
        }

        let _section = metrics.stopwatch.start_section("declared_ethereum_call");

        let start = Instant::now();
        let calls: Vec<_> = runnables
            .iter()
            .map(|r| &r.hosted_triggers)
            .flatten()
            .filter_map(|trigger| {
                trigger
                    .mapping_trigger
                    .trigger
                    .as_onchain()
                    .map(|t| (trigger.host.host_metrics(), t))
            })
            .filter_map(|(metrics, trigger)| match trigger {
                MappingTrigger::Log { calls, .. } => Some(
                    calls
                        .clone()
                        .into_iter()
                        .map(move |call| (metrics.cheap_clone(), call)),
                ),
                MappingTrigger::Block { .. } | MappingTrigger::Call { .. } => None,
            })
            .flatten()
            .collect();

        // Deduplicate calls. Unfortunately, we can't get `DeclaredCall` to
        // implement `Hash` or `Ord` easily, so we can only deduplicate by
        // comparing the whole call not with a `HashSet` or `BTreeSet`.
        // Since that can be inefficient, we don't deduplicate if we have an
        // enormous amount of calls; in that case though, things will likely
        // blow up because of the amount of I/O that many calls cause.
        // Cutting off at 1000 is fairly arbitrary
        let calls = if calls.len() < 1000 {
            let mut uniq_calls = Vec::new();
            for (metrics, call) in calls {
                if !uniq_calls.iter().any(|(_, c)| c == &call) {
                    uniq_calls.push((metrics, call));
                }
            }
            uniq_calls
        } else {
            calls
        };

        let calls_count = calls.len();
        let results = self.eth_calls(logger, block_ptr, calls).await?;
        log_results(
            logger,
            &results,
            calls_count,
            runnables.len(),
            start.elapsed(),
        );

        Ok(runnables)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: UnresolvedMapping,
    pub context: Option<DataSourceContext>,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSource, anyhow::Error> {
        let UnresolvedDataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,
        } = self;

        let mapping = mapping.resolve(resolver, logger).await.with_context(|| {
            format!(
                "failed to resolve data source {} with source_address {:?} and source_start_block {}",
                name, source.address, source.start_block
            )
        })?;

        DataSource::from_manifest(kind, network, name, source, mapping, context, manifest_idx)
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: TemplateSource,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug)]
pub struct DataSourceTemplate {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub manifest_idx: u32,
    pub source: TemplateSource,
    pub mapping: Mapping,
}

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for UnresolvedDataSourceTemplate {
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSourceTemplate, anyhow::Error> {
        let UnresolvedDataSourceTemplate {
            kind,
            network,
            name,
            source,
            mapping,
        } = self;

        let mapping = mapping
            .resolve(resolver, logger)
            .await
            .with_context(|| format!("failed to resolve data source template {}", name))?;

        Ok(DataSourceTemplate {
            kind,
            network,
            name,
            manifest_idx,
            source,
            mapping,
        })
    }
}

impl blockchain::DataSourceTemplate<Chain> for DataSourceTemplate {
    fn name(&self) -> &str {
        &self.name
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        Some(self.mapping.runtime.cheap_clone())
    }

    fn manifest_idx(&self) -> u32 {
        self.manifest_idx
    }

    fn kind(&self) -> &str {
        &self.kind
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub kind: String,
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<UnresolvedMappingABI>,
    #[serde(default)]
    pub block_handlers: Vec<MappingBlockHandler>,
    #[serde(default)]
    pub call_handlers: Vec<MappingCallHandler>,
    #[serde(default)]
    pub event_handlers: Vec<MappingEventHandler>,
    pub file: Link,
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub kind: String,
    pub api_version: semver::Version,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<Arc<MappingABI>>,
    pub block_handlers: Vec<MappingBlockHandler>,
    pub call_handlers: Vec<MappingCallHandler>,
    pub event_handlers: Vec<MappingEventHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

impl Mapping {
    pub fn requires_archive(&self) -> anyhow::Result<bool> {
        calls_host_fn(&self.runtime, "ethereum.call")
    }

    pub fn has_call_handler(&self) -> bool {
        !self.call_handlers.is_empty()
    }

    pub fn has_block_handler_with_call_filter(&self) -> bool {
        self.block_handlers
            .iter()
            .any(|handler| matches!(handler.filter, Some(BlockHandlerFilter::Call)))
    }

    pub fn find_abi(&self, abi_name: &str) -> Result<Arc<MappingABI>, Error> {
        Ok(self
            .abis
            .iter()
            .find(|abi| abi.name == abi_name)
            .ok_or_else(|| anyhow!("No ABI entry with name `{}` found", abi_name))?
            .cheap_clone())
    }
}

impl UnresolvedMapping {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<Mapping, anyhow::Error> {
        let UnresolvedMapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            block_handlers,
            call_handlers,
            event_handlers,
            file: link,
        } = self;

        let api_version = semver::Version::parse(&api_version)?;

        let (abis, runtime) = try_join(
            // resolve each abi
            abis.into_iter()
                .map(|unresolved_abi| async {
                    Result::<_, Error>::Ok(Arc::new(
                        unresolved_abi.resolve(resolver, logger).await?,
                    ))
                })
                .collect::<FuturesOrdered<_>>()
                .try_collect::<Vec<_>>(),
            async {
                let module_bytes = resolver.cat(logger, &link).await?;
                Ok(Arc::new(module_bytes))
            },
        )
        .await
        .with_context(|| format!("failed to resolve mapping {}", link.link))?;

        Ok(Mapping {
            kind,
            api_version,
            language,
            entities,
            abis,
            block_handlers: block_handlers.clone(),
            call_handlers: call_handlers.clone(),
            event_handlers: event_handlers.clone(),
            runtime,
            link,
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct UnresolvedMappingABI {
    pub name: String,
    pub file: Link,
}

impl UnresolvedMappingABI {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<MappingABI, anyhow::Error> {
        let contract_bytes = resolver.cat(logger, &self.file).await.with_context(|| {
            format!(
                "failed to resolve ABI {} from {}",
                self.name, self.file.link
            )
        })?;
        let contract = Contract::load(&*contract_bytes)?;
        Ok(MappingABI {
            name: self.name,
            contract,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MappingABI {
    pub name: String,
    pub contract: Contract,
}

impl MappingABI {
    pub fn function(
        &self,
        contract_name: &str,
        name: &str,
        signature: Option<&str>,
    ) -> Result<&Function, Error> {
        let contract = &self.contract;
        let function = match signature {
            // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
            // functions this always picks the same overloaded variant, which is incorrect
            // and may lead to encoding/decoding errors
            None => contract.function(name).with_context(|| {
                format!(
                    "Unknown function \"{}::{}\" called from WASM runtime",
                    contract_name, name
                )
            })?,

            // Behavior for apiVersion >= 0.0.04: look up function by signature of
            // the form `functionName(uint256,string) returns (bytes32,string)`; this
            // correctly picks the correct variant of an overloaded function
            Some(ref signature) => contract
                .functions_by_name(name)
                .with_context(|| {
                    format!(
                        "Unknown function \"{}::{}\" called from WASM runtime",
                        contract_name, name
                    )
                })?
                .iter()
                .find(|f| signature == &f.signature())
                .with_context(|| {
                    format!(
                        "Unknown function \"{}::{}\" with signature `{}` \
                             called from WASM runtime",
                        contract_name, name, signature,
                    )
                })?,
        };
        Ok(function)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingBlockHandler {
    pub handler: String,
    pub filter: Option<BlockHandlerFilter>,
}

impl MappingBlockHandler {
    pub fn kind(&self) -> &str {
        match &self.filter {
            Some(filter) => match filter {
                BlockHandlerFilter::Call => "block_filter_call",
                BlockHandlerFilter::Once => "block_filter_once",
                BlockHandlerFilter::Polling { .. } => "block_filter_polling",
            },
            None => BLOCK_HANDLER_KIND,
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum BlockHandlerFilter {
    // Call filter will trigger on all blocks where the data source contract
    // address has been called
    Call,
    // This filter will trigger once at the startBlock
    Once,
    // This filter will trigger in a recurring interval set by the `every` field.
    Polling { every: NonZeroU32 },
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingCallHandler {
    pub function: String,
    pub handler: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingEventHandler {
    pub event: String,
    pub topic0: Option<H256>,
    #[serde(deserialize_with = "deserialize_h256_vec", default)]
    pub topic1: Option<Vec<H256>>,
    #[serde(deserialize_with = "deserialize_h256_vec", default)]
    pub topic2: Option<Vec<H256>>,
    #[serde(deserialize_with = "deserialize_h256_vec", default)]
    pub topic3: Option<Vec<H256>>,
    pub handler: String,
    #[serde(default)]
    pub receipt: bool,
    #[serde(default)]
    pub calls: CallDecls,
}

// Custom deserializer for H256 fields that removes the '0x' prefix before parsing
fn deserialize_h256_vec<'de, D>(deserializer: D) -> Result<Option<Vec<H256>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<Vec<String>> = Option::deserialize(deserializer)?;

    match s {
        Some(vec) => {
            let mut h256_vec = Vec::new();
            for hex_str in vec {
                // Remove '0x' prefix if present
                let clean_hex_str = hex_str.trim_start_matches("0x");
                // Ensure the hex string is 64 characters long, after removing '0x'
                let padded_hex_str = format!("{:0>64}", clean_hex_str);
                // Parse the padded string into H256, handling potential errors
                h256_vec.push(
                    H256::from_str(&padded_hex_str)
                        .map_err(|e| D::Error::custom(format!("Failed to parse H256: {}", e)))?,
                );
            }
            Ok(Some(h256_vec))
        }
        None => Ok(None),
    }
}

impl MappingEventHandler {
    pub fn topic0(&self) -> H256 {
        self.topic0
            .unwrap_or_else(|| string_to_h256(&self.event.replace("indexed ", "")))
    }

    pub fn matches(&self, log: &Log) -> bool {
        let matches_topic = |index: usize, topic_opt: &Option<Vec<H256>>| -> bool {
            topic_opt.as_ref().map_or(true, |topic_vec| {
                log.topics
                    .get(index)
                    .map_or(false, |log_topic| topic_vec.contains(log_topic))
            })
        };

        if let Some(topic0) = log.topics.get(0) {
            return self.topic0() == *topic0
                && matches_topic(1, &self.topic1)
                && matches_topic(2, &self.topic2)
                && matches_topic(3, &self.topic3);
        }

        // Logs without topic0 should simply be skipped
        false
    }

    pub fn has_additional_topics(&self) -> bool {
        self.topic1.as_ref().map_or(false, |v| !v.is_empty())
            || self.topic2.as_ref().map_or(false, |v| !v.is_empty())
            || self.topic3.as_ref().map_or(false, |v| !v.is_empty())
    }
}

/// Hashes a string to a H256 hash.
fn string_to_h256(s: &str) -> H256 {
    let mut result = [0u8; 32];
    let data = s.replace(' ', "").into_bytes();
    let mut sponge = Keccak::new_keccak256();
    sponge.update(&data);
    sponge.finalize(&mut result);

    // This was deprecated but the replacement seems to not be available in the
    // version web3 uses.
    #[allow(deprecated)]
    H256::from_slice(&result)
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct TemplateSource {
    pub abi: String,
}

/// Internal representation of declared calls. In the manifest that's
/// written as part of an event handler as
/// ```yaml
/// calls:
///   - myCall1: Contract[address].function(arg1, arg2, ...)
///   - ..
/// ```
///
/// The `address` and `arg` fields can be either `event.address` or
/// `event.params.<name>`. Each entry under `calls` gets turned into a
/// `CallDcl`
#[derive(Clone, CheapClone, Debug, Default, Hash, Eq, PartialEq)]
pub struct CallDecls {
    pub decls: Arc<Vec<CallDecl>>,
    readonly: (),
}

/// A single call declaration, like `myCall1:
/// Contract[address].function(arg1, arg2, ...)`
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct CallDecl {
    /// A user-defined label
    pub label: String,
    /// The call expression
    pub expr: CallExpr,
    readonly: (),
}
impl CallDecl {
    fn address(&self, log: &Log, params: &[LogParam]) -> Result<H160, Error> {
        let address = match &self.expr.address {
            CallArg::Address => log.address,
            CallArg::HexAddress(address) => *address,
            CallArg::Param(name) => {
                let value = params
                    .iter()
                    .find(|param| &param.name == name.as_str())
                    .ok_or_else(|| anyhow!("unknown param {name}"))?
                    .value
                    .clone();
                value
                    .into_address()
                    .ok_or_else(|| anyhow!("param {name} is not an address"))?
            }
        };
        Ok(address)
    }

    fn args(&self, log: &Log, params: &[LogParam]) -> Result<Vec<Token>, Error> {
        self.expr
            .args
            .iter()
            .map(|arg| match arg {
                CallArg::Address => Ok(Token::Address(log.address)),
                CallArg::HexAddress(address) => Ok(Token::Address(*address)),
                CallArg::Param(name) => {
                    let value = params
                        .iter()
                        .find(|param| &param.name == name.as_str())
                        .ok_or_else(|| anyhow!("unknown param {name}"))?
                        .value
                        .clone();
                    Ok(value)
                }
            })
            .collect()
    }
}

impl<'de> de::Deserialize<'de> for CallDecls {
    fn deserialize<D>(deserializer: D) -> Result<CallDecls, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let decls: std::collections::HashMap<String, String> =
            de::Deserialize::deserialize(deserializer)?;
        let decls = decls
            .into_iter()
            .map(|(name, expr)| {
                expr.parse::<CallExpr>().map(|expr| CallDecl {
                    label: name,
                    expr,
                    readonly: (),
                })
            })
            .collect::<Result<_, _>>()
            .map(|decls| Arc::new(decls))
            .map_err(de::Error::custom)?;
        Ok(CallDecls {
            decls,
            readonly: (),
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct CallExpr {
    pub abi: Word,
    pub address: CallArg,
    pub func: Word,
    pub args: Vec<CallArg>,
    readonly: (),
}

/// Parse expressions of the form `Contract[address].function(arg1, arg2,
/// ...)` where the `address` and the args are either `event.address` or
/// `event.params.<name>`.
///
/// The parser is pretty awful as it generates error messages that aren't
/// very helpful. We should replace all this with a real parser, most likely
/// `combine` which is what `graphql_parser` uses
impl FromStr for CallExpr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        lazy_static! {
            static ref RE: Regex = Regex::new(
                r"(?x)
                (?P<abi>[a-zA-Z0-9_]+)\[
                    (?P<address>[^]]+)\]
                \.
                (?P<func>[a-zA-Z0-9_]+)\(
                    (?P<args>[^)]*)
                \)"
            )
            .unwrap();
        }
        let x = RE
            .captures(s)
            .ok_or_else(|| anyhow!("invalid call expression `{s}`"))?;
        let abi = Word::from(x.name("abi").unwrap().as_str());
        let address = x.name("address").unwrap().as_str().parse()?;
        let func = Word::from(x.name("func").unwrap().as_str());
        let args: Vec<CallArg> = x
            .name("args")
            .unwrap()
            .as_str()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().parse::<CallArg>())
            .collect::<Result<_, _>>()?;
        Ok(CallExpr {
            abi,
            address,
            func,
            args,
            readonly: (),
        })
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum CallArg {
    HexAddress(Address),
    Address,
    Param(Word),
}

lazy_static! {
    // Matches a 40-character hexadecimal string prefixed with '0x', typical for Ethereum addresses
    static ref ADDR_RE: Regex = Regex::new(r"^0x[0-9a-fA-F]{40}$").unwrap();
}

impl FromStr for CallArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if ADDR_RE.is_match(s) {
            if let Ok(parsed_address) = Address::from_str(s) {
                return Ok(CallArg::HexAddress(parsed_address));
            }
        }

        let mut parts = s.split('.');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("event"), Some("address"), None) => Ok(CallArg::Address),
            (Some("event"), Some("params"), Some(param)) => Ok(CallArg::Param(Word::from(param))),
            _ => Err(anyhow!("invalid call argument `{}`", s)),
        }
    }
}

#[test]
fn test_call_expr() {
    let expr: CallExpr = "ERC20[event.address].balanceOf(event.params.token)"
        .parse()
        .unwrap();
    assert_eq!(expr.abi, "ERC20");
    assert_eq!(expr.address, CallArg::Address);
    assert_eq!(expr.func, "balanceOf");
    assert_eq!(expr.args, vec![CallArg::Param("token".into())]);

    let expr: CallExpr = "Pool[event.params.pool].fees(event.params.token0, event.params.token1)"
        .parse()
        .unwrap();
    assert_eq!(expr.abi, "Pool");
    assert_eq!(expr.address, CallArg::Param("pool".into()));
    assert_eq!(expr.func, "fees");
    assert_eq!(
        expr.args,
        vec![
            CallArg::Param("token0".into()),
            CallArg::Param("token1".into())
        ]
    );

    let expr: CallExpr = "Pool[event.address].growth()".parse().unwrap();
    assert_eq!(expr.abi, "Pool");
    assert_eq!(expr.address, CallArg::Address);
    assert_eq!(expr.func, "growth");
    assert_eq!(expr.args, vec![]);

    let expr: CallExpr = "Pool[0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF].growth(0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF)"
        .parse()
        .unwrap();
    let call_arg =
        CallArg::HexAddress(H160::from_str("0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF").unwrap());
    assert_eq!(expr.abi, "Pool");
    assert_eq!(expr.address, call_arg);
    assert_eq!(expr.func, "growth");
    assert_eq!(expr.args, vec![call_arg]);
}
