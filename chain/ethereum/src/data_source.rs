use anyhow::{anyhow, Error};
use anyhow::{ensure, Context};
use ethabi::{Address, Event, Function, LogParam, ParamType, RawLog};
use graph::prelude::{info, Logger};
use std::str::FromStr;
use std::{convert::TryFrom, sync::Arc};
use tiny_keccak::keccak256;
use web3::types::Log;

use graph::{
    blockchain::{self, Blockchain},
    prelude::{
        BlockNumber, CheapClone, DataSourceTemplateInfo, EthereumBlockTriggerType, EthereumCall,
        EthereumTrigger, LightEthereumBlock, LightEthereumBlockExt, MappingTrigger,
    },
};

use graph::data::subgraph::{
    BlockHandlerFilter, DataSourceContext, Mapping, MappingABI, MappingBlockHandler,
    MappingCallHandler, MappingEventHandler, Source,
};

/// Runtime representation of a data source.
// Note: Not great for memory usage that this needs to be `Clone`, considering how there may be tens
// of thousands of data sources in memory at once.
#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
    pub contract_abi: Arc<MappingABI>,
}

// ETHDEP: The whole DataSource struct needs to move to chain::ethereum
impl blockchain::DataSource for DataSource {
    type C = crate::Chain;

    fn match_and_decode(
        &self,
        _trigger: &<Self::C as Blockchain>::TriggerData,
        _block: Arc<<Self::C as Blockchain>::Block>,
        _logger: &Logger,
    ) -> Result<Option<<Self::C as Blockchain>::MappingTrigger>, Error> {
        todo!()
    }

    fn mapping(&self) -> &Mapping {
        &self.mapping
    }

    fn source(&self) -> &Source {
        &self.source
    }

    fn from_manifest(
        kind: String,
        network: Option<String>,
        name: String,
        source: Source,
        mapping: Mapping,
        context: Option<DataSourceContext>,
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
            source,
            mapping,
            context: Arc::new(context),
            creation_block,
            contract_abi,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn network(&self) -> Option<&str> {
        self.network.as_ref().map(|s| s.as_str())
    }

    fn context(&self) -> Option<&DataSourceContext> {
        self.context.as_ref().as_ref()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        self.creation_block
    }

    fn is_duplicate_of(&self, other: &Self) -> bool {
        let DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,

            // The creation block is ignored for detection duplicate data sources.
            // Contract ABI equality is implicit in `source` and `mapping.abis` equality.
            creation_block: _,
            contract_abi: _,
        } = self;

        // mapping_request_sender, host_metrics, and (most of) host_exports are operational structs
        // used at runtime but not needed to define uniqueness; each runtime host should be for a
        // unique data source.
        kind == &other.kind
            && network == &other.network
            && name == &other.name
            && source == &other.source
            && mapping.abis == other.mapping.abis
            && mapping.event_handlers == other.mapping.event_handlers
            && mapping.call_handlers == other.mapping.call_handlers
            && mapping.block_handlers == other.mapping.block_handlers
            && context == &other.context
    }
}

impl DataSource {
    fn handlers_for_log(&self, log: &Log) -> Result<Vec<MappingEventHandler>, Error> {
        // Get signature from the log
        let topic0 = log.topics.get(0).context("Ethereum event has no topics")?;

        let handlers = self
            .mapping
            .event_handlers
            .iter()
            .filter(|handler| *topic0 == handler.topic0())
            .cloned()
            .collect::<Vec<_>>();

        Ok(handlers)
    }

    fn handler_for_call(&self, call: &EthereumCall) -> Result<Option<MappingCallHandler>, Error> {
        // First four bytes of the input for the call are the first four
        // bytes of hash of the function signature
        ensure!(
            call.input.0.len() >= 4,
            "Ethereum call has input with less than 4 bytes"
        );

        let target_method_id = &call.input.0[..4];

        Ok(self
            .mapping
            .call_handlers
            .iter()
            .find(move |handler| {
                let fhash = keccak256(handler.function.as_bytes());
                let actual_method_id = [fhash[0], fhash[1], fhash[2], fhash[3]];
                target_method_id == actual_method_id
            })
            .cloned())
    }

    fn handler_for_block(
        &self,
        trigger_type: &EthereumBlockTriggerType,
    ) -> Option<MappingBlockHandler> {
        match trigger_type {
            EthereumBlockTriggerType::Every => self
                .mapping
                .block_handlers
                .iter()
                .find(move |handler| handler.filter == None)
                .cloned(),
            EthereumBlockTriggerType::WithCallTo(_address) => self
                .mapping
                .block_handlers
                .iter()
                .find(move |handler| {
                    handler.filter.is_some()
                        && handler.filter.clone().unwrap() == BlockHandlerFilter::Call
                })
                .cloned(),
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
                    .map(|input| format!("{}", event_param_type_signature(&input.kind)))
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
                Array(inner) => format!("{}[]", event_param_type_signature(&*inner)),
                FixedBytes(size) => format!("bytes{}", size),
                FixedArray(inner, size) => {
                    format!("{}[{}]", event_param_type_signature(&*inner), size)
                }
                Tuple(components) => format!(
                    "({})",
                    components
                        .iter()
                        .map(|component| event_param_type_signature(&component))
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
                let parens = signature.find("(").unwrap_or(0);
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
                ethabi::StateMutability::Payable | ethabi::StateMutability::NonPayable => true,
                ethabi::StateMutability::Pure | ethabi::StateMutability::View => false,
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
                arguments.push_str(")");
                // `operation(address,uint256,bool)`
                let actual_signature = vec![function.name.clone(), arguments].join("(");
                target_signature == actual_signature
            })
    }

    fn matches_trigger_address(&self, trigger: &EthereumTrigger) -> bool {
        let ds_address = match self.source.address {
            Some(addr) => addr,

            // 'wildcard' data sources match any trigger address.
            None => return true,
        };

        let trigger_address = match trigger {
            EthereumTrigger::Block(_, EthereumBlockTriggerType::WithCallTo(address)) => address,
            EthereumTrigger::Call(call) => &call.to,
            EthereumTrigger::Log(log) => &log.address,

            // Unfiltered block triggers match any data source address.
            EthereumTrigger::Block(_, EthereumBlockTriggerType::Every) => return true,
        };

        ds_address == *trigger_address
    }

    /// Checks if `trigger` matches this data source, and if so decodes it into a `MappingTrigger`.
    /// A return of `Ok(None)` mean the trigger does not match.
    pub fn match_and_decode(
        &self,
        trigger: &EthereumTrigger,
        block: Arc<LightEthereumBlock>,
        logger: &Logger,
    ) -> Result<Option<MappingTrigger>, Error> {
        if !self.matches_trigger_address(&trigger) {
            return Ok(None);
        }

        if self.source.start_block > block.number() {
            return Ok(None);
        }

        match trigger {
            EthereumTrigger::Block(_, trigger_type) => {
                let handler = match self.handler_for_block(trigger_type) {
                    Some(handler) => handler,
                    None => return Ok(None),
                };
                Ok(Some(MappingTrigger::Block { block, handler }))
            }
            EthereumTrigger::Log(log) => {
                let potential_handlers = self.handlers_for_log(log)?;

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
                                info!(
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

                let transaction = Arc::new(
                    block
                        .transaction_for_log(&log)
                        .context("Found no transaction for event")?,
                );

                Ok(Some(MappingTrigger::Log {
                    block,
                    transaction,
                    log: log.cheap_clone(),
                    params,
                    handler: event_handler,
                }))
            }
            EthereumTrigger::Call(call) => {
                // Identify the call handler for this call
                let handler = match self.handler_for_call(&call)? {
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
                let tokens = function_abi
                    .decode_input(&call.input.0[4..])
                    .with_context(|| {
                        format!(
                            "Generating function inputs for the call {:?} failed, raw input: {}",
                            &function_abi,
                            hex::encode(&call.input.0)
                        )
                    })?;

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
                        .transaction_for_call(&call)
                        .context("Found no transaction for call")?,
                );

                Ok(Some(MappingTrigger::Call {
                    block,
                    transaction,
                    call: call.cheap_clone(),
                    inputs,
                    outputs,
                    handler,
                }))
            }
        }
    }
}

impl TryFrom<DataSourceTemplateInfo> for DataSource {
    type Error = anyhow::Error;

    fn try_from(info: DataSourceTemplateInfo) -> Result<Self, anyhow::Error> {
        let DataSourceTemplateInfo {
            template,
            params,
            context,
            creation_block,
        } = info;

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
            kind: template.kind,
            network: template.network,
            name: template.name,
            source: Source {
                address: Some(address),
                abi: template.source.abi,
                start_block: 0,
            },
            mapping: template.mapping,
            context: Arc::new(context),
            creation_block: Some(creation_block),
            contract_abi,
        })
    }
}
