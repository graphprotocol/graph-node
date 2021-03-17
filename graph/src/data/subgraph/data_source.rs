use anyhow::{anyhow, Error};
use anyhow::{ensure, Context};
use std::{convert::TryFrom, sync::Arc};
use tiny_keccak::keccak256;
use web3::types::Log;

use crate::prelude::{BlockNumber, EthereumBlockTriggerType, EthereumCall};

use super::{
    BlockHandlerFilter, DataSource, MappingBlockHandler, MappingCallHandler, MappingEventHandler,
};

impl super::DataSource {
    pub fn matches_log(&self, log: &Log) -> bool {
        // The runtime host matches the contract address of the `Log`
        // if the data source contains the same contract address or
        // if the data source doesn't have a contract address at all
        let matches_log_address = self.source.address.map_or(true, |addr| addr == log.address);

        let matches_log_signature = {
            let topic0 = match log.topics.iter().next() {
                Some(topic0) => topic0,
                None => return false,
            };

            self.mapping
                .event_handlers
                .iter()
                .any(|handler| *topic0 == handler.topic0())
        };

        matches_log_address
            && matches_log_signature
            && self.source.start_block
                <= BlockNumber::try_from(log.block_number.unwrap().as_u64()).unwrap()
    }

    pub fn matches_call(&self, call: &EthereumCall) -> bool {
        // The runtime host matches the contract address of the `EthereumCall`
        // if the data source contains the same contract address or
        // if the data source doesn't have a contract address at all
        let matches_call_address = self.source.address.map_or(true, |addr| addr == call.to);

        let matches_call_function = {
            let target_method_id = &call.input.0[..4];
            self.mapping.call_handlers.iter().any(|handler| {
                let fhash = keccak256(handler.function.as_bytes());
                let actual_method_id = [fhash[0], fhash[1], fhash[2], fhash[3]];
                target_method_id == actual_method_id
            })
        };

        matches_call_address
            && matches_call_function
            && self.source.start_block <= call.block_number
    }

    pub fn matches_block(
        &self,
        block_trigger_type: &EthereumBlockTriggerType,
        block_number: BlockNumber,
    ) -> bool {
        let matches_block_trigger = {
            let source_address_matches = match block_trigger_type {
                EthereumBlockTriggerType::WithCallTo(address) => {
                    self.source
                        .address
                        // Do not match if this datasource has no address
                        .map_or(false, |addr| addr == *address)
                }
                EthereumBlockTriggerType::Every => true,
            };
            source_address_matches && self.handler_for_block(block_trigger_type).is_ok()
        };

        matches_block_trigger && self.source.start_block <= block_number
    }

    pub fn handlers_for_log(
        &self,
        log: &Arc<Log>,
    ) -> Result<Vec<MappingEventHandler>, anyhow::Error> {
        // Get signature from the log
        let topic0 = log.topics.get(0).context("Ethereum event has no topics")?;

        let handlers = self
            .mapping
            .event_handlers
            .iter()
            .filter(|handler| *topic0 == handler.topic0())
            .cloned()
            .collect::<Vec<_>>();

        ensure!(
            !handlers.is_empty(),
            "No event handler found for event in data source \"{}\"",
            self.name,
        );

        Ok(handlers)
    }

    pub fn handler_for_call(&self, call: &EthereumCall) -> Result<MappingCallHandler, Error> {
        // First four bytes of the input for the call are the first four
        // bytes of hash of the function signature
        ensure!(
            call.input.0.len() >= 4,
            "Ethereum call has input with less than 4 bytes"
        );

        let target_method_id = &call.input.0[..4];

        self.mapping
            .call_handlers
            .iter()
            .find(move |handler| {
                let fhash = keccak256(handler.function.as_bytes());
                let actual_method_id = [fhash[0], fhash[1], fhash[2], fhash[3]];
                target_method_id == actual_method_id
            })
            .cloned()
            .with_context(|| {
                anyhow!(
                    "No call handler found for call in data source \"{}\"",
                    self.name,
                )
            })
    }

    pub fn handler_for_block(
        &self,
        trigger_type: &EthereumBlockTriggerType,
    ) -> Result<MappingBlockHandler, anyhow::Error> {
        match trigger_type {
            EthereumBlockTriggerType::Every => self
                .mapping
                .block_handlers
                .iter()
                .find(move |handler| handler.filter == None)
                .cloned()
                .with_context(|| {
                    anyhow!(
                        "No block handler for `Every` block trigger \
                         type found in data source \"{}\"",
                        self.name,
                    )
                }),
            EthereumBlockTriggerType::WithCallTo(_address) => self
                .mapping
                .block_handlers
                .iter()
                .find(move |handler| {
                    handler.filter.is_some()
                        && handler.filter.clone().unwrap() == BlockHandlerFilter::Call
                })
                .cloned()
                .with_context(|| {
                    anyhow!(
                        "No block handler for `WithCallTo` block trigger \
                         type found in data source \"{}\"",
                        self.name,
                    )
                }),
        }
    }

    pub fn is_duplicate_of(&self, other: &Self) -> bool {
        let DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,

            // The creation block is ignored for detection duplicate data sources.
            creation_block: _,
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
