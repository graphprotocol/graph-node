#[path = "protobuf/dfuse.ethereum.codec.v1.rs"]
mod pbcodec;

use graph::{
    blockchain::{Block as BlockchainBlock, BlockPtr},
    prelude::{
        web3,
        web3::types::TransactionReceipt as w3TransactionReceipt,
        web3::types::{Bytes, H160, H2048, H256, H64, U256, U64},
        BlockNumber, EthereumBlock, EthereumBlockWithCalls, EthereumCall, LightEthereumBlock,
    },
};
use std::convert::TryFrom;
use std::sync::Arc;

use crate::chain::BlockFinality;

pub use pbcodec::*;

impl Into<web3::types::U256> for &BigInt {
    fn into(self) -> web3::types::U256 {
        web3::types::U256::from_big_endian(&self.bytes)
    }
}

pub struct CallAt<'a> {
    call: &'a Call,
    block: &'a Block,
    trace: &'a TransactionTrace,
}

impl<'a> CallAt<'a> {
    pub fn new(call: &'a Call, block: &'a Block, trace: &'a TransactionTrace) -> Self {
        Self { call, block, trace }
    }
}

impl<'a> Into<EthereumCall> for CallAt<'a> {
    fn into(self) -> EthereumCall {
        EthereumCall {
            from: H160::from_slice(&self.call.caller),
            to: H160::from_slice(&self.call.address),
            value: self
                .call
                .value
                .as_ref()
                .map_or_else(|| U256::from(0), |v| v.into()),
            gas_used: U256::from(self.call.gas_consumed),
            input: Bytes(self.call.input.clone()),
            output: Bytes(self.call.return_data.clone()),
            block_hash: H256::from_slice(&self.block.hash),
            block_number: self.block.number as i32,
            transaction_hash: Some(H256::from_slice(&self.trace.hash)),
            transaction_index: self.trace.index as u64,
        }
    }
}

impl Into<web3::types::Call> for Call {
    fn into(self) -> web3::types::Call {
        web3::types::Call {
            from: H160::from_slice(&self.caller),
            to: H160::from_slice(&self.address),
            value: self
                .value
                .as_ref()
                .map_or_else(|| U256::from(0), |v| v.into()),
            gas: U256::from(self.gas_limit),
            input: Bytes::from(self.input.clone()),
            call_type: CallType::from_i32(self.call_type)
                .expect("CallType invalid enum value")
                .into(),
        }
    }
}

impl Into<web3::types::CallType> for CallType {
    fn into(self) -> web3::types::CallType {
        match self {
            CallType::Unspecified => web3::types::CallType::None,
            CallType::Call => web3::types::CallType::Call,
            CallType::Callcode => web3::types::CallType::CallCode,
            CallType::Delegate => web3::types::CallType::DelegateCall,
            CallType::Static => web3::types::CallType::StaticCall,

            // FIXME (SF): Really not sure what this should map to, we are using None for now, need to revisit
            CallType::Create => web3::types::CallType::None,
        }
    }
}

pub struct LogAt<'a> {
    log: &'a Log,
    block: &'a Block,
    trace: &'a TransactionTrace,
}

impl<'a> LogAt<'a> {
    pub fn new(log: &'a Log, block: &'a Block, trace: &'a TransactionTrace) -> Self {
        Self { log, block, trace }
    }
}

impl<'a> Into<web3::types::Log> for LogAt<'a> {
    fn into(self) -> web3::types::Log {
        web3::types::Log {
            address: H160::from_slice(&self.log.address),
            topics: self
                .log
                .topics
                .iter()
                .map(|t| H256::from_slice(t))
                .collect(),
            data: Bytes::from(self.log.data.clone()),
            block_hash: Some(H256::from_slice(&self.block.hash)),
            block_number: Some(U64::from(self.block.number)),
            transaction_hash: Some(H256::from_slice(&self.trace.hash)),
            transaction_index: Some(U64::from(self.trace.index as u64)),
            log_index: Some(U256::from(self.log.block_index)),
            transaction_log_index: Some(U256::from(self.log.index)),
            log_type: None,
            removed: None,
        }
    }
}

impl Into<web3::types::U64> for TransactionTraceStatus {
    fn into(self) -> web3::types::U64 {
        let status: Option<web3::types::U64> = self.into();
        status.unwrap_or_else(|| web3::types::U64::from(0))
    }
}

impl Into<Option<web3::types::U64>> for TransactionTraceStatus {
    fn into(self) -> Option<web3::types::U64> {
        match self {
            Self::Unknown => None,
            Self::Succeeded => Some(web3::types::U64::from(1)),
            Self::Failed => Some(web3::types::U64::from(0)),
            Self::Reverted => Some(web3::types::U64::from(0)),
        }
    }
}

pub struct TransactionTraceAt<'a> {
    trace: &'a TransactionTrace,
    block: &'a Block,
}

impl<'a> TransactionTraceAt<'a> {
    pub fn new(trace: &'a TransactionTrace, block: &'a Block) -> Self {
        Self { trace, block }
    }
}

impl<'a> Into<web3::types::Transaction> for TransactionTraceAt<'a> {
    fn into(self) -> web3::types::Transaction {
        web3::types::Transaction {
            hash: H256::from_slice(&self.trace.hash),
            nonce: U256::from(self.trace.nonce),
            block_hash: Some(H256::from_slice(&self.block.hash)),
            block_number: Some(U64::from(self.block.number)),
            transaction_index: Some(U64::from(self.trace.index as u64)),
            from: Some(H160::from_slice(&self.trace.from)),
            to: Some(H160::from_slice(&self.trace.to)),
            value: self
                .trace
                .value
                .as_ref()
                .map_or_else(|| U256::from(0), |x| x.into()),
            gas_price: self
                .trace
                .gas_price
                .as_ref()
                .map_or_else(|| U256::from(0), |x| x.into()),
            gas: U256::from(self.trace.gas_used),
            input: Bytes::from(self.trace.input.clone()),
            v: None,
            r: None,
            s: None,
            raw: None,
        }
    }
}

impl Into<BlockFinality> for &Block {
    fn into(self) -> BlockFinality {
        BlockFinality::NonFinal(self.into())
    }
}

impl Into<EthereumBlockWithCalls> for &Block {
    fn into(self) -> EthereumBlockWithCalls {
        let header = self
            .header
            .as_ref()
            .expect("block header should always be present from gRPC Firehose");

        EthereumBlockWithCalls {
            ethereum_block: EthereumBlock {
                block: Arc::new(LightEthereumBlock {
                    hash: Some(H256::from_slice(&self.hash)),
                    number: Some(U64::from(self.number)),
                    author: H160::from_slice(&header.coinbase),
                    parent_hash: H256::from_slice(&header.parent_hash),
                    uncles_hash: H256::from_slice(&header.uncle_hash),
                    state_root: H256::from_slice(&header.state_root),
                    transactions_root: H256::from_slice(&header.transactions_root),
                    receipts_root: H256::from_slice(&header.receipt_root),
                    gas_used: U256::from(header.gas_used),
                    gas_limit: U256::from(header.gas_limit),
                    base_fee_per_gas: None,
                    extra_data: Bytes::from(header.extra_data.clone()),
                    logs_bloom: match &header.logs_bloom.len() {
                        0 => None,
                        _ => Some(H2048::from_slice(&header.logs_bloom)),
                    },
                    timestamp: U256::from(
                        header
                            .timestamp
                            .as_ref()
                            .map_or_else(|| U256::default(), |v| U256::from(v.seconds)),
                    ),
                    difficulty: header
                        .difficulty
                        .as_ref()
                        .map_or_else(|| U256::default(), |v| v.into()),
                    // FIXME (SF): Not sure we have such equivalent stuff in Firehose, need to double-check (is this important?)
                    total_difficulty: None,
                    // FIXME (SF): Firehose does not have seal fields, are they really used? Might be required for POA chains only also, I've seen that stuff on xDai (is this important?)
                    seal_fields: vec![],
                    uncles: self
                        .uncles
                        .iter()
                        .map(|u| H256::from_slice(&u.hash))
                        .collect(),
                    transactions: self
                        .transaction_traces
                        .iter()
                        .map(|t| TransactionTraceAt::new(t, &self).into())
                        .collect(),
                    size: Some(U256::from(self.size)),
                    mix_hash: Some(H256::from_slice(&header.mix_hash)),
                    nonce: Some(H64::from_low_u64_be(header.nonce)),
                }),
                transaction_receipts: self
                    .transaction_traces
                    .iter()
                    .filter_map(|t| {
                        t.receipt.as_ref().map(|r| w3TransactionReceipt {
                            transaction_hash: H256::from_slice(&t.hash),
                            transaction_index: U64::from(t.index),
                            block_hash: Some(H256::from_slice(&self.hash)),
                            block_number: Some(U64::from(self.number)),
                            cumulative_gas_used: U256::from(r.cumulative_gas_used),
                            // FIXME (SF): What is the rule here about gas_used being None, when it's 0?
                            gas_used: Some(U256::from(t.gas_used)),
                            contract_address: {
                                match t.calls.len() {
                                    0 => None,
                                    _ => match CallType::from_i32(t.calls[0].call_type)
                                        .expect("invalid enum type")
                                    {
                                        CallType::Create => {
                                            Some(H160::from_slice(&t.calls[0].address))
                                        }
                                        _ => None,
                                    },
                                }
                            },
                            logs: r
                                .logs
                                .iter()
                                .map(|l| LogAt::new(l, &self, t).into())
                                .collect(),
                            status: TransactionTraceStatus::from_i32(t.status).unwrap().into(),
                            root: match r.state_root.len() {
                                0 => None, // FIXME (SF): should this instead map to [0;32]?
                                // FIXME (SF): if len < 32, what do we do?
                                _ => Some(H256::from_slice(&r.state_root)),
                            },
                            logs_bloom: H2048::from_slice(&r.logs_bloom),
                        })
                    })
                    .collect(),
            },
            calls: Some(
                self.transaction_traces
                    .iter()
                    .flat_map(|trace| {
                        trace
                            .calls
                            .iter()
                            .map(|call| CallAt::new(call, self, trace).into())
                            .collect::<Vec<EthereumCall>>()
                    })
                    .collect(),
            ),
        }
    }
}

impl From<Block> for BlockPtr {
    fn from(b: Block) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        BlockPtr::from((H256::from_slice(b.hash.as_ref()), b.number))
    }
}

impl BlockchainBlock for Block {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.number).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        let parent_hash = &self.header.as_ref().unwrap().parent_hash;

        match parent_hash.len() {
            0 => None,
            _ => Some(BlockPtr::from((
                H256::from_slice(parent_hash.as_ref()),
                self.number - 1,
            ))),
        }
    }
}
