#[rustfmt::skip]
#[path = "protobuf/sf.ethereum.r#type.v2.rs"]
mod pbcodec;

use anyhow::format_err;
use graph::{
    blockchain::{
        self, Block as BlockchainBlock, BlockPtr, BlockTime, ChainStoreBlock, ChainStoreData,
    },
    prelude::{
        alloy::{
            self,
            consensus::{ReceiptEnvelope, ReceiptWithBloom, TxEnvelope, TxType},
            primitives::{aliases::B2048, Address, Bloom, Bytes, LogData, B256, U256},
            rpc::types::{
                AccessList, AccessListItem, Block as AlloyBlock, Transaction,
                TransactionReceipt as AlloyTransactionReceipt,
            },
        },
        BlockNumber, Error, EthereumBlock, EthereumBlockWithCalls, EthereumCall,
        LightEthereumBlock,
    },
};
use std::sync::Arc;
use std::{convert::TryFrom, fmt::Debug};

use crate::chain::BlockFinality;

pub use pbcodec::*;

trait TryDecodeProto<U, V>: Sized
where
    U: TryFrom<Self>,
    <U as TryFrom<Self>>::Error: Debug,
    V: From<U>,
{
    fn try_decode_proto(self, label: &'static str) -> Result<V, Error> {
        let u = U::try_from(self).map_err(|e| format_err!("invalid {}: {:?}", label, e))?;
        let v = V::from(u);
        Ok(v)
    }
}

// impl TryDecodeProto<[u8; 256], H2048> for &[u8] {}
// impl TryDecodeProto<[u8; 32], H256> for &[u8] {}
// impl TryDecodeProto<[u8; 20], H160> for &[u8] {}

impl TryDecodeProto<[u8; 32], B256> for &[u8] {}
impl TryDecodeProto<[u8; 256], B2048> for &[u8] {}
impl TryDecodeProto<[u8; 20], Address> for &[u8] {}

impl From<&BigInt> for alloy::primitives::U256 {
    fn from(val: &BigInt) -> Self {
        alloy::primitives::U256::from_be_slice(&val.bytes)
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

impl<'a> TryInto<EthereumCall> for CallAt<'a> {
    type Error = Error;

    fn try_into(self) -> Result<EthereumCall, Self::Error> {
        Ok(EthereumCall {
            from: self.call.caller.try_decode_proto("call from address")?,
            to: self.call.address.try_decode_proto("call to address")?,
            value: self
                .call
                .value
                .as_ref()
                .map_or_else(|| alloy::primitives::U256::from(0), |v| v.into()),
            gas_used: self.call.gas_consumed,
            input: Bytes::from(self.call.input.clone()),
            output: Bytes::from(self.call.return_data.clone()),
            block_hash: self.block.hash.try_decode_proto("call block hash")?,
            block_number: self.block.number as i32,
            transaction_hash: Some(self.trace.hash.try_decode_proto("call transaction hash")?),
            transaction_index: self.trace.index as u64,
        })
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

impl<'a> TryInto<alloy::rpc::types::Log> for LogAt<'a> {
    type Error = Error;

    fn try_into(self) -> Result<alloy::rpc::types::Log, Self::Error> {
        let topics = self
            .log
            .topics
            .iter()
            .map(|t| t.try_decode_proto("topic"))
            .collect::<Result<Vec<B256>, Error>>()?;

        Ok(alloy::rpc::types::Log {
            inner: alloy::primitives::Log {
                address: self.log.address.try_decode_proto("log address")?,
                data: LogData::new(topics, self.log.data.clone().into())
                    .ok_or_else(|| format_err!("invalid log data"))?,
            },
            block_hash: Some(self.block.hash.try_decode_proto("log block hash")?),
            block_number: Some(self.block.number),
            transaction_hash: Some(self.trace.hash.try_decode_proto("log transaction hash")?),
            transaction_index: Some(self.trace.index as u64),
            log_index: Some(self.log.block_index as u64),
            removed: false,
            block_timestamp: self
                .block
                .header
                .as_ref()
                .map(|h| h.timestamp.as_ref().map(|t| t.seconds as u64))
                .flatten(),
        })
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

impl<'a> TryInto<Transaction> for TransactionTraceAt<'a> {
    type Error = Error;

    fn try_into(self) -> Result<Transaction, Self::Error> {
        use alloy::{
            consensus::transaction::Recovered,
            consensus::{
                Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip7702, TxLegacy,
            },
            primitives::{Bytes, TxKind, U256},
            rpc::types::Transaction as AlloyTransaction,
        };

        // Extract data from trace and block
        let block_hash = self.block.hash.try_decode_proto("transaction block hash")?;
        let block_number = self.block.number;
        let transaction_index = Some(self.trace.index as u64);
        let from_address = self
            .trace
            .from
            .try_decode_proto("transaction from address")?;
        let to = get_to_address(self.trace)?;
        let value = self.trace.value.as_ref().map_or(U256::ZERO, |x| x.into());
        let gas_price = self.trace.gas_price.as_ref().map_or(0u128, |x| {
            let val: U256 = x.into();
            val.to::<u128>()
        });
        let gas_limit = self.trace.gas_limit;
        let input = Bytes::from(self.trace.input.clone());

        let tx_type = u64::try_from(self.trace.r#type).map_err(|_| {
            format_err!(
                "Invalid transaction type value {} in transaction trace. Transaction type must be a valid u64.",
                self.trace.r#type
            )
        })?;

        let tx_type = TxType::try_from(tx_type).map_err(|_| {
            format_err!(
                "Unsupported transaction type {} in transaction trace. Only standard Ethereum transaction types (Legacy=0, EIP-2930=1, EIP-1559=2, EIP-4844=3, EIP-7702=4) are supported.",
                tx_type
            )
        })?;

        let nonce = self.trace.nonce;

        // Extract EIP-1559 fee fields from trace
        let max_fee_per_gas_u128 = self.trace.max_fee_per_gas.as_ref().map_or(gas_price, |x| {
            let val: U256 = x.into();
            val.to::<u128>()
        });

        let max_priority_fee_per_gas_u128 =
            self.trace
                .max_priority_fee_per_gas
                .as_ref()
                .map_or(0u128, |x| {
                    let val: U256 = x.into();
                    val.to::<u128>()
                });

        // Extract access list from trace
        let access_list: AccessList = self
            .trace
            .access_list
            .iter()
            .map(|access_tuple| {
                let address = Address::from_slice(&access_tuple.address);
                let storage_keys = access_tuple
                    .storage_keys
                    .iter()
                    .map(|key| B256::from_slice(key))
                    .collect();
                AccessListItem {
                    address,
                    storage_keys,
                }
            })
            .collect::<Vec<_>>()
            .into();

        // Extract actual signature components from trace
        let signature = extract_signature_from_trace(self.trace, tx_type)?;

        let to_kind = match to {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        };

        let envelope = match tx_type {
            TxType::Legacy => {
                let tx = TxLegacy {
                    chain_id: None,
                    nonce,
                    gas_price,
                    gas_limit,
                    to: to_kind,
                    value,
                    input: input.clone(),
                };
                let signed_tx = Signed::new_unchecked(tx, signature, B256::ZERO); // TODO(alloy_migration): extract actual transaction hash from trace
                TxEnvelope::Legacy(signed_tx)
            }
            TxType::Eip2930 => {
                let tx = TxEip2930 {
                    chain_id: 1, // TODO(alloy_migration): extract actual chain_id from trace
                    nonce,
                    gas_price,
                    gas_limit,
                    to: to_kind,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    input: input.clone(),
                };
                let signed_tx = Signed::new_unchecked(tx, signature, B256::ZERO); // TODO(alloy_migration): extract actual transaction hash from trace
                TxEnvelope::Eip2930(signed_tx)
            }
            TxType::Eip1559 => {
                let tx = TxEip1559 {
                    chain_id: 1, // TODO(alloy_migration): extract actual chain_id from trace
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas_u128,
                    max_priority_fee_per_gas: max_priority_fee_per_gas_u128,
                    to: to_kind,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    input: input.clone(),
                };
                let signed_tx = Signed::new_unchecked(tx, signature, B256::ZERO); // TODO(alloy_migration): extract actual transaction hash from trace
                TxEnvelope::Eip1559(signed_tx)
            }
            TxType::Eip4844 => {
                let to_address = to.ok_or_else(|| {
                    format_err!("EIP-4844 transactions cannot be contract creation transactions. The 'to' field must contain a valid address.")
                })?;

                let tx_eip4844 = TxEip4844 {
                    chain_id: 1, // TODO(alloy_migration): extract actual chain_id from trace
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas_u128,
                    max_priority_fee_per_gas: max_priority_fee_per_gas_u128,
                    to: to_address,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    blob_versioned_hashes: Vec::new(), // TODO(alloy_migration): extract actual blob hashes from trace
                    max_fee_per_blob_gas: 0u128, // TODO(alloy_migration): extract actual blob gas fee from trace
                    input: input.clone(),
                };
                let tx = TxEip4844Variant::TxEip4844(tx_eip4844);
                let signed_tx = Signed::new_unchecked(tx, signature, B256::ZERO); // TODO(alloy_migration): extract actual transaction hash from trace
                TxEnvelope::Eip4844(signed_tx)
            }
            TxType::Eip7702 => {
                let to_address = to.ok_or_else(|| {
                    format_err!("EIP-7702 transactions cannot be contract creation transactions. The 'to' field must contain a valid address.")
                })?;

                let tx = TxEip7702 {
                    chain_id: 1, // TODO(alloy_migration): extract actual chain_id from trace
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas_u128,
                    max_priority_fee_per_gas: max_priority_fee_per_gas_u128,
                    to: to_address,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    authorization_list: Vec::new(), // TODO(alloy_migration): extract actual authorization list from trace
                    input: input.clone(),
                };
                let signed_tx = Signed::new_unchecked(tx, signature, B256::ZERO); // TODO(alloy_migration): extract actual transaction hash from trace
                TxEnvelope::Eip7702(signed_tx)
            }
        };

        let recovered = Recovered::new_unchecked(envelope, from_address);

        Ok(AlloyTransaction {
            inner: recovered,
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            transaction_index,
            effective_gas_price: if gas_price > 0 { Some(gas_price) } else { None }, // TODO(alloy_migration): calculate actual effective gas price from trace
        })
    }
}

impl TryInto<BlockFinality> for &Block {
    type Error = Error;

    fn try_into(self) -> Result<BlockFinality, Self::Error> {
        Ok(BlockFinality::NonFinal(self.try_into()?))
    }
}

impl TryInto<AlloyBlock> for &Block {
    type Error = Error;

    fn try_into(self) -> Result<AlloyBlock, Self::Error> {
        let header = self.header();

        let block_hash = self.hash.try_decode_proto("block hash")?;
        let consensus_header = alloy::consensus::Header {
            number: header.number,
            beneficiary: header.coinbase.try_decode_proto("author / coinbase")?,
            parent_hash: header.parent_hash.try_decode_proto("parent hash")?,
            ommers_hash: header.uncle_hash.try_decode_proto("uncle hash")?,
            state_root: header.state_root.try_decode_proto("state root")?,
            transactions_root: header
                .transactions_root
                .try_decode_proto("transactions root")?,
            receipts_root: header.receipt_root.try_decode_proto("receipt root")?,
            gas_used: header.gas_used,
            gas_limit: header.gas_limit,
            base_fee_per_gas: header.base_fee_per_gas.as_ref().map(|v| {
                let val: U256 = v.into();
                val.to::<u64>()
            }),
            extra_data: Bytes::from(header.extra_data.clone()),
            logs_bloom: if header.logs_bloom.is_empty() {
                Bloom::ZERO
            } else {
                Bloom::try_from(header.logs_bloom.as_slice())?
            },
            timestamp: header.timestamp.as_ref().map_or(0, |v| v.seconds as u64),
            difficulty: header
                .difficulty
                .as_ref()
                .map_or_else(|| U256::ZERO, |v| v.into()),

            mix_hash: header.mix_hash.try_decode_proto("mix hash")?,
            nonce: header.nonce.into(),

            withdrawals_root: None, // TODO(alloy_migration): extract from header if available
            blob_gas_used: None,    // TODO(alloy_migration): extract from header if available
            excess_blob_gas: None,  // TODO(alloy_migration): extract from header if available
            parent_beacon_block_root: None, // TODO(alloy_migration): extract from header if available
            requests_hash: None, // TODO(alloy_migration): extract from header if available
        };

        let rpc_header = alloy::rpc::types::Header {
            hash: block_hash,
            inner: consensus_header,
            total_difficulty: header.total_difficulty.as_ref().map(|v| v.into()),
            size: Some(U256::from(self.size)),
        };

        let transactions = self
            .transaction_traces
            .iter()
            .map(|t| TransactionTraceAt::new(t, self).try_into())
            .collect::<Result<Vec<Transaction>, Error>>()?;

        let uncles = self
            .uncles
            .iter()
            .map(|u| u.hash.try_decode_proto("uncle hash"))
            .collect::<Result<Vec<B256>, _>>()?;

        Ok(AlloyBlock::new(
            rpc_header,
            alloy::rpc::types::BlockTransactions::Full(transactions),
        )
        .with_uncles(uncles))
    }
}

impl TryInto<EthereumBlockWithCalls> for &Block {
    type Error = Error;

    fn try_into(self) -> Result<EthereumBlockWithCalls, Self::Error> {
        let alloy_block: AlloyBlock = self.try_into()?;

        let transaction_receipts = self
            .transaction_traces
            .iter()
            .filter_map(|t| transaction_trace_to_alloy_txn_reciept(t, self).transpose())
            .collect::<Result<Vec<_>, Error>>()?
            .into_iter()
            // Transaction receipts will be shared along the code, so we put them into an
            // Arc here to avoid excessive cloning.
            .map(Arc::new)
            .collect();

        #[allow(unreachable_code)]
        let block = EthereumBlockWithCalls {
            ethereum_block: EthereumBlock {
                block: Arc::new(LightEthereumBlock::new(alloy_block)),
                transaction_receipts,
            },
            // Comment (437a9f17-67cc-478f-80a3-804fe554b227): This Some() will avoid calls in the triggers_in_block
            // TODO: Refactor in a way that this is no longer needed.
            calls: Some(
                self.transaction_traces
                    .iter()
                    .flat_map(|trace| {
                        trace
                            .calls
                            .iter()
                            .filter(|call| !call.status_reverted && !call.status_failed)
                            .map(|call| CallAt::new(call, self, trace).try_into())
                            .collect::<Vec<Result<EthereumCall, Error>>>()
                    })
                    .collect::<Result<_, _>>()?,
            ),
        };

        Ok(block)
    }
}

fn transaction_trace_to_alloy_txn_reciept(
    t: &TransactionTrace,
    block: &Block,
) -> Result<Option<AlloyTransactionReceipt<ReceiptEnvelope<alloy::rpc::types::Log>>>, Error> {
    use alloy::consensus::{Eip658Value, Receipt};
    let r = t.receipt.as_ref();

    if r.is_none() {
        return Ok(None);
    }

    let r = r.unwrap();

    let contract_address = match t.calls.len() {
        0 => None,
        _ => {
            match CallType::try_from(t.calls[0].call_type).map_err(|_| {
                graph::anyhow::anyhow!("invalid call type: {}", t.calls[0].call_type)
            })? {
                CallType::Create => Some(
                    t.calls[0]
                        .address
                        .try_decode_proto("transaction contract address")?,
                ),
                _ => None,
            }
        }
    };

    let state_root = match &r.state_root {
        b if b.is_empty() => None,
        _ => Some(r.state_root.try_decode_proto("transaction state root")?),
    };

    let status = match TransactionTraceStatus::try_from(t.status)
        .map_err(|_| format_err!("invalid transaction trace status: {}", t.status))?
    {
        TransactionTraceStatus::Unknown => {
            return Err(format_err!(
                "Transaction trace has UNKNOWN status; datasource is broken"
            ))
        }
        TransactionTraceStatus::Succeeded => true,
        TransactionTraceStatus::Failed | TransactionTraceStatus::Reverted => false,
    };

    // [EIP-658]: https://eips.ethereum.org/EIPS/eip-658
    // Before EIP-658, the state root field was used to indicate the status of the transaction.
    // After EIP-658, the status field is used to indicate the status of the transaction.
    let status = match state_root {
        Some(root) => Eip658Value::PostState(root),
        None => Eip658Value::Eip658(status),
    };

    let logs: Vec<alloy::rpc::types::Log> = r
        .logs
        .iter()
        .map(|l| LogAt::new(l, block, t).try_into())
        .collect::<Result<Vec<_>, Error>>()?;

    let core_receipt = Receipt {
        status,
        cumulative_gas_used: r.cumulative_gas_used,
        logs,
    };

    let logs_bloom = Bloom::try_from(r.logs_bloom.as_slice())?;

    let receipt_with_bloom = ReceiptWithBloom::new(core_receipt, logs_bloom);

    let tx_type = TxType::try_from(u64::try_from(t.r#type).map_err(|_| {
        format_err!(
            "Invalid transaction type value {} in transaction receipt. Transaction type must be a valid u64.",
            t.r#type
        )
    })?).map_err(|_| {
        format_err!(
            "Unsupported transaction type {} in transaction receipt. Only standard Ethereum transaction types (Legacy=0, EIP-2930=1, EIP-1559=2, EIP-4844=3, EIP-7702=4) are supported.",
            t.r#type
        )
    })?;

    let envelope = match tx_type {
        TxType::Legacy => ReceiptEnvelope::Legacy(receipt_with_bloom),
        TxType::Eip2930 => ReceiptEnvelope::Eip2930(receipt_with_bloom),
        TxType::Eip1559 => ReceiptEnvelope::Eip1559(receipt_with_bloom),
        TxType::Eip4844 => ReceiptEnvelope::Eip4844(receipt_with_bloom),
        TxType::Eip7702 => ReceiptEnvelope::Eip7702(receipt_with_bloom),
    };

    Ok(Some(AlloyTransactionReceipt {
        transaction_hash: t.hash.try_decode_proto("transaction hash")?,
        transaction_index: Some(t.index as u64),
        block_hash: Some(block.hash.try_decode_proto("transaction block hash")?),
        block_number: Some(block.number),
        gas_used: t.gas_used,
        contract_address,
        from: t.from.try_decode_proto("transaction from")?,
        to: get_to_address(t)?,
        effective_gas_price: 0, // TODO(alloy_migration): calculate actual effective gas price from trace
        blob_gas_used: None, // TODO(alloy_migration): extract blob gas used from trace if applicable
        blob_gas_price: None, // TODO(alloy_migration): extract blob gas price from trace if applicable
        inner: envelope,
    }))
}

impl BlockHeader {
    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        match self.parent_hash.len() {
            0 => None,
            _ => Some(BlockPtr::from((
                B256::from_slice(self.parent_hash.as_ref()),
                self.number - 1,
            ))),
        }
    }
}

impl<'a> From<&'a BlockHeader> for BlockPtr {
    fn from(b: &'a BlockHeader) -> BlockPtr {
        BlockPtr::from((B256::from_slice(b.hash.as_ref()), b.number))
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        BlockPtr::from((B256::from_slice(b.hash.as_ref()), b.number))
    }
}

impl Block {
    pub fn header(&self) -> &BlockHeader {
        self.header.as_ref().unwrap()
    }

    pub fn ptr(&self) -> BlockPtr {
        BlockPtr::from(self.header())
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }
}

impl BlockchainBlock for Block {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().number).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }

    // This implementation provides the timestamp so that it works with block _meta's timestamp.
    // However, the firehose types will not populate the transaction receipts so switching back
    // from firehose ingestor to the firehose ingestor will prevent non final block from being
    // processed using the block stored by firehose.
    fn data(&self) -> Result<jsonrpc_core::serde_json::Value, jsonrpc_core::serde_json::Error> {
        self.header().to_json()
    }

    fn timestamp(&self) -> BlockTime {
        let ts = self.header().timestamp.as_ref().unwrap();
        BlockTime::since_epoch(ts.seconds, ts.nanos as u32)
    }
}

impl HeaderOnlyBlock {
    pub fn header(&self) -> &BlockHeader {
        self.header.as_ref().unwrap()
    }
}

impl From<&BlockHeader> for ChainStoreData {
    fn from(val: &BlockHeader) -> Self {
        ChainStoreData {
            block: ChainStoreBlock::new(
                val.timestamp.as_ref().unwrap().seconds,
                jsonrpc_core::Value::Null,
            ),
        }
    }
}

impl BlockHeader {
    fn to_json(&self) -> Result<jsonrpc_core::serde_json::Value, jsonrpc_core::serde_json::Error> {
        let chain_store_data: ChainStoreData = self.into();

        jsonrpc_core::to_value(chain_store_data)
    }
}

impl<'a> From<&'a HeaderOnlyBlock> for BlockPtr {
    fn from(b: &'a HeaderOnlyBlock) -> BlockPtr {
        BlockPtr::from(b.header())
    }
}

impl BlockchainBlock for HeaderOnlyBlock {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().number).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }

    // This implementation provides the timestamp so that it works with block _meta's timestamp.
    // However, the firehose types will not populate the transaction receipts so switching back
    // from firehose ingestor to the firehose ingestor will prevent non final block from being
    // processed using the block stored by firehose.
    fn data(&self) -> Result<jsonrpc_core::serde_json::Value, jsonrpc_core::serde_json::Error> {
        self.header().to_json()
    }

    fn timestamp(&self) -> blockchain::BlockTime {
        let ts = self.header().timestamp.as_ref().unwrap();
        blockchain::BlockTime::since_epoch(ts.seconds, ts.nanos as u32)
    }
}

#[cfg(test)]
mod test {
    use graph::{blockchain::Block as _, prelude::chrono::Utc};
    use prost_types::Timestamp;

    use crate::codec::BlockHeader;

    use super::Block;

    #[test]
    fn ensure_block_serialization() {
        let now = Utc::now().timestamp();
        let mut block = Block::default();
        let mut header = BlockHeader::default();
        header.timestamp = Some(Timestamp {
            seconds: now,
            nanos: 0,
        });

        block.header = Some(header);

        let str_block = block.data().unwrap().to_string();

        assert_eq!(
            str_block,
            // if you're confused when reading this, format needs {{ to escape {
            format!(r#"{{"block":{{"data":null,"timestamp":"{}"}}}}"#, now)
        );
    }
}

fn extract_signature_from_trace(
    _trace: &TransactionTrace,
    _tx_type: TxType,
) -> Result<alloy::signers::Signature, Error> {
    use alloy::primitives::{Signature as PrimitiveSignature, U256};

    // Create a dummy signature with r = 0, s = 0 and even y-parity (false)
    let dummy = PrimitiveSignature::new(U256::ZERO, U256::ZERO, false);

    Ok(dummy.into())
}

fn get_to_address(trace: &TransactionTrace) -> Result<Option<Address>, Error> {
    // Try to detect contract creation transactions, which have no 'to' address
    let is_contract_creation = trace.to.len() == 0
        || trace.calls.get(0).map_or(false, |call| {
            CallType::try_from(call.call_type)
                .map_or(false, |call_type| call_type == CallType::Create)
        });

    if is_contract_creation {
        Ok(None)
    } else {
        Ok(Some(trace.to.try_decode_proto("transaction to address")?))
    }
}
