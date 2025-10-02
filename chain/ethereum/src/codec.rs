#[rustfmt::skip]
#[path = "protobuf/sf.ethereum.r#type.v2.rs"]
mod pbcodec;

use anyhow::format_err;
use graph::{
    blockchain::{
        self, Block as BlockchainBlock, BlockPtr, BlockTime, ChainStoreBlock, ChainStoreData,
    },
    components::ethereum::{AnyBlock, AnyHeader, AnyRpcHeader, AnyRpcTransaction, AnyTxEnvelope},
    prelude::{
        alloy::{
            self,
            consensus::{ReceiptWithBloom, TxEnvelope, TxType},
            network::AnyReceiptEnvelope,
            primitives::{aliases::B2048, Address, Bloom, Bytes, LogData, B256, U256},
            rpc::types::{self as alloy_rpc_types, AccessList, AccessListItem, Transaction},
            serde::WithOtherFields,
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

impl TryDecodeProto<[u8; 32], B256> for &[u8] {}
impl TryDecodeProto<[u8; 256], B2048> for &[u8] {}
impl TryDecodeProto<[u8; 20], Address> for &[u8] {}

impl From<&BigInt> for U256 {
    fn from(val: &BigInt) -> Self {
        U256::from_be_slice(&val.bytes)
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
                .map_or_else(|| U256::from(0), |v| v.into()),
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

impl<'a> TryInto<Transaction<AnyTxEnvelope>> for TransactionTraceAt<'a> {
    type Error = Error;

    fn try_into(self) -> Result<Transaction<AnyTxEnvelope>, Self::Error> {
        use alloy::{
            consensus::transaction::Recovered,
            consensus::{
                Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip7702, TxLegacy,
            },
            network::{AnyTxEnvelope, AnyTxType, UnknownTxEnvelope, UnknownTypedTransaction},
            primitives::{Bytes, TxKind, U256},
            rpc::types::Transaction as AlloyTransaction,
            serde::OtherFields,
        };
        use std::collections::BTreeMap;

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

        let tx_type_u64 = u64::try_from(self.trace.r#type).map_err(|_| {
            format_err!(
                "Invalid transaction type value {} in transaction trace. Transaction type must be a valid u64.",
                self.trace.r#type
            )
        })?;

        // Try to convert to known Ethereum transaction type
        let tx_type_result = TxType::try_from(tx_type_u64);

        // If this is an unknown transaction type, create an UnknownTxEnvelope
        if tx_type_result.is_err() {
            let mut fields_map = BTreeMap::new();

            fields_map.insert(
                "nonce".to_string(),
                jsonrpc_core::serde_json::json!(format!("0x{:x}", self.trace.nonce)),
            );
            fields_map.insert(
                "from".to_string(),
                jsonrpc_core::serde_json::json!(format!("{:?}", from_address)),
            );
            if let Some(to_addr) = to {
                fields_map.insert(
                    "to".to_string(),
                    jsonrpc_core::serde_json::json!(format!("{:?}", to_addr)),
                );
            }
            fields_map.insert(
                "value".to_string(),
                jsonrpc_core::serde_json::json!(format!("0x{:x}", value)),
            );
            fields_map.insert(
                "gas".to_string(),
                jsonrpc_core::serde_json::json!(format!("0x{:x}", gas_limit)),
            );
            fields_map.insert(
                "gasPrice".to_string(),
                jsonrpc_core::serde_json::json!(format!("0x{:x}", gas_price)),
            );
            fields_map.insert(
                "input".to_string(),
                jsonrpc_core::serde_json::json!(format!("0x{}", hex::encode(&input))),
            );

            let fields = OtherFields::new(fields_map);
            let unknown_tx = UnknownTypedTransaction {
                ty: AnyTxType(tx_type_u64 as u8),
                fields,
                memo: Default::default(),
            };

            let tx_hash = self.trace.hash.try_decode_proto("transaction hash")?;
            let unknown_envelope = UnknownTxEnvelope {
                hash: tx_hash,
                inner: unknown_tx,
            };

            let any_envelope = AnyTxEnvelope::Unknown(unknown_envelope);
            let recovered = Recovered::new_unchecked(any_envelope, from_address);

            return Ok(AlloyTransaction {
                inner: recovered,
                block_hash: Some(block_hash),
                block_number: Some(block_number),
                transaction_index,
                effective_gas_price: if gas_price > 0 { Some(gas_price) } else { None },
            });
        }

        let tx_type = tx_type_result.unwrap();
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
                let signed_tx = Signed::new_unchecked(
                    tx,
                    signature,
                    self.trace.hash.try_decode_proto("transaction hash")?,
                );
                TxEnvelope::Legacy(signed_tx)
            }
            TxType::Eip2930 => {
                let tx = TxEip2930 {
                    // Firehose protobuf doesn't provide chain_id for transactions.
                    // Using 0 as placeholder since the transaction has already been validated on-chain.
                    chain_id: 0,
                    nonce,
                    gas_price,
                    gas_limit,
                    to: to_kind,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    input: input.clone(),
                };
                let signed_tx = Signed::new_unchecked(
                    tx,
                    signature,
                    self.trace.hash.try_decode_proto("transaction hash")?,
                );
                TxEnvelope::Eip2930(signed_tx)
            }
            TxType::Eip1559 => {
                let tx = TxEip1559 {
                    // Firehose protobuf doesn't provide chain_id for transactions.
                    // Using 0 as placeholder since the transaction has already been validated on-chain.
                    chain_id: 0,
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas_u128,
                    max_priority_fee_per_gas: max_priority_fee_per_gas_u128,
                    to: to_kind,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    input: input.clone(),
                };
                let signed_tx = Signed::new_unchecked(
                    tx,
                    signature,
                    self.trace.hash.try_decode_proto("transaction hash")?,
                );
                TxEnvelope::Eip1559(signed_tx)
            }
            TxType::Eip4844 => {
                let to_address = to.ok_or_else(|| {
                    format_err!("EIP-4844 transactions cannot be contract creation transactions. The 'to' field must contain a valid address.")
                })?;

                let blob_versioned_hashes: Vec<B256> = self
                    .trace
                    .blob_hashes
                    .iter()
                    .map(|hash| B256::from_slice(hash))
                    .collect();

                let max_fee_per_blob_gas_u128 =
                    self.trace.blob_gas_fee_cap.as_ref().map_or(0u128, |x| {
                        let val: U256 = x.into();
                        val.to::<u128>()
                    });

                let tx_eip4844 = TxEip4844 {
                    // Firehose protobuf doesn't provide chain_id for transactions.
                    // Using 0 as placeholder since the transaction has already been validated on-chain.
                    chain_id: 0,
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas_u128,
                    max_priority_fee_per_gas: max_priority_fee_per_gas_u128,
                    to: to_address,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    blob_versioned_hashes,
                    max_fee_per_blob_gas: max_fee_per_blob_gas_u128,
                    input: input.clone(),
                };
                let tx = TxEip4844Variant::TxEip4844(tx_eip4844);
                let signed_tx = Signed::new_unchecked(
                    tx,
                    signature,
                    self.trace.hash.try_decode_proto("transaction hash")?,
                );
                TxEnvelope::Eip4844(signed_tx)
            }
            TxType::Eip7702 => {
                let to_address = to.ok_or_else(|| {
                    format_err!("EIP-7702 transactions cannot be contract creation transactions. The 'to' field must contain a valid address.")
                })?;

                // Convert set_code_authorizations to alloy authorization list
                let authorization_list: Vec<alloy::eips::eip7702::SignedAuthorization> = self
                    .trace
                    .set_code_authorizations
                    .iter()
                    .map(|auth| {
                        let inner = alloy::eips::eip7702::Authorization {
                            chain_id: U256::from_be_slice(&auth.chain_id),
                            address: Address::from_slice(&auth.address),
                            nonce: auth.nonce,
                        };

                        let r = U256::from_be_slice(&auth.r);
                        let s = U256::from_be_slice(&auth.s);
                        let y_parity = auth.v as u8;

                        alloy::eips::eip7702::SignedAuthorization::new_unchecked(
                            inner, y_parity, r, s,
                        )
                    })
                    .collect();

                let tx = TxEip7702 {
                    // Firehose protobuf doesn't provide chain_id for transactions.
                    // Using 0 as placeholder since the transaction has already been validated on-chain.
                    chain_id: 0,
                    nonce,
                    gas_limit,
                    max_fee_per_gas: max_fee_per_gas_u128,
                    max_priority_fee_per_gas: max_priority_fee_per_gas_u128,
                    to: to_address,
                    value,
                    access_list: access_list.clone(), // Use actual access list from trace
                    authorization_list,
                    input: input.clone(),
                };
                let signed_tx = Signed::new_unchecked(
                    tx,
                    signature,
                    self.trace.hash.try_decode_proto("transaction hash")?,
                );
                TxEnvelope::Eip7702(signed_tx)
            }
        };

        let any_envelope = AnyTxEnvelope::Ethereum(envelope);
        let recovered = Recovered::new_unchecked(any_envelope, from_address);

        Ok(AlloyTransaction {
            inner: recovered,
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            transaction_index,
            effective_gas_price: if gas_price > 0 { Some(gas_price) } else { None }, // gas_price already contains effective gas price per protobuf spec
        })
    }
}

impl TryInto<BlockFinality> for &Block {
    type Error = Error;

    fn try_into(self) -> Result<BlockFinality, Self::Error> {
        Ok(BlockFinality::NonFinal(self.try_into()?))
    }
}

impl TryInto<AnyBlock> for &Block {
    type Error = Error;

    fn try_into(self) -> Result<AnyBlock, Self::Error> {
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

            withdrawals_root: if header.withdrawals_root.is_empty() {
                None
            } else {
                Some(
                    header
                        .withdrawals_root
                        .try_decode_proto("withdrawals root")?,
                )
            },
            blob_gas_used: header.blob_gas_used,
            excess_blob_gas: header.excess_blob_gas,
            parent_beacon_block_root: if header.parent_beacon_root.is_empty() {
                None
            } else {
                Some(
                    header
                        .parent_beacon_root
                        .try_decode_proto("parent beacon root")?,
                )
            },
            requests_hash: if header.requests_hash.is_empty() {
                None
            } else {
                Some(header.requests_hash.try_decode_proto("requests hash")?)
            },
        };

        let rpc_header = alloy::rpc::types::Header {
            hash: block_hash,
            inner: consensus_header,
            total_difficulty: {
                #[allow(deprecated)]
                let total_difficulty = &header.total_difficulty;
                total_difficulty.as_ref().map(|v| v.into())
            },
            size: Some(U256::from(self.size)),
        };

        let transactions = self
            .transaction_traces
            .iter()
            .map(|t| TransactionTraceAt::new(t, self).try_into())
            .collect::<Result<Vec<Transaction<AnyTxEnvelope>>, Error>>()?;

        let uncles = self
            .uncles
            .iter()
            .map(|u| u.hash.try_decode_proto("uncle hash"))
            .collect::<Result<Vec<B256>, _>>()?;

        use alloy::rpc::types::Block;

        let any_header: AnyRpcHeader = rpc_header.map(AnyHeader::from);

        let any_transactions: Vec<AnyRpcTransaction> = transactions
            .into_iter()
            .map(|tx| AnyRpcTransaction::new(WithOtherFields::new(tx)))
            .collect();

        let any_block = Block {
            header: any_header,
            transactions: alloy::rpc::types::BlockTransactions::Full(any_transactions),
            uncles,
            withdrawals: None,
        };

        Ok(AnyBlock::new(WithOtherFields::new(any_block)))
    }
}

impl TryInto<EthereumBlockWithCalls> for &Block {
    type Error = Error;

    fn try_into(self) -> Result<EthereumBlockWithCalls, Self::Error> {
        let alloy_block: AnyBlock = self.try_into()?;

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
) -> Result<Option<alloy::network::AnyTransactionReceipt>, Error> {
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

    let tx_type_u64 = u64::try_from(t.r#type).map_err(|_| {
        format_err!(
            "Invalid transaction type value {} in transaction receipt. Transaction type must be a valid u64.",
            t.r#type
        )
    })?;

    let any_envelope = AnyReceiptEnvelope {
        inner: receipt_with_bloom,
        r#type: tx_type_u64 as u8,
    };

    let receipt = alloy_rpc_types::TransactionReceipt {
        transaction_hash: t.hash.try_decode_proto("transaction hash")?,
        transaction_index: Some(t.index as u64),
        block_hash: Some(block.hash.try_decode_proto("transaction block hash")?),
        block_number: Some(block.number),
        gas_used: t.gas_used,
        contract_address,
        from: t.from.try_decode_proto("transaction from")?,
        to: get_to_address(t)?,
        effective_gas_price: t.gas_price.as_ref().map_or(0u128, |x| {
            let val: U256 = x.into();
            val.to::<u128>()
        }), // gas_price already contains effective gas price per protobuf spec
        blob_gas_used: r.blob_gas_used,
        blob_gas_price: r.blob_gas_price.as_ref().map(|x| {
            let val: U256 = x.into();
            val.to::<u128>()
        }),
        inner: any_envelope,
    };

    Ok(Some(WithOtherFields::new(receipt)))
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

    #[test]
    fn test_unknown_transaction_type_conversion() {
        use super::TransactionTraceAt;
        use crate::codec::TransactionTrace;
        use graph::prelude::alloy::network::AnyTxEnvelope;
        use graph::prelude::alloy::primitives::B256;

        let mut block = Block::default();
        let mut header = BlockHeader::default();
        header.number = 123456;
        header.timestamp = Some(Timestamp {
            seconds: 1234567890,
            nanos: 0,
        });
        block.header = Some(header);
        block.number = 123456;
        block.hash = vec![0u8; 32];

        let mut trace = TransactionTrace::default();
        trace.r#type = 126; // 0x7e Optimism deposit transaction
        trace.hash = vec![1u8; 32];
        trace.from = vec![2u8; 20];
        trace.to = vec![3u8; 20];
        trace.nonce = 42;
        trace.gas_limit = 21000;
        trace.index = 0;

        let trace_at = TransactionTraceAt::new(&trace, &block);
        let result: Result<
            graph::prelude::alloy::rpc::types::Transaction<AnyTxEnvelope>,
            graph::prelude::Error,
        > = trace_at.try_into();

        assert!(
            result.is_ok(),
            "Should successfully convert unknown transaction type"
        );

        let tx = result.unwrap();

        match tx.inner.inner() {
            AnyTxEnvelope::Unknown(unknown_envelope) => {
                assert_eq!(unknown_envelope.inner.ty.0, 126);
                assert_eq!(unknown_envelope.hash, B256::from_slice(&trace.hash));
                assert!(
                    !unknown_envelope.inner.fields.is_empty(),
                    "OtherFields should contain transaction data"
                );
            }
            _ => panic!("Expected AnyTxEnvelope::Unknown, got Ethereum variant"),
        }

        assert_eq!(tx.block_number, Some(123456));
        assert_eq!(tx.transaction_index, Some(0));
        assert_eq!(tx.block_hash, Some(B256::from_slice(&block.hash)));
    }

    #[test]
    fn test_unknown_receipt_type_conversion() {
        use super::transaction_trace_to_alloy_txn_reciept;
        use crate::codec::TransactionTrace;

        let mut block = Block::default();
        let mut header = BlockHeader::default();
        header.number = 123456;
        block.header = Some(header);
        block.hash = vec![0u8; 32];

        let mut trace = TransactionTrace::default();
        trace.r#type = 126; // 0x7e Optimism deposit transaction
        trace.hash = vec![1u8; 32];
        trace.from = vec![2u8; 20];
        trace.to = vec![3u8; 20];
        trace.index = 0;
        trace.gas_used = 21000;
        trace.status = 1;

        let mut receipt = super::TransactionReceipt::default();
        receipt.cumulative_gas_used = 21000;
        receipt.logs_bloom = vec![0u8; 256];
        trace.receipt = Some(receipt);

        let result = transaction_trace_to_alloy_txn_reciept(&trace, &block);

        assert!(
            result.is_ok(),
            "Should successfully convert receipt with unknown transaction type"
        );

        let receipt_opt = result.unwrap();
        assert!(receipt_opt.is_some(), "Receipt should be present");

        let receipt = receipt_opt.unwrap();

        assert_eq!(receipt.inner.inner.r#type, 126);
        assert_eq!(receipt.gas_used, 21000);
        assert_eq!(receipt.transaction_index, Some(0));
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
