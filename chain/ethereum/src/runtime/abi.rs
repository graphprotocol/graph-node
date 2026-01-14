use super::runtime_adapter::UnresolvedContractCall;
use crate::trigger::{
    EthereumBlockData, EthereumCallData, EthereumEventData, EthereumTransactionData,
};
use anyhow::anyhow;
use async_trait::async_trait;
use graph::abi;
use graph::prelude::alloy;
use graph::prelude::alloy::consensus::TxReceipt;
use graph::prelude::alloy::network::ReceiptResponse;
use graph::prelude::alloy::rpc::types::{Log, TransactionReceipt};
use graph::prelude::alloy::serde::WithOtherFields;
use graph::{
    prelude::BigInt,
    runtime::{
        asc_get, asc_new, asc_new_or_null, gas::GasCounter, AscHeap, AscIndexId, AscPtr, AscType,
        DeterministicHostError, FromAscObj, HostExportError, IndexForAscTypeId, ToAscObj,
    },
};
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{
    Array, AscAddress, AscBigInt, AscEnum, AscH160, AscString, AscWrapped, EthereumValueKind,
    Uint8Array,
};
use semver::Version;

type AscB256 = Uint8Array;
type AscH2048 = Uint8Array;

pub struct AscLogParamArray(Array<AscPtr<AscLogParam>>);

impl AscType for AscLogParamArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }
    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

#[async_trait]
impl ToAscObj<AscLogParamArray> for &[abi::DynSolParam] {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscLogParamArray, HostExportError> {
        let mut content = Vec::with_capacity(self.len());
        for x in *self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscLogParamArray(Array::new(&content, heap, gas).await?))
    }
}

impl AscIndexId for AscLogParamArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::ArrayEventParam;
}

pub struct AscTopicArray(Array<AscPtr<AscB256>>);

impl AscType for AscTopicArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

#[async_trait]
impl ToAscObj<AscTopicArray> for &[alloy::primitives::B256] {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTopicArray, HostExportError> {
        let mut topics = Vec::with_capacity(self.len());
        for topic in *self {
            topics.push(asc_new(heap, topic, gas).await?);
        }
        Ok(AscTopicArray(Array::new(&topics, heap, gas).await?))
    }
}

impl AscIndexId for AscTopicArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::ArrayB256;
}

pub struct AscLogArray(Array<AscPtr<AscEthereumLog>>);

impl AscType for AscLogArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

#[async_trait]
impl ToAscObj<AscLogArray> for &[Log] {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscLogArray, HostExportError> {
        let mut logs = Vec::with_capacity(self.len());
        for log in *self {
            logs.push(asc_new(heap, log, gas).await?);
        }

        Ok(AscLogArray(Array::new(&logs, heap, gas).await?))
    }
}

impl AscIndexId for AscLogArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::ArrayLog;
}

#[repr(C)]
#[derive(AscType)]
#[allow(non_camel_case_types)]
pub struct AscUnresolvedContractCall_0_0_4 {
    pub contract_name: AscPtr<AscString>,
    pub contract_address: AscPtr<AscAddress>,
    pub function_name: AscPtr<AscString>,
    pub function_signature: AscPtr<AscString>,
    pub function_args: AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>,
}

impl AscIndexId for AscUnresolvedContractCall_0_0_4 {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::SmartContractCall;
}

impl FromAscObj<AscUnresolvedContractCall_0_0_4> for UnresolvedContractCall {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_call: AscUnresolvedContractCall_0_0_4,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        Ok(UnresolvedContractCall {
            contract_name: asc_get(heap, asc_call.contract_name, gas, depth)?,
            contract_address: asc_get(heap, asc_call.contract_address, gas, depth)?,
            function_name: asc_get(heap, asc_call.function_name, gas, depth)?,
            function_signature: Some(asc_get(heap, asc_call.function_signature, gas, depth)?),
            function_args: asc_get(heap, asc_call.function_args, gas, depth)?,
        })
    }
}

#[repr(C)]
#[derive(AscType)]
pub struct AscUnresolvedContractCall {
    pub contract_name: AscPtr<AscString>,
    pub contract_address: AscPtr<AscAddress>,
    pub function_name: AscPtr<AscString>,
    pub function_args: AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>,
}

impl FromAscObj<AscUnresolvedContractCall> for UnresolvedContractCall {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        asc_call: AscUnresolvedContractCall,
        heap: &H,
        gas: &GasCounter,
        depth: usize,
    ) -> Result<Self, DeterministicHostError> {
        Ok(UnresolvedContractCall {
            contract_name: asc_get(heap, asc_call.contract_name, gas, depth)?,
            contract_address: asc_get(heap, asc_call.contract_address, gas, depth)?,
            function_name: asc_get(heap, asc_call.function_name, gas, depth)?,
            function_signature: None,
            function_args: asc_get(heap, asc_call.function_args, gas, depth)?,
        })
    }
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumBlock {
    pub hash: AscPtr<AscB256>,
    pub parent_hash: AscPtr<AscB256>,
    pub uncles_hash: AscPtr<AscB256>,
    pub author: AscPtr<AscH160>,
    pub state_root: AscPtr<AscB256>,
    pub transactions_root: AscPtr<AscB256>,
    pub receipts_root: AscPtr<AscB256>,
    pub number: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_limit: AscPtr<AscBigInt>,
    pub timestamp: AscPtr<AscBigInt>,
    pub difficulty: AscPtr<AscBigInt>,
    pub total_difficulty: AscPtr<AscBigInt>,
    pub size: AscPtr<AscBigInt>,
}

impl AscIndexId for AscEthereumBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumBlock;
}

#[repr(C)]
#[derive(AscType)]
#[allow(non_camel_case_types)]
pub(crate) struct AscEthereumBlock_0_0_6 {
    pub hash: AscPtr<AscB256>,
    pub parent_hash: AscPtr<AscB256>,
    pub uncles_hash: AscPtr<AscB256>,
    pub author: AscPtr<AscH160>,
    pub state_root: AscPtr<AscB256>,
    pub transactions_root: AscPtr<AscB256>,
    pub receipts_root: AscPtr<AscB256>,
    pub number: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_limit: AscPtr<AscBigInt>,
    pub timestamp: AscPtr<AscBigInt>,
    pub difficulty: AscPtr<AscBigInt>,
    pub total_difficulty: AscPtr<AscBigInt>,
    pub size: AscPtr<AscBigInt>,
    pub base_fee_per_block: AscPtr<AscBigInt>,
}

impl AscIndexId for AscEthereumBlock_0_0_6 {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumBlock;
}

#[repr(C)]
#[derive(AscType)]
#[allow(non_camel_case_types)]
pub(crate) struct AscEthereumTransaction_0_0_1 {
    pub hash: AscPtr<AscB256>,
    pub index: AscPtr<AscBigInt>,
    pub from: AscPtr<AscH160>,
    pub to: AscPtr<AscH160>,
    pub value: AscPtr<AscBigInt>,
    pub gas_limit: AscPtr<AscBigInt>,
    pub gas_price: AscPtr<AscBigInt>,
}

impl AscIndexId for AscEthereumTransaction_0_0_1 {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumTransaction;
}

#[repr(C)]
#[derive(AscType)]
#[allow(non_camel_case_types)]
pub(crate) struct AscEthereumTransaction_0_0_2 {
    pub hash: AscPtr<AscB256>,
    pub index: AscPtr<AscBigInt>,
    pub from: AscPtr<AscH160>,
    pub to: AscPtr<AscH160>,
    pub value: AscPtr<AscBigInt>,
    pub gas_limit: AscPtr<AscBigInt>,
    pub gas_price: AscPtr<AscBigInt>,
    pub input: AscPtr<Uint8Array>,
}

impl AscIndexId for AscEthereumTransaction_0_0_2 {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumTransaction;
}

#[repr(C)]
#[derive(AscType)]
#[allow(non_camel_case_types)]
pub(crate) struct AscEthereumTransaction_0_0_6 {
    pub hash: AscPtr<AscB256>,
    pub index: AscPtr<AscBigInt>,
    pub from: AscPtr<AscH160>,
    pub to: AscPtr<AscH160>,
    pub value: AscPtr<AscBigInt>,
    pub gas_limit: AscPtr<AscBigInt>,
    pub gas_price: AscPtr<AscBigInt>,
    pub input: AscPtr<Uint8Array>,
    pub nonce: AscPtr<AscBigInt>,
}

impl AscIndexId for AscEthereumTransaction_0_0_6 {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumTransaction;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumEvent<T, B>
where
    T: AscType,
    B: AscType,
{
    pub address: AscPtr<AscAddress>,
    pub log_index: AscPtr<AscBigInt>,
    pub transaction_log_index: AscPtr<AscBigInt>,
    pub log_type: AscPtr<AscString>,
    pub block: AscPtr<B>,
    pub transaction: AscPtr<T>,
    pub params: AscPtr<AscLogParamArray>,
}

impl AscIndexId for AscEthereumEvent<AscEthereumTransaction_0_0_1, AscEthereumBlock> {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumEvent;
}

impl AscIndexId for AscEthereumEvent<AscEthereumTransaction_0_0_2, AscEthereumBlock> {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumEvent;
}

impl AscIndexId for AscEthereumEvent<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6> {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumEvent;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumLog {
    pub address: AscPtr<AscAddress>,
    pub topics: AscPtr<AscTopicArray>,
    pub data: AscPtr<Uint8Array>,
    pub block_hash: AscPtr<AscB256>,
    pub block_number: AscPtr<AscB256>,
    pub transaction_hash: AscPtr<AscB256>,
    pub transaction_index: AscPtr<AscBigInt>,
    pub log_index: AscPtr<AscBigInt>,
    pub transaction_log_index: AscPtr<AscBigInt>,
    pub log_type: AscPtr<AscString>,
    pub removed: AscPtr<AscWrapped<bool>>,
}

impl AscIndexId for AscEthereumLog {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::Log;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumTransactionReceipt {
    pub transaction_hash: AscPtr<AscB256>,
    pub transaction_index: AscPtr<AscBigInt>,
    pub block_hash: AscPtr<AscB256>,
    pub block_number: AscPtr<AscBigInt>,
    pub cumulative_gas_used: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub contract_address: AscPtr<AscAddress>,
    pub logs: AscPtr<AscLogArray>,
    pub status: AscPtr<AscBigInt>,
    pub root: AscPtr<AscB256>,
    pub logs_bloom: AscPtr<AscH2048>,
}

impl AscIndexId for AscEthereumTransactionReceipt {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TransactionReceipt;
}

/// Introduced in API Version 0.0.7, this is the same as [`AscEthereumEvent`] with an added
/// `receipt` field.
#[repr(C)]
#[derive(AscType)]
#[allow(non_camel_case_types)]
pub(crate) struct AscEthereumEvent_0_0_7<T, B>
where
    T: AscType,
    B: AscType,
{
    pub address: AscPtr<AscAddress>,
    pub log_index: AscPtr<AscBigInt>,
    pub transaction_log_index: AscPtr<AscBigInt>,
    pub log_type: AscPtr<AscString>,
    pub block: AscPtr<B>,
    pub transaction: AscPtr<T>,
    pub params: AscPtr<AscLogParamArray>,
    pub receipt: AscPtr<AscEthereumTransactionReceipt>,
}

impl AscIndexId for AscEthereumEvent_0_0_7<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6> {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumEvent;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscLogParam {
    pub name: AscPtr<AscString>,
    pub value: AscPtr<AscEnum<EthereumValueKind>>,
}

impl AscIndexId for AscLogParam {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EventParam;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumCall {
    pub address: AscPtr<AscAddress>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<AscEthereumTransaction_0_0_1>,
    pub inputs: AscPtr<AscLogParamArray>,
    pub outputs: AscPtr<AscLogParamArray>,
}

impl AscIndexId for AscEthereumCall {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumCall;
}

#[repr(C)]
#[derive(AscType)]
#[allow(non_camel_case_types)]
pub(crate) struct AscEthereumCall_0_0_3<T, B>
where
    T: AscType,
    B: AscType,
{
    pub to: AscPtr<AscAddress>,
    pub from: AscPtr<AscAddress>,
    pub block: AscPtr<B>,
    pub transaction: AscPtr<T>,
    pub inputs: AscPtr<AscLogParamArray>,
    pub outputs: AscPtr<AscLogParamArray>,
}

impl<T, B> AscIndexId for AscEthereumCall_0_0_3<T, B>
where
    T: AscType,
    B: AscType,
{
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumCall;
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumBlock> for EthereumBlockData<'a> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumBlock, HostExportError> {
        let size = asc_new_or_null_u256(heap, &self.size(), gas).await?;

        Ok(AscEthereumBlock {
            hash: asc_new(heap, self.hash(), gas).await?,
            parent_hash: asc_new(heap, self.parent_hash(), gas).await?,
            uncles_hash: asc_new(heap, self.uncles_hash(), gas).await?,
            author: asc_new(heap, self.author(), gas).await?,
            state_root: asc_new(heap, self.state_root(), gas).await?,
            transactions_root: asc_new(heap, self.transactions_root(), gas).await?,
            receipts_root: asc_new(heap, self.receipts_root(), gas).await?,
            number: asc_new(heap, &BigInt::from(self.number()), gas).await?,
            gas_used: asc_new(heap, &BigInt::from(self.gas_used()), gas).await?,
            gas_limit: asc_new(heap, &BigInt::from(self.gas_limit()), gas).await?,
            timestamp: asc_new(heap, &BigInt::from(self.timestamp()), gas).await?,
            difficulty: asc_new(heap, &BigInt::from_unsigned_u256(self.difficulty()), gas).await?,
            total_difficulty: asc_new(
                heap,
                &BigInt::from_unsigned_u256(self.total_difficulty()),
                gas,
            )
            .await?,
            size,
        })
    }
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumBlock_0_0_6> for EthereumBlockData<'a> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumBlock_0_0_6, HostExportError> {
        let size = asc_new_or_null_u256(heap, &self.size(), gas).await?;
        let base_fee_per_block = asc_new_or_null_u64(heap, &self.base_fee_per_gas(), gas).await?;

        Ok(AscEthereumBlock_0_0_6 {
            hash: asc_new(heap, self.hash(), gas).await?,
            parent_hash: asc_new(heap, self.parent_hash(), gas).await?,
            uncles_hash: asc_new(heap, self.uncles_hash(), gas).await?,
            author: asc_new(heap, self.author(), gas).await?,
            state_root: asc_new(heap, self.state_root(), gas).await?,
            transactions_root: asc_new(heap, self.transactions_root(), gas).await?,
            receipts_root: asc_new(heap, self.receipts_root(), gas).await?,
            number: asc_new(heap, &BigInt::from(self.number()), gas).await?,
            gas_used: asc_new(heap, &BigInt::from(self.gas_used()), gas).await?,
            gas_limit: asc_new(heap, &BigInt::from(self.gas_limit()), gas).await?,
            timestamp: asc_new(heap, &BigInt::from(self.timestamp()), gas).await?,
            difficulty: asc_new(heap, &BigInt::from_unsigned_u256(self.difficulty()), gas).await?,
            total_difficulty: asc_new(
                heap,
                &BigInt::from_unsigned_u256(self.total_difficulty()),
                gas,
            )
            .await?,
            size,
            base_fee_per_block,
        })
    }
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumTransaction_0_0_1> for EthereumTransactionData<'a> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumTransaction_0_0_1, HostExportError> {
        Ok(AscEthereumTransaction_0_0_1 {
            hash: asc_new(heap, &self.hash(), gas).await?,
            index: asc_new(heap, &BigInt::from(self.index()), gas).await?,
            from: asc_new(heap, &self.from(), gas).await?,
            to: asc_new_or_null(heap, &self.to(), gas).await?,
            value: asc_new(heap, &BigInt::from_unsigned_u256(&self.value()), gas).await?,
            gas_limit: asc_new(heap, &BigInt::from(self.gas_limit()), gas).await?,
            gas_price: asc_new(heap, &BigInt::from(self.gas_price()), gas).await?,
        })
    }
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumTransaction_0_0_2> for EthereumTransactionData<'a> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumTransaction_0_0_2, HostExportError> {
        Ok(AscEthereumTransaction_0_0_2 {
            hash: asc_new(heap, &self.hash(), gas).await?,
            index: asc_new(heap, &BigInt::from(self.index()), gas).await?,
            from: asc_new(heap, &self.from(), gas).await?,
            to: asc_new_or_null(heap, &self.to(), gas).await?,
            value: asc_new(heap, &BigInt::from_unsigned_u256(&self.value()), gas).await?,
            gas_limit: asc_new(heap, &BigInt::from(self.gas_limit()), gas).await?,
            gas_price: asc_new(heap, &BigInt::from(self.gas_price()), gas).await?,
            input: asc_new(heap, self.input(), gas).await?,
        })
    }
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumTransaction_0_0_6> for EthereumTransactionData<'a> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumTransaction_0_0_6, HostExportError> {
        Ok(AscEthereumTransaction_0_0_6 {
            hash: asc_new(heap, &self.hash(), gas).await?,
            index: asc_new(heap, &BigInt::from(self.index()), gas).await?,
            from: asc_new(heap, &self.from(), gas).await?,
            to: asc_new_or_null(heap, &self.to(), gas).await?,
            value: asc_new(heap, &BigInt::from_unsigned_u256(&self.value()), gas).await?,
            gas_limit: asc_new(heap, &BigInt::from(self.gas_limit()), gas).await?,
            gas_price: asc_new(heap, &BigInt::from(self.gas_price()), gas).await?,
            input: asc_new(heap, self.input(), gas).await?,
            nonce: asc_new(heap, &BigInt::from(self.nonce()), gas).await?,
        })
    }
}

#[async_trait]
impl<'a, T, B> ToAscObj<AscEthereumEvent<T, B>> for EthereumEventData<'a>
where
    T: AscType + AscIndexId + Send,
    B: AscType + AscIndexId + Send,
    EthereumTransactionData<'a>: ToAscObj<T>,
    EthereumBlockData<'a>: ToAscObj<B>,
{
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumEvent<T, B>, HostExportError> {
        Ok(AscEthereumEvent {
            address: asc_new(heap, self.address(), gas).await?,
            log_index: asc_new(heap, &BigInt::from(self.log_index()), gas).await?,
            transaction_log_index: asc_new(heap, &BigInt::from(self.transaction_log_index()), gas)
                .await?,
            log_type: asc_new_or_null(heap, &self.log_type().as_ref(), gas).await?,
            block: asc_new::<B, EthereumBlockData, _>(heap, &self.block, gas).await?,
            transaction: asc_new::<T, EthereumTransactionData, _>(heap, &self.transaction, gas)
                .await?,
            params: asc_new(heap, &self.params, gas).await?,
        })
    }
}

#[async_trait]
impl<'a, T, B, Inner> ToAscObj<AscEthereumEvent_0_0_7<T, B>>
    for (
        EthereumEventData<'a>,
        Option<&WithOtherFields<TransactionReceipt<Inner>>>,
    )
where
    T: AscType + AscIndexId + Send,
    B: AscType + AscIndexId + Send,
    EthereumTransactionData<'a>: ToAscObj<T>,
    EthereumBlockData<'a>: ToAscObj<B>,
    Inner: Send + Sync,
    TransactionReceipt<Inner>: ToAscObj<AscEthereumTransactionReceipt>,
{
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumEvent_0_0_7<T, B>, HostExportError> {
        let (event_data, optional_receipt) = self;
        let AscEthereumEvent {
            address,
            log_index,
            transaction_log_index,
            log_type,
            block,
            transaction,
            params,
        } = event_data.to_asc_obj(heap, gas).await?;
        let receipt = if let Some(receipt_data) = optional_receipt {
            asc_new(heap, &receipt_data.inner(), gas).await?
        } else {
            AscPtr::null()
        };
        Ok(AscEthereumEvent_0_0_7 {
            address,
            log_index,
            transaction_log_index,
            log_type,
            block,
            transaction,
            params,
            receipt,
        })
    }
}

async fn asc_new_or_null_u256<H: AscHeap + ?Sized>(
    heap: &mut H,
    value: &Option<alloy::primitives::U256>,
    gas: &GasCounter,
) -> Result<AscPtr<AscBigInt>, HostExportError> {
    match value {
        Some(value) => asc_new(heap, &BigInt::from_unsigned_u256(value), gas).await,
        None => Ok(AscPtr::null()),
    }
}

async fn asc_new_or_null_u64<H: AscHeap + ?Sized>(
    heap: &mut H,
    value: &Option<u64>,
    gas: &GasCounter,
) -> Result<AscPtr<AscBigInt>, HostExportError> {
    match value {
        Some(value) => asc_new(heap, &BigInt::from(*value), gas).await,
        None => Ok(AscPtr::null()),
    }
}

#[async_trait]
impl ToAscObj<AscEthereumLog> for Log {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumLog, HostExportError> {
        Ok(AscEthereumLog {
            address: asc_new(heap, &self.address(), gas).await?,
            topics: asc_new(heap, &self.topics(), gas).await?,
            data: asc_new(heap, self.data().data.as_ref(), gas).await?,
            block_hash: asc_new_or_null(heap, &self.block_hash, gas).await?,
            block_number: asc_new_or_null_u64(heap, &self.block_number, gas).await?,
            transaction_hash: asc_new_or_null(heap, &self.transaction_hash, gas).await?,
            transaction_index: asc_new_or_null_u64(heap, &self.transaction_index, gas).await?,
            log_index: asc_new_or_null_u64(heap, &self.log_index, gas).await?,
            transaction_log_index: AscPtr::null(), // Non-standard field, not available in alloy
            log_type: AscPtr::null(),              // Non-standard field, not available in alloy
            removed: asc_new(
                heap,
                &AscWrapped {
                    inner: self.removed,
                },
                gas,
            )
            .await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscEthereumTransactionReceipt>
    for TransactionReceipt<alloy::network::AnyReceiptEnvelope<Log>>
{
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumTransactionReceipt, HostExportError> {
        let transaction_index = self
            .transaction_index
            .ok_or(HostExportError::Unknown(anyhow!(
                "Transaction index is missing"
            )))?;
        let status = match self.inner.status_or_post_state().as_eip658() {
            Some(success) => asc_new(heap, &BigInt::from(success as u64), gas).await?,
            None => AscPtr::null(), // Pre-EIP-658 (pre-Byzantium) receipt
        };
        Ok(AscEthereumTransactionReceipt {
            transaction_hash: asc_new(heap, &self.transaction_hash, gas).await?,
            transaction_index: asc_new(heap, &BigInt::from(transaction_index), gas).await?,
            block_hash: asc_new_or_null(heap, &self.block_hash, gas).await?,
            block_number: asc_new_or_null_u64(heap, &self.block_number, gas).await?,
            cumulative_gas_used: asc_new(heap, &BigInt::from(self.cumulative_gas_used()), gas)
                .await?,
            gas_used: asc_new(heap, &BigInt::from(self.gas_used), gas).await?,
            contract_address: asc_new_or_null(heap, &self.contract_address, gas).await?,
            logs: asc_new(heap, &self.logs(), gas).await?,
            status,
            root: asc_new_or_null(heap, &self.state_root(), gas).await?,
            logs_bloom: asc_new(heap, self.inner.bloom().as_slice(), gas).await?,
        })
    }
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumCall> for EthereumCallData<'a> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEthereumCall, HostExportError> {
        Ok(AscEthereumCall {
            address: asc_new(heap, self.to(), gas).await?,
            block: asc_new(heap, &self.block, gas).await?,
            transaction: asc_new(heap, &self.transaction, gas).await?,
            inputs: asc_new(heap, &self.inputs, gas).await?,
            outputs: asc_new(heap, &self.outputs, gas).await?,
        })
    }
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_2, AscEthereumBlock>>
    for EthereumCallData<'a>
{
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<
        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_2, AscEthereumBlock>,
        HostExportError,
    > {
        Ok(AscEthereumCall_0_0_3 {
            to: asc_new(heap, self.to(), gas).await?,
            from: asc_new(heap, self.from(), gas).await?,
            block: asc_new(heap, &self.block, gas).await?,
            transaction: asc_new(heap, &self.transaction, gas).await?,
            inputs: asc_new(heap, &self.inputs, gas).await?,
            outputs: asc_new(heap, &self.outputs, gas).await?,
        })
    }
}

#[async_trait]
impl<'a> ToAscObj<AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>>
    for EthereumCallData<'a>
{
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<
        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>,
        HostExportError,
    > {
        Ok(AscEthereumCall_0_0_3 {
            to: asc_new(heap, self.to(), gas).await?,
            from: asc_new(heap, self.from(), gas).await?,
            block: asc_new(heap, &self.block, gas).await?,
            transaction: asc_new(heap, &self.transaction, gas).await?,
            inputs: asc_new(heap, &self.inputs, gas).await?,
            outputs: asc_new(heap, &self.outputs, gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscLogParam> for abi::DynSolParam {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscLogParam, HostExportError> {
        Ok(AscLogParam {
            name: asc_new(heap, self.name.as_str(), gas).await?,
            value: asc_new(heap, &self.value, gas).await?,
        })
    }
}
