use graph::prelude::BigInt;
use graph::runtime::{asc_get, asc_new, AscPtr, DeterministicHostError, FromAscObj, ToAscObj};
use graph::runtime::{AscHeap, AscIndexId, AscType, IndexForAscTypeId};
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{
    Array, AscAddress, AscBigInt, AscEnum, AscH160, AscString, EthereumValueKind, Uint8Array,
};
use semver::Version;

use crate::trigger::{
    EthereumBlockData, EthereumCallData, EthereumEventData, EthereumTransactionData,
};

use super::runtime_adapter::UnresolvedContractCall;

type AscH256 = Uint8Array;

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

impl ToAscObj<AscLogParamArray> for Vec<ethabi::LogParam> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscLogParamArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscLogParamArray(Array::new(&*content, heap)?))
    }
}

impl AscIndexId for AscLogParamArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::ArrayEventParam;
}

#[repr(C)]
#[derive(AscType)]
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
    ) -> Result<Self, DeterministicHostError> {
        Ok(UnresolvedContractCall {
            contract_name: asc_get(heap, asc_call.contract_name)?,
            contract_address: asc_get(heap, asc_call.contract_address)?,
            function_name: asc_get(heap, asc_call.function_name)?,
            function_signature: Some(asc_get(heap, asc_call.function_signature)?),
            function_args: asc_get(heap, asc_call.function_args)?,
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
    ) -> Result<Self, DeterministicHostError> {
        Ok(UnresolvedContractCall {
            contract_name: asc_get(heap, asc_call.contract_name)?,
            contract_address: asc_get(heap, asc_call.contract_address)?,
            function_name: asc_get(heap, asc_call.function_name)?,
            function_signature: None,
            function_args: asc_get(heap, asc_call.function_args)?,
        })
    }
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumBlock {
    pub hash: AscPtr<AscH256>,
    pub parent_hash: AscPtr<AscH256>,
    pub uncles_hash: AscPtr<AscH256>,
    pub author: AscPtr<AscH160>,
    pub state_root: AscPtr<AscH256>,
    pub transactions_root: AscPtr<AscH256>,
    pub receipts_root: AscPtr<AscH256>,
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
pub(crate) struct AscEthereumBlock_0_0_6 {
    pub hash: AscPtr<AscH256>,
    pub parent_hash: AscPtr<AscH256>,
    pub uncles_hash: AscPtr<AscH256>,
    pub author: AscPtr<AscH160>,
    pub state_root: AscPtr<AscH256>,
    pub transactions_root: AscPtr<AscH256>,
    pub receipts_root: AscPtr<AscH256>,
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
pub(crate) struct AscEthereumTransaction_0_0_1 {
    pub hash: AscPtr<AscH256>,
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
pub(crate) struct AscEthereumTransaction_0_0_2 {
    pub hash: AscPtr<AscH256>,
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
pub(crate) struct AscEthereumTransaction_0_0_6 {
    pub hash: AscPtr<AscH256>,
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

impl ToAscObj<AscEthereumBlock> for EthereumBlockData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumBlock, DeterministicHostError> {
        Ok(AscEthereumBlock {
            hash: asc_new(heap, &self.hash)?,
            parent_hash: asc_new(heap, &self.parent_hash)?,
            uncles_hash: asc_new(heap, &self.uncles_hash)?,
            author: asc_new(heap, &self.author)?,
            state_root: asc_new(heap, &self.state_root)?,
            transactions_root: asc_new(heap, &self.transactions_root)?,
            receipts_root: asc_new(heap, &self.receipts_root)?,
            number: asc_new(heap, &BigInt::from(self.number))?,
            gas_used: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_used))?,
            gas_limit: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_limit))?,
            timestamp: asc_new(heap, &BigInt::from_unsigned_u256(&self.timestamp))?,
            difficulty: asc_new(heap, &BigInt::from_unsigned_u256(&self.difficulty))?,
            total_difficulty: asc_new(heap, &BigInt::from_unsigned_u256(&self.total_difficulty))?,
            size: self
                .size
                .map(|size| asc_new(heap, &BigInt::from_unsigned_u256(&size)))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}

impl ToAscObj<AscEthereumBlock_0_0_6> for EthereumBlockData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumBlock_0_0_6, DeterministicHostError> {
        Ok(AscEthereumBlock_0_0_6 {
            hash: asc_new(heap, &self.hash)?,
            parent_hash: asc_new(heap, &self.parent_hash)?,
            uncles_hash: asc_new(heap, &self.uncles_hash)?,
            author: asc_new(heap, &self.author)?,
            state_root: asc_new(heap, &self.state_root)?,
            transactions_root: asc_new(heap, &self.transactions_root)?,
            receipts_root: asc_new(heap, &self.receipts_root)?,
            number: asc_new(heap, &BigInt::from(self.number))?,
            gas_used: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_used))?,
            gas_limit: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_limit))?,
            timestamp: asc_new(heap, &BigInt::from_unsigned_u256(&self.timestamp))?,
            difficulty: asc_new(heap, &BigInt::from_unsigned_u256(&self.difficulty))?,
            total_difficulty: asc_new(heap, &BigInt::from_unsigned_u256(&self.total_difficulty))?,
            size: self
                .size
                .map(|size| asc_new(heap, &BigInt::from_unsigned_u256(&size)))
                .unwrap_or(Ok(AscPtr::null()))?,
            base_fee_per_block: self
                .base_fee_per_gas
                .map(|base_fee| asc_new(heap, &BigInt::from_unsigned_u256(&base_fee)))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}

impl ToAscObj<AscEthereumTransaction_0_0_1> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumTransaction_0_0_1, DeterministicHostError> {
        Ok(AscEthereumTransaction_0_0_1 {
            hash: asc_new(heap, &self.hash)?,
            index: asc_new(heap, &BigInt::from(self.index))?,
            from: asc_new(heap, &self.from)?,
            to: self
                .to
                .map(|to| asc_new(heap, &to))
                .unwrap_or(Ok(AscPtr::null()))?,
            value: asc_new(heap, &BigInt::from_unsigned_u256(&self.value))?,
            gas_limit: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_limit))?,
            gas_price: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_price))?,
        })
    }
}

impl ToAscObj<AscEthereumTransaction_0_0_2> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumTransaction_0_0_2, DeterministicHostError> {
        Ok(AscEthereumTransaction_0_0_2 {
            hash: asc_new(heap, &self.hash)?,
            index: asc_new(heap, &BigInt::from(self.index))?,
            from: asc_new(heap, &self.from)?,
            to: self
                .to
                .map(|to| asc_new(heap, &to))
                .unwrap_or(Ok(AscPtr::null()))?,
            value: asc_new(heap, &BigInt::from_unsigned_u256(&self.value))?,
            gas_limit: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_limit))?,
            gas_price: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_price))?,
            input: asc_new(heap, &*self.input.0)?,
        })
    }
}

impl ToAscObj<AscEthereumTransaction_0_0_6> for EthereumTransactionData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumTransaction_0_0_6, DeterministicHostError> {
        Ok(AscEthereumTransaction_0_0_6 {
            hash: asc_new(heap, &self.hash)?,
            index: asc_new(heap, &BigInt::from(self.index))?,
            from: asc_new(heap, &self.from)?,
            to: self
                .to
                .map(|to| asc_new(heap, &to))
                .unwrap_or(Ok(AscPtr::null()))?,
            value: asc_new(heap, &BigInt::from_unsigned_u256(&self.value))?,
            gas_limit: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_limit))?,
            gas_price: asc_new(heap, &BigInt::from_unsigned_u256(&self.gas_price))?,
            input: asc_new(heap, &*self.input.0)?,
            nonce: asc_new(heap, &BigInt::from_unsigned_u256(&self.nonce))?,
        })
    }
}

impl<T, B> ToAscObj<AscEthereumEvent<T, B>> for EthereumEventData
where
    T: AscType + AscIndexId,
    B: AscType + AscIndexId,
    EthereumTransactionData: ToAscObj<T>,
    EthereumBlockData: ToAscObj<B>,
{
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumEvent<T, B>, DeterministicHostError> {
        Ok(AscEthereumEvent {
            address: asc_new(heap, &self.address)?,
            log_index: asc_new(heap, &BigInt::from_unsigned_u256(&self.log_index))?,
            transaction_log_index: asc_new(
                heap,
                &BigInt::from_unsigned_u256(&self.transaction_log_index),
            )?,
            log_type: self
                .log_type
                .clone()
                .map(|log_type| asc_new(heap, &log_type))
                .unwrap_or(Ok(AscPtr::null()))?,
            block: asc_new::<B, EthereumBlockData, _>(heap, &self.block)?,
            transaction: asc_new::<T, EthereumTransactionData, _>(heap, &self.transaction)?,
            params: asc_new(heap, &self.params)?,
        })
    }
}

impl ToAscObj<AscEthereumCall> for EthereumCallData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEthereumCall, DeterministicHostError> {
        Ok(AscEthereumCall {
            address: asc_new(heap, &self.to)?,
            block: asc_new(heap, &self.block)?,
            transaction: asc_new(heap, &self.transaction)?,
            inputs: asc_new(heap, &self.inputs)?,
            outputs: asc_new(heap, &self.outputs)?,
        })
    }
}

impl ToAscObj<AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_2, AscEthereumBlock>>
    for EthereumCallData
{
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<
        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_2, AscEthereumBlock>,
        DeterministicHostError,
    > {
        Ok(AscEthereumCall_0_0_3 {
            to: asc_new(heap, &self.to)?,
            from: asc_new(heap, &self.from)?,
            block: asc_new(heap, &self.block)?,
            transaction: asc_new(heap, &self.transaction)?,
            inputs: asc_new(heap, &self.inputs)?,
            outputs: asc_new(heap, &self.outputs)?,
        })
    }
}

impl ToAscObj<AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>>
    for EthereumCallData
{
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<
        AscEthereumCall_0_0_3<AscEthereumTransaction_0_0_6, AscEthereumBlock_0_0_6>,
        DeterministicHostError,
    > {
        Ok(AscEthereumCall_0_0_3 {
            to: asc_new(heap, &self.to)?,
            from: asc_new(heap, &self.from)?,
            block: asc_new(heap, &self.block)?,
            transaction: asc_new(heap, &self.transaction)?,
            inputs: asc_new(heap, &self.inputs)?,
            outputs: asc_new(heap, &self.outputs)?,
        })
    }
}

impl ToAscObj<AscLogParam> for ethabi::LogParam {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscLogParam, DeterministicHostError> {
        Ok(AscLogParam {
            name: asc_new(heap, self.name.as_str())?,
            value: asc_new(heap, &self.value)?,
        })
    }
}
