use graph::runtime::wasm::asc_abi::class::{Array, AscEnum, EnumPayload};
use graph::runtime::WasmInstanceContext;
use graph::wasmtime::StoreContextMut;
use graph::{
    prelude::BigInt,
    runtime::{asc_new, gas::GasCounter, AscHeap, HostExportError, ToAscObj},
};

use crate::{
    codec,
    trigger::{StarknetBlockTrigger, StarknetEventTrigger},
};

pub(crate) use super::generated::*;

impl ToAscObj<AscBlock> for codec::Block {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBlock, HostExportError> {
        Ok(AscBlock {
            number: asc_new(store, heap, &BigInt::from(self.height), gas)?,
            hash: asc_new(store, heap, self.hash.as_slice(), gas)?,
            prev_hash: asc_new(store, heap, self.prev_hash.as_slice(), gas)?,
            timestamp: asc_new(store, heap, &BigInt::from(self.timestamp), gas)?,
        })
    }
}

impl ToAscObj<AscTransaction> for codec::Transaction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTransaction, HostExportError> {
        Ok(AscTransaction {
            r#type: asc_new(
                store,
                heap,
                &codec::TransactionType::from_i32(self.r#type)
                    .expect("invalid TransactionType value"),
                gas,
            )?,
            hash: asc_new(store, heap, self.hash.as_slice(), gas)?,
        })
    }
}

impl ToAscObj<AscTransactionTypeEnum> for codec::TransactionType {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _store: &mut StoreContextMut<WasmInstanceContext>,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscTransactionTypeEnum, HostExportError> {
        Ok(AscTransactionTypeEnum(AscEnum {
            kind: match self {
                codec::TransactionType::Deploy => AscTransactionType::Deploy,
                codec::TransactionType::InvokeFunction => AscTransactionType::InvokeFunction,
                codec::TransactionType::Declare => AscTransactionType::Declare,
                codec::TransactionType::L1Handler => AscTransactionType::L1Handler,
                codec::TransactionType::DeployAccount => AscTransactionType::DeployAccount,
            },
            _padding: 0,
            payload: EnumPayload(0),
        }))
    }
}

impl ToAscObj<AscBytesArray> for Vec<Vec<u8>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBytesArray, HostExportError> {
        let content: Result<Vec<_>, _> = self
            .iter()
            .map(|x| asc_new(store, heap, x.as_slice(), gas))
            .collect();

        Ok(AscBytesArray(Array::new(store, &content?, heap, gas)?))
    }
}

impl ToAscObj<AscBlock> for StarknetBlockTrigger {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBlock, HostExportError> {
        self.block.to_asc_obj(store, heap, gas)
    }
}

impl ToAscObj<AscEvent> for StarknetEventTrigger {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEvent, HostExportError> {
        Ok(AscEvent {
            from_addr: asc_new(store, heap, self.event.from_addr.as_slice(), gas)?,
            keys: asc_new(store, heap, &self.event.keys, gas)?,
            data: asc_new(store, heap, &self.event.data, gas)?,
            block: asc_new(store, heap, self.block.as_ref(), gas)?,
            transaction: asc_new(store, heap, self.transaction.as_ref(), gas)?,
        })
    }
}
