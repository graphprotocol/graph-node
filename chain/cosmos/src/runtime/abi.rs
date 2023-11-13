use crate::protobuf::*;
use graph::runtime::{HostExportError, WasmInstanceContext};
pub use graph::semver::Version;
use graph::wasmtime::StoreContextMut;

pub use graph::runtime::{
    asc_new, gas::GasCounter, AscHeap, AscIndexId, AscPtr, AscType, AscValue,
    DeterministicHostError, IndexForAscTypeId, ToAscObj,
};
/*
TODO: AscBytesArray seem to be generic to all chains, but AscIndexId pins it to Cosmos
****************** this can be moved to runtime graph/runtime/src/asc_heap.rs, but  IndexForAscTypeId::CosmosBytesArray ******
*/
pub struct AscBytesArray(pub Array<AscPtr<Uint8Array>>);

impl ToAscObj<AscBytesArray> for Vec<Vec<u8>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBytesArray, HostExportError> {
        let content: Result<Vec<_>, _> = self
            .iter()
            .map(|x| {
                asc_new(
                    store,
                    heap,
                    &graph::runtime::wasm::asc_abi::class::Bytes(x),
                    gas,
                )
            })
            .collect();

        Ok(AscBytesArray(Array::new(store, &content?, heap, gas)?))
    }
}

//this can be moved to runtime
impl AscType for AscBytesArray {
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

//we will have to keep this chain specific (Inner/Outer)
impl AscIndexId for AscBytesArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosBytesArray;
}

/************************************************************************** */
// this can be moved to runtime - prost_types::Any
impl ToAscObj<AscAny> for prost_types::Any {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAny, HostExportError> {
        Ok(AscAny {
            type_url: asc_new(store, heap, &self.type_url, gas)?,
            value: asc_new(
                store,
                heap,
                &graph::runtime::wasm::asc_abi::class::Bytes(&self.value),
                gas,
            )?,
            ..Default::default()
        })
    }
}

//this can be moved to runtime - prost_types::Any
impl ToAscObj<AscAnyArray> for Vec<prost_types::Any> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAnyArray, HostExportError> {
        let content: Result<Vec<_>, _> =
            self.iter().map(|x| asc_new(store, heap, x, gas)).collect();

        Ok(AscAnyArray(Array::new(store, &content?, heap, gas)?))
    }
}
