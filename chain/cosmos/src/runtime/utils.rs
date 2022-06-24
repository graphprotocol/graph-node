use crate::protobuf::*;
use anyhow::anyhow;
pub use graph::semver::Version;

pub use graph::runtime::{
    asc_new, gas::GasCounter, AscHeap, AscIndexId, AscPtr, AscType, AscValue,
    DeterministicHostError, IndexForAscTypeId, ToAscObj,
};

/****************** move this two to runtime graph/runtime/src/asc_heap.rs *****************************/
pub struct Bytes<'a>(pub &'a Vec<u8>);

/****************** move this two to runtime graph/runtime/src/asc_heap.rs *****************************/
impl ToAscObj<Uint8Array> for Bytes<'_> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, DeterministicHostError> {
        self.0.to_asc_obj(heap, gas)
    }
}

/****************** this can be moved to runtime graph/runtime/src/asc_heap.rs, but  IndexForAscTypeId::CosmosBytesArray *******/

pub struct AscBytesArray(pub Array<AscPtr<Uint8Array>>);

impl ToAscObj<AscBytesArray> for Vec<Vec<u8>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBytesArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> =
            self.iter().map(|x| asc_new(heap, &Bytes(x), gas)).collect();

        Ok(AscBytesArray(Array::new(&content?, heap, gas)?))
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

//we will have to keep this chain specific
impl AscIndexId for AscBytesArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosBytesArray; //12345
}

/************************************************************************** */

//this one (whole thing) is weird, there is also another Any in protobuf
impl ToAscObj<AscAny> for prost_types::Any {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAny, DeterministicHostError> {
        Ok(AscAny {
            type_url: asc_new(heap, &self.type_url, gas)?,
            value: asc_new(heap, &Bytes(&self.value), gas)?,
            ..Default::default()
        })
    }
}

//AscAnyArray is generated for protobuf
// pub struct AscAnyArray(pub  Array<AscPtr<AscAny>>);

impl ToAscObj<AscAnyArray> for Vec<prost_types::Any> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAnyArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscAnyArray(Array::new(&content?, heap, gas)?))
    }
}

// impl AscType for AscAnyArray {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         self.0.to_asc_bytes()
//     }

//     fn from_asc_bytes(
//         asc_obj: &[u8],
//         api_version: &Version,
//     ) -> Result<Self, DeterministicHostError> {
//         Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
//     }
// }

// impl AscIndexId for AscAnyArray {
//     const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosAnyArray;
// }

/****************** move this two to runtime graph/runtime/src/asc_heap.rs *****************************/
/// Create an error for a missing field in a type.
fn missing_field_error(type_name: &str, field_name: &str) -> DeterministicHostError {
    DeterministicHostError::from(anyhow!("{} missing {}", type_name, field_name))
}

/****************** move this two to runtime graph/runtime/src/asc_heap.rs *****************************/
/// Map an optional object to its Asc equivalent if Some, otherwise return null.
pub fn asc_new_or_null<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
    gas: &GasCounter,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o, gas),
        None => Ok(AscPtr::null()),
    }
}

/****************** move this two to runtime graph/runtime/src/asc_heap.rs *****************************/
/// Map an optional object to its Asc equivalent if Some, otherwise return a missing field error.
pub fn asc_new_or_missing<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
    gas: &GasCounter,
    type_name: &str,
    field_name: &str,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o, gas),
        None => Err(missing_field_error(type_name, field_name)),
    }
}
