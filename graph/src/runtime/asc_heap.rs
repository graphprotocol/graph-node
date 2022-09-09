use std::mem::MaybeUninit;

use semver::Version;

use super::{
    gas::GasCounter, AscIndexId, AscPtr, AscType, DeterministicHostError, IndexForAscTypeId,
};
/// A type that can read and write to the Asc heap. Call `asc_new` and `asc_get`
/// for reading and writing Rust structs from and to Asc.
///
/// The implementor must provide the direct Asc interface with `raw_new` and `get`.
pub trait AscHeap {
    /// Allocate new space and write `bytes`, return the allocated address.
    fn raw_new(&mut self, bytes: &[u8], gas: &GasCounter) -> Result<u32, DeterministicHostError>;

    fn read<'a>(
        &self,
        offset: u32,
        buffer: &'a mut [MaybeUninit<u8>],
        gas: &GasCounter,
    ) -> Result<&'a mut [u8], DeterministicHostError>;

    fn read_u32(&self, offset: u32, gas: &GasCounter) -> Result<u32, DeterministicHostError>;

    fn api_version(&self) -> Version;

    fn asc_type_id(
        &mut self,
        type_id_index: IndexForAscTypeId,
    ) -> Result<u32, DeterministicHostError>;
}

/// Instantiate `rust_obj` as an Asc object of class `C`.
/// Returns a pointer to the Asc heap.
///
/// This operation is expensive as it requires a call to `raw_new` for every
/// nested object.
pub fn asc_new<C, T: ?Sized, H: AscHeap + ?Sized>(
    heap: &mut H,
    rust_obj: &T,
    gas: &GasCounter,
) -> Result<AscPtr<C>, DeterministicHostError>
where
    C: AscType + AscIndexId,
    T: ToAscObj<C>,
{
    let obj = rust_obj.to_asc_obj(heap, gas)?;
    AscPtr::alloc_obj(obj, heap, gas)
}

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

/// Create an error for a missing field in a type.
fn missing_field_error(type_name: &str, field_name: &str) -> DeterministicHostError {
    DeterministicHostError::from(anyhow::anyhow!("{} missing {}", type_name, field_name))
}

///  Read the rust representation of an Asc object of class `C`.
///
///  This operation is expensive as it requires a call to `get` for every
///  nested object.
pub fn asc_get<T, C, H: AscHeap + ?Sized>(
    heap: &H,
    asc_ptr: AscPtr<C>,
    gas: &GasCounter,
) -> Result<T, DeterministicHostError>
where
    C: AscType + AscIndexId,
    T: FromAscObj<C>,
{
    T::from_asc_obj(asc_ptr.read_ptr(heap, gas)?, heap, gas)
}

/// Type that can be converted to an Asc object of class `C`.
pub trait ToAscObj<C: AscType> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<C, DeterministicHostError>;
}

impl<C: AscType, T: ToAscObj<C>> ToAscObj<C> for &T {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<C, DeterministicHostError> {
        (*self).to_asc_obj(heap, gas)
    }
}

impl ToAscObj<bool> for bool {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<bool, DeterministicHostError> {
        Ok(*self)
    }
}

/// Type that can be converted from an Asc object of class `C`.
pub trait FromAscObj<C: AscType>: Sized {
    fn from_asc_obj<H: AscHeap + ?Sized>(
        obj: C,
        heap: &H,
        gas: &GasCounter,
    ) -> Result<Self, DeterministicHostError>;
}
