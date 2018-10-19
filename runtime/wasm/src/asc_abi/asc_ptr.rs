use super::{class::EnumPayload, AscHeap, AscType};
use std::fmt;
use std::marker::PhantomData;
use std::mem::{self, size_of};
use wasmi::{FromRuntimeValue, RuntimeValue};

/// A pointer to an object in the Asc heap.
pub struct AscPtr<C>(u32, PhantomData<C>);

impl<T> Copy for AscPtr<T> {}

impl<T> Clone for AscPtr<T> {
    fn clone(&self) -> Self {
        AscPtr(self.0, PhantomData)
    }
}

impl<T> Default for AscPtr<T> {
    fn default() -> Self {
        AscPtr(0, PhantomData)
    }
}

impl<T> fmt::Debug for AscPtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<C: AscType> AscPtr<C> {
    /// Read from `self` into the Rust struct `C`.
    pub(super) fn read_ptr<H: AscHeap>(self, heap: &H) -> C {
        C::from_asc_bytes(&heap.get(self.0, C::asc_size(self, heap)).unwrap())
    }

    /// Allocate `asc_obj` as an Asc object of class `C`.
    pub(super) fn alloc_obj<H: AscHeap>(asc_obj: &C, heap: &H) -> AscPtr<C> {
        AscPtr(heap.raw_new(&asc_obj.to_asc_bytes()).unwrap(), PhantomData)
    }

    /// Helper used by arrays and strings to read their length.
    pub(super) fn read_u32<H: AscHeap>(&self, heap: &H) -> u32 {
        // Read the bytes pointed to by `self` as the bytes of a `u32`.
        let raw_bytes = heap.get(self.0, size_of::<u32>() as u32).unwrap();
        let mut u32_bytes: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
        u32_bytes.copy_from_slice(&raw_bytes);

        // Get the u32 from the bytes. This is just `u32::from_bytes` which is unstable.
        unsafe { mem::transmute(u32_bytes) }
    }

    /// Conversion to `u64` for use with `AscEnum`.
    pub(crate) fn to_payload(&self) -> u64 {
        self.0 as u64
    }
}

impl<C> From<AscPtr<C>> for RuntimeValue {
    fn from(ptr: AscPtr<C>) -> RuntimeValue {
        RuntimeValue::from(ptr.0)
    }
}

impl<C> FromRuntimeValue for AscPtr<C> {
    fn from_runtime_value(val: RuntimeValue) -> Option<Self> {
        u32::from_runtime_value(val).map(|ptr| AscPtr(ptr, PhantomData))
    }
}

impl<C> From<EnumPayload> for AscPtr<C> {
    fn from(payload: EnumPayload) -> Self {
        AscPtr(payload.0 as u32, PhantomData)
    }
}

impl<C> From<AscPtr<C>> for EnumPayload {
    fn from(x: AscPtr<C>) -> EnumPayload {
        EnumPayload(x.0 as u64)
    }
}
