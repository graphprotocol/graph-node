use super::class::ArrayBuffer;
use super::{AscHeap, AscType};
use std::marker::PhantomData;
use std::mem;
use wasmi::{FromRuntimeValue, RuntimeValue};

/// A pointer to an object in the ASc heap.
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

impl<C: AscType> AscPtr<C> {
    /// Read from `self` into the Rust struct `C`.
    pub(super) fn read_ptr<H: AscHeap>(self, heap: &H) -> C {
        C::from_asc_bytes(&heap.get(self.0, C::asc_size(self, heap)).unwrap())
    }

    /// Allocate `asc_obj` as an ASc object of class `C`.
    pub(super) fn alloc_obj<H: AscHeap>(asc_obj: &C, heap: &H) -> AscPtr<C> {
        AscPtr(heap.raw_new(&asc_obj.to_asc_bytes()).unwrap(), PhantomData)
    }
}

impl<T> AscPtr<ArrayBuffer<T>> {
    pub(super) fn read_byte_length<H: AscHeap>(&self, heap: &H) -> u32 {
        // The byte length is a u32 and is the first field.
        let raw_length_bytes = heap.get(self.0, mem::size_of::<u32>() as u32).unwrap();
        // Read the u32 bytes.
        let mut byte_length_bytes: [u8; 4] = [0; 4];
        byte_length_bytes.copy_from_slice(&raw_length_bytes);
        // Get the u32 from the bytes. This is just `u32::from_bytes` which is unstable.
        unsafe { mem::transmute(byte_length_bytes) }
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
