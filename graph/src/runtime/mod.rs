//! Facilities for creating and reading objects on the memory of an AssemblyScript (Asc) WASM
//! module. Objects are passed through the `asc_new` and `asc_get` methods of an `AscHeap`
//! implementation. These methods take types that implement `To`/`FromAscObj` and are therefore
//! convertible to/from an `AscType`.

mod asc_heap;
mod asc_ptr;

pub use asc_heap::{asc_get, asc_new, try_asc_get, AscHeap, FromAscObj, ToAscObj, TryFromAscObj};
pub use asc_ptr::AscPtr;

use anyhow::Error;
use std::convert::TryInto;
use std::fmt;
use std::mem::size_of;

/// A type that has a direct correspondence to an Asc type.
///
/// This can be derived for structs that are `#[repr(C)]`, contain no padding
/// and whose fields are all `AscValue`. Enums can derive if they are `#[repr(u32)]`.
///
/// Special classes like `ArrayBuffer` use custom impls.
///
/// See https://github.com/graphprotocol/graph-node/issues/607 for more considerations.
pub trait AscType: Sized {
    /// Transform the Rust representation of this instance into an sequence of
    /// bytes that is precisely the memory layout of a corresponding Asc instance.
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError>;

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError>;

    /// Size of the corresponding Asc instance in bytes.
    fn asc_size<H: AscHeap + ?Sized>(
        _ptr: AscPtr<Self>,
        _heap: &H,
    ) -> Result<u32, DeterministicHostError> {
        Ok(std::mem::size_of::<Self>() as u32)
    }
}

// Only implemented because of structs that derive AscType and
// contain fields that are PhantomData.
impl<T> AscType for std::marker::PhantomData<T> {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        Ok(vec![])
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
        assert!(asc_obj.len() == 0);

        Ok(Self)
    }
}

/// An Asc primitive or an `AscPtr` into the Asc heap. A type marked as
/// `AscValue` must have the same byte representation in Rust and Asc, including
/// same size, and size must be equal to alignment.
pub trait AscValue: AscType + Copy + Default {}

impl AscType for bool {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        Ok(vec![*self as u8])
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
        if asc_obj.len() != 1 {
            Err(DeterministicHostError(anyhow::anyhow!(
                "Incorrect size for bool. Expected 1, got {},",
                asc_obj.len()
            )))
        } else {
            Ok(asc_obj[0] != 0)
        }
    }
}

impl AscValue for bool {}
impl<T> AscValue for AscPtr<T> {}

macro_rules! impl_asc_type {
    ($($T:ty),*) => {
        $(
            impl AscType for $T {
                fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
                    Ok(self.to_le_bytes().to_vec())
                }

                fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
                    let bytes = asc_obj.try_into().map_err(|_| {
                        DeterministicHostError(anyhow::anyhow!(
                            "Incorrect size for {}. Expected {}, got {},",
                            stringify!($T),
                            size_of::<Self>(),
                            asc_obj.len()
                        ))
                    })?;

                    Ok(Self::from_le_bytes(bytes))
                }
            }

            impl AscValue for $T {}
        )*
    };
}

impl_asc_type!(u8, u16, u32, u64, i8, i32, i64, f32, f64);

#[derive(Debug)]
pub struct DeterministicHostError(pub Error);

impl fmt::Display for DeterministicHostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for DeterministicHostError {}

#[derive(thiserror::Error, Debug)]
pub enum HostExportError {
    #[error("{0:#}")]
    Unknown(anyhow::Error),

    #[error("{0:#}")]
    PossibleReorg(anyhow::Error),

    #[error("{0:#}")]
    Deterministic(anyhow::Error),
}

impl From<anyhow::Error> for HostExportError {
    fn from(e: anyhow::Error) -> Self {
        HostExportError::Unknown(e)
    }
}

impl From<DeterministicHostError> for HostExportError {
    fn from(value: DeterministicHostError) -> Self {
        HostExportError::Deterministic(value.0)
    }
}
