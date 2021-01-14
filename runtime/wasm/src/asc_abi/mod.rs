//! Facilities for creating and reading objects on the memory of an
//! AssemblyScript (Asc) WASM module. Objects are passed through the `asc_new`
//! and `asc_get` methods of an `AscHeap` implementation. These methods take
//! types that implement `To`/`FromAscObj` and are therefore convertible to/from
//! an `AscType`. Implementations of `AscType` live in the `class` module.
//! Implementations of `To`/`FromAscObj` live in the `to_from` module.

pub use self::asc_ptr::AscPtr;
use crate::error::DeterministicHostError;
use graph::prelude::anyhow;
use std::convert::TryInto;
use std::mem::size_of;

pub mod asc_ptr;
pub mod class;

// WASM is little-endian, and for simplicity we currently assume that the host
// is also little-endian.
#[cfg(target_endian = "big")]
compile_error!("big-endian targets are currently unsupported");

/// A type that can read and write to the Asc heap. Call `asc_new` and `asc_get`
/// for reading and writing Rust structs from and to Asc.
///
/// The implementor must provide the direct Asc interface with `raw_new` and `get`.
pub trait AscHeap: Sized {
    /// Allocate new space and write `bytes`, return the allocated address.
    fn raw_new(&mut self, bytes: &[u8]) -> Result<u32, DeterministicHostError>;

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, DeterministicHostError>;

    /// Instantiate `rust_obj` as an Asc object of class `C`.
    /// Returns a pointer to the Asc heap.
    ///
    /// This operation is expensive as it requires a call to `raw_new` for every
    /// nested object.
    fn asc_new<C, T: ?Sized>(&mut self, rust_obj: &T) -> Result<AscPtr<C>, DeterministicHostError>
    where
        C: AscType,
        T: ToAscObj<C>,
    {
        let obj = rust_obj.to_asc_obj(self)?;
        AscPtr::alloc_obj(&obj, self)
    }

    ///  Read the rust representation of an Asc object of class `C`.
    ///
    ///  This operation is expensive as it requires a call to `get` for every
    ///  nested object.
    fn asc_get<T, C>(&self, asc_ptr: AscPtr<C>) -> Result<T, DeterministicHostError>
    where
        C: AscType,
        T: FromAscObj<C>,
    {
        T::from_asc_obj(asc_ptr.read_ptr(self)?, self)
    }

    fn try_asc_get<T, C>(&self, asc_ptr: AscPtr<C>) -> Result<T, DeterministicHostError>
    where
        C: AscType,
        T: TryFromAscObj<C>,
    {
        T::try_from_asc_obj(asc_ptr.read_ptr(self)?, self)
    }
}

/// Type that can be converted to an Asc object of class `C`.
pub trait ToAscObj<C: AscType> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> Result<C, DeterministicHostError>;
}

/// Type that can be converted from an Asc object of class `C`.
pub trait FromAscObj<C: AscType> {
    fn from_asc_obj<H: AscHeap>(obj: C, heap: &H) -> Result<Self, DeterministicHostError>
    where
        Self: Sized;
}

pub trait TryFromAscObj<C: AscType>: Sized {
    fn try_from_asc_obj<H: AscHeap>(obj: C, heap: &H) -> Result<Self, DeterministicHostError>;
}

// `AscType` is not really public, implementors should live inside the `class` module.

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
    fn asc_size<H: AscHeap>(_ptr: AscPtr<Self>, _heap: &H) -> Result<u32, DeterministicHostError> {
        Ok(size_of::<Self>() as u32)
    }
}

// `AscValue` also isn't really public.

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
