//! Facilities for creating and reading objects on the memory of an
//! AssemblyScript (Asc) WASM module. Objects are passed through the `asc_new`
//! and `asc_get` methods of an `AscHeap` implementation. These methods take
//! types that implement `To`/`FromAscObj` and are therefore convertible to/from
//! an `AscType`. Implementations of `AscType` live in the `class` module.
//! Implementations of `To`/`FromAscObj` live in the `to_from` module.

pub use self::asc_ptr::AscPtr;
use graph::prelude::anyhow;
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
    fn raw_new(&mut self, bytes: &[u8]) -> u32;

    fn get(&self, offset: u32, size: u32) -> Vec<u8>;

    /// Instatiate `rust_obj` as an Asc object of class `C`.
    /// Returns a pointer to the Asc heap.
    ///
    /// This operation is expensive as it requires a call to `raw_new` for every
    /// nested object.
    fn asc_new<C, T: ?Sized>(&mut self, rust_obj: &T) -> AscPtr<C>
    where
        C: AscType,
        T: ToAscObj<C>,
    {
        AscPtr::alloc_obj(&rust_obj.to_asc_obj(self), self)
    }

    ///  Read the rust representation of an Asc object of class `C`.
    ///
    ///  This operation is expensive as it requires a call to `get` for every
    ///  nested object.
    fn asc_get<T, C>(&self, asc_ptr: AscPtr<C>) -> T
    where
        C: AscType,
        T: FromAscObj<C>,
    {
        T::from_asc_obj(asc_ptr.read_ptr(self), self)
    }

    fn try_asc_get<T, C>(&self, asc_ptr: AscPtr<C>) -> Result<T, anyhow::Error>
    where
        C: AscType,
        T: TryFromAscObj<C>,
    {
        T::try_from_asc_obj(asc_ptr.read_ptr(self), self)
    }
}

/// Type that can be converted to an Asc object of class `C`.
pub trait ToAscObj<C: AscType> {
    fn to_asc_obj<H: AscHeap>(&self, heap: &mut H) -> C;
}

/// Type that can be converted from an Asc object of class `C`.
pub trait FromAscObj<C: AscType> {
    fn from_asc_obj<H: AscHeap>(obj: C, heap: &H) -> Self;
}

pub trait TryFromAscObj<C: AscType>: Sized {
    fn try_from_asc_obj<H: AscHeap>(obj: C, heap: &H) -> Result<Self, anyhow::Error>;
}

// `AscType` is not really public, implementors should live inside the `class` module.

/// A type that has a direct corespondence to an Asc type.
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
    fn to_asc_bytes(&self) -> Vec<u8>;

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(asc_obj: &[u8]) -> Self;

    /// Size of the corresponding Asc instance in bytes.
    fn asc_size<H: AscHeap>(_ptr: AscPtr<Self>, _heap: &H) -> u32 {
        size_of::<Self>() as u32
    }
}

// `AscValue` also isn't really public.

/// An Asc primitive or an `AscPtr` into the Asc heap. A type marked as
/// `AscValue` must have the same byte representation in Rust and Asc, including
/// same size, and size must be equal to alignment.
pub trait AscValue: AscType + Copy + Default {}

impl AscType for bool {
    fn to_asc_bytes(&self) -> Vec<u8> {
        vec![*self as u8]
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        asc_obj[0] != 0
    }
}

impl AscType for i8 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        vec![*self as u8]
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        asc_obj[0] as i8
    }
}

impl AscType for i16 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        Self::from_le_bytes([asc_obj[0], asc_obj[1]])
    }
}

impl AscType for i32 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        Self::from_le_bytes([asc_obj[0], asc_obj[1], asc_obj[2], asc_obj[3]])
    }
}

impl AscType for i64 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        Self::from_le_bytes([
            asc_obj[0], asc_obj[1], asc_obj[2], asc_obj[3], asc_obj[4], asc_obj[5], asc_obj[6],
            asc_obj[7],
        ])
    }
}

impl AscType for u8 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        vec![*self]
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        asc_obj[0]
    }
}

impl AscType for u16 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        Self::from_le_bytes([asc_obj[0], asc_obj[1]])
    }
}

impl AscType for u32 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        Self::from_le_bytes([asc_obj[0], asc_obj[1], asc_obj[2], asc_obj[3]])
    }
}

impl AscType for u64 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        assert_eq!(asc_obj.len(), size_of::<Self>());
        Self::from_le_bytes([
            asc_obj[0], asc_obj[1], asc_obj[2], asc_obj[3], asc_obj[4], asc_obj[5], asc_obj[6],
            asc_obj[7],
        ])
    }
}

impl AscType for f32 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_bits().to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        Self::from_bits(u32::from_asc_bytes(asc_obj))
    }
}

impl AscType for f64 {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.to_bits().to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        Self::from_bits(u64::from_asc_bytes(asc_obj))
    }
}

impl AscValue for bool {}
impl AscValue for i8 {}
impl AscValue for i16 {}
impl AscValue for i32 {}
impl AscValue for i64 {}
impl AscValue for u8 {}
impl AscValue for u16 {}
impl AscValue for u32 {}
impl AscValue for u64 {}
impl AscValue for f32 {}
impl AscValue for f64 {}
