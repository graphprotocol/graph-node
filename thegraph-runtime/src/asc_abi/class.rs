use super::{AscHeap, AscPtr, AscType, AscValue};
use ethabi;
use std::mem::{self, size_of, size_of_val};

///! Rust types that have with a direct correspondence to an Asc class,
///! with their `AscType` implementations.

/// Asc std ArrayBuffer: "a generic, fixed-length raw binary data buffer".
/// See https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#arrays
pub(crate) struct ArrayBuffer<T> {
    byte_length: u32,
    // Asc allocators always align at 8 bytes, we already have 4 bytes from
    // `byte_length_size` so with 4 more bytes we align the contents at 8
    // bytes. No Asc type has alignment greater than 8, so the
    // elements in `content` will be aligned for any element type.
    _padding: [u8; 4],
    // In Asc this slice is layed out inline with the ArrayBuffer.
    pub(super) content: Box<[T]>,
}

impl<T: AscValue> ArrayBuffer<T> {
    pub(super) fn new(content: &[T]) -> Self {
        // An `AscValue` has the same size in Rust and Asc so:
        let byte_length = size_of::<T>() * content.len();

        assert!(
            byte_length <= u32::max_value() as usize,
            "slice cannot fit in WASM memory"
        );
        let byte_length = byte_length as u32;

        ArrayBuffer {
            byte_length,
            _padding: [0; 4],
            content: content.into(),
        }
    }
}

impl<T: AscValue> AscType for ArrayBuffer<T> {
    fn to_asc_bytes(&self) -> Vec<u8> {
        let mut asc_layout: Vec<u8> = Vec::new();

        // This is just `self.byte_length.to_bytes()` which is unstable.
        let byte_length: [u8; 4] = unsafe { mem::transmute(self.byte_length) };
        asc_layout.extend(&byte_length);
        let padding: [u8; 4] = [0, 0, 0, 0];
        asc_layout.extend(&padding);
        for elem in self.content.iter() {
            asc_layout.extend(elem.to_asc_bytes())
        }
        asc_layout
    }

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        // Pointer for our current position within `asc_obj`,
        // initially at the start of the content,
        // skipping `byte_length` and the padding.
        let mut offset = size_of::<u32>() + 4;

        // Read the content.
        let mut content = Vec::new();
        while offset < asc_obj.len() {
            let elem_bytes = &asc_obj[offset..(offset + size_of::<T>())];
            content.push(T::from_asc_bytes(elem_bytes));
            offset += size_of::<T>();
        }

        ArrayBuffer::new(&content)
    }

    fn asc_size<H: AscHeap>(ptr: AscPtr<Self>, heap: &H) -> u32 {
        let byte_length = ptr.read_u32(heap);
        let byte_length_size = size_of::<u32>() as u32;
        let padding_size = size_of::<u32>() as u32;
        byte_length_size + padding_size + byte_length
    }
}

/// Asc std string: "Strings are encoded as UTF-16LE in AssemblyScript, and are
/// prefixed with their length (in character codes) as a 32-bit integer". See
/// https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#strings
pub(crate) struct AscString {
    // In number of UTF-16 code units (2 bytes each).
    length: u32,
    // The sequence of UTF-16LE code units that form the string.
    pub(super) content: Box<[u16]>,
}

impl AscString {
    pub(super) fn new(content: &[u16]) -> Self {
        assert!(
            size_of_val(content) <= u32::max_value() as usize,
            "string cannot fit in WASM memory"
        );

        AscString {
            length: content.len() as u32,
            content: content.into(),
        }
    }
}

impl AscType for AscString {
    fn to_asc_bytes(&self) -> Vec<u8> {
        let mut asc_layout: Vec<u8> = Vec::new();

        // This is just `self.length.to_bytes()` which is unstable.
        let length: [u8; 4] = unsafe { mem::transmute(self.length) };
        asc_layout.extend(&length);

        // Write the code points, in little-endian (LE) order.
        for &code_unit in self.content.iter() {
            let low_byte = code_unit as u8;
            let high_byte = (code_unit >> 8) as u8;
            asc_layout.push(low_byte);
            asc_layout.push(high_byte);
        }

        asc_layout
    }

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        // Pointer for our current position within `asc_obj`,
        // initially at the start of the content skipping `length`.
        let mut offset = size_of::<u32>();

        // Read the content.
        let mut content = Vec::new();
        while offset < asc_obj.len() {
            let code_point_bytes = [asc_obj[offset], asc_obj[offset + 1]];

            // This is just `u16::from_bytes` which is unstable.
            let code_point = unsafe { mem::transmute(code_point_bytes) };
            content.push(code_point);
            offset += size_of::<u16>();
        }
        AscString::new(&content)
    }

    fn asc_size<H: AscHeap>(ptr: AscPtr<Self>, heap: &H) -> u32 {
        let length = ptr.read_u32(heap);
        let length_size = size_of::<u32>() as u32;
        let code_point_size = size_of::<u16>() as u32;
        length_size + code_point_size * length
    }
}

/// Growable array backed by an `ArrayBuffer`.
/// See https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#arrays
#[repr(C)]
pub(crate) struct Array<T> {
    buffer: AscPtr<ArrayBuffer<T>>,
    length: u32,
}

impl<T: AscValue> Array<T> {
    pub(super) fn new<H: AscHeap>(content: &[T], heap: &H) -> Self {
        Array {
            buffer: heap.asc_new(content),
            length: content.len() as u32,
        }
    }

    pub(super) fn get_buffer<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<T> {
        self.buffer.read_ptr(heap)
    }
}

impl<T> AscType for Array<T> {}

/// In Asc, we represent a Rust enum as a discriminant `kind: D`, which is an
/// Asc enum so in Rust it's a `#[repr(u32)]` enum, plus an arbitrary `AscValue`
/// payload.
#[repr(C)]
pub(crate) struct AscEnum<D: AscValue> {
    pub kind: D,
    pub payload: u64, // All `AscValue`s fit in 64 bits.
}

impl<D: AscValue> AscType for AscEnum<D> {}

#[repr(u32)]
#[derive(Copy, Clone)]
pub(crate) enum TokenKind {
    Address,
    FixedBytes,
    Bytes,
    Int,
    Uint,
    Bool,
    String,
    FixedArray,
    Array,
}

impl TokenKind {
    pub(crate) fn get_kind(token: &ethabi::Token) -> Self {
        match token {
            ethabi::Token::Address(_) => TokenKind::Address,
            ethabi::Token::FixedBytes(_) => TokenKind::FixedBytes,
            ethabi::Token::Bytes(_) => TokenKind::Bytes,
            ethabi::Token::Int(_) => TokenKind::Int,
            ethabi::Token::Uint(_) => TokenKind::Uint,
            ethabi::Token::Bool(_) => TokenKind::Bool,
            ethabi::Token::String(_) => TokenKind::String,
            ethabi::Token::FixedArray(_) => TokenKind::FixedArray,
            ethabi::Token::Array(_) => TokenKind::Array,
        }
    }
}

impl Default for TokenKind {
    fn default() -> Self {
        TokenKind::Address
    }
}

impl AscType for TokenKind {}
impl AscValue for TokenKind {}

pub(crate) type AscTokenArray = AscPtr<Array<AscPtr<AscEnum<TokenKind>>>>;
