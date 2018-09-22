use super::{AscHeap, AscPtr, AscType, AscValue};
use ethabi;
use graph::data::store;
use graph::serde_json;
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
    pub content: Box<[T]>,
}

impl<T: AscValue> ArrayBuffer<T> {
    pub fn new(content: &[T]) -> Self {
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

/// A typed, indexable view of an `ArrayBuffer` of Asc primitives. In Asc it's
/// an abstract class with subclasses for each primitive, for example
/// `Uint8Array` is `TypedArray<u8>`.
///  See https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#arrays
#[repr(C)]
pub(crate) struct TypedArray<T> {
    pub buffer: AscPtr<ArrayBuffer<T>>,
    byte_offset: u32,
    byte_length: u32,
}

impl<T: AscValue> TypedArray<T> {
    pub(crate) fn new<H: AscHeap>(content: &[T], heap: &H) -> Self {
        let buffer = ArrayBuffer::new(content);
        TypedArray {
            buffer: AscPtr::alloc_obj(&buffer, heap),
            byte_offset: 0,
            byte_length: buffer.byte_length,
        }
    }

    pub(crate) fn get_buffer<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<T> {
        self.buffer.read_ptr(heap)
    }
}

impl<T> AscType for TypedArray<T> {}

pub(crate) type Uint8Array = TypedArray<u8>;
pub(crate) type Uint64Array = TypedArray<u64>;

/// Asc std string: "Strings are encoded as UTF-16LE in AssemblyScript, and are
/// prefixed with their length (in character codes) as a 32-bit integer". See
/// https://github.com/AssemblyScript/assemblyscript/wiki/Memory-Layout-&-Management#strings
pub(crate) struct AscString {
    // In number of UTF-16 code units (2 bytes each).
    length: u32,
    // The sequence of UTF-16LE code units that form the string.
    pub content: Box<[u16]>,
}

impl AscString {
    pub fn new(content: &[u16]) -> Self {
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
    pub fn new<H: AscHeap>(content: &[T], heap: &H) -> Self {
        Array {
            buffer: heap.asc_new(content),
            length: content.len() as u32,
        }
    }

    pub fn get_buffer<H: AscHeap>(&self, heap: &H) -> ArrayBuffer<T> {
        self.buffer.read_ptr(heap)
    }
}

impl<T> AscType for Array<T> {}

/// Represents any `AscValue` since they all fit in 64 bits.
#[repr(C)]
#[derive(Copy, Clone, Default)]
pub(crate) struct EnumPayload(pub u64);

impl AscType for EnumPayload {}
impl AscValue for EnumPayload {}

impl From<EnumPayload> for i32 {
    fn from(payload: EnumPayload) -> i32 {
        // This is just `i32::from_bytes` which is unstable.
        unsafe { ::std::mem::transmute::<u32, i32>(payload.0 as u32) }
    }
}

impl From<EnumPayload> for f32 {
    fn from(payload: EnumPayload) -> f32 {
        f64::from_bits(payload.0) as f32
    }
}

impl From<EnumPayload> for bool {
    fn from(payload: EnumPayload) -> bool {
        payload.0 != 0
    }
}

impl From<i32> for EnumPayload {
    fn from(x: i32) -> EnumPayload {
        // This is just `i32::from_bytes` which is unstable.
        EnumPayload(unsafe { ::std::mem::transmute::<i32, u32>(x) } as u64)
    }
}

impl From<f32> for EnumPayload {
    fn from(x: f32) -> EnumPayload {
        EnumPayload((x as f64).to_bits())
    }
}

impl From<bool> for EnumPayload {
    fn from(b: bool) -> EnumPayload {
        EnumPayload(if b { 1 } else { 0 })
    }
}

impl From<i64> for EnumPayload {
    fn from(x: i64) -> EnumPayload {
        // This is just `u64::from_bytes` which is unstable.
        EnumPayload(unsafe { ::std::mem::transmute::<i64, u64>(x) })
    }
}

/// In Asc, we represent a Rust enum as a discriminant `kind: D`, which is an
/// Asc enum so in Rust it's a `#[repr(u32)]` enum, plus an arbitrary `AscValue`
/// payload.
#[repr(C)]
pub(crate) struct AscEnum<D: AscValue> {
    pub kind: D,
    pub payload: EnumPayload,
}

impl<D: AscValue> AscType for AscEnum<D> {}

pub(crate) type AscEnumArray<D> = AscPtr<Array<AscPtr<AscEnum<D>>>>;

#[repr(u32)]
#[derive(Copy, Clone)]
pub(crate) enum EthereumValueKind {
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

impl EthereumValueKind {
    pub(crate) fn get_kind(token: &ethabi::Token) -> Self {
        match token {
            ethabi::Token::Address(_) => EthereumValueKind::Address,
            ethabi::Token::FixedBytes(_) => EthereumValueKind::FixedBytes,
            ethabi::Token::Bytes(_) => EthereumValueKind::Bytes,
            ethabi::Token::Int(_) => EthereumValueKind::Int,
            ethabi::Token::Uint(_) => EthereumValueKind::Uint,
            ethabi::Token::Bool(_) => EthereumValueKind::Bool,
            ethabi::Token::String(_) => EthereumValueKind::String,
            ethabi::Token::FixedArray(_) => EthereumValueKind::FixedArray,
            ethabi::Token::Array(_) => EthereumValueKind::Array,
        }
    }
}

impl Default for EthereumValueKind {
    fn default() -> Self {
        EthereumValueKind::Address
    }
}

impl AscType for EthereumValueKind {}
impl AscValue for EthereumValueKind {}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum StoreValueKind {
    String,
    Int,
    Float,
    Bool,
    Array,
    Null,
    Bytes,
    BigInt,
}

impl StoreValueKind {
    pub(crate) fn get_kind(value: &store::Value) -> StoreValueKind {
        use self::store::Value;

        match value {
            Value::String(_) => StoreValueKind::String,
            Value::Int(_) => StoreValueKind::Int,
            Value::Float(_) => StoreValueKind::Float,
            Value::Bool(_) => StoreValueKind::Bool,
            Value::List(_) => StoreValueKind::Array,
            Value::Null => StoreValueKind::Null,
            Value::Bytes(_) => StoreValueKind::Bytes,
            Value::BigInt(_) => StoreValueKind::BigInt,
        }
    }
}

impl Default for StoreValueKind {
    fn default() -> Self {
        StoreValueKind::Null
    }
}

impl AscType for StoreValueKind {}
impl AscValue for StoreValueKind {}

#[repr(C)]
pub(crate) struct AscLogParam {
    pub name: AscPtr<AscString>,
    pub value: AscPtr<AscEnum<EthereumValueKind>>,
}

impl AscType for AscLogParam {}

pub(crate) type Bytes = Uint8Array;
/// Big ints are represented in two's complement and in little-endian order.
pub(crate) type BigInt = Uint8Array;
pub(crate) type AscAddress = Uint8Array;
pub(crate) type AscH160 = Uint8Array;
pub(crate) type AscH256 = Uint8Array;
pub(crate) type AscU128 = Uint64Array;
pub(crate) type AscU256 = Uint64Array;

pub(crate) type AscLogParamArray = Array<AscPtr<AscLogParam>>;

#[repr(C)]
pub(crate) struct AscEthereumBlock {
    pub hash: AscPtr<AscH256>,
    pub parent_hash: AscPtr<AscH256>,
    pub uncles_hash: AscPtr<AscH256>,
    pub author: AscPtr<AscH160>,
    pub state_root: AscPtr<AscH256>,
    pub transactions_root: AscPtr<AscH256>,
    pub receipts_root: AscPtr<AscH256>,
    pub number: AscPtr<AscU128>,
    pub gas_used: AscPtr<AscU256>,
    pub gas_limit: AscPtr<AscU256>,
    pub timestamp: AscPtr<AscU256>,
    pub difficulty: AscPtr<AscU256>,
    pub total_difficulty: AscPtr<AscU256>,
}

impl AscType for AscEthereumBlock {}

#[repr(C)]
pub(crate) struct AscEthereumTransaction {
    pub hash: AscPtr<AscH256>,
    pub block_hash: AscPtr<AscH256>,
    pub block_number: AscPtr<AscU256>,
    pub gas_used: AscPtr<AscU256>,
}

impl AscType for AscEthereumTransaction {}

#[repr(C)]
pub(crate) struct AscEthereumEvent {
    pub address: AscPtr<AscAddress>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<AscEthereumTransaction>,
    pub params: AscPtr<AscLogParamArray>,
}

impl AscType for AscEthereumEvent {}

#[repr(C)]
pub(crate) struct AscTypedMapEntry<K, V> {
    pub key: AscPtr<K>,
    pub value: AscPtr<V>,
}

impl<K, V> AscType for AscTypedMapEntry<K, V> {}

pub(crate) type AscTypedMapEntryArray<K, V> = Array<AscPtr<AscTypedMapEntry<K, V>>>;

#[repr(C)]
pub(crate) struct AscTypedMap<K, V> {
    pub entries: AscPtr<AscTypedMapEntryArray<K, V>>,
}

impl<K, V> AscType for AscTypedMap<K, V> {}

pub(crate) type AscEntity = AscTypedMap<AscString, AscEnum<StoreValueKind>>;
pub(crate) type AscJson = AscTypedMap<AscString, AscEnum<JsonValueKind>>;

#[repr(C)]
pub(crate) struct AscUnresolvedContractCall {
    pub contract_name: AscPtr<AscString>,
    pub contract_address: AscPtr<AscAddress>,
    pub function_name: AscPtr<AscString>,
    pub function_args: AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>,
}

impl AscType for AscUnresolvedContractCall {}

#[repr(u32)]
#[derive(Copy, Clone)]
pub(crate) enum JsonValueKind {
    Null,
    Bool,
    Number,
    String,
    Array,
    Object,
}

impl Default for JsonValueKind {
    fn default() -> Self {
        JsonValueKind::Null
    }
}

impl AscType for JsonValueKind {}
impl AscValue for JsonValueKind {}

impl JsonValueKind {
    pub(crate) fn get_kind(token: &serde_json::Value) -> Self {
        use graph::serde_json::Value;

        match token {
            Value::Null => JsonValueKind::Null,
            Value::Bool(_) => JsonValueKind::Bool,
            Value::Number(_) => JsonValueKind::Number,
            Value::String(_) => JsonValueKind::String,
            Value::Array(_) => JsonValueKind::Array,
            Value::Object(_) => JsonValueKind::Object,
        }
    }
}
