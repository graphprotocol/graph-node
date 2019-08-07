use super::{AscHeap, AscPtr, AscType, AscValue};
use ethabi;
use graph::data::store;
use graph::prelude::serde_json;
use graph::prelude::slog;
use graph_runtime_derive::AscType;
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};

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
    padding: [u8; 4],
    // In Asc this slice is layed out inline with the ArrayBuffer.
    content: Box<[u8]>,
    ty: PhantomData<T>,
}

impl<T: AscValue> ArrayBuffer<T> {
    fn new(values: &[T]) -> Self {
        let content = values
            .iter()
            .map(AscType::to_asc_bytes)
            // An `AscValue` has size equal to alignment, no padding required.
            .fold(vec![], |mut bytes, value| {
                bytes.extend(value);
                bytes
            });

        assert!(
            content.len() <= u32::max_value() as usize,
            "slice cannot fit in WASM memory"
        );
        let byte_length = content.len() as u32;

        ArrayBuffer {
            byte_length,
            padding: [0; 4],
            content: content.into(),
            ty: PhantomData,
        }
    }

    /// Read `length` elements of type `T` starting at `byte_offset`.
    ///
    /// Panics if that tries to read beyond the length of `self.content`.
    fn get(&self, byte_offset: u32, length: u32) -> Vec<T> {
        let length = length as usize;
        let byte_offset = byte_offset as usize;
        self.content[byte_offset..]
            .chunks(size_of::<T>())
            .take(length)
            .fold(vec![], |mut values, bytes| {
                values.push(T::from_asc_bytes(bytes));
                values
            })
    }
}

impl<T> AscType for ArrayBuffer<T> {
    fn to_asc_bytes(&self) -> Vec<u8> {
        let mut asc_layout: Vec<u8> = Vec::new();

        let byte_length: [u8; 4] = self.byte_length.to_le_bytes();
        asc_layout.extend(&byte_length);
        asc_layout.extend(&self.padding);
        asc_layout.extend(self.content.iter());

        // Allocate extra capacity to next power of two, as required by asc.
        let header_size = size_of_val(&byte_length) + size_of_val(&self.padding);
        let total_size = self.byte_length as usize + header_size;
        let total_capacity = total_size.next_power_of_two();
        let extra_capacity = total_capacity - total_size;
        asc_layout.extend(vec![0; extra_capacity]);
        assert_eq!(asc_layout.len(), total_capacity);

        asc_layout
    }

    /// The Rust representation of an Asc object as layed out in Asc memory.
    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        // Skip `byte_length` and the padding.
        let content_offset = size_of::<u32>() + 4;
        ArrayBuffer {
            byte_length: u32::from_asc_bytes(&asc_obj[..size_of::<u32>()]),
            padding: [0; 4],
            content: asc_obj[content_offset..].to_vec().into(),
            ty: PhantomData,
        }
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
#[derive(AscType)]
pub(crate) struct TypedArray<T> {
    pub buffer: AscPtr<ArrayBuffer<T>>,
    /// Byte position in `buffer` of the array start.
    byte_offset: u32,
    byte_length: u32,
}

impl<T: AscValue> TypedArray<T> {
    pub(crate) fn new<H: AscHeap>(content: &[T], heap: &mut H) -> Self {
        let buffer = ArrayBuffer::new(content);
        TypedArray {
            buffer: AscPtr::alloc_obj(&buffer, heap),
            byte_offset: 0,
            byte_length: buffer.byte_length,
        }
    }

    pub(crate) fn to_vec<H: AscHeap>(&self, heap: &H) -> Vec<T> {
        self.buffer
            .read_ptr(heap)
            .get(self.byte_offset, self.byte_length / size_of::<T>() as u32)
    }
}

pub(crate) type Uint8Array = TypedArray<u8>;

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

        let length: [u8; 4] = self.length.to_le_bytes();
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

            let code_point = u16::from_le_bytes(code_point_bytes);
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
#[derive(AscType)]
pub(crate) struct Array<T> {
    buffer: AscPtr<ArrayBuffer<T>>,
    length: u32,
}

impl<T: AscValue> Array<T> {
    pub fn new<H: AscHeap>(content: &[T], heap: &mut H) -> Self {
        Array {
            buffer: AscPtr::alloc_obj(&ArrayBuffer::new(content), heap),
            // If this cast would overflow, the above line has already panicked.
            length: content.len() as u32,
        }
    }

    pub(crate) fn to_vec<H: AscHeap>(&self, heap: &H) -> Vec<T> {
        self.buffer.read_ptr(heap).get(0, self.length)
    }
}

/// Represents any `AscValue` since they all fit in 64 bits.
#[repr(C)]
#[derive(Copy, Clone, Default)]
pub(crate) struct EnumPayload(pub u64);

impl AscType for EnumPayload {
    fn to_asc_bytes(&self) -> Vec<u8> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8]) -> Self {
        EnumPayload(u64::from_asc_bytes(asc_obj))
    }
}

impl AscValue for EnumPayload {}

impl From<EnumPayload> for i32 {
    fn from(payload: EnumPayload) -> i32 {
        payload.0 as i32
    }
}

impl From<EnumPayload> for f64 {
    fn from(payload: EnumPayload) -> f64 {
        f64::from_bits(payload.0)
    }
}

impl From<EnumPayload> for bool {
    fn from(payload: EnumPayload) -> bool {
        payload.0 != 0
    }
}

impl From<i32> for EnumPayload {
    fn from(x: i32) -> EnumPayload {
        EnumPayload(x as u64)
    }
}

impl From<f64> for EnumPayload {
    fn from(x: f64) -> EnumPayload {
        EnumPayload(x.to_bits())
    }
}

impl From<bool> for EnumPayload {
    fn from(b: bool) -> EnumPayload {
        EnumPayload(if b { 1 } else { 0 })
    }
}

impl From<i64> for EnumPayload {
    fn from(x: i64) -> EnumPayload {
        EnumPayload(x as u64)
    }
}

/// In Asc, we represent a Rust enum as a discriminant `kind: D`, which is an
/// Asc enum so in Rust it's a `#[repr(u32)]` enum, plus an arbitrary `AscValue`
/// payload.
#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEnum<D: AscValue> {
    pub kind: D,
    pub _padding: u32, // Make padding explicit.
    pub payload: EnumPayload,
}

pub(crate) type AscEnumArray<D> = AscPtr<Array<AscPtr<AscEnum<D>>>>;

#[repr(u32)]
#[derive(AscType, Copy, Clone)]
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
    Tuple,
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
            ethabi::Token::Tuple(_) => EthereumValueKind::Tuple,
        }
    }
}

impl Default for EthereumValueKind {
    fn default() -> Self {
        EthereumValueKind::Address
    }
}

impl AscValue for EthereumValueKind {}

#[repr(u32)]
#[derive(AscType, Copy, Clone)]
pub enum StoreValueKind {
    String,
    Int,
    BigDecimal,
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
            Value::BigDecimal(_) => StoreValueKind::BigDecimal,
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

impl AscValue for StoreValueKind {}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscLogParam {
    pub name: AscPtr<AscString>,
    pub value: AscPtr<AscEnum<EthereumValueKind>>,
}

pub(crate) type Bytes = Uint8Array;

/// Big ints are represented using signed number representation. Note: This differs
/// from how U256 and U128 are represented (they use two's complement). So whenever
/// we convert between them, we need to make sure we handle signed and unsigned
/// cases correctly.
pub(crate) type AscBigInt = Uint8Array;

pub(crate) type AscAddress = Uint8Array;
pub(crate) type AscH160 = Uint8Array;
pub(crate) type AscH256 = Uint8Array;

pub(crate) type AscLogParamArray = Array<AscPtr<AscLogParam>>;

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumBlock {
    pub hash: AscPtr<AscH256>,
    pub parent_hash: AscPtr<AscH256>,
    pub uncles_hash: AscPtr<AscH256>,
    pub author: AscPtr<AscH160>,
    pub state_root: AscPtr<AscH256>,
    pub transactions_root: AscPtr<AscH256>,
    pub receipts_root: AscPtr<AscH256>,
    pub number: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_limit: AscPtr<AscBigInt>,
    pub timestamp: AscPtr<AscBigInt>,
    pub difficulty: AscPtr<AscBigInt>,
    pub total_difficulty: AscPtr<AscBigInt>,
    pub size: AscPtr<AscBigInt>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumTransaction {
    pub hash: AscPtr<AscH256>,
    pub index: AscPtr<AscBigInt>,
    pub from: AscPtr<AscH160>,
    pub to: AscPtr<AscH160>,
    pub value: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_price: AscPtr<AscBigInt>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumTransaction_0_0_2 {
    pub hash: AscPtr<AscH256>,
    pub index: AscPtr<AscBigInt>,
    pub from: AscPtr<AscH160>,
    pub to: AscPtr<AscH160>,
    pub value: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub gas_price: AscPtr<AscBigInt>,
    pub input: AscPtr<Bytes>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumEvent<T>
where
    T: AscType,
{
    pub address: AscPtr<AscAddress>,
    pub log_index: AscPtr<AscBigInt>,
    pub transaction_log_index: AscPtr<AscBigInt>,
    pub log_type: AscPtr<AscString>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<T>,
    pub params: AscPtr<AscLogParamArray>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumCall {
    pub address: AscPtr<AscAddress>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<AscEthereumTransaction>,
    pub inputs: AscPtr<AscLogParamArray>,
    pub outputs: AscPtr<AscLogParamArray>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEthereumCall_0_0_3 {
    pub to: AscPtr<AscAddress>,
    pub from: AscPtr<AscAddress>,
    pub block: AscPtr<AscEthereumBlock>,
    pub transaction: AscPtr<AscEthereumTransaction>,
    pub inputs: AscPtr<AscLogParamArray>,
    pub outputs: AscPtr<AscLogParamArray>,
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTypedMapEntry<K, V> {
    pub key: AscPtr<K>,
    pub value: AscPtr<V>,
}

pub(crate) type AscTypedMapEntryArray<K, V> = Array<AscPtr<AscTypedMapEntry<K, V>>>;

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTypedMap<K, V> {
    pub entries: AscPtr<AscTypedMapEntryArray<K, V>>,
}

pub(crate) type AscEntity = AscTypedMap<AscString, AscEnum<StoreValueKind>>;
pub(crate) type AscJson = AscTypedMap<AscString, AscEnum<JsonValueKind>>;

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscUnresolvedContractCall {
    pub contract_name: AscPtr<AscString>,
    pub contract_address: AscPtr<AscAddress>,
    pub function_name: AscPtr<AscString>,
    pub function_args: AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>,
}

#[repr(u32)]
#[derive(AscType, Copy, Clone)]
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

impl AscValue for JsonValueKind {}

impl JsonValueKind {
    pub(crate) fn get_kind(token: &serde_json::Value) -> Self {
        use serde_json::Value;

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

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBigDecimal {
    pub digits: AscPtr<AscBigInt>,

    // Decimal exponent. This is the opposite of `scale` in rust BigDecimal.
    pub exp: AscPtr<AscBigInt>,
}

#[repr(u32)]
pub(crate) enum LogLevel {
    Critical,
    Error,
    Warning,
    Info,
    Debug,
}

impl From<LogLevel> for slog::Level {
    fn from(level: LogLevel) -> slog::Level {
        match level {
            LogLevel::Critical => slog::Level::Critical,
            LogLevel::Error => slog::Level::Error,
            LogLevel::Warning => slog::Level::Warning,
            LogLevel::Info => slog::Level::Info,
            LogLevel::Debug => slog::Level::Debug,
        }
    }
}
