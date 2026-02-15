# WASM ABI Memory Layout Reference

This document describes the binary layout of objects that graph-node
serializes into WASM linear memory when invoking subgraph handler functions.
It covers both the legacy **v0.0.4** layout (AssemblyScript v0.6) and the
current **v0.0.5+** layout (AssemblyScript >= v0.19.2), for core types and
Ethereum chain types.

Source files are referenced relative to the repository root.

## Table of Contents

- [1. Overview](#1-overview)
- [2. Object Headers and Allocation](#2-object-headers-and-allocation)
- [3. Pointer Type: AscPtr](#3-pointer-type-ascptr)
- [4. Primitive Types](#4-primitive-types)
- [5. Core Types](#5-core-types)
  - [5.1 AscString](#51-ascstring)
  - [5.2 ArrayBuffer](#52-arraybuffer)
  - [5.3 TypedArray\<T\>](#53-typedarrayt)
  - [5.4 Array\<T\>](#54-arrayt)
  - [5.5 AscEnum\<D\>](#55-ascenumd)
  - [5.6 EnumPayload](#56-enumpayload)
  - [5.7 AscTypedMapEntry\<K,V\>](#57-asctypedmapentrykvgt)
  - [5.8 AscTypedMap\<K,V\>](#58-asctypedmapkv)
  - [5.9 AscEntity](#59-ascentity)
  - [5.10 AscBigDecimal](#510-ascbigdecimal)
  - [5.11 AscResult\<V,E\>](#511-ascresultve)
  - [5.12 AscWrapped\<V\>](#512-ascwrappedv)
- [6. Ethereum Types](#6-ethereum-types)
  - [6.1 AscEthereumBlock](#61-ascethereumblock)
  - [6.2 AscEthereumTransaction](#62-ascethereumtransaction)
  - [6.3 AscEthereumEvent](#63-ascethereuevent)
  - [6.4 AscLogParam](#64-asclogparam)
  - [6.5 AscEthereumLog](#65-ascethereumlog)
  - [6.6 AscEthereumTransactionReceipt](#66-ascethereumtransactionreceipt)
  - [6.7 AscEthereumCall](#67-ascethereumcall)
  - [6.8 AscUnresolvedContractCall](#68-ascunresolvedcontractcall)
- [7. API Version Compatibility Matrix](#7-api-version-compatibility-matrix)
- [8. Allocation Pattern](#8-allocation-pattern)

---

## 1. Overview

When a subgraph handler is invoked, graph-node must convert Rust-side
trigger data (blocks, transactions, events, etc.) into objects in WASM
linear memory that AssemblyScript code can read. This serialization crosses
the **ABI boundary** between graph-node and subgraph code.

### Key Traits

| Trait | Role | Defined in |
|-------|------|------------|
| `AscType` | Defines `to_asc_bytes()` / `from_asc_bytes()` for a type's memory layout | `graph/src/runtime/mod.rs` |
| `AscIndexId` | Provides the `rt_id` used in v0.0.5+ object headers (`INDEX_ASC_TYPE_ID`) | `graph/src/runtime/mod.rs` |
| `ToAscObj<C>` | Converts a Rust value into its Asc representation `C` | `graph/src/runtime/asc_heap.rs` |
| `FromAscObj<C>` | Converts an Asc representation back to Rust | `graph/src/runtime/asc_heap.rs` |
| `AscHeap` | Interface to the WASM linear memory (read/write/allocate) | `graph/src/runtime/asc_heap.rs` |

### Handler Invocation Flow

```
Rust trigger data
       |
       v
  ToAscObj::to_asc_obj()     -- recursively converts nested objects
       |
       v
  AscType::to_asc_bytes()    -- serializes each object to bytes
       |
       v
  AscPtr::alloc_obj()        -- writes bytes into WASM linear memory
       |                         (adds header in v0.0.5+)
       v
  Single u32 pointer arg      -- passed to the WASM handler function
```

Allocation is **bottom-up**: leaf objects (strings, byte arrays, BigInts)
are allocated first, producing `AscPtr` values that are then embedded in
parent structs. The final top-level struct pointer is passed to the WASM
handler as a `u32` argument.

---

## 2. Object Headers and Allocation

### v0.0.4 (AssemblyScript v0.6)

No object header. Variable-length types (strings, array buffers) carry
**inline length prefixes** as part of their serialized content.

Allocation writes `to_asc_bytes()` directly via `raw_new()`, and the
returned heap offset is the `AscPtr`.

### v0.0.5+ (AssemblyScript >= v0.19.2)

Every object is preceded by a **20-byte header**. The `AscPtr` points past
the header to the start of the content.

```
                  AscPtr points here
                        |
                        v
+----------+----------+----------+-------+---------+---------+---------+
| mm_info  | gc_info  | gc_info2 | rt_id | rt_size | content | padding |
|  4 bytes |  4 bytes |  4 bytes | 4 B   |  4 B    | N bytes | 0-15 B  |
+----------+----------+----------+-------+---------+---------+---------+
|<---------- 20-byte header ----------->|
```

| Field | Size | Description |
|-------|------|-------------|
| `mm_info` | 4B LE u32 | `16 + full_length` where `full_length = content + alignment_padding` |
| `gc_info` | 4B LE u32 | Always 0 (GC not used by graph-node) |
| `gc_info2` | 4B LE u32 | Always 0 (GC not used by graph-node) |
| `rt_id` | 4B LE u32 | Class identifier from `IndexForAscTypeId` (equivalent to AS `idof<T>`) |
| `rt_size` | 4B LE u32 | Content length in bytes (returned by `content_len()`) |

**16-byte alignment padding** is appended after the content:

```
padding = (16 - (20 + content_length) % 16) % 16
```

The total allocation is `20 + content_length + padding` bytes.

Reference: `graph/src/runtime/mod.rs` (lines 409-413),
`graph/src/runtime/asc_ptr.rs` (lines 87-123, 145-170)

---

## 3. Pointer Type: AscPtr

```
+--------+
| offset |   4 bytes, little-endian u32
+--------+
```

- **Size**: 4 bytes
- **Encoding**: Little-endian `u32`
- **Null**: Value `0` represents a null pointer
- **v0.0.4**: Points to the first byte of serialized content
- **v0.0.5+**: Points to the first byte of content (header is at negative
  offsets; `rt_size` is at `ptr - 4`)

An `AscPtr<C>` is itself an `AscValue` and can be embedded in other
`#[repr(C)]` structs. It is also used as the `EnumPayload` representation
for pointer-carrying enum variants.

Reference: `graph/src/runtime/asc_ptr.rs`

---

## 4. Primitive Types

All primitives use little-endian encoding. Size equals alignment.

| Rust Type | Size | Encoding |
|-----------|------|----------|
| `u8` | 1 byte | LE |
| `u16` | 2 bytes | LE |
| `u32` | 4 bytes | LE |
| `u64` | 8 bytes | LE |
| `i8` | 1 byte | LE |
| `i32` | 4 bytes | LE |
| `i64` | 8 bytes | LE |
| `f32` | 4 bytes | LE IEEE 754 |
| `f64` | 8 bytes | LE IEEE 754 |
| `bool` | 1 byte | `0` = false, nonzero = true |

Reference: `graph/src/runtime/mod.rs` (lines 94-144)

---

## 5. Core Types

### 5.1 AscString

UTF-16LE encoded string.

**v0.0.4** (`runtime/wasm/src/asc_abi/v0_0_4.rs`):

```
+--------+-----------------------------------+
| length | UTF-16LE code units               |
| 4 B LE |  length * 2 bytes                 |
+--------+-----------------------------------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `length` | 4B | Number of UTF-16 code units (u32 LE) |
| 4 | content | `length * 2` B | UTF-16LE encoded characters |

`asc_size = 4 + length * 2`

**v0.0.5+** (`runtime/wasm/src/asc_abi/v0_0_5.rs`):

```
+-----------------------------------+------------------+
| UTF-16LE code units               | power-of-2 pad   |
|  rt_size bytes                    |                   |
+-----------------------------------+------------------+
```

No inline length prefix. The string length comes from the header's
`rt_size` field (which stores `code_unit_count * 2`, the byte length of
the UTF-16LE data). Extra padding is added to reach the next power of two
of `(byte_count + 20)`.

**`INDEX_ASC_TYPE_ID`**: `String = 0`

### 5.2 ArrayBuffer

Raw binary data buffer.

**v0.0.4** (`runtime/wasm/src/asc_abi/v0_0_4.rs`):

```
+-------------+---------+---------+----------------+
| byte_length | padding | content | power-of-2 pad |
|   4 B LE    |  4 B    | N bytes |                |
+-------------+---------+---------+----------------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `byte_length` | 4B | Content length in bytes (u32 LE) |
| 4 | padding | 4B | Zero padding for 8-byte alignment |
| 8 | content | N B | Raw data bytes |
| 8+N | padding | variable | Zeroes to reach next power of two of `(8 + N)` |

`asc_size = 4 + 4 + byte_length`

**v0.0.5+** (`runtime/wasm/src/asc_abi/v0_0_5.rs`):

```
+---------+----------------+
| content | power-of-2 pad |
| N bytes |                |
+---------+----------------+
```

No inline length. The `rt_size` header field contains the byte length.
Extra padding is added to reach the next power of two of `(N + 20)`.

**`INDEX_ASC_TYPE_ID`**: `ArrayBuffer = 1`

### 5.3 TypedArray\<T\>

A typed view over an `ArrayBuffer`. Subtypes: `Uint8Array` (`TypedArray<u8>`),
`Int32Array` (`TypedArray<i32>`), etc.

Type aliases:
- `AscBigInt = Uint8Array = TypedArray<u8>` (signed number bytes, LE)
- `AscAddress = Uint8Array` (20 bytes)
- `AscH160 = Uint8Array` (20 bytes)

**v0.0.4** (`runtime/wasm/src/asc_abi/v0_0_4.rs`):

```
+--------+-------------+-------------+
| buffer | byte_offset | byte_length |
| 4 B    |   4 B LE    |   4 B LE    |
+--------+-------------+-------------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `buffer` | 4B | `AscPtr<ArrayBuffer>` |
| 4 | `byte_offset` | 4B | Byte position in buffer where data starts (u32 LE) |
| 8 | `byte_length` | 4B | Length of the data in bytes (u32 LE) |

Total: **12 bytes** (PhantomData is zero-sized)

**v0.0.5+** (`runtime/wasm/src/asc_abi/v0_0_5.rs`):

```
+--------+------------+-------------+
| buffer | data_start | byte_length |
| 4 B    |   4 B LE   |   4 B LE    |
+--------+------------+-------------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `buffer` | 4B | `AscPtr<ArrayBuffer>` |
| 4 | `data_start` | 4B | **Absolute** address in WASM memory where data starts (u32 LE) |
| 8 | `byte_length` | 4B | Length of the data in bytes (u32 LE) |

Total: **12 bytes**

The key difference: v0.0.4 uses a relative `byte_offset` within the buffer,
while v0.0.5+ uses an absolute `data_start` address. When reading back, the
offset is computed as `data_start - buffer.wasm_ptr()`.

**`INDEX_ASC_TYPE_ID`** (selected by element type):

| Element Type | ID |
|---|---|
| `i8` | `Int8Array = 2` |
| `i16` | `Int16Array = 3` |
| `i32` | `Int32Array = 4` |
| `i64` | `Int64Array = 5` |
| `u8` | `Uint8Array = 6` |
| `u16` | `Uint16Array = 7` |
| `u32` | `Uint32Array = 8` |
| `u64` | `Uint64Array = 9` |
| `f32` | `Float32Array = 10` |
| `f64` | `Float64Array = 11` |

### 5.4 Array\<T\>

Growable array backed by an `ArrayBuffer`.

**v0.0.4** (`runtime/wasm/src/asc_abi/v0_0_4.rs`):

```
+--------+--------+
| buffer | length |
| 4 B    | 4 B LE |
+--------+--------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `buffer` | 4B | `AscPtr<ArrayBuffer>` containing the elements |
| 4 | `length` | 4B | Number of elements (u32 LE) |

Total: **8 bytes**

**v0.0.5+** (`runtime/wasm/src/asc_abi/v0_0_5.rs`):

```
+--------+-------------------+--------------------+--------+
| buffer | buffer_data_start | buffer_data_length | length |
| 4 B    |      4 B LE       |      4 B LE        | 4 B LE |
+--------+-------------------+--------------------+--------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `buffer` | 4B | `AscPtr<ArrayBuffer>` |
| 4 | `buffer_data_start` | 4B | Absolute address where data starts (u32 LE) |
| 8 | `buffer_data_length` | 4B | Length of backing data in bytes (u32 LE) |
| 12 | `length` | 4B | Number of elements (i32 LE) |

Total: **16 bytes**

Note that `length` is `i32` in v0.0.5+ (signed) versus `u32` in v0.0.4.

**`INDEX_ASC_TYPE_ID`** (selected by element type):

| Element Type | ID |
|---|---|
| `bool` | `ArrayBool = 13` |
| `Uint8Array` | `ArrayUint8Array = 14` |
| `AscPtr<AscEnum<EthereumValueKind>>` | `ArrayEthereumValue = 15` |
| `AscPtr<AscEnum<StoreValueKind>>` | `ArrayStoreValue = 16` |
| `AscPtr<AscEnum<JsonValueKind>>` | `ArrayJsonValue = 17` |
| `AscPtr<AscString>` | `ArrayString = 18` |
| `AscPtr<AscLogParam>` | `ArrayEventParam = 19` |
| `u8` | `ArrayU8 = 41` |
| `u16` | `ArrayU16 = 42` |
| `u32` | `ArrayU32 = 43` |
| `u64` | `ArrayU64 = 44` |
| `i8` | `ArrayI8 = 45` |
| `i16` | `ArrayI16 = 46` |
| `i32` | `ArrayI32 = 47` |
| `i64` | `ArrayI64 = 48` |
| `f32` | `ArrayF32 = 49` |
| `f64` | `ArrayF64 = 50` |
| `AscPtr<AscBigDecimal>` | `ArrayBigDecimal = 51` |

### 5.5 AscEnum\<D\>

Discriminated union used for `EthereumValue`, `StoreValue`, `JsonValue`, etc.

```
+------+----------+---------+
| kind | _padding | payload |
| 4 B  |   4 B    |  8 B    |
+------+----------+---------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `kind` | 4B | Discriminant enum (u32 LE) |
| 4 | `_padding` | 4B | Explicit padding (always 0) |
| 8 | `payload` | 8B | `EnumPayload` (u64 LE), interpretation depends on `kind` |

Total: **16 bytes** (same layout for both v0.0.4 and v0.0.5+)

The `_padding` field is explicit to satisfy `#[repr(C)]` alignment
requirements (the `u64` payload requires 8-byte alignment).

**Discriminant values for `EthereumValueKind`** (`INDEX_ASC_TYPE_ID = EthereumValue = 30`):

| Discriminant | Kind | Payload |
|---|---|---|
| 0 | `Address` | `AscPtr<AscAddress>` (20 bytes) |
| 1 | `FixedBytes` | `AscPtr<Uint8Array>` |
| 2 | `Bytes` | `AscPtr<Uint8Array>` |
| 3 | `Int` | `AscPtr<AscBigInt>` (signed LE bytes) |
| 4 | `Uint` | `AscPtr<AscBigInt>` (unsigned LE bytes) |
| 5 | `Bool` | `0` or `1` (direct u64 value) |
| 6 | `String` | `AscPtr<AscString>` |
| 7 | `FixedArray` | `AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>` |
| 8 | `Array` | `AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>` |
| 9 | `Tuple` | `AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>` |
| 10 | `Function` | `AscPtr<Uint8Array>` (24 bytes) |

**Discriminant values for `StoreValueKind`** (`INDEX_ASC_TYPE_ID = StoreValue = 31`):

| Discriminant | Kind | Payload |
|---|---|---|
| 0 | `String` | `AscPtr<AscString>` |
| 1 | `Int` | `i32` value (sign-extended in u64) |
| 2 | `BigDecimal` | `AscPtr<AscBigDecimal>` |
| 3 | `Bool` | `0` or `1` (direct u64 value) |
| 4 | `Array` | `AscPtr<Array<AscPtr<AscEnum<StoreValueKind>>>>` |
| 5 | `Null` | `0` (unused) |
| 6 | `Bytes` | `AscPtr<Uint8Array>` |
| 7 | `BigInt` | `AscPtr<Uint8Array>` (signed LE bytes) |
| 8 | `Int8` | `i64` value (direct in u64) |
| 9 | `Timestamp` | `i64` value (microseconds since epoch, direct in u64) |

**Discriminant values for `JsonValueKind`** (`INDEX_ASC_TYPE_ID = JsonValue = 32`):

| Discriminant | Kind | Payload |
|---|---|---|
| 0 | `Null` | `0` (unused) |
| 1 | `Bool` | `0` or `1` (direct u64 value) |
| 2 | `Number` | `AscPtr<AscString>` (number as string) |
| 3 | `String` | `AscPtr<AscString>` |
| 4 | `Array` | `AscPtr<Array<AscPtr<AscEnum<JsonValueKind>>>>` |
| 5 | `Object` | `AscPtr<AscTypedMap<AscString, AscEnum<JsonValueKind>>>` |

Reference: `runtime/wasm/src/asc_abi/class.rs` (lines 499-563),
`runtime/wasm/src/to_from/external.rs`

### 5.6 EnumPayload

Untyped 8-byte value that can hold a scalar or a pointer.

```
+---------+
| value   |
|  8 B LE |
+---------+
```

- **Size**: 8 bytes (u64 LE)
- Holds either a scalar (`i32`, `i64`, `f64`, `bool` zero-extended to u64)
  or an `AscPtr` (lower 32 bits contain the pointer, upper 32 bits are zero)

Reference: `runtime/wasm/src/asc_abi/class.rs` (lines 416-497)

### 5.7 AscTypedMapEntry\<K,V\>

A key-value pair. Used as the element type of `AscTypedMap` entries arrays.

```
+-----+-------+
| key | value |
| 4 B |  4 B  |
+-----+-------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `key` | 4B | `AscPtr<K>` |
| 4 | `value` | 4B | `AscPtr<V>` |

Total: **8 bytes**

**`INDEX_ASC_TYPE_ID`**:

| Parameterization | ID |
|---|---|
| `<AscString, AscEnum<StoreValueKind>>` | `TypedMapEntryStringStoreValue = 34` |
| `<AscString, AscEnum<JsonValueKind>>` | `TypedMapEntryStringJsonValue = 35` |

Reference: `runtime/wasm/src/asc_abi/class.rs` (lines 613-632)

### 5.8 AscTypedMap\<K,V\>

An ordered map backed by an array of entries.

```
+---------+
| entries |
|   4 B   |
+---------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `entries` | 4B | `AscPtr<Array<AscPtr<AscTypedMapEntry<K,V>>>>` |

Total: **4 bytes**

**`INDEX_ASC_TYPE_ID`**:

| Parameterization | ID |
|---|---|
| `<AscString, AscEnum<StoreValueKind>>` | `TypedMapStringStoreValue = 36` |
| `<AscString, AscEnum<JsonValueKind>>` | `TypedMapStringJsonValue = 37` |

Reference: `runtime/wasm/src/asc_abi/class.rs` (lines 634-659)

### 5.9 AscEntity

Type alias: `AscTypedMap<AscString, AscEnum<StoreValueKind>>`

An entity is a typed map where keys are strings and values are store values.
Its full object graph looks like:

```
AscEntity (4B)
  |
  +-> entries: Array<AscPtr<AscTypedMapEntry<AscString, AscEnum<StoreValueKind>>>>
        |
        +-> ArrayBuffer containing AscPtr elements
              |
              +-> AscTypedMapEntry (8B each)
                    |
                    +-> key: AscString (UTF-16LE)
                    +-> value: AscEnum<StoreValueKind> (16B)
                                |
                                +-> payload may point to:
                                      AscString, AscBigDecimal,
                                      Uint8Array, Array<...>, etc.
```

### 5.10 AscBigDecimal

```
+--------+-----+
| digits | exp |
|  4 B   | 4 B |
+--------+-----+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `digits` | 4B | `AscPtr<AscBigInt>` (the significand as signed LE bytes) |
| 4 | `exp` | 4B | `AscPtr<AscBigInt>` (the exponent as signed LE bytes) |

Total: **8 bytes**

**Sign flip on exponent**: The Rust `BigDecimal::as_bigint_and_exponent()`
returns a *negative* exponent (called `scale`). Graph-node **negates** this
before serializing: `exp = -negative_exp`. So a Rust BigDecimal with scale
3 (meaning `digits * 10^-3`) is stored with `exp = 3`.

**`INDEX_ASC_TYPE_ID`**: `BigDecimal = 12`

Reference: `runtime/wasm/src/asc_abi/class.rs` (lines 693-704),
`runtime/wasm/src/to_from/external.rs` (lines 91-138)

### 5.11 AscResult\<V,E\>

```
+-------+-------+
| value | error |
|  4 B  |  4 B  |
+-------+-------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `value` | 4B | `AscPtr<AscWrapped<V>>` (null if error) |
| 4 | `error` | 4B | `AscPtr<AscWrapped<E>>` (null if success) |

Total: **8 bytes**

Exactly one of `value`/`error` is non-null.

**`INDEX_ASC_TYPE_ID`**:

| Parameterization | ID |
|---|---|
| `<AscPtr<AscJson>, bool>` | `ResultTypedMapStringJsonValueBool = 39` |
| `<AscPtr<AscEnum<JsonValueKind>>, bool>` | `ResultJsonValueBool = 40` |

Reference: `runtime/wasm/src/asc_abi/class.rs` (lines 727-745)

### 5.12 AscWrapped\<V\>

A simple wrapper holding a single `AscValue`.

```
+-------+
| inner |
| siz V |
+-------+
```

| Offset | Field | Size | Description |
|--------|-------|------|-------------|
| 0 | `inner` | `size_of(V)` | The wrapped value |

Total: **size_of(V)** (e.g., 4B for `AscPtr`, 1B for `bool`)

**`INDEX_ASC_TYPE_ID`**:

| Parameterization | ID |
|---|---|
| `AscPtr<AscJson>` | `WrappedTypedMapStringJsonValue = 27` |
| `bool` | `WrappedBool = 28` |
| `AscPtr<AscEnum<JsonValueKind>>` | `WrappedJsonValue = 29` |

Reference: `runtime/wasm/src/asc_abi/class.rs` (lines 747-767)

---

## 6. Ethereum Types

All Ethereum Asc structs are `#[repr(C)]` and consist entirely of `AscPtr`
fields (4 bytes each). Their layouts are identical across v0.0.4 and v0.0.5+
(only the header/allocation mechanism differs).

Reference: `chain/ethereum/src/runtime/abi.rs`

### 6.1 AscEthereumBlock

**`AscEthereumBlock`** (API versions < 0.0.6):

```
+------+-------------+------------+--------+------------+-------------------+
| hash | parent_hash | uncles_hash| author | state_root | transactions_root |
| 4 B  |    4 B      |    4 B     |  4 B   |    4 B     |       4 B         |
+------+-------------+------------+--------+------------+-------------------+
+---------------+--------+----------+-----------+-----------+------------+
| receipts_root | number | gas_used | gas_limit | timestamp | difficulty |
|     4 B       |  4 B   |   4 B    |    4 B    |    4 B    |    4 B     |
+---------------+--------+----------+-----------+-----------+------------+
+------------------+------+
| total_difficulty | size |
|       4 B        | 4 B  |
+------------------+------+
```

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `hash` | 4B | `AscPtr<Uint8Array>` (B256, 32 bytes) |
| 4 | `parent_hash` | 4B | `AscPtr<Uint8Array>` (B256) |
| 8 | `uncles_hash` | 4B | `AscPtr<Uint8Array>` (B256) |
| 12 | `author` | 4B | `AscPtr<Uint8Array>` (Address, 20 bytes) |
| 16 | `state_root` | 4B | `AscPtr<Uint8Array>` (B256) |
| 20 | `transactions_root` | 4B | `AscPtr<Uint8Array>` (B256) |
| 24 | `receipts_root` | 4B | `AscPtr<Uint8Array>` (B256) |
| 28 | `number` | 4B | `AscPtr<AscBigInt>` |
| 32 | `gas_used` | 4B | `AscPtr<AscBigInt>` |
| 36 | `gas_limit` | 4B | `AscPtr<AscBigInt>` |
| 40 | `timestamp` | 4B | `AscPtr<AscBigInt>` |
| 44 | `difficulty` | 4B | `AscPtr<AscBigInt>` |
| 48 | `total_difficulty` | 4B | `AscPtr<AscBigInt>` |
| 52 | `size` | 4B | `AscPtr<AscBigInt>` (nullable) |

Total: **56 bytes** (14 pointers)

**`AscEthereumBlock_0_0_6`** (API versions >= 0.0.6):

Same as above with one additional field:

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 56 | `base_fee_per_block` | 4B | `AscPtr<AscBigInt>` (nullable) |

Total: **60 bytes** (15 pointers)

**`INDEX_ASC_TYPE_ID`**: `EthereumBlock = 25`

### 6.2 AscEthereumTransaction

Three variants exist for different API versions.

**`AscEthereumTransaction_0_0_1`** (API versions < 0.0.2):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `hash` | 4B | `AscPtr<Uint8Array>` (B256) |
| 4 | `index` | 4B | `AscPtr<AscBigInt>` |
| 8 | `from` | 4B | `AscPtr<Uint8Array>` (Address) |
| 12 | `to` | 4B | `AscPtr<Uint8Array>` (Address, nullable) |
| 16 | `value` | 4B | `AscPtr<AscBigInt>` |
| 20 | `gas_limit` | 4B | `AscPtr<AscBigInt>` |
| 24 | `gas_price` | 4B | `AscPtr<AscBigInt>` |

Total: **28 bytes** (7 pointers)

**`AscEthereumTransaction_0_0_2`** (API versions >= 0.0.2, < 0.0.6):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0-24 | *(same as 0.0.1)* | 28B | |
| 28 | `input` | 4B | `AscPtr<Uint8Array>` |

Total: **32 bytes** (8 pointers)

**`AscEthereumTransaction_0_0_6`** (API versions >= 0.0.6):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0-28 | *(same as 0.0.2)* | 32B | |
| 32 | `nonce` | 4B | `AscPtr<AscBigInt>` |

Total: **36 bytes** (9 pointers)

**`INDEX_ASC_TYPE_ID`**: `EthereumTransaction = 24`

### 6.3 AscEthereumEvent

**`AscEthereumEvent<T, B>`** (API versions < 0.0.7):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `address` | 4B | `AscPtr<AscAddress>` |
| 4 | `log_index` | 4B | `AscPtr<AscBigInt>` |
| 8 | `transaction_log_index` | 4B | `AscPtr<AscBigInt>` |
| 12 | `log_type` | 4B | `AscPtr<AscString>` (nullable) |
| 16 | `block` | 4B | `AscPtr<B>` |
| 20 | `transaction` | 4B | `AscPtr<T>` |
| 24 | `params` | 4B | `AscPtr<AscLogParamArray>` |

Total: **28 bytes** (7 pointers)

The type parameters `T` and `B` are selected based on API version (see
[compatibility matrix](#7-api-version-compatibility-matrix)).

**`AscEthereumEvent_0_0_7<T, B>`** (API versions >= 0.0.7):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0-24 | *(same as above)* | 28B | |
| 28 | `receipt` | 4B | `AscPtr<AscEthereumTransactionReceipt>` (nullable) |

Total: **32 bytes** (8 pointers)

**`INDEX_ASC_TYPE_ID`**: `EthereumEvent = 33`

### 6.4 AscLogParam

A single decoded event parameter (name + value).

```
+------+-------+
| name | value |
| 4 B  |  4 B  |
+------+-------+
```

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `name` | 4B | `AscPtr<AscString>` |
| 4 | `value` | 4B | `AscPtr<AscEnum<EthereumValueKind>>` |

Total: **8 bytes** (2 pointers)

**`INDEX_ASC_TYPE_ID`**: `EventParam = 23`

### 6.5 AscEthereumLog

Raw Ethereum log (used inside transaction receipts in API >= 0.0.7).

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `address` | 4B | `AscPtr<AscAddress>` |
| 4 | `topics` | 4B | `AscPtr<AscTopicArray>` (Array of B256) |
| 8 | `data` | 4B | `AscPtr<Uint8Array>` |
| 12 | `block_hash` | 4B | `AscPtr<Uint8Array>` (B256, nullable) |
| 16 | `block_number` | 4B | `AscPtr<AscBigInt>` (nullable) |
| 20 | `transaction_hash` | 4B | `AscPtr<Uint8Array>` (B256, nullable) |
| 24 | `transaction_index` | 4B | `AscPtr<AscBigInt>` (nullable) |
| 28 | `log_index` | 4B | `AscPtr<AscBigInt>` (nullable) |
| 32 | `transaction_log_index` | 4B | `AscPtr<AscBigInt>` (always null) |
| 36 | `log_type` | 4B | `AscPtr<AscString>` (always null) |
| 40 | `removed` | 4B | `AscPtr<AscWrapped<bool>>` |

Total: **44 bytes** (11 pointers)

**`INDEX_ASC_TYPE_ID`**: `Log = 1001`

### 6.6 AscEthereumTransactionReceipt

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `transaction_hash` | 4B | `AscPtr<Uint8Array>` (B256) |
| 4 | `transaction_index` | 4B | `AscPtr<AscBigInt>` |
| 8 | `block_hash` | 4B | `AscPtr<Uint8Array>` (B256, nullable) |
| 12 | `block_number` | 4B | `AscPtr<AscBigInt>` (nullable) |
| 16 | `cumulative_gas_used` | 4B | `AscPtr<AscBigInt>` |
| 20 | `gas_used` | 4B | `AscPtr<AscBigInt>` |
| 24 | `contract_address` | 4B | `AscPtr<AscAddress>` (nullable) |
| 28 | `logs` | 4B | `AscPtr<AscLogArray>` (Array of AscEthereumLog) |
| 32 | `status` | 4B | `AscPtr<AscBigInt>` (nullable, pre-Byzantium) |
| 36 | `root` | 4B | `AscPtr<Uint8Array>` (B256, nullable) |
| 40 | `logs_bloom` | 4B | `AscPtr<Uint8Array>` (256 bytes) |

Total: **44 bytes** (11 pointers)

**`INDEX_ASC_TYPE_ID`**: `TransactionReceipt = 1000`

### 6.7 AscEthereumCall

**`AscEthereumCall`** (API versions < 0.0.3):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `address` | 4B | `AscPtr<AscAddress>` (call target) |
| 4 | `block` | 4B | `AscPtr<AscEthereumBlock>` |
| 8 | `transaction` | 4B | `AscPtr<AscEthereumTransaction_0_0_1>` |
| 12 | `inputs` | 4B | `AscPtr<AscLogParamArray>` |
| 16 | `outputs` | 4B | `AscPtr<AscLogParamArray>` |

Total: **20 bytes** (5 pointers)

**`AscEthereumCall_0_0_3<T, B>`** (API versions >= 0.0.3):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `to` | 4B | `AscPtr<AscAddress>` |
| 4 | `from` | 4B | `AscPtr<AscAddress>` |
| 8 | `block` | 4B | `AscPtr<B>` |
| 12 | `transaction` | 4B | `AscPtr<T>` |
| 16 | `inputs` | 4B | `AscPtr<AscLogParamArray>` |
| 20 | `outputs` | 4B | `AscPtr<AscLogParamArray>` |

Total: **24 bytes** (6 pointers)

Added `from` address and renamed `address` to `to`.

**`INDEX_ASC_TYPE_ID`**: `EthereumCall = 26`

### 6.8 AscUnresolvedContractCall

Used for `eth_call` from subgraph code (read direction: WASM -> Rust).

**`AscUnresolvedContractCall_0_0_4`** (API versions <= 0.0.4):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `contract_name` | 4B | `AscPtr<AscString>` |
| 4 | `contract_address` | 4B | `AscPtr<AscAddress>` |
| 8 | `function_name` | 4B | `AscPtr<AscString>` |
| 12 | `function_signature` | 4B | `AscPtr<AscString>` |
| 16 | `function_args` | 4B | `AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>` |

Total: **20 bytes** (5 pointers)

**`AscUnresolvedContractCall`** (API versions > 0.0.4):

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | `contract_name` | 4B | `AscPtr<AscString>` |
| 4 | `contract_address` | 4B | `AscPtr<AscAddress>` |
| 8 | `function_name` | 4B | `AscPtr<AscString>` |
| 12 | `function_args` | 4B | `AscPtr<Array<AscPtr<AscEnum<EthereumValueKind>>>>` |

Total: **16 bytes** (4 pointers)

The `function_signature` field was removed; function resolution uses name only.

**`INDEX_ASC_TYPE_ID`**: `SmartContractCall = 22` (only for the `_0_0_4` variant)

---

## 7. API Version Compatibility Matrix

The API version determines which struct variants are used when serializing
triggers into WASM memory.

### Event Handlers

| API Version | Transaction Struct | Block Struct | Event Struct |
|---|---|---|---|
| < 0.0.2 | `AscEthereumTransaction_0_0_1` (28B) | `AscEthereumBlock` (56B) | `AscEthereumEvent` (28B) |
| >= 0.0.2, < 0.0.6 | `AscEthereumTransaction_0_0_2` (32B) | `AscEthereumBlock` (56B) | `AscEthereumEvent` (28B) |
| >= 0.0.6, < 0.0.7 | `AscEthereumTransaction_0_0_6` (36B) | `AscEthereumBlock_0_0_6` (60B) | `AscEthereumEvent` (28B) |
| >= 0.0.7 | `AscEthereumTransaction_0_0_6` (36B) | `AscEthereumBlock_0_0_6` (60B) | `AscEthereumEvent_0_0_7` (32B) |

### Call Handlers

| API Version | Transaction Struct | Block Struct | Call Struct |
|---|---|---|---|
| < 0.0.3 | `AscEthereumTransaction_0_0_1` (28B) | `AscEthereumBlock` (56B) | `AscEthereumCall` (20B) |
| >= 0.0.3, < 0.0.6 | `AscEthereumTransaction_0_0_2` (32B) | `AscEthereumBlock` (56B) | `AscEthereumCall_0_0_3` (24B) |
| >= 0.0.6 | `AscEthereumTransaction_0_0_6` (36B) | `AscEthereumBlock_0_0_6` (60B) | `AscEthereumCall_0_0_3` (24B) |

### Block Handlers

| API Version | Block Struct |
|---|---|
| < 0.0.6 | `AscEthereumBlock` (56B) |
| >= 0.0.6 | `AscEthereumBlock_0_0_6` (60B) |

### Memory Layout Version

| API Version | Layout |
|---|---|
| <= 0.0.4 | v0.0.4 (no header, inline length prefixes) |
| >= 0.0.5 | v0.0.5+ (20-byte header, 16-byte aligned) |

Reference: `chain/ethereum/src/trigger.rs` (lines 127-230)

---

## 8. Allocation Pattern

### Bottom-Up Allocation

Objects are allocated leaves-first. The `asc_new` function calls
`to_asc_obj` which recursively allocates child objects before constructing
the parent. Each `asc_new` call produces an `AscPtr` that is embedded in
the parent struct.

### Arena Allocator

Graph-node uses an arena allocator for WASM heap allocations. The AS
allocator provides a bulk memory region, and graph-node's `raw_new` performs
bump-pointer sub-allocation within it.

### Example: Ethereum Event Handler Allocation Sequence

For an event with one `uint256` parameter, the allocation sequence is
(simplified, showing just the types allocated bottom-up):

```
1.  AscString          "paramName"         -- log param name
2.  AscBigInt          <uint256 bytes>     -- the uint256 value
3.  AscEnum            EthereumValueKind   -- wraps the BigInt as Uint
4.  AscLogParam        (name, value)       -- references #1 and #3
5.  ArrayBuffer        [AscPtr to #4]      -- backing buffer for params array
6.  Array              params              -- references #5
7.  Uint8Array (x7)    hash, from, to...   -- transaction field byte arrays
8.  AscBigInt (x5)     index, value...     -- transaction field big ints
9.  AscEthereumTx      transaction         -- references #7, #8
10. Uint8Array (x7)    hash, parent...     -- block field byte arrays
11. AscBigInt (x6)     number, gas...      -- block field big ints
12. AscEthereumBlock   block               -- references #10, #11
13. Uint8Array         event address       -- 20-byte address
14. AscBigInt          log_index           -- event log index
15. AscBigInt          tx_log_index        -- transaction log index
16. AscEthereumEvent   event               -- references all above
                       ^
                       |
              Pointer passed to WASM handler as u32 argument
```

Each numbered step is a separate `raw_new` call that bumps the heap pointer.
The final event pointer (#16) is the single `u32` argument to the WASM
handler function.
