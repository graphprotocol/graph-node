# Rust Subgraph ABI Specification

Status: Draft
Version: 0.0.1
Audience: graph-node maintainers and contributors
Companion PR: [#6462](https://github.com/graphprotocol/graph-node/pull/6462)

This document specifies the binary interface (ABI) used between graph-node and
subgraph mappings compiled from Rust to `wasm32-unknown-unknown`. It is the
authoritative reference for the protocol implemented in
`runtime/wasm/src/rust_abi/`.

It is intentionally written in the style of a protocol specification: it
describes wire formats, function signatures, and host semantics, not user
ergonomics. For an end-user view of how to write Rust subgraphs, see the
Graphite SDK documentation.

---

## 1. Motivation

graph-node currently supports a single mapping language, AssemblyScript (AS),
served by the `asc_abi/` serialization layer. The AS ABI was designed at a
time when AS was the only realistic option for compiling high-level code to
WASM with a managed heap. Several years of operational experience have
exposed structural problems in that ABI that cannot be fixed without breaking
existing subgraphs.

The Rust ABI introduces a parallel serialization layer (`rust_abi/`) that
sits next to `asc_abi/` and is selected by manifest. The runtime (host
exports, store, chain ingestion, gas accounting) is unchanged. Only the
boundary between WASM and the host is replaced.

### 1.1 Problems with the AS ABI

The following issues motivate a new ABI rather than an incremental fix:

1. **No closures, no first-class functions.** AS lacks closures and trait
   objects, which forces the host-language bindings to be hand-written and
   stringly-typed. The `AscPtr<T>` machinery encodes type information in
   Rust generics on the host side and in conventions on the AS side, with no
   compile-time link between them.

2. **Broken nullable handling.** AS represents nullability inconsistently
   (`AscNullableString`, sentinel pointers, separate "is_null" flags
   depending on the type). The host and the mapping disagree about how to
   round-trip a nullable value of certain types, and the only safe path is
   to special-case each one.

3. **Opaque errors.** When deserialization fails on the host side, the
   diagnostic is typically a generic "failed to read AscPtr" with no
   indication of which field or which type the failure originated in.
   Mapping authors get a runtime trap with no actionable context.

4. **Managed heap coupling.** The AS ABI requires the host to call into the
   AS runtime to allocate memory on the mapping's behalf (`__alloc`,
   `__new`, `__pin`, GC roots). This couples graph-node to AS-runtime
   internals: an AS compiler change can break the host without any
   corresponding source change in graph-node.

5. **No versioning story.** `apiVersion` exists in the manifest but
   describes the AS schema layout, not the wire protocol. Adding a new host
   function or changing a field layout requires ad-hoc compatibility code
   scattered across `asc_abi/`.

6. **BigInt endianness and signedness ambiguity.** AS `BigInt` is serialized
   as little-endian unsigned bytes plus a sign flag. Several historical
   bugs have been traced to mismatches between SDK and host on the
   interpretation of these bytes.

### 1.2 Why Rust → WASM is safe to adopt

Rust targeting `wasm32-unknown-unknown` is a battle-tested toolchain. The
Substreams ecosystem already runs Rust modules in production with the same
target triple, the same `wasmtime` runtime version family, and the same
class of host-imported functions used here. There is no novel compiler or
runtime risk in this proposal: the only new surface is the ABI itself.

---

## 2. Design Principles

The Rust ABI is built around the following principles. Each is a direct
response to a specific problem in section 1.1.

1. **Pointer + length, always.** Every variable-length value crossing the
   boundary is passed as `(ptr: u32, len: u32)`. There are no opaque
   handles, no tagged pointers, no implicit headers in front of buffers.
   The host never needs to peek at WASM memory layout to find out how long
   a buffer is.

2. **No managed heap.** The mapping owns its memory. The host never calls
   into a runtime allocator on the mapping's behalf. The mapping exposes
   exactly two memory-management functions: `allocate(size)` and
   `reset_arena()`. A bump allocator with arena reset semantics is
   sufficient and is what the Graphite SDK currently ships.

3. **Explicit TLV serialization.** Structured values (entities, dynamic
   values) use a single, explicit Type-Length-Value format with a closed
   tag table. There is no implicit type information; every value carries
   exactly one byte indicating its kind.

4. **Language detection via manifest.** Selection between `asc_abi` and
   `rust_abi` is driven by `mapping.kind` in `subgraph.yaml`. The runtime
   additionally inspects WASM imports for the `graphite` namespace as a
   defensive cross-check.

5. **Versioned from day one.** `apiVersion` in the manifest names the
   protocol version. Any change to the wire format, host function set,
   or value tag table is a version bump. Section 8 defines what counts as
   breaking.

6. **No ABI in the runtime.** The Rust ABI is a serialization layer. It
   contains no business logic. Adding a new host function means writing
   one impl in `HostExports` (shared with AS) and two thin wrappers (one
   AS, one Rust). The runtime semantics are identical for both languages.

---

## 3. Comparison with the AssemblyScript ABI

The Rust ABI was designed by enumerating mistakes in the AS ABI and
consciously not making them. This section is included so that future
maintainers understand what the design is reacting against.

| Aspect | AS ABI (`asc_abi`) | Rust ABI (`rust_abi`) |
|---|---|---|
| Pointer type | `AscPtr<T>` parameterized by Rust phantom type | `(ptr: u32, len: u32)` |
| Type erasure | Phantom types on host, no enforcement on guest | Closed tag table, checked on both sides |
| Memory ownership | Host allocates into guest heap via `__alloc`/`__new` | Guest owns its heap; host only reads/writes raw bytes |
| Nullable handling | Per-type, inconsistent | Single `Null` value tag (`0x00`) |
| BigInt encoding | LE unsigned bytes + sign flag | `to_signed_bytes_le()` / `from_signed_bytes_le()` |
| String encoding | UTF-16, length-prefixed AS string header | UTF-8 bytes, `(ptr, len)` |
| Versioning | `apiVersion` describes AS class layout | `apiVersion` describes wire protocol |
| Namespace | `index` (for AS host imports) | `graphite` |
| Entry point | AS `_start` runs at instantiation | None; handlers are pure `extern "C"` |
| GC | AS runtime manages roots, requires `__pin`/`__unpin` | None |
| Error reporting | Trap with no field context | Errors carry deserialization position and type |

The single most consequential difference is **memory ownership**. The AS ABI
requires the host to know enough about the AS runtime to call its allocator;
the Rust ABI requires only that the guest exposes `allocate(size: u32) -> u32`.
This decouples graph-node from any particular Rust toolchain version and
makes the protocol portable to other languages with the same property
(e.g., a future C/Zig/Go-via-TinyGo target).

---

## 4. Protocol Specification

This section is normative.

### 4.1 Target Triple

Rust mappings MUST be compiled to `wasm32-unknown-unknown`. WASI is not
supported and will not be linked.

### 4.2 Required Exports

A Rust subgraph WASM module MUST export the following functions in addition
to its handlers:

```text
(func (export "memory")        ;; standard linear memory export
      (memory 1))

(func (export "allocate")
      (param i32) (result i32))

(func (export "reset_arena"))
```

Semantics:

- `memory` is the standard WASM linear memory. The host reads handler input
  arguments out of it and writes host-function results into it.
- `allocate(size: u32) -> u32` reserves at least `size` bytes of contiguous
  memory and returns a pointer to the first byte. The pointer MUST remain
  valid until the next call to `reset_arena`. Implementations are NOT
  required to track or free individual allocations; a bump allocator is
  the reference implementation.
- `reset_arena()` invalidates all pointers previously returned by
  `allocate`. The host calls this once after every handler invocation.
  The mapping MUST NOT call `reset_arena` itself during a handler.

The reference SDK enforces a 4 MiB allocation cap inside `allocate` to
catch runaway recursion and decoder bugs early.

### 4.3 Handler Signature

Every handler exported by the mapping MUST have the following signature:

```rust
#[unsafe(no_mangle)]
pub extern "C" fn handle_xxx(event_ptr: u32, event_len: u32) -> u32;
```

Calling convention:

- `event_ptr` is a pointer into the mapping's linear memory.
- `event_len` is the length, in bytes, of the serialized trigger payload.
- The byte slice `[event_ptr, event_ptr + event_len)` MUST be valid for
  the duration of the call.
- The return value is a status code: `0` indicates success; any non-zero
  value indicates an error. The host treats non-zero returns as a
  deterministic mapping failure for that trigger.

The host invokes handlers as follows (pseudocode):

```text
let bytes = trigger.to_rust_bytes();
let ptr = call("allocate", bytes.len() as u32);
write_memory(ptr, &bytes);
let rc = call(handler_name, ptr, bytes.len() as u32);
call("reset_arena");
if rc != 0 { fail_deterministic(); }
```

The handler is therefore stateless from the runtime's perspective. State
is held only in the host (store, gas, data source list).

### 4.4 Host Import Namespace

All host functions are imported from the `graphite` namespace. The host
links only the functions that the module actually imports; missing
functions are not an error at link time. Calling an unlinked function
traps deterministically.

Detection of a Rust ABI module is performed by scanning the module's
imports for any function imported from `graphite`. This is done in
`is_rust_module()` in `runtime/wasm/src/rust_abi/host.rs` and is used as
a defensive cross-check against the manifest `mapping.kind` field.

### 4.5 Memory Conventions

All multi-byte integers in the wire format are **little-endian**. This
applies to length prefixes, fixed-width integer values inside TLV, and
fixed-layout trigger fields. The single exception is `block.difficulty`,
which is serialized as 32 big-endian bytes because that is its natural
on-chain representation.

All strings are **UTF-8**. The host validates UTF-8 on every string
read; invalid UTF-8 traps with a deterministic error.

Buffer ownership crossing the boundary:

- **Mapping → host**: the mapping passes `(ptr, len)`. The host reads the
  bytes into a host-side `Vec<u8>` (charging gas per byte) and never
  retains the pointer past the host function call.
- **Host → mapping**: the host writes into a buffer the mapping has already
  allocated. The mapping passes `(out_ptr, out_cap)`; the host writes at
  most `out_cap` bytes and returns the actual length used. If the host
  needs to write more than `out_cap` bytes, it returns the sentinel
  `u32::MAX` and writes nothing. The mapping is then expected to grow its
  buffer and retry.

### 4.6 TLV Format for Entity Values

Entity field values are serialized in a Type-Length-Value format. The
grammar is:

```text
EntityData := field_count:u32 Field*
Field      := key_len:u32 key:[u8; key_len] Value
Value      := tag:u8 ValueBody(tag)
```

Note that `key` is a UTF-8 string and is **not** itself a tagged Value;
field keys are always strings.

The `ValueBody(tag)` shape is determined by the tag:

| Tag (hex) | Name | Body |
|---|---|---|
| `0x00` | Null | (none) |
| `0x01` | String | `len:u32` `bytes:[u8; len]` (UTF-8) |
| `0x02` | Int | `i32` little-endian (4 bytes) |
| `0x03` | Int8 | `i64` little-endian (8 bytes) |
| `0x04` | BigInt | `len:u32` `bytes:[u8; len]` (signed two's-complement, little-endian) |
| `0x05` | BigDecimal | `len:u32` `bytes:[u8; len]` (UTF-8 decimal string) |
| `0x06` | Bool | `0x00` (false) or `0x01` (true) (1 byte) |
| `0x07` | Bytes | `len:u32` `bytes:[u8; len]` |
| `0x08` | Address | `bytes:[u8; 20]` (no length prefix) |
| `0x09` | Array | `len:u32` `Value*` (nested, tagged) |

Notes:

- `Address` is a fixed-width specialization of `Bytes`. On the host side
  it is materialized as `Value::Bytes` carrying 20 bytes. The separate
  tag exists so that schema-aware decoders on the SDK side can produce
  `[u8; 20]` directly without a runtime length check.
- `BigInt` uses Rust's `num_bigint::BigInt::to_signed_bytes_le` /
  `from_signed_bytes_le`. A length of zero encodes the number 0.
- `BigDecimal` is serialized as its canonical string representation. This
  trades a few bytes of size for unambiguous round-tripping across two
  independent implementations.
- `Timestamp` (a graph-core internal type) is normalized at serialization
  time to a `BigInt` containing microseconds since the Unix epoch. There
  is no separate `Timestamp` tag.
- The decoder on the host side rejects unknown tags with
  `io::ErrorKind::InvalidData`.

The canonical host-side tag byte values are defined as named constants in
the `tags` module in `runtime/wasm/src/rust_abi/types.rs` (`tags::NULL`,
`tags::STRING`, `tags::INT`, `tags::INT8`, `tags::BIG_INT`,
`tags::BIG_DECIMAL`, `tags::BOOL`, `tags::BYTES`, `tags::ADDRESS`,
`tags::ARRAY`). The `ValueTag` enum discriminants are derived from those
constants, so there is exactly one place in the host codebase where the
on-wire bytes are defined. Any edit to those constants is, by definition,
a breaking ABI change (see section 8.1).

The reference implementation lives in
`runtime/wasm/src/rust_abi/entity.rs`.

#### 4.6.1 Worked Example

The entity `{ id: "tx-1", value: 42, active: true }` serializes to:

```text
03 00 00 00              ; field_count = 3
02 00 00 00 69 64        ; key_len=2, "id"
01 04 00 00 00 74 78 2d 31 ; tag=String, len=4, "tx-1"
05 00 00 00 76 61 6c 75 65 ; key_len=5, "value"
02 2a 00 00 00           ; tag=Int, 42 LE
06 00 00 00 61 63 74 69 76 65 ; key_len=6, "active"
06 01                    ; tag=Bool, true
```

Field order is not specified. Decoders MUST NOT rely on iteration order;
the host implementation uses `HashMap<String, Value>`.

### 4.7 Trigger Serialization

Trigger payloads use a fixed binary layout, not the TLV format. Triggers
are produced by graph-node and consumed by SDK codegen, so both sides know
the schema statically; tagging would be wasted bytes.

The reference implementation lives in
`runtime/wasm/src/rust_abi/trigger.rs`. The chain-specific producers live
under `chain/<chain>/src/trigger.rs` and implement the `ToRustBytes`
trait.

#### 4.7.1 `RustLogTrigger`

Used for Ethereum event log handlers.

```text
address          : [u8; 20]
tx_hash          : [u8; 32]
log_index        : u64    (LE)
block_number     : u64    (LE)
block_timestamp  : u64    (LE, Unix seconds)
topic_count      : u32    (LE)
topics           : [u8; 32] * topic_count
data_len         : u32    (LE)
data             : [u8; data_len]
```

Total fixed prefix: 96 bytes. Then `32 * topic_count + 4 + data_len` bytes
of variable payload.

#### 4.7.2 `RustCallTrigger`

Used for Ethereum call handlers.

```text
to               : [u8; 20]
from             : [u8; 20]
tx_hash          : [u8; 32]
block_number     : u64    (LE)
block_timestamp  : u64    (LE, Unix seconds)
block_hash       : [u8; 32]
input_len        : u32    (LE)
input            : [u8; input_len]
output_len       : u32    (LE)
output           : [u8; output_len]
```

Total fixed prefix: 120 bytes.

#### 4.7.3 `RustBlockTrigger`

Used for Ethereum block handlers.

```text
hash             : [u8; 32]
parent_hash      : [u8; 32]
number           : u64    (LE)
timestamp        : u64    (LE, Unix seconds)
author           : [u8; 20]
gas_used         : u64    (LE)
gas_limit        : u64    (LE)
difficulty       : [u8; 32]   (BIG-endian U256)
base_fee_per_gas : u64    (LE; 0 if pre-EIP-1559)
```

Total: 156 bytes (fixed-width).

#### 4.7.4 Other Chains

Only Ethereum implements `ToRustBytes` at present. NEAR has a stub
returning `unimplemented!()`. Adding a new chain requires only an impl of
`ToRustBytes for <Chain>::MappingTrigger`; no changes to `rust_abi/` are
required.

### 4.8 Error Codes

Handler return values:

| Value | Meaning |
|---|---|
| `0` | Success. The host commits store mutations and proceeds. |
| non-zero | Deterministic error. The host treats this as a failed handler. |

Sentinel return values from host functions that write into a guest buffer:

| Value | Meaning |
|---|---|
| `0` | Not found / null result (e.g., `store_get` for a missing entity, `ethereum_call` revert) |
| `u32::MAX` | Output buffer too small. Guest must grow its buffer and retry. |
| any other | Length, in bytes, of the data written into the guest buffer. |

Panics in the mapping are caught by a panic hook in the SDK and forwarded
to the host via the `abort` import (see section 5.10), which produces a
deterministic trap with the panic message, file, and line number. Decode
errors inside generated handler wrappers are logged via `log_log` before
the handler returns a non-zero status.

---

## 5. Host Function Reference

All functions are imported from the `graphite` namespace. Where a function
is `async` on the host side, the `func_wrap_async` linker variant is used;
the wasmtime fuel meter pauses for the duration of the awaited future.

Gas costs:

- Reading bytes from guest memory costs `GAS_COST_LOAD * len` gas.
- Writing bytes to guest memory costs `GAS_COST_STORE * len` gas.
- `ethereum_call` consumes a flat `5_000_000_000` units of gas in
  addition to the per-byte memory costs.
- The wasmtime fuel meter is configured with a budget of 10 billion
  units per handler invocation. Exceeding this budget produces
  `Trap::OutOfFuel`, which is reported as a deterministic error.

In the signatures below, all integer parameters are `u32` (i32 in WASM
terms) unless noted.

### 5.1 `store_set`

```text
fn store_set(
    entity_type_ptr: u32, entity_type_len: u32,
    id_ptr: u32,          id_len: u32,
    data_ptr: u32,        data_len: u32,
);
```

Stores an entity. `entity_type` and `id` are UTF-8 strings. `data` is a
TLV-encoded `EntityData` map (section 4.6).

The host deserializes the entity data, looks up the schema for
`entity_type` (this is where the runtime applies type checking and
nullability rules), and forwards to `HostExports::store_set`. Errors
during deserialization or schema validation surface as host traps.

Async on the host side because `store_set` may need to await an in-memory
write to the entity cache.

### 5.2 `store_get`

```text
fn store_get(
    entity_type_ptr: u32, entity_type_len: u32,
    id_ptr: u32,          id_len: u32,
    out_ptr: u32,         out_cap: u32,
) -> u32;
```

Returns the TLV-serialized entity data, or:

- `0` if the entity does not exist;
- `u32::MAX` if the entity exists but is larger than `out_cap` (the guest
  is expected to grow `out_cap` and retry; the SDK retries from 16 KiB
  up to 256 KiB before giving up).

Otherwise the return value is the number of bytes written into
`[out_ptr, out_ptr + out_cap)`.

### 5.3 `store_remove`

```text
fn store_remove(
    entity_type_ptr: u32, entity_type_len: u32,
    id_ptr: u32,          id_len: u32,
);
```

Removes an entity by id. No return value. Errors trap.

### 5.4 `crypto_keccak256`

```text
fn crypto_keccak256(
    input_ptr: u32, input_len: u32,
    out_ptr: u32,
);
```

Computes Keccak-256 over `[input_ptr, input_ptr + input_len)` and writes
the 32-byte digest into `[out_ptr, out_ptr + 32)`. The output buffer is
always exactly 32 bytes; no length is returned.

This function is synchronous on the host side (`func_wrap`, not
`func_wrap_async`) because it does no I/O.

### 5.5 `log_log`

```text
fn log_log(
    level: u32,
    message_ptr: u32, message_len: u32,
);
```

Forwards a UTF-8 log message to the host logger. `level` matches the
existing AS log levels: 0 = critical, 1 = error, 2 = warning, 3 = info,
4 = debug. Critical level traps the handler.

### 5.6 `data_source_address`

```text
fn data_source_address(out_ptr: u32);
```

Writes the current data source's contract address (20 bytes) into
`[out_ptr, out_ptr + 20)`. The output is always 20 bytes; no length is
returned.

### 5.7 `data_source_network`

```text
fn data_source_network(out_ptr: u32, out_cap: u32) -> u32;
```

Writes the network name as UTF-8 bytes into the guest buffer. Returns the
number of bytes written, or `u32::MAX` if the buffer is too small.

### 5.8 `data_source_create`

```text
fn data_source_create(
    name_ptr: u32, name_len: u32,
    params_ptr: u32, params_len: u32,
);
```

Spawns a new dynamic data source by template name. `params` is a
TLV-style serialized `Vec<String>`:

```text
count:u32 (str_len:u32 str_bytes:[u8; str_len])*
```

This is the same shape as a TLV `Array` of `String` values, minus the
outer `0x09` array tag and minus the per-element `0x01` string tags
(since the type is statically known).

### 5.9 `ipfs_cat`

```text
fn ipfs_cat(
    hash_ptr: u32, hash_len: u32,
    out_ptr: u32,  out_cap: u32,
) -> u32;
```

Fetches the bytes pinned at the given IPFS hash and writes them into the
guest buffer. Sentinel returns follow the same convention as `store_get`:

- `u32::MAX` if `out_cap` is too small
- otherwise the number of bytes written

If the underlying host fetch returns `HostExportError::PossibleReorg`,
the host marks the instance as `possible_reorg = true` so the block can
be retried, then traps.

### 5.10 `ethereum_call`

```text
fn ethereum_call(
    addr_ptr: u32, addr_len: u32,
    data_ptr: u32, data_len: u32,
    out_ptr: u32,  out_cap: u32,
) -> u32;
```

Performs an `eth_call` against the configured Ethereum endpoint at the
current block. `addr_len` MUST be exactly 20; the host traps otherwise.

Returns:

- `0` if the call reverted;
- `u32::MAX` if the response is larger than `out_cap`;
- otherwise the number of return bytes written into the guest buffer.

Charges a flat `5_000_000_000` gas units in addition to per-byte memory
costs. This matches the cost the AS ABI charges for `ethereum.call`.

`HostExportError::PossibleReorg` is propagated by setting
`possible_reorg = true` on the instance and trapping. This is
indistinguishable from any other host trap from the mapping's perspective.

### 5.11 `abort`

```text
fn abort(
    message_ptr: u32, message_len: u32,
    file_ptr: u32,    file_len: u32,
    line: u32,
);
```

Forwards a panic from the mapping's panic hook to the host. The host
marks the trap as deterministic and surfaces the panic message and line
number in the error chain. The `file` arguments are read by the host but
currently not echoed back into the error (this may change without a
protocol bump; it's a logging detail).

### 5.12 Summary Table

| Function | Returns | Async on host | Notes |
|---|---|---|---|
| `store_set` | (none) | yes | TLV entity payload |
| `store_get` | u32 (len / 0 / u32::MAX) | yes | Buffer-grow protocol |
| `store_remove` | (none) | yes | |
| `crypto_keccak256` | (none) | no | Output is fixed 32 bytes |
| `log_log` | (none) | yes | Critical level traps |
| `data_source_address` | (none) | yes | Output is fixed 20 bytes |
| `data_source_network` | u32 | yes | Buffer-grow protocol |
| `data_source_create` | (none) | yes | Template name + string params |
| `ipfs_cat` | u32 | yes | Buffer-grow protocol; reorg-aware |
| `ethereum_call` | u32 | yes | Reorg-aware; flat 5B gas |
| `abort` | (none, traps) | no | Panic forwarding |

---

## 6. Manifest Format

A Rust subgraph is signalled by the `mapping.kind` field in
`subgraph.yaml`:

```yaml
specVersion: 0.0.5
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: ERC20
    network: mainnet
    source:
      address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
      abi: ERC20
      startBlock: 24756400
    mapping:
      kind: wasm/rust              # NEW: was "wasm/assemblyscript"
      apiVersion: 0.0.1            # Rust ABI version
      language: rust               # NEW
      file: ./target/wasm32-unknown-unknown/release/erc20.wasm
      entities:
        - Transfer
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
      eventHandlers:
        - event: Transfer(indexed address,indexed address,uint256)
          handler: handle_transfer
```

Recognized values for `mapping.kind`:

- `wasm/assemblyscript` → AS ABI (existing behaviour, unchanged)
- `wasm/rust` → Rust ABI (this specification)

The parser is `MappingLanguage::from_kind` in
`runtime/wasm/src/rust_abi/mod.rs`. Unknown values are rejected at
manifest validation time.

The `apiVersion` field for `wasm/rust` modules names the Rust ABI
version, not the AS schema version. The two version namespaces are
independent.

### 6.1 Linker Dispatch

In `build_linker()`, the host dispatches on `MappingLanguage`:

- `AssemblyScript`: link the existing `link_as_exports`/AS host functions,
  populate `AscHeapCtx`, run `_start` after instantiation.
- `Rust`: link `link_rust_host_functions`, skip `id_of_type`, skip
  `_start`, configure wasmtime with fuel metering enabled.

The Rust module path also bypasses the `parity_wasm` gas-injection
pipeline. `parity_wasm` cannot parse modern WASM features (e.g.,
`memory.copy`, opcode 252), and modules emitted by current Rust
toolchains use them. Gas accounting for Rust modules is delivered
entirely by wasmtime fuel metering, not by code injection.

---

## 7. Maintenance Model

This section is non-normative but is included to set expectations about
the cost of maintaining the Rust ABI alongside the AS ABI.

### 7.1 Adding a New Host Function

The runtime business logic for host exports lives in `HostExports`,
which is shared between both ABIs. To add a new host function,
contributors do the following:

1. **Implement once in `HostExports`.** Add the method that does the real
   work (store access, cryptography, network call, etc.). This is shared
   between AS and Rust.
2. **Add an AS wrapper** in `runtime/wasm/src/host_exports.rs` /
   `asc_abi/`. This handles `AscPtr<T>` deserialization, AS-specific
   nullable conventions, and AS gas charging.
3. **Add a Rust wrapper** in `runtime/wasm/src/rust_abi/host.rs`. This
   reads `(ptr, len)` arguments, deserializes via `FromRustWasm`,
   forwards to `HostExports`, and writes any output back via the
   buffer-grow protocol.

The Rust wrapper is typically 20–40 lines and is mechanical to write.
The async vs sync choice is dictated by whether `HostExports` is async
for that operation. There is no heap manipulation, no `AscPtr<T>`
juggling, and no runtime version negotiation.

### 7.2 Maintenance Surface

Explicitly, the maintenance surface for the Rust ABI is **the
serialization layer only**. Specifically:

- `runtime/wasm/src/rust_abi/types.rs` — primitive `ToRustWasm` /
  `FromRustWasm` impls
- `runtime/wasm/src/rust_abi/entity.rs` — TLV format
- `runtime/wasm/src/rust_abi/trigger.rs` — fixed-layout trigger format
- `runtime/wasm/src/rust_abi/host.rs` — linker glue
- `runtime/wasm/src/rust_abi/mod.rs` — `MappingLanguage` enum

Total: ~1,450 lines, of which `host.rs` (linker glue) is roughly a
third. None of this code touches the store, gas accounting, chain
ingestion, or scheduler. A breaking change in graph-node's runtime
internals never requires a Rust ABI change unless it adds a new host
function.

### 7.3 Test Strategy

Three layers of tests cover the Rust ABI:

1. **Unit tests** in each `rust_abi/` file (round-trip serialization for
   each `Value` variant, each trigger type, each primitive). 14 unit
   tests today.
2. **WASM integration test** at `tests/integration/tests/wasm_handler.rs`,
   which loads a real Rust-compiled ERC20 mapping into wasmtime,
   serializes a `RustLogTrigger` using the exact production format,
   invokes `handle_transfer(ptr, len)`, and asserts that the resulting
   `store_set` call carries the expected entity fields.
3. **Live integration test** at `scripts/live-test.sh`, which deploys an
   ERC20 mapping to a running graph-node fork and indexes real USDC
   `Transfer` events from Ethereum mainnet, verifying that GraphQL
   queries return the correct field values.

The unit tests are cheap and run on every commit. The integration tests
require a Rust toolchain with the `wasm32-unknown-unknown` target and a
synchronized graph-node instance respectively.

---

## 8. Versioning

`apiVersion` in the manifest mapping section names the Rust ABI version.
The current version is `0.0.1`.

### 8.1 What Counts as Breaking

Any of the following constitute a breaking change and require an
`apiVersion` bump:

1. Adding, removing, or renumbering a `ValueTag` in the TLV table.
2. Changing the wire layout of any existing `ValueBody`.
3. Changing the byte layout of `RustLogTrigger`, `RustCallTrigger`, or
   `RustBlockTrigger` (field order, size, endianness).
4. Changing the signature (parameter count, parameter types, return
   type) of any host function in the `graphite` namespace.
5. Changing the meaning of a sentinel return value (`0`, `u32::MAX`).
6. Changing the calling convention for handlers (e.g., adding a third
   argument).
7. Changing the required exports (`memory`, `allocate`, `reset_arena`).

### 8.2 What Does Not Count as Breaking

The following are explicitly non-breaking and may be done within a
single `apiVersion`:

1. Adding a new host function (modules that don't import it are
   unaffected; modules that do can detect availability at link time
   because the host only links functions present in the import section).
2. Adding a new chain `ToRustBytes` impl.
3. Changing internal gas costs, as long as the protocol-visible cost
   model (per-byte read/write, flat per-call surcharges) is preserved.
4. Improving error messages.
5. Tightening validation in the host (e.g., rejecting previously
   silently-accepted malformed payloads), provided the change is
   announced and SDK versions are bumped in lockstep.

### 8.3 Compatibility Negotiation

There is no runtime negotiation. The host knows the version it
implements; the manifest declares the version the mapping was built
against; if they don't match, the manifest is rejected at validation
time. There is no support for running multiple Rust ABI versions
simultaneously inside a single graph-node process.

### 8.4 Relationship to AS `apiVersion`

The Rust ABI `apiVersion` is independent of the AS ABI `apiVersion`
namespace. A Rust subgraph at `apiVersion: 0.0.1` and an AS subgraph at
`apiVersion: 0.0.7` describe completely different things. The
`mapping.kind` field disambiguates.

---

## 9. Open Issues

Items that are deliberately left out of `0.0.1` and that future versions
may address:

1. **Shared constant crate.** The `ValueTag` enum and trigger layouts are
   currently defined twice — once in `runtime/wasm/src/rust_abi/` and
   once in the Graphite SDK. A shared `graphite-abi` crate (no_std,
   types-only) would eliminate the drift risk. Tracking this in the
   implementation plan; not a blocker for `0.0.1`.

2. **Test vectors.** This document defines the wire format
   prose-and-table; it does not currently ship a set of binary test
   vectors that both the host and SDK can validate against. The unit
   tests in `rust_abi/` cover round-tripping but not cross-validation.

3. **Offchain triggers.** Offchain (`subgraph` data source) trigger
   serialization is currently stubbed as empty bytes. This is a known
   limitation, not a design decision.

4. **Non-Ethereum chains.** Only Ethereum implements `ToRustBytes`. NEAR
   has a stub. Other chains will require a per-chain serialization impl
   when they want to opt into Rust mappings.

5. **`apiVersion` initial value.** Whether to start at `0.0.1` (own
   namespace) or `0.0.8` (after the latest AS version) is a policy
   question for upstream review. This document assumes `0.0.1`.

---

## 10. References

- Implementation: `runtime/wasm/src/rust_abi/` in this repository
- Companion PR: [#6462](https://github.com/graphprotocol/graph-node/pull/6462)
- Reference SDK: [graphite](https://github.com/cargopete/graphite)
- AS ABI for comparison: `runtime/wasm/src/asc_abi/`
- Host exports (shared between ABIs): `runtime/wasm/src/host_exports.rs`
- Wasmtime fuel metering:
  [`Config::consume_fuel`](https://docs.rs/wasmtime/latest/wasmtime/struct.Config.html#method.consume_fuel)
