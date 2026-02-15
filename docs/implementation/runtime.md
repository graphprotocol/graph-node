# Runtime

The runtime executes subgraph mapping handlers inside a WebAssembly
sandbox. Subgraph developers write handlers in AssemblyScript that react
to blockchain events (blocks, transactions, log events); the runtime
compiles these into WASM, instantiates them for each trigger, and
collects the resulting entity changes. The main crates involved are
`runtime/wasm` (the WASM engine and host function layer) and
`runtime/derive` (proc macros for Rust-to-AssemblyScript type
conversion).

The key types are `ValidModule` (a compiled, pre-linked WASM module
ready for instantiation), `RuntimeHost` (one per data source, owns the
module and dispatches triggers), `WasmInstance` (a live WASM instance
executing a single handler), and `MappingContext` (per-trigger state
carrying the entity cache, block pointer, and proof-of-indexing
accumulator).

## Module Compilation

When a subgraph is deployed, each WASM binary goes through a
multi-stage processing pipeline before any trigger can run. The raw
bytes are first parsed with `parity_wasm`. The start section, if
present, is extracted and re-exported under the name `gn::start` so
that it can be called explicitly after the AssemblyScript heap is set
up rather than during instantiation. Gas metering instructions are then
injected via `wasm_instrument::gas_metering::inject` using the cost
table in `gas_rules.rs`. The instrumented bytes are compiled into
native code by wasmtime.

All WASM instances in the process share a single global
`wasmtime::Engine`, created once on first use. The engine is configured
with Cranelift, NaN canonicalization for determinism, epoch-based
interruption for timeouts, and a pooling allocator. The pooling
allocator pre-reserves a fixed number of instance slots (controlled by
`GRAPH_WASM_INSTANCE_POOL_SIZE`) so that instantiating a module reuses
a pre-allocated slot rather than performing fresh `mmap`/`munmap` calls
on every trigger.

After compilation, a `wasmtime::Linker` is built and all host functions
are registered into it. The linker is then used to create an
`InstancePre`, which captures the fully-resolved link between the
compiled module and its imports. This `InstancePre` is stored inside
`ValidModule` and reused for every trigger instantiation, avoiding
repeated symbol resolution. The entire pipeline lives in
`ValidModule::new()` in `runtime/wasm/src/mapping.rs`.

## Threading Model

Subgraph runners execute on dedicated OS threads spawned via
`graph::spawn_thread`, not on tokio worker threads. WASM handlers run
directly in the async context of those threads — there is no dedicated
mapping thread and no channel-based handoff between an async task and a
blocking executor. Because the OS threads are separate from the tokio
thread pool, blocking inside a WASM handler does not starve the async
executor that handles networking and database I/O.

Timeout enforcement uses wasmtime's epoch interruption mechanism. A
single global background task increments the engine's epoch counter at
a fixed interval (the configured handler timeout). Each WASM store sets
its deadline to 2 epochs, so the effective timeout for any handler is
between 1x and 2x the configured interval. When the deadline expires,
wasmtime raises a trap that the runtime classifies as a non-deterministic
timeout. The epoch counter is started lazily by `ensure_epoch_counter()`
in `runtime/wasm/src/mapping.rs`.

## Trigger Execution Flow

The path from a blockchain event to a completed handler invocation
proceeds as follows:

1. The block stream delivers a batch of triggers to the subgraph
   runner.
2. The trigger decoder matches each trigger against the data sources
   registered for the subgraph and finds the corresponding
   `RuntimeHost`.
3. `RuntimeHost::process_mapping_trigger` is called, which delegates to
   `run_mapping` in `runtime/wasm/src/host.rs`.
4. `run_mapping` builds a `MappingContext` with a fresh entity cache
   snapshot, then calls
   `WasmInstance::from_valid_module_with_ctx` to instantiate the module
   from the cached `InstancePre`. This is a fast operation because
   symbol resolution was already done at compilation time and the
   pooling allocator provides a pre-reserved slot.
5. The start function (`gn::start`) is executed if present, followed by
   the AssemblyScript `_start` entry point for newer API versions.
6. The trigger data is converted into its AssemblyScript representation
   (via the `AscType`/`AscHeap` traits and the `ToAscPtr`
   implementation for the trigger type), and the handler function named
   in the subgraph manifest is invoked.
7. The handler's entity changes, newly created data sources, and other
   side effects are collected in a `BlockState` and returned to the
   caller.

Handler isolation is enforced through the `enter_handler` /
`exit_handler` protocol on the block state. Before a handler runs,
`enter_handler` takes a snapshot of the entity cache. On success,
`exit_handler` commits the handler's changes. On a deterministic error,
`exit_handler_and_discard_changes_due_to_error` reverts the entity
cache to the snapshot and records the error as a `SubgraphError`. This
ensures that a failing handler never leaves partial writes in the
entity cache. The execution logic lives in `WasmInstance::invoke_handler`
in `runtime/wasm/src/module/instance.rs`.

## Host Functions

Host functions are the bridge between WASM handlers and the Rust
runtime. They fall into two categories: built-in functions and
chain-specific functions.

Built-in functions cover entity storage (`store.get`, `store.set`,
`store.remove`, `store.loadRelated`), IPFS access (`ipfs.cat`,
`ipfs.map`, `ipfs.getBlock`), cryptographic primitives
(`crypto.keccak256`), JSON and YAML parsing, big-number arithmetic
(`bigInt.*`, `bigDecimal.*`), type conversions, data source creation,
logging, and ENS resolution. They are registered in `build_linker()`
via the `link!` macro, which wraps each Rust function into a
`wasmtime::Linker` entry, handling gas accounting, error classification,
and the conversion between WASM values and Rust types.

Chain-specific functions (e.g. `ethereum.call`, `ethereum.encode`) are
provided by the `RuntimeAdapter::host_fns()` trait method. They are
registered through `link_chain_host_fn()`, which looks up the target
function by name at call time from the `MappingContext`'s `host_fns`
map rather than capturing a concrete closure at link time. This allows
different chains to expose different host functions without changing the
linker setup.

All host functions are registered once at module validation time and
baked into the `InstancePre`. The central registration point is
`build_linker()` in `runtime/wasm/src/module/instance.rs`.

Rust-to-AssemblyScript type conversion is handled by the `AscType` and
`AscHeap` traits, with proc macros in `runtime/derive` generating the
boilerplate for mapping between Rust structs and their AssemblyScript
memory layout.

## Gas Metering

Gas metering limits the computational cost of each handler invocation.
Gas-accounting instructions are injected into the WASM bytecode at
compile time by `wasm_instrument`, so every WASM instruction
contributes to the gas count without requiring per-instruction
interpreter hooks at runtime.

The cost table in `runtime/wasm/src/gas_rules.rs` assigns a gas value
to each WASM instruction class. Representative costs: loads cost 1573
gas, stores 2263, arithmetic 25-26, division and remainder 72-82,
direct calls 951, indirect calls 1995, and memory growth 435,000.
These values originate from benchmarks in the Substrate project and are
conservative for wasmtime.

Host functions also consume gas proportional to their work, tracked via
`HostExports::track_gas_and_ops()`. The special `gas` import — the
function called by the injected metering instructions — is registered
as a synchronous `func_wrap` rather than an async closure because it is
invoked tens of thousands of times per handler and must be as cheap as
possible.

Each `WasmInstance` carries its own `GasCounter`. When the counter
exceeds the per-handler limit, a deterministic error is raised and the
handler's changes are reverted.

## Error Handling

Errors during handler execution are classified by their
`DeterminismLevel`, defined in `runtime/wasm/src/error.rs`:

- **Deterministic** errors are reproducible regardless of when or where
  the handler runs. Examples include integer division by zero, memory
  out-of-bounds access, unreachable code, and gas exhaustion. WASM
  traps are classified by `is_trap_deterministic()` in
  `runtime/wasm/src/module/mod.rs`: traps like `MemoryOutOfBounds`,
  `IntegerDivisionByZero`, and `UnreachableCodeReached` are always
  deterministic. When a deterministic error occurs, the handler's
  entity changes are discarded and the error is recorded as a
  `SubgraphError` on the deployment.

- **Non-deterministic** errors depend on external conditions and may
  resolve on retry. Network failures and timeouts (wasmtime's
  `Trap::Interrupt`) fall into this category. These errors bubble up to
  the subgraph runner, which will retry the trigger.

- **PossibleReorg** indicates that the block being processed might not
  be on the canonical chain. The runtime sets a `possible_reorg` flag
  in `WasmInstanceData` when a host function detects this condition.

- **Unimplemented** is a catch-all for errors that have not yet been
  classified. It exists as a transitional category and should be phased
  out over time.

Host functions that encounter errors set the `deterministic_host_trap`
or `possible_reorg` flags on `WasmInstanceData`. After the handler
returns (or traps), `invoke_handler` inspects these flags to determine
how to classify the overall result. Panic safety is provided by
`catch_unwind` around the handler call in `run_mapping`.
