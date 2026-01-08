use std::convert::TryFrom;
use std::mem::MaybeUninit;

use anyhow::anyhow;
use anyhow::Error;
use async_trait::async_trait;
use graph::blockchain::Blockchain;
use graph::data_source::subgraph;
use graph::parking_lot::RwLock;
use graph::util::mem::init_slice;
use semver::Version;
use wasmtime::AsContext;
use wasmtime::AsContextMut;
use wasmtime::Memory;

use graph::data_source::{offchain, MappingTrigger, TriggerWithHandler};
use graph::prelude::*;
use graph::runtime::AscPtr;
use graph::runtime::{
    asc_new,
    gas::{Gas, GasCounter},
    AscHeap, AscIndexId, AscType, DeterministicHostError, FromAscObj, HostExportError,
    IndexForAscTypeId,
};
pub use into_wasm_ret::IntoWasmRet;

use crate::error::DeterminismLevel;
use crate::gas_rules::{GAS_COST_LOAD, GAS_COST_STORE};
pub use crate::host_exports;

pub use context::*;
pub use instance::*;
mod context;
mod instance;
mod into_wasm_ret;

// Convenience for a 'top-level' asc_get, with depth 0.
fn asc_get<T, C: AscType, H: AscHeap + ?Sized>(
    heap: &H,
    ptr: AscPtr<C>,
    gas: &GasCounter,
) -> Result<T, DeterministicHostError>
where
    C: AscType + AscIndexId,
    T: FromAscObj<C>,
{
    graph::runtime::asc_get(heap, ptr, gas, 0)
}

pub trait IntoTrap {
    fn determinism_level(&self) -> DeterminismLevel;
    // fn into_trap(self) -> Trap;
}

/// A flexible interface for writing a type to AS memory, any pointer can be returned.
/// Use `AscPtr::erased` to convert `AscPtr<T>` into `AscPtr<()>`.
#[async_trait]
pub trait ToAscPtr {
    async fn to_asc_ptr<H: AscHeap + Send>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError>;
}

#[async_trait]
impl ToAscPtr for offchain::TriggerData {
    async fn to_asc_ptr<H: AscHeap + Send>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        asc_new(heap, self.data.as_ref() as &[u8], gas)
            .await
            .map(|ptr| ptr.erase())
    }
}

#[async_trait]
impl ToAscPtr for subgraph::MappingEntityTrigger {
    async fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        asc_new(heap, &self.data.entity.entity.sorted_ref(), gas)
            .await
            .map(|ptr| ptr.erase())
    }
}

#[async_trait]
impl<C: Blockchain> ToAscPtr for MappingTrigger<C>
where
    C::MappingTrigger: ToAscPtr,
{
    async fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        match self {
            MappingTrigger::Onchain(trigger) => trigger.to_asc_ptr(heap, gas).await,
            MappingTrigger::Offchain(trigger) => trigger.to_asc_ptr(heap, gas).await,
            MappingTrigger::Subgraph(trigger) => trigger.to_asc_ptr(heap, gas).await,
        }
    }
}

#[async_trait]
impl<T: ToAscPtr + Send> ToAscPtr for TriggerWithHandler<T> {
    async fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        self.trigger.to_asc_ptr(heap, gas).await
    }
}

fn is_trap_deterministic(trap: &Error) -> bool {
    let trap = match trap.downcast_ref() {
        Some(trap) => trap,
        None => return false,
    };

    use wasmtime::Trap::*;

    // We try to be exhaustive, even though `TrapCode` is non-exhaustive.
    match trap {
        MemoryOutOfBounds
        | HeapMisaligned
        | TableOutOfBounds
        | IndirectCallToNull
        | BadSignature
        | IntegerOverflow
        | IntegerDivisionByZero
        | BadConversionToInteger
        | UnreachableCodeReached => true,

        // `Interrupt`: Can be a timeout, at least as wasmtime currently implements it.
        // `StackOverflow`: We may want to have a configurable stack size.
        // `None`: A host trap, so we need to check the `deterministic_host_trap` flag in the context.
        Interrupt | StackOverflow | _ => false,
    }
}

struct Arena {
    // First free byte in the current arena. Set on the first call to `raw_new`.
    start: i32,
    // Number of free bytes starting from `arena_start_ptr`.
    size: i32,
}

impl Arena {
    fn new() -> Self {
        Self { start: 0, size: 0 }
    }
}

#[derive(Copy, Clone)]
pub struct ExperimentalFeatures {
    pub allow_non_deterministic_ipfs: bool,
}

pub struct AscHeapCtx {
    // Function wrapper for `idof<T>` from AssemblyScript
    id_of_type: Option<wasmtime::TypedFunc<u32, u32>>,

    // Function exported by the wasm module that will allocate the request number of bytes and
    // return a pointer to the first byte of allocated space.
    memory_allocate: wasmtime::TypedFunc<i32, i32>,

    api_version: semver::Version,

    // In the future there may be multiple memories, but currently there is only one memory per
    // module. And at least AS calls it "memory". There is no uninitialized memory in Wasm, memory
    // is zeroed when initialized or grown.
    memory: Memory,

    arena: RwLock<Arena>,
}

impl AscHeapCtx {
    pub(crate) fn new(
        instance: &wasmtime::Instance,
        ctx: &mut WasmInstanceContext<'_>,
        api_version: Version,
    ) -> anyhow::Result<Arc<AscHeapCtx>> {
        // Provide access to the WASM runtime linear memory
        let memory = instance
            .get_memory(ctx.as_context_mut(), "memory")
            .context("Failed to find memory export in the WASM module")?;

        let memory_allocate = match &api_version {
            version if *version <= Version::new(0, 0, 4) => instance
                .get_func(ctx.as_context_mut(), "memory.allocate")
                .context("`memory.allocate` function not found"),
            _ => instance
                .get_func(ctx.as_context_mut(), "allocate")
                .context("`allocate` function not found"),
        }?
        .typed(ctx.as_context())?
        .clone();

        let id_of_type = match &api_version {
            version if *version <= Version::new(0, 0, 4) => None,
            _ => Some(
                instance
                    .get_func(ctx.as_context_mut(), "id_of_type")
                    .context("`id_of_type` function not found")?
                    .typed(ctx)?
                    .clone(),
            ),
        };

        Ok(Arc::new(AscHeapCtx {
            memory_allocate,
            memory,
            arena: RwLock::new(Arena::new()),
            api_version,
            id_of_type,
        }))
    }

    fn arena_start_ptr(&self) -> i32 {
        self.arena.read().start
    }

    fn arena_free_size(&self) -> i32 {
        self.arena.read().size
    }

    fn set_arena(&self, start_ptr: i32, size: i32) {
        let mut arena = self.arena.write();
        arena.start = start_ptr;
        arena.size = size;
    }

    fn allocated(&self, size: i32) {
        let mut arena = self.arena.write();
        arena.start += size;
        arena.size -= size;
    }
}

fn host_export_error_from_trap(trap: Error, context: String) -> HostExportError {
    let trap_is_deterministic = is_trap_deterministic(&trap);
    let e = trap.context(context);
    match trap_is_deterministic {
        true => HostExportError::Deterministic(e),
        false => HostExportError::Unknown(e),
    }
}

#[async_trait]
impl AscHeap for WasmInstanceContext<'_> {
    async fn raw_new(
        &mut self,
        bytes: &[u8],
        gas: &GasCounter,
    ) -> Result<u32, DeterministicHostError> {
        // The cost of writing to wasm memory from the host is the same as of writing from wasm
        // using load instructions.
        gas.consume_host_fn_with_metrics(
            Gas::new(GAS_COST_STORE as u64 * bytes.len() as u64),
            "raw_new",
        )?;

        // We request large chunks from the AssemblyScript allocator to use as arenas that we
        // manage directly.

        static MIN_ARENA_SIZE: i32 = 10_000;

        let size = i32::try_from(bytes.len()).unwrap();
        if size > self.asc_heap().arena_free_size() {
            // Allocate a new arena. Any free space left in the previous arena is left unused. This
            // causes at most half of memory to be wasted, which is acceptable.
            let mut arena_size = size.max(MIN_ARENA_SIZE);

            // Unwrap: This may panic if more memory needs to be requested from the OS and that
            // fails. This error is not deterministic since it depends on the operating conditions
            // of the node.
            let memory_allocate = &self.asc_heap().cheap_clone().memory_allocate;
            let mut start_ptr = memory_allocate
                .call_async(self.as_context_mut(), arena_size)
                .await
                .unwrap();

            match &self.asc_heap().api_version {
                version if *version <= Version::new(0, 0, 4) => {}
                _ => {
                    // This arithmetic is done because when you call AssemblyScripts's `__alloc`
                    // function, it isn't typed and it just returns `mmInfo` on it's header,
                    // differently from allocating on regular types (`__new` for example).
                    // `mmInfo` has size of 4, and everything allocated on AssemblyScript memory
                    // should have alignment of 16, this means we need to do a 12 offset on these
                    // big chunks of untyped allocation.
                    start_ptr += 12;
                    arena_size -= 12;
                }
            };
            self.asc_heap().set_arena(start_ptr, arena_size);
        };

        let ptr = self.asc_heap().arena_start_ptr() as usize;

        // Unwrap: We have just allocated enough space for `bytes`.
        let memory = self.asc_heap().memory;
        memory.write(self.as_context_mut(), ptr, bytes).unwrap();
        self.asc_heap().allocated(size);

        Ok(ptr as u32)
    }

    fn read_u32(&self, offset: u32, gas: &GasCounter) -> Result<u32, DeterministicHostError> {
        gas.consume_host_fn_with_metrics(Gas::new(GAS_COST_LOAD as u64 * 4), "read_u32")?;
        let mut bytes = [0; 4];
        self.asc_heap()
            .memory
            .read(self, offset as usize, &mut bytes)
            .map_err(|_| {
                DeterministicHostError::from(anyhow!(
                    "Heap access out of bounds. Offset: {} Size: {}",
                    offset,
                    4
                ))
            })?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read<'a>(
        &self,
        offset: u32,
        buffer: &'a mut [MaybeUninit<u8>],
        gas: &GasCounter,
    ) -> Result<&'a mut [u8], DeterministicHostError> {
        // The cost of reading wasm memory from the host is the same as of reading from wasm using
        // load instructions.
        gas.consume_host_fn_with_metrics(
            Gas::new(GAS_COST_LOAD as u64 * (buffer.len() as u64)),
            "read",
        )?;

        let offset = offset as usize;

        // TODO: Do we still need this? Can we use read directly?
        let src = self
            .asc_heap()
            .memory
            .data(self)
            .get(offset..)
            .and_then(|s| s.get(..buffer.len()))
            .ok_or(DeterministicHostError::from(anyhow!(
                "Heap access out of bounds. Offset: {} Size: {}",
                offset,
                buffer.len()
            )))?;

        Ok(init_slice(src, buffer))
    }

    fn api_version(&self) -> &Version {
        &self.asc_heap().api_version
    }

    async fn asc_type_id(
        &mut self,
        type_id_index: IndexForAscTypeId,
    ) -> Result<u32, HostExportError> {
        let asc_heap = self.asc_heap().cheap_clone();
        let func = asc_heap.id_of_type.as_ref().unwrap();

        // Unwrap ok because it's only called on correct apiVersion, look for AscPtr::generate_header
        func.call_async(self.as_context_mut(), type_id_index as u32)
            .await
            .map_err(|trap| {
                host_export_error_from_trap(
                    trap,
                    format!("Failed to call 'asc_type_id' with '{:?}'", type_id_index),
                )
            })
    }
}
