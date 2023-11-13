use std::convert::TryFrom;
use std::mem::MaybeUninit;

use crate::blockchain::Blockchain;
use crate::util::mem::init_slice;
use anyhow::anyhow;
use anyhow::Error;
use semver::Version;
use wasmtime::StoreContext;
use wasmtime::StoreContextMut;
use wasmtime::{AsContextMut, Memory};

use crate::data_source::{offchain, MappingTrigger, TriggerWithHandler};
use crate::prelude::*;
use crate::runtime::AscPtr;
use crate::runtime::{
    asc_new,
    gas::{Gas, GasCounter},
    AscHeap, AscIndexId, AscType, DeterministicHostError, FromAscObj, HostExportError,
    IndexForAscTypeId,
};
pub use into_wasm_ret::IntoWasmRet;
pub use stopwatch::TimeoutStopwatch;

use crate::runtime::error::DeterminismLevel;
pub use crate::runtime::host_exports;
use crate::runtime::wasm::gas_rules::{GAS_COST_LOAD, GAS_COST_STORE};

pub use context::*;
pub use instance::*;
mod context;
mod instance;
mod into_wasm_ret;
pub mod stopwatch;

// Convenience for a 'top-level' asc_get, with depth 0.
fn asc_get<T, C: AscType, H: AscHeap + ?Sized>(
    store: &StoreContext<WasmInstanceContext>,
    heap: &H,
    ptr: AscPtr<C>,
    gas: &GasCounter,
) -> Result<T, DeterministicHostError>
where
    C: AscType + AscIndexId,
    T: FromAscObj<C>,
{
    crate::runtime::asc_get(&store, heap, ptr, gas, 0)
}

pub trait IntoTrap {
    fn determinism_level(&self) -> DeterminismLevel;
    // fn into_trap(self) -> Trap;
}

/// A flexible interface for writing a type to AS memory, any pointer can be returned.
/// Use `AscPtr::erased` to convert `AscPtr<T>` into `AscPtr<()>`.
pub trait ToAscPtr {
    fn to_asc_ptr<H: AscHeap>(
        self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError>;
}

impl ToAscPtr for offchain::TriggerData {
    fn to_asc_ptr<H: AscHeap>(
        self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        asc_new(store, heap, self.data.as_ref() as &[u8], gas).map(|ptr| ptr.erase())
    }
}

impl<C: Blockchain> ToAscPtr for MappingTrigger<C>
where
    C::MappingTrigger: ToAscPtr,
{
    fn to_asc_ptr<H: AscHeap>(
        self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        match self {
            MappingTrigger::Onchain(trigger) => trigger.to_asc_ptr(store, heap, gas),
            MappingTrigger::Offchain(trigger) => trigger.to_asc_ptr(store, heap, gas),
        }
    }
}

impl<T: ToAscPtr> ToAscPtr for TriggerWithHandler<T> {
    fn to_asc_ptr<H: AscHeap>(
        self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, HostExportError> {
        self.trigger.to_asc_ptr(store, heap, gas)
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

    // store: &'a wasmtime::Store<WasmInstanceContext<C>>,

    // In the future there may be multiple memories, but currently there is only one memory per
    // module. And at least AS calls it "memory". There is no uninitialized memory in Wasm, memory
    // is zeroed when initialized or grown.
    memory: Memory,

    // First free byte in the current arena. Set on the first call to `raw_new`.
    arena_start_ptr: i32,

    // Number of free bytes starting from `arena_start_ptr`.
    arena_free_size: i32,
}

impl AscHeapCtx {
    pub fn new(
        instance: &wasmtime::Instance,
        store: &mut StoreContextMut<WasmInstanceContext>,
        api_version: Version,
    ) -> anyhow::Result<AscHeapCtx> {
        // Provide access to the WASM runtime linear memory
        let memory = instance
            .get_memory(store.as_context_mut(), "memory")
            .context("Failed to find memory export in the WASM module")?;

        let memory_allocate = match &api_version {
            version if *version <= Version::new(0, 0, 4) => instance
                .get_func(store.as_context_mut(), "memory.allocate")
                .context("`memory.allocate` function not found"),
            _ => instance
                .get_func(store.as_context_mut(), "allocate")
                .context("`allocate` function not found"),
        }?
        .typed(store.as_context_mut())?
        .clone();

        let id_of_type = match &api_version {
            version if *version <= Version::new(0, 0, 4) => None,
            _ => Some(
                instance
                    .get_func(store.as_context_mut(), "id_of_type")
                    .context("`id_of_type` function not found")?
                    .typed(store.as_context_mut())?
                    .clone(),
            ),
        };

        Ok(AscHeapCtx {
            memory_allocate,
            memory,
            arena_start_ptr: 0,
            arena_free_size: 0,
            api_version,
            id_of_type,
        })
    }
}

fn host_export_error_from_trap(trap: Error, context: String) -> HostExportError {
    let trap_is_deterministic = is_trap_deterministic(&trap);
    let e = Error::from(trap).context(context);
    match trap_is_deterministic {
        true => HostExportError::Deterministic(e),
        false => HostExportError::Unknown(e),
    }
}

impl AscHeap for AscHeapCtx {
    fn raw_new(
        &mut self,
        store: &mut StoreContextMut<WasmInstanceContext>,
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
        if size > self.arena_free_size {
            // Allocate a new arena. Any free space left in the previous arena is left unused. This
            // causes at most half of memory to be wasted, which is acceptable.
            let arena_size = size.max(MIN_ARENA_SIZE);

            // Unwrap: This may panic if more memory needs to be requested from the OS and that
            // fails. This error is not deterministic since it depends on the operating conditions
            // of the node.
            self.arena_start_ptr = self
                .memory_allocate
                .call(store.as_context_mut(), arena_size)
                .unwrap();
            self.arena_free_size = arena_size;

            match &self.api_version {
                version if *version <= Version::new(0, 0, 4) => {}
                _ => {
                    // This arithmetic is done because when you call AssemblyScripts's `__alloc`
                    // function, it isn't typed and it just returns `mmInfo` on it's header,
                    // differently from allocating on regular types (`__new` for example).
                    // `mmInfo` has size of 4, and everything allocated on AssemblyScript memory
                    // should have alignment of 16, this means we need to do a 12 offset on these
                    // big chunks of untyped allocation.
                    self.arena_start_ptr += 12;
                    self.arena_free_size -= 12;
                }
            };
        };

        let ptr = self.arena_start_ptr as usize;

        // Unwrap: We have just allocated enough space for `bytes`.
        self.memory
            .write(store.as_context_mut(), ptr, bytes)
            .unwrap();
        self.arena_start_ptr += size;
        self.arena_free_size -= size;

        Ok(ptr as u32)
    }

    fn read_u32(
        &self,
        store: &StoreContext<WasmInstanceContext>,
        offset: u32,
        gas: &GasCounter,
    ) -> Result<u32, DeterministicHostError> {
        gas.consume_host_fn_with_metrics(Gas::new(GAS_COST_LOAD as u64 * 4), "read_u32")?;
        let mut bytes = [0; 4];
        self.memory
            .read(store, offset as usize, &mut bytes)
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
        store: &StoreContext<WasmInstanceContext>,
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
            .memory
            .data(store)
            .get(offset..)
            .and_then(|s| s.get(..buffer.len()))
            .ok_or(DeterministicHostError::from(anyhow!(
                "Heap access out of bounds. Offset: {} Size: {}",
                offset,
                buffer.len()
            )))?;

        Ok(init_slice(src, buffer))
    }

    fn api_version(&self, _store: &StoreContext<WasmInstanceContext>) -> Version {
        self.api_version.clone()
    }

    fn asc_type_id(
        &self,
        store: &mut StoreContextMut<WasmInstanceContext>,
        type_id_index: IndexForAscTypeId,
    ) -> Result<u32, HostExportError> {
        self.id_of_type
            .as_ref()
            .unwrap() // Unwrap ok because it's only called on correct apiVersion, look for AscPtr::generate_header
            .call(store.as_context_mut(), type_id_index as u32)
            .map_err(|trap| {
                host_export_error_from_trap(
                    trap,
                    format!("Failed to call 'asc_type_id' with '{:?}'", type_id_index),
                )
            })
    }
}
