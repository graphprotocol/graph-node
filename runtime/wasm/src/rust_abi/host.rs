//! Rust ABI host function bindings.
//!
//! Links host functions for Rust WASM modules using the `graphite` namespace.
//! These use ptr+len calling convention instead of AS's AscPtr.

use std::collections::BTreeMap;

use super::entity::deserialize_entity_data;
use crate::gas_rules::{GAS_COST_LOAD, GAS_COST_STORE};
use crate::module::{WasmInstanceContext, WasmInstanceData};
use graph::prelude::*;
use graph::runtime::gas::Gas;
use wasmtime::{AsContext, AsContextMut, Caller, Linker, Memory};

/// Read bytes from WASM memory with gas metering.
fn read_bytes_with_gas(
    memory: &Memory,
    store: impl AsContext,
    ptr: u32,
    len: u32,
    gas: &graph::runtime::gas::GasCounter,
) -> Result<Vec<u8>, anyhow::Error> {
    // Charge gas for memory read
    gas.consume_host_fn_with_metrics(
        Gas::new(GAS_COST_LOAD as u64 * len as u64),
        "rust_abi_read",
    )?;

    let data = memory.data(&store);
    let start = ptr as usize;
    let end = start + len as usize;

    if end > data.len() {
        anyhow::bail!(
            "memory access out of bounds: {}..{} (memory size: {})",
            start,
            end,
            data.len()
        );
    }

    Ok(data[start..end].to_vec())
}

/// Read a string from WASM memory.
fn read_string_with_gas(
    memory: &Memory,
    store: impl AsContext,
    ptr: u32,
    len: u32,
    gas: &graph::runtime::gas::GasCounter,
) -> Result<String, anyhow::Error> {
    let bytes = read_bytes_with_gas(memory, store, ptr, len, gas)?;
    String::from_utf8(bytes).map_err(|e| anyhow::anyhow!("invalid UTF-8: {}", e))
}

/// Write bytes to WASM memory with gas metering.
fn write_bytes_with_gas(
    memory: &Memory,
    mut store: impl AsContextMut,
    ptr: u32,
    data: &[u8],
    gas: &graph::runtime::gas::GasCounter,
) -> Result<(), anyhow::Error> {
    // Charge gas for memory write
    gas.consume_host_fn_with_metrics(
        Gas::new(GAS_COST_STORE as u64 * data.len() as u64),
        "rust_abi_write",
    )?;

    let mem_data = memory.data_mut(&mut store);
    let start = ptr as usize;
    let end = start + data.len();

    if end > mem_data.len() {
        anyhow::bail!(
            "memory access out of bounds: {}..{} (memory size: {})",
            start,
            end,
            mem_data.len()
        );
    }

    mem_data[start..end].copy_from_slice(data);
    Ok(())
}

/// Get memory from a WASM instance.
fn get_memory(caller: &mut Caller<'_, WasmInstanceData>) -> Result<Memory, anyhow::Error> {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| anyhow::anyhow!("failed to get WASM memory export"))
}

/// Link Rust ABI host functions to a wasmtime Linker.
///
/// This registers all host functions in the `graphite` namespace
/// with ptr+len calling convention. Only links functions that the
/// module actually imports.
pub fn link_rust_host_functions(
    linker: &mut Linker<WasmInstanceData>,
    import_name_to_modules: &BTreeMap<String, Vec<String>>,
) -> anyhow::Result<()> {
    // Helper to check if a function is imported from graphite namespace
    let is_graphite_import = |name: &str| -> bool {
        import_name_to_modules
            .get(name)
            .map(|modules| modules.iter().any(|m| m == "graphite"))
            .unwrap_or(false)
    };

    // ========== Store Operations ==========

    if is_graphite_import("store_set") {
        linker.func_wrap_async(
            "graphite",
            "store_set",
            |mut caller: Caller<'_, WasmInstanceData>,
             (entity_type_ptr, entity_type_len, id_ptr, id_len, data_ptr, data_len): (
                u32,
                u32,
                u32,
                u32,
                u32,
                u32,
            )| {
                Box::new(async move {
                    let memory = get_memory(&mut caller)?;
                    let gas = caller.data().gas.cheap_clone();

                    let entity_type =
                        read_string_with_gas(&memory, &caller, entity_type_ptr, entity_type_len, &gas)?;
                    let id = read_string_with_gas(&memory, &caller, id_ptr, id_len, &gas)?;
                    let data_bytes =
                        read_bytes_with_gas(&memory, &caller, data_ptr, data_len, &gas)?;

                    let entity_data = deserialize_entity_data(&data_bytes)
                        .map_err(|e| anyhow::anyhow!("failed to deserialize entity: {}", e))?;

                    // Call the actual store_set through WasmInstanceContext
                    let mut ctx = std::pin::pin!(WasmInstanceContext::new(&mut caller));
                    ctx.rust_store_set(&gas, &entity_type, &id, entity_data)
                        .await?;

                    Ok(())
                })
            },
        )?;
    }

    if is_graphite_import("store_get") {
        linker.func_wrap_async(
            "graphite",
            "store_get",
            |mut caller: Caller<'_, WasmInstanceData>,
             (entity_type_ptr, entity_type_len, id_ptr, id_len, out_ptr, out_cap): (
                u32,
                u32,
                u32,
                u32,
                u32,
                u32,
            )| {
                Box::new(async move {
                    let memory = get_memory(&mut caller)?;
                    let gas = caller.data().gas.cheap_clone();

                    let entity_type =
                        read_string_with_gas(&memory, &caller, entity_type_ptr, entity_type_len, &gas)?;
                    let id = read_string_with_gas(&memory, &caller, id_ptr, id_len, &gas)?;

                    // Call the actual store_get through WasmInstanceContext
                    let mut ctx = std::pin::pin!(WasmInstanceContext::new(&mut caller));
                    let result = ctx.rust_store_get(&gas, &entity_type, &id).await?;

                    match result {
                        Some(bytes) => {
                            if bytes.len() > out_cap as usize {
                                // Buffer too small, return required size as error indicator
                                Ok(u32::MAX)
                            } else {
                                let memory = get_memory(&mut caller)?;
                                write_bytes_with_gas(&memory, &mut caller, out_ptr, &bytes, &gas)?;
                                Ok(bytes.len() as u32)
                            }
                        }
                        None => Ok(0), // Not found
                    }
                })
            },
        )?;
    }

    if is_graphite_import("store_remove") {
        linker.func_wrap_async(
            "graphite",
            "store_remove",
            |mut caller: Caller<'_, WasmInstanceData>,
             (entity_type_ptr, entity_type_len, id_ptr, id_len): (u32, u32, u32, u32)| {
                Box::new(async move {
                    let memory = get_memory(&mut caller)?;
                    let gas = caller.data().gas.cheap_clone();

                    let entity_type =
                        read_string_with_gas(&memory, &caller, entity_type_ptr, entity_type_len, &gas)?;
                    let id = read_string_with_gas(&memory, &caller, id_ptr, id_len, &gas)?;

                    let mut ctx = std::pin::pin!(WasmInstanceContext::new(&mut caller));
                    ctx.rust_store_remove(&gas, &entity_type, &id).await?;

                    Ok(())
                })
            },
        )?;
    }

    // ========== Crypto Operations ==========

    if is_graphite_import("crypto_keccak256") {
        linker.func_wrap(
            "graphite",
            "crypto_keccak256",
            |mut caller: Caller<'_, WasmInstanceData>,
             input_ptr: u32,
             input_len: u32,
             out_ptr: u32|
             -> anyhow::Result<()> {
                let memory = get_memory(&mut caller)?;
                let gas = caller.data().gas.cheap_clone();

                let input = read_bytes_with_gas(&memory, &caller, input_ptr, input_len, &gas)?;
                let hash = tiny_keccak::keccak256(&input);

                write_bytes_with_gas(&memory, &mut caller, out_ptr, &hash, &gas)?;
                Ok(())
            },
        )?;
    }

    // ========== Logging ==========

    if is_graphite_import("log_log") {
        linker.func_wrap_async(
            "graphite",
            "log_log",
            |mut caller: Caller<'_, WasmInstanceData>,
             (level, message_ptr, message_len): (u32, u32, u32)| {
                Box::new(async move {
                    let memory = get_memory(&mut caller)?;
                    let gas = caller.data().gas.cheap_clone();

                    let message =
                        read_string_with_gas(&memory, &caller, message_ptr, message_len, &gas)?;

                    let mut ctx = std::pin::pin!(WasmInstanceContext::new(&mut caller));
                    ctx.rust_log(&gas, level, &message).await?;

                    Ok(())
                })
            },
        )?;
    }

    // ========== Data Source Operations ==========

    if is_graphite_import("data_source_address") {
        linker.func_wrap_async(
            "graphite",
            "data_source_address",
            |mut caller: Caller<'_, WasmInstanceData>, (out_ptr,): (u32,)| {
                Box::new(async move {
                    let gas = caller.data().gas.cheap_clone();

                    let mut ctx = std::pin::pin!(WasmInstanceContext::new(&mut caller));
                    let address = ctx.rust_data_source_address(&gas).await?;

                    let memory = get_memory(&mut caller)?;
                    write_bytes_with_gas(&memory, &mut caller, out_ptr, &address, &gas)?;
                    Ok(())
                })
            },
        )?;
    }

    if is_graphite_import("data_source_network") {
        linker.func_wrap_async(
            "graphite",
            "data_source_network",
            |mut caller: Caller<'_, WasmInstanceData>, (out_ptr, out_cap): (u32, u32)| {
                Box::new(async move {
                    let gas = caller.data().gas.cheap_clone();

                    let mut ctx = std::pin::pin!(WasmInstanceContext::new(&mut caller));
                    let network = ctx.rust_data_source_network(&gas).await?;

                    let bytes = network.as_bytes();
                    if bytes.len() > out_cap as usize {
                        return Ok(u32::MAX);
                    }

                    let memory = get_memory(&mut caller)?;
                    write_bytes_with_gas(&memory, &mut caller, out_ptr, bytes, &gas)?;
                    Ok(bytes.len() as u32)
                })
            },
        )?;
    }

    // ========== Abort ==========

    if is_graphite_import("abort") {
        linker.func_wrap(
            "graphite",
            "abort",
            |mut caller: Caller<'_, WasmInstanceData>,
             message_ptr: u32,
             message_len: u32,
             _file_ptr: u32,
             _file_len: u32,
             line: u32|
             -> anyhow::Result<()> {
                let memory = get_memory(&mut caller)?;
                let gas = caller.data().gas.cheap_clone();

                let message = read_string_with_gas(&memory, &caller, message_ptr, message_len, &gas)
                    .unwrap_or_else(|_| "<failed to read message>".to_string());

                // Mark as deterministic trap
                caller.data_mut().deterministic_host_trap = true;

                Err(anyhow::anyhow!("abort at line {}: {}", line, message))
            },
        )?;
    }

    Ok(())
}

/// Check if a WASM module uses Rust ABI by looking for graphite namespace imports.
pub fn is_rust_module(import_name_to_modules: &BTreeMap<String, Vec<String>>) -> bool {
    import_name_to_modules
        .values()
        .any(|modules| modules.iter().any(|m| m == "graphite"))
}
