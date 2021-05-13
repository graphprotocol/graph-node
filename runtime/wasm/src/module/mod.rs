use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Deref;
use std::rc::Rc;
use std::time::Instant;

use never::Never;
use semver::Version;
use wasmtime::{Memory, Trap};

use crate::error::DeterminismLevel;
use crate::host_exports;
use crate::mapping::MappingContext;
use anyhow::Error;
use ethabi::LogParam;
use graph::prelude::*;
use graph::{components::subgraph::MappingError, runtime::AscPtr};
use graph::{data::store, runtime::AscHeap};
use graph::{data::subgraph::schema::SubgraphError, runtime::DeterministicHostError};
use host_exports::HostExportError;
use web3::types::{Log, Transaction, U256};

use crate::asc_abi::class::*;
use crate::host_exports::{EthereumCallError, HostExports};
use crate::mapping::ValidModule;
use crate::UnresolvedContractCall;

mod into_wasm_ret;
mod stopwatch;

use into_wasm_ret::IntoWasmRet;
use stopwatch::TimeoutStopwatch;

#[cfg(test)]
mod test;

const TRAP_TIMEOUT: &str = "trap: interrupt";

pub trait IntoTrap {
    fn determinism_level(&self) -> DeterminismLevel;
    fn into_trap(self) -> Trap;
}

/// Handle to a WASM instance, which is terminated if and only if this is dropped.
pub(crate) struct WasmInstance {
    instance: wasmtime::Instance,

    // This is the only reference to `WasmInstanceContext` that's not within the instance itself, so
    // we can always borrow the `RefCell` with no concern for race conditions.
    //
    // Also this is the only strong reference, so the instance will be dropped once this is dropped.
    // The weak references are circulary held by instance itself through host exports.
    instance_ctx: Rc<RefCell<Option<WasmInstanceContext>>>,
}

impl Drop for WasmInstance {
    fn drop(&mut self) {
        // Assert that the instance will be dropped.
        assert_eq!(Rc::strong_count(&self.instance_ctx), 1);
    }
}

/// Proxies to the WasmInstanceContext.
impl AscHeap for WasmInstance {
    fn raw_new(&mut self, bytes: &[u8]) -> Result<u32, DeterministicHostError> {
        let mut ctx = RefMut::map(self.instance_ctx.borrow_mut(), |i| i.as_mut().unwrap());
        ctx.raw_new(bytes)
    }

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, DeterministicHostError> {
        self.instance_ctx().get(offset, size)
    }

    fn api_version(&self) -> Version {
        self.instance_ctx().api_version()
    }
}

impl WasmInstance {
    pub(crate) fn handle_json_callback(
        mut self,
        handler_name: &str,
        value: &serde_json::Value,
        user_data: &store::Value,
    ) -> Result<BlockState, anyhow::Error> {
        let value = self.asc_new(value)?;
        let user_data = self.asc_new(user_data)?;

        self.instance_ctx_mut().ctx.state.enter_handler();

        // Invoke the callback
        self.instance
            .get_func(handler_name)
            .with_context(|| format!("function {} not found", handler_name))?
            .typed()?
            .call((value.wasm_ptr(), user_data.wasm_ptr()))
            .with_context(|| format!("Failed to handle callback '{}'", handler_name))?;

        self.instance_ctx_mut().ctx.state.exit_handler();

        Ok(self.take_ctx().ctx.state)
    }

    pub(crate) fn handle_ethereum_log(
        mut self,
        block: Arc<LightEthereumBlock>,
        handler_name: &str,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
    ) -> Result<BlockState, MappingError> {
        // Prepare an EthereumEvent for the WASM runtime
        // Decide on the destination type using the mapping
        // api version provided in the subgraph manifest
        let event = if self.instance_ctx().ctx.host_exports.api_version >= Version::new(0, 0, 2) {
            self.asc_new::<AscEthereumEvent<AscEthereumTransaction_0_0_2>, _>(&EthereumEventData {
                block: EthereumBlockData::from(block.as_ref()),
                transaction: EthereumTransactionData::from(transaction.deref()),
                address: log.address,
                log_index: log.log_index.unwrap_or(U256::zero()),
                transaction_log_index: log.log_index.unwrap_or(U256::zero()),
                log_type: log.log_type.clone(),
                params,
            })?
            .erase()
        } else {
            self.asc_new::<AscEthereumEvent<AscEthereumTransaction>, _>(&EthereumEventData {
                block: EthereumBlockData::from(block.as_ref()),
                transaction: EthereumTransactionData::from(transaction.deref()),
                address: log.address,
                log_index: log.log_index.unwrap_or(U256::zero()),
                transaction_log_index: log.log_index.unwrap_or(U256::zero()),
                log_type: log.log_type.clone(),
                params,
            })?
            .erase()
        };

        self.invoke_handler(handler_name, event)
    }

    pub(crate) fn handle_ethereum_call(
        mut self,
        block: Arc<LightEthereumBlock>,
        handler_name: &str,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
    ) -> Result<BlockState, MappingError> {
        let call = EthereumCallData {
            to: call.to,
            from: call.from,
            block: EthereumBlockData::from(block.as_ref()),
            transaction: EthereumTransactionData::from(transaction.deref()),
            inputs,
            outputs,
        };
        let arg = if self.instance_ctx().ctx.host_exports.api_version >= Version::new(0, 0, 3) {
            self.asc_new::<AscEthereumCall_0_0_3, _>(&call)?.erase()
        } else {
            self.asc_new::<AscEthereumCall, _>(&call)?.erase()
        };

        self.invoke_handler(handler_name, arg)
    }

    pub(crate) fn handle_ethereum_block(
        mut self,
        block: Arc<LightEthereumBlock>,
        handler_name: &str,
    ) -> Result<BlockState, MappingError> {
        let block = EthereumBlockData::from(block.as_ref());

        // Prepare an EthereumBlock for the WASM runtime
        let arg = self.asc_new(&block)?;

        self.invoke_handler(handler_name, arg)
    }

    pub(crate) fn take_ctx(&mut self) -> WasmInstanceContext {
        self.instance_ctx.borrow_mut().take().unwrap()
    }

    pub(crate) fn instance_ctx(&self) -> std::cell::Ref<'_, WasmInstanceContext> {
        std::cell::Ref::map(self.instance_ctx.borrow(), |i| i.as_ref().unwrap())
    }

    pub(crate) fn instance_ctx_mut(&self) -> std::cell::RefMut<'_, WasmInstanceContext> {
        std::cell::RefMut::map(self.instance_ctx.borrow_mut(), |i| i.as_mut().unwrap())
    }

    #[cfg(test)]
    pub(crate) fn get_func(&self, func_name: &str) -> wasmtime::Func {
        self.instance.get_func(func_name).unwrap()
    }

    fn invoke_handler<C>(
        &mut self,
        handler: &str,
        arg: AscPtr<C>,
    ) -> Result<BlockState, MappingError> {
        let func = self
            .instance
            .get_func(handler)
            .with_context(|| format!("function {} not found", handler))?;

        // Caution: Make sure all exit paths from this function call `exit_handler`.
        self.instance_ctx_mut().ctx.state.enter_handler();

        // This `match` will return early if there was a non-deterministic trap.
        let deterministic_error: Option<Error> = match func.typed()?.call(arg.wasm_ptr()) {
            Ok(()) => None,
            Err(trap) if self.instance_ctx().possible_reorg => {
                self.instance_ctx_mut().ctx.state.exit_handler();
                return Err(MappingError::PossibleReorg(trap.into()));
            }
            Err(trap) if trap.to_string().contains(TRAP_TIMEOUT) => {
                self.instance_ctx_mut().ctx.state.exit_handler();
                return Err(MappingError::Unknown(Error::from(trap).context(format!(
                    "Handler '{}' hit the timeout of '{}' seconds",
                    handler,
                    self.instance_ctx().timeout.unwrap().as_secs()
                ))));
            }
            Err(trap) => {
                use wasmtime::TrapCode::*;
                let trap_code = trap.trap_code();
                let e = Error::from(trap);
                match trap_code {
                    Some(MemoryOutOfBounds)
                    | Some(HeapMisaligned)
                    | Some(TableOutOfBounds)
                    | Some(IndirectCallToNull)
                    | Some(BadSignature)
                    | Some(IntegerOverflow)
                    | Some(IntegerDivisionByZero)
                    | Some(BadConversionToInteger)
                    | Some(UnreachableCodeReached) => Some(e),
                    _ if self.instance_ctx().deterministic_host_trap => Some(e),
                    _ => {
                        self.instance_ctx_mut().ctx.state.exit_handler();
                        return Err(MappingError::Unknown(e));
                    }
                }
            }
        };

        if let Some(deterministic_error) = deterministic_error {
            let message = format!("{:#}", deterministic_error).replace("\n", "\t");

            // Log the error and restore the updates snapshot, effectively reverting the handler.
            error!(&self.instance_ctx().ctx.logger,
                "Handler skipped due to execution failure";
                "handler" => handler,
                "error" => &message,
            );
            let subgraph_error = SubgraphError {
                subgraph_id: self.instance_ctx().ctx.host_exports.subgraph_id.clone(),
                message,
                block_ptr: Some(self.instance_ctx().ctx.block_ptr.cheap_clone()),
                handler: Some(handler.to_string()),
                deterministic: true,
            };
            self.instance_ctx_mut()
                .ctx
                .state
                .exit_handler_and_discard_changes_due_to_error(subgraph_error);
        } else {
            self.instance_ctx_mut().ctx.state.exit_handler();
        }

        Ok(self.take_ctx().ctx.state)
    }
}

#[derive(Copy, Clone)]
pub struct ExperimentalFeatures {
    pub allow_non_deterministic_ipfs: bool,
    pub allow_non_deterministic_arweave: bool,
    pub allow_non_deterministic_3box: bool,
}

pub(crate) struct WasmInstanceContext {
    // In the future there may be multiple memories, but currently there is only one memory per
    // module. And at least AS calls it "memory". There is no uninitialized memory in Wasm, memory
    // is zeroed when initialized or grown.
    memory: Memory,

    // Function exported by the wasm module that will allocate the request number of bytes and
    // return a pointer to the first byte of allocated space.
    memory_allocate: wasmtime::TypedFunc<i32, i32>,

    pub ctx: MappingContext,
    pub(crate) valid_module: Arc<ValidModule>,
    pub(crate) host_metrics: Arc<HostMetrics>,
    pub(crate) timeout: Option<Duration>,

    // Used by ipfs.map.
    pub(crate) timeout_stopwatch: Arc<std::sync::Mutex<TimeoutStopwatch>>,

    // First free byte in the current arena. Set on the first call to `raw_new`.
    arena_start_ptr: i32,

    // Number of free bytes starting from `arena_start_ptr`.
    arena_free_size: i32,

    // A trap ocurred due to a possible reorg detection.
    possible_reorg: bool,

    // A host export trap ocurred for a deterministic reason.
    deterministic_host_trap: bool,

    pub(crate) experimental_features: ExperimentalFeatures,
}

impl WasmInstance {
    /// Instantiates the module and sets it to be interrupted after `timeout`.
    pub fn from_valid_module_with_ctx(
        valid_module: Arc<ValidModule>,
        ctx: MappingContext,
        host_metrics: Arc<HostMetrics>,
        timeout: Option<Duration>,
        experimental_features: ExperimentalFeatures,
    ) -> Result<WasmInstance, anyhow::Error> {
        let mut linker = wasmtime::Linker::new(&wasmtime::Store::new(valid_module.module.engine()));

        // Used by exports to access the instance context. There are two ways this can be set:
        // - After instantiation, if no host export is called in the start function.
        // - During the start function, if it calls a host export.
        // Either way, after instantiation this will have been set.
        let shared_ctx: Rc<RefCell<Option<WasmInstanceContext>>> = Rc::new(RefCell::new(None));

        // We will move the ctx only once, to init `shared_ctx`. But we don't statically know where
        // it will be moved so we need this ugly thing.
        let ctx: Rc<RefCell<Option<MappingContext>>> = Rc::new(RefCell::new(Some(ctx)));

        // Start the timeout watchdog task.
        let timeout_stopwatch = Arc::new(std::sync::Mutex::new(TimeoutStopwatch::start_new()));
        if let Some(timeout) = timeout {
            // This task is likely to outlive the instance, which is fine.
            let interrupt_handle = linker.store().interrupt_handle().unwrap();
            let timeout_stopwatch = timeout_stopwatch.clone();
            graph::spawn_allow_panic(async move {
                let minimum_wait = Duration::from_secs(1);
                loop {
                    let time_left =
                        timeout.checked_sub(timeout_stopwatch.lock().unwrap().elapsed());
                    match time_left {
                        None => break interrupt_handle.interrupt(), // Timed out.

                        Some(time) if time < minimum_wait => break interrupt_handle.interrupt(),
                        Some(time) => tokio::time::delay_for(time).await,
                    }
                }
            });
        }

        macro_rules! link {
            ($wasm_name:expr, $rust_name:ident, $($param:ident),*) => {
                link!($wasm_name, $rust_name, "host_export_other", $($param),*)
            };

            ($wasm_name:expr, $rust_name:ident, $section:expr, $($param:ident),*) => {
                let modules = valid_module
                    .import_name_to_modules
                    .get($wasm_name)
                    .into_iter()
                    .flatten();

                // link an import with all the modules that require it.
                for module in modules {
                    let func_shared_ctx = Rc::downgrade(&shared_ctx);
                    let valid_module = valid_module.cheap_clone();
                    let host_metrics = host_metrics.cheap_clone();
                    let timeout_stopwatch = timeout_stopwatch.cheap_clone();
                    let ctx = ctx.cheap_clone();
                    linker.func(
                        module,
                        $wasm_name,
                        move |caller: wasmtime::Caller, $($param: u32),*| {
                            let instance = func_shared_ctx.upgrade().unwrap();
                            let mut instance = instance.borrow_mut();

                            // Happens when calling a host fn in Wasm start.
                            if instance.is_none() {
                                *instance = Some(WasmInstanceContext::from_caller(
                                    caller,
                                    ctx.borrow_mut().take().unwrap(),
                                    valid_module.cheap_clone(),
                                    host_metrics.cheap_clone(),
                                    timeout,
                                    timeout_stopwatch.cheap_clone(),
                                    experimental_features.clone()
                                ).unwrap())
                            }

                            let instance = instance.as_mut().unwrap();
                            let _section = instance.host_metrics.stopwatch.start_section($section);

                            let result = instance.$rust_name(
                                $($param.into()),*
                            );
                            match result {
                                Ok(result) => Ok(result.into_wasm_ret()),
                                Err(e) => {
                                    match IntoTrap::determinism_level(&e) {
                                        DeterminismLevel::Deterministic => {
                                            instance.deterministic_host_trap = true;
                                        },
                                        _ => {},
                                    }
                                    Err(IntoTrap::into_trap(e))
                                }
                            }
                        }
                    )?;
                }
            };
        }

        let modules = valid_module
            .import_name_to_modules
            .get("ethereum.call")
            .into_iter()
            .flatten();

        for module in modules {
            let func_shared_ctx = Rc::downgrade(&shared_ctx);
            linker.func(module, "ethereum.call", move |call_ptr: u32| {
                let start = Instant::now();
                let instance = func_shared_ctx.upgrade().unwrap();
                let mut instance = instance.borrow_mut();

                let instance = match &mut *instance {
                    Some(instance) => instance,

                    // Happens when calling a host fn in Wasm start.
                    None => {
                        return Err(
                            anyhow!("ethereum.call is not allowed in global variables").into()
                        )
                    }
                };

                let stopwatch = &instance.host_metrics.stopwatch;
                let _section = stopwatch.start_section("host_export_ethereum_call");

                // For apiVersion >= 0.0.4 the call passed from the mapping includes the
                // function signature; subgraphs using an apiVersion < 0.0.4 don't pass
                // the the signature along with the call.
                let arg = if instance.ctx.host_exports.api_version >= Version::new(0, 0, 4) {
                    instance.asc_get::<_, AscUnresolvedContractCall_0_0_4>(call_ptr.into())
                } else {
                    instance.asc_get::<_, AscUnresolvedContractCall>(call_ptr.into())
                }
                .map_err(|e| {
                    instance.deterministic_host_trap = true;
                    e.0
                })?;

                let ret = instance
                    .ethereum_call(arg)
                    .map_err(|e| match e {
                        HostExportError::Deterministic(e) => {
                            instance.deterministic_host_trap = true;
                            e
                        }
                        HostExportError::Unknown(e) => e,
                    })?
                    .wasm_ptr();
                instance
                    .host_metrics
                    .observe_host_fn_execution_time(start.elapsed().as_secs_f64(), "ethereum_call");
                Ok(ret)
            })?;
        }

        link!("ethereum.encode", ethereum_encode, params_ptr);
        link!("ethereum.decode", ethereum_decode, params_ptr, data_ptr);

        link!("abort", abort, message_ptr, file_name_ptr, line, column);

        link!("store.get", store_get, "host_export_store_get", entity, id);
        link!(
            "store.set",
            store_set,
            "host_export_store_set",
            entity,
            id,
            data
        );

        link!("ipfs.cat", ipfs_cat, "host_export_ipfs_cat", hash_ptr);
        link!(
            "ipfs.map",
            ipfs_map,
            "host_export_ipfs_map",
            link_ptr,
            callback,
            user_data,
            flags
        );

        link!("store.remove", store_remove, entity_ptr, id_ptr);

        link!("typeConversion.bytesToString", bytes_to_string, ptr);
        link!("typeConversion.bytesToHex", bytes_to_hex, ptr);
        link!("typeConversion.bigIntToString", big_int_to_string, ptr);
        link!("typeConversion.bigIntToHex", big_int_to_hex, ptr);
        link!("typeConversion.stringToH160", string_to_h160, ptr);
        link!("typeConversion.bytesToBase58", bytes_to_base58, ptr);

        link!("json.fromBytes", json_from_bytes, ptr);
        link!("json.try_fromBytes", json_try_from_bytes, ptr);
        link!("json.toI64", json_to_i64, ptr);
        link!("json.toU64", json_to_u64, ptr);
        link!("json.toF64", json_to_f64, ptr);
        link!("json.toBigInt", json_to_big_int, ptr);

        link!("crypto.keccak256", crypto_keccak_256, ptr);

        link!("bigInt.plus", big_int_plus, x_ptr, y_ptr);
        link!("bigInt.minus", big_int_minus, x_ptr, y_ptr);
        link!("bigInt.times", big_int_times, x_ptr, y_ptr);
        link!("bigInt.dividedBy", big_int_divided_by, x_ptr, y_ptr);
        link!("bigInt.dividedByDecimal", big_int_divided_by_decimal, x, y);
        link!("bigInt.mod", big_int_mod, x_ptr, y_ptr);
        link!("bigInt.pow", big_int_pow, x_ptr, exp);
        link!("bigInt.fromString", big_int_from_string, ptr);
        link!("bigInt.bitOr", big_int_bit_or, x_ptr, y_ptr);
        link!("bigInt.bitAnd", big_int_bit_and, x_ptr, y_ptr);
        link!("bigInt.leftShift", big_int_left_shift, x_ptr, bits);
        link!("bigInt.rightShift", big_int_right_shift, x_ptr, bits);

        link!("bigDecimal.toString", big_decimal_to_string, ptr);
        link!("bigDecimal.fromString", big_decimal_from_string, ptr);
        link!("bigDecimal.plus", big_decimal_plus, x_ptr, y_ptr);
        link!("bigDecimal.minus", big_decimal_minus, x_ptr, y_ptr);
        link!("bigDecimal.times", big_decimal_times, x_ptr, y_ptr);
        link!("bigDecimal.dividedBy", big_decimal_divided_by, x, y);
        link!("bigDecimal.equals", big_decimal_equals, x_ptr, y_ptr);

        link!("dataSource.create", data_source_create, name, params);
        link!(
            "dataSource.createWithContext",
            data_source_create_with_context,
            name,
            params,
            context
        );
        link!("dataSource.address", data_source_address,);
        link!("dataSource.network", data_source_network,);
        link!("dataSource.context", data_source_context,);

        link!("ens.nameByHash", ens_name_by_hash, ptr);

        link!("log.log", log_log, level, msg_ptr);

        link!("arweave.transactionData", arweave_transaction_data, ptr);

        link!("box.profile", box_profile, ptr);

        let instance = linker.instantiate(&valid_module.module)?;

        // Usually `shared_ctx` is still `None` because no host fns were called during start.
        if shared_ctx.borrow().is_none() {
            *shared_ctx.borrow_mut() = Some(WasmInstanceContext::from_instance(
                &instance,
                ctx.borrow_mut().take().unwrap(),
                valid_module,
                host_metrics,
                timeout,
                timeout_stopwatch,
                experimental_features,
            )?);
        }

        Ok(WasmInstance {
            instance,
            instance_ctx: shared_ctx,
        })
    }
}

impl AscHeap for WasmInstanceContext {
    fn raw_new(&mut self, bytes: &[u8]) -> Result<u32, DeterministicHostError> {
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
            self.arena_start_ptr = self.memory_allocate.call(arena_size).unwrap();
            self.arena_free_size = arena_size;
        };

        let ptr = self.arena_start_ptr as usize;

        // Unwrap: We have just allocated enough space for `bytes`.
        self.memory.write(ptr, bytes).unwrap();
        self.arena_start_ptr += size;
        self.arena_free_size -= size;

        Ok(ptr as u32)
    }

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, DeterministicHostError> {
        let offset = offset as usize;
        let size = size as usize;

        let mut data = vec![0; size];

        self.memory.read(offset, &mut data).map_err(|_| {
            DeterministicHostError(anyhow!(
                "Heap access out of bounds. Offset: {} Size: {}",
                offset,
                size
            ))
        })?;

        Ok(data)
    }

    fn api_version(&self) -> Version {
        self.ctx.host_exports.api_version.clone()
    }
}

impl WasmInstanceContext {
    fn from_instance(
        instance: &wasmtime::Instance,
        ctx: MappingContext,
        valid_module: Arc<ValidModule>,
        host_metrics: Arc<HostMetrics>,
        timeout: Option<Duration>,
        timeout_stopwatch: Arc<std::sync::Mutex<TimeoutStopwatch>>,
        experimental_features: ExperimentalFeatures,
    ) -> Result<Self, anyhow::Error> {
        // Provide access to the WASM runtime linear memory
        let memory = instance
            .get_memory("memory")
            .context("Failed to find memory export in the WASM module")?;

        let memory_allocate = instance
            .get_func("memory.allocate")
            .context("`memory.allocate` function not found")?
            .typed()?
            .clone();

        Ok(WasmInstanceContext {
            memory_allocate,
            memory,
            ctx,
            valid_module,
            host_metrics,
            timeout,
            timeout_stopwatch,
            arena_free_size: 0,
            arena_start_ptr: 0,
            possible_reorg: false,
            deterministic_host_trap: false,
            experimental_features,
        })
    }

    fn from_caller(
        caller: wasmtime::Caller,
        ctx: MappingContext,
        valid_module: Arc<ValidModule>,
        host_metrics: Arc<HostMetrics>,
        timeout: Option<Duration>,
        timeout_stopwatch: Arc<std::sync::Mutex<TimeoutStopwatch>>,
        experimental_features: ExperimentalFeatures,
    ) -> Result<Self, anyhow::Error> {
        let memory = caller
            .get_export("memory")
            .and_then(|e| e.into_memory())
            .context("Failed to find memory export in the WASM module")?;

        let memory_allocate = caller
            .get_export("memory.allocate")
            .and_then(|e| e.into_func())
            .context("`memory.allocate` function not found")?
            .typed()?
            .clone();

        Ok(WasmInstanceContext {
            memory_allocate,
            memory,
            ctx,
            valid_module,
            host_metrics,
            timeout,
            timeout_stopwatch,
            arena_free_size: 0,
            arena_start_ptr: 0,
            possible_reorg: false,
            deterministic_host_trap: false,
            experimental_features,
        })
    }
}

// Implementation of externals.
impl WasmInstanceContext {
    /// function abort(message?: string | null, fileName?: string | null, lineNumber?: u32, columnNumber?: u32): void
    /// Always returns a trap.
    fn abort(
        &mut self,
        message_ptr: AscPtr<AscString>,
        file_name_ptr: AscPtr<AscString>,
        line_number: u32,
        column_number: u32,
    ) -> Result<Never, DeterministicHostError> {
        let message = match message_ptr.is_null() {
            false => Some(self.asc_get(message_ptr)?),
            true => None,
        };
        let file_name = match file_name_ptr.is_null() {
            false => Some(self.asc_get(file_name_ptr)?),
            true => None,
        };
        let line_number = match line_number {
            0 => None,
            _ => Some(line_number),
        };
        let column_number = match column_number {
            0 => None,
            _ => Some(column_number),
        };

        self.ctx
            .host_exports
            .abort(message, file_name, line_number, column_number)
    }

    /// function store.set(entity: string, id: string, data: Entity): void
    fn store_set(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<(), HostExportError> {
        let stopwatch = &self.host_metrics.stopwatch;
        stopwatch.start_section("host_export_store_set__wasm_instance_context_store_set");

        let entity = self.asc_get(entity_ptr)?;
        let id = self.asc_get(id_ptr)?;
        let data = self.try_asc_get(data_ptr)?;

        self.ctx.host_exports.store_set(
            &self.ctx.logger,
            &mut self.ctx.state,
            &self.ctx.proof_of_indexing,
            entity,
            id,
            data,
            stopwatch,
        )?;
        Ok(())
    }

    /// function store.remove(entity: string, id: string): void
    fn store_remove(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<(), HostExportError> {
        let entity = self.asc_get(entity_ptr)?;
        let id = self.asc_get(id_ptr)?;
        self.ctx.host_exports.store_remove(
            &self.ctx.logger,
            &mut self.ctx.state,
            &self.ctx.proof_of_indexing,
            entity,
            id,
        )
    }

    /// function store.get(entity: string, id: string): Entity | null
    fn store_get(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscEntity>, HostExportError> {
        let _timer = self
            .host_metrics
            .cheap_clone()
            .time_host_fn_execution_region("store_get");
        let entity_ptr = self.asc_get(entity_ptr)?;
        let id_ptr = self.asc_get(id_ptr)?;
        let entity_option =
            self.ctx
                .host_exports
                .store_get(&mut self.ctx.state, entity_ptr, id_ptr)?;

        let ret = match entity_option {
            Some(entity) => {
                let _section = self
                    .host_metrics
                    .stopwatch
                    .start_section("store_get_asc_new");
                self.asc_new(&entity.sorted())?
            }
            None => AscPtr::null(),
        };

        Ok(ret)
    }

    /// function ethereum.call(call: SmartContractCall): Array<Token> | null
    fn ethereum_call(
        &mut self,
        call: UnresolvedContractCall,
    ) -> Result<AscEnumArray<EthereumValueKind>, HostExportError> {
        let result =
            self.ctx
                .host_exports
                .ethereum_call(&self.ctx.logger, &self.ctx.block_ptr, call);
        match result {
            Ok(Some(tokens)) => Ok(self.asc_new(tokens.as_slice())?),
            Ok(None) => Ok(AscPtr::null()),
            Err(EthereumCallError::Unknown(e)) => Err(HostExportError::Unknown(e.into())),
            Err(EthereumCallError::PossibleReorg(e)) => {
                self.possible_reorg = true;
                Err(HostExportError::Unknown(e))
            }
        }
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    fn bytes_to_string(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscString>, DeterministicHostError> {
        let string = host_exports::bytes_to_string(&self.ctx.logger, self.asc_get(bytes_ptr)?);
        self.asc_new(&string)
    }

    /// Converts bytes to a hex string.
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    /// References:
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    /// https://github.com/ethereum/web3.js/blob/f98fe1462625a6c865125fecc9cb6b414f0a5e83/packages/web3-utils/src/utils.js#L283
    fn bytes_to_hex(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscString>, DeterministicHostError> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr)?;
        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let hex = format!("0x{}", hex::encode(bytes));
        self.asc_new(&hex)
    }

    /// function typeConversion.bigIntToString(n: Uint8Array): string
    fn big_int_to_string(
        &mut self,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscString>, DeterministicHostError> {
        let n: BigInt = self.asc_get(big_int_ptr)?;
        self.asc_new(&n.to_string())
    }

    /// function bigInt.fromString(x: string): BigInt
    fn big_int_from_string(
        &mut self,
        string_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_from_string(self.asc_get(string_ptr)?)?;
        self.asc_new(&result)
    }

    /// function typeConversion.bigIntToHex(n: Uint8Array): string
    fn big_int_to_hex(
        &mut self,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscString>, DeterministicHostError> {
        let n: BigInt = self.asc_get(big_int_ptr)?;
        let hex = self.ctx.host_exports.big_int_to_hex(n)?;
        self.asc_new(&hex)
    }

    /// function typeConversion.stringToH160(s: String): H160
    fn string_to_h160(
        &mut self,
        str_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscH160>, DeterministicHostError> {
        let s: String = self.asc_get(str_ptr)?;
        let h160 = host_exports::string_to_h160(&s)?;
        self.asc_new(&h160)
    }

    /// function json.fromBytes(bytes: Bytes): JSONValue
    fn json_from_bytes(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscEnum<JsonValueKind>>, DeterministicHostError> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr)?;

        let result = host_exports::json_from_bytes(&bytes)
            .with_context(|| {
                format!(
                    "Failed to parse JSON from byte array. Bytes (truncated to 1024 chars): `{:?}`",
                    &bytes[..bytes.len().min(1024)],
                )
            })
            .map_err(DeterministicHostError)?;
        self.asc_new(&result)
    }

    /// function json.try_fromBytes(bytes: Bytes): Result<JSONValue, boolean>
    fn json_try_from_bytes(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscResult<AscEnum<JsonValueKind>, bool>>, DeterministicHostError> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr)?;
        let result = host_exports::json_from_bytes(&bytes).map_err(|e| {
            warn!(
                &self.ctx.logger,
                "Failed to parse JSON from byte array";
                "bytes" => format!("{:?}", bytes),
                "error" => format!("{}", e)
            );

            // Map JSON errors to boolean to match the `Result<JSONValue, boolean>`
            // result type expected by mappings
            true
        });
        self.asc_new(&result)
    }

    /// function ipfs.cat(link: String): Bytes
    fn ipfs_cat(
        &mut self,
        link_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        if !self.experimental_features.allow_non_deterministic_ipfs {
            return Err(HostExportError::Deterministic(anyhow!(
                "`ipfs.cat` is deprecated. Improved support for IPFS will be added in the future"
            )));
        }

        let link = self.asc_get(link_ptr)?;
        let ipfs_res = self.ctx.host_exports.ipfs_cat(&self.ctx.logger, link);
        match ipfs_res {
            Ok(bytes) => self.asc_new(&*bytes).map_err(Into::into),

            // Return null in case of error.
            Err(e) => {
                info!(&self.ctx.logger, "Failed ipfs.cat, returning `null`";
                                    "link" => self.asc_get::<String, _>(link_ptr)?,
                                    "error" => e.to_string());
                Ok(AscPtr::null())
            }
        }
    }

    /// function ipfs.map(link: String, callback: String, flags: String[]): void
    fn ipfs_map(
        &mut self,
        link_ptr: AscPtr<AscString>,
        callback: AscPtr<AscString>,
        user_data: AscPtr<AscEnum<StoreValueKind>>,
        flags: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<(), HostExportError> {
        if !self.experimental_features.allow_non_deterministic_ipfs {
            return Err(HostExportError::Deterministic(anyhow!(
                "`ipfs.map` is deprecated. Improved support for IPFS will be added in the future"
            )));
        }

        let link: String = self.asc_get(link_ptr)?;
        let callback: String = self.asc_get(callback)?;
        let user_data: store::Value = self.try_asc_get(user_data)?;

        let flags = self.asc_get(flags)?;

        // Pause the timeout while running ipfs_map, ensure it will be restarted by using a guard.
        self.timeout_stopwatch.lock().unwrap().stop();
        let defer_stopwatch = self.timeout_stopwatch.clone();
        let _stopwatch_guard = defer::defer(|| defer_stopwatch.lock().unwrap().start());

        let start_time = Instant::now();
        let output_states = HostExports::ipfs_map(
            &self.ctx.host_exports.link_resolver.clone(),
            self,
            link.clone(),
            &*callback,
            user_data,
            flags,
        )?;

        debug!(
            &self.ctx.logger,
            "Successfully processed file with ipfs.map";
            "link" => &link,
            "callback" => &*callback,
            "n_calls" => output_states.len(),
            "time" => format!("{}ms", start_time.elapsed().as_millis())
        );
        for output_state in output_states {
            self.ctx.state.extend(output_state);
        }

        Ok(())
    }

    /// Expects a decimal string.
    /// function json.toI64(json: String): i64
    fn json_to_i64(&mut self, json_ptr: AscPtr<AscString>) -> Result<i64, DeterministicHostError> {
        self.ctx.host_exports.json_to_i64(self.asc_get(json_ptr)?)
    }

    /// Expects a decimal string.
    /// function json.toU64(json: String): u64
    fn json_to_u64(&mut self, json_ptr: AscPtr<AscString>) -> Result<u64, DeterministicHostError> {
        self.ctx.host_exports.json_to_u64(self.asc_get(json_ptr)?)
    }

    /// Expects a decimal string.
    /// function json.toF64(json: String): f64
    fn json_to_f64(&mut self, json_ptr: AscPtr<AscString>) -> Result<f64, DeterministicHostError> {
        self.ctx.host_exports.json_to_f64(self.asc_get(json_ptr)?)
    }

    /// Expects a decimal string.
    /// function json.toBigInt(json: String): BigInt
    fn json_to_big_int(
        &mut self,
        json_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let big_int = self
            .ctx
            .host_exports
            .json_to_big_int(self.asc_get(json_ptr)?)?;
        self.asc_new(&*big_int)
    }

    /// function crypto.keccak256(input: Bytes): Bytes
    fn crypto_keccak_256(
        &mut self,
        input_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<Uint8Array>, DeterministicHostError> {
        let input = self
            .ctx
            .host_exports
            .crypto_keccak_256(self.asc_get(input_ptr)?)?;
        self.asc_new(input.as_ref())
    }

    /// function bigInt.plus(x: BigInt, y: BigInt): BigInt
    fn big_int_plus(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_plus(self.asc_get(x_ptr)?, self.asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.minus(x: BigInt, y: BigInt): BigInt
    fn big_int_minus(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_minus(self.asc_get(x_ptr)?, self.asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.times(x: BigInt, y: BigInt): BigInt
    fn big_int_times(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_times(self.asc_get(x_ptr)?, self.asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.dividedBy(x: BigInt, y: BigInt): BigInt
    fn big_int_divided_by(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_divided_by(self.asc_get(x_ptr)?, self.asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.dividedByDecimal(x: BigInt, y: BigDecimal): BigDecimal
    fn big_int_divided_by_decimal(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, DeterministicHostError> {
        let x = BigDecimal::new(self.asc_get::<BigInt, _>(x_ptr)?, 0);
        let result = self
            .ctx
            .host_exports
            .big_decimal_divided_by(x, self.try_asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.mod(x: BigInt, y: BigInt): BigInt
    fn big_int_mod(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_mod(self.asc_get(x_ptr)?, self.asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.pow(x: BigInt, exp: u8): BigInt
    fn big_int_pow(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        exp: u32,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let exp = u8::try_from(exp).map_err(|e| DeterministicHostError(e.into()))?;
        let result = self
            .ctx
            .host_exports
            .big_int_pow(self.asc_get(x_ptr)?, exp)?;
        self.asc_new(&result)
    }

    /// function bigInt.bitOr(x: BigInt, y: BigInt): BigInt
    fn big_int_bit_or(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_bit_or(self.asc_get(x_ptr)?, self.asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.bitAnd(x: BigInt, y: BigInt): BigInt
    fn big_int_bit_and(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_int_bit_and(self.asc_get(x_ptr)?, self.asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigInt.leftShift(x: BigInt, bits: u8): BigInt
    fn big_int_left_shift(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        bits: u32,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let bits = u8::try_from(bits).map_err(|e| DeterministicHostError(e.into()))?;
        let result = self
            .ctx
            .host_exports
            .big_int_left_shift(self.asc_get(x_ptr)?, bits)?;
        self.asc_new(&result)
    }

    /// function bigInt.rightShift(x: BigInt, bits: u8): BigInt
    fn big_int_right_shift(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        bits: u32,
    ) -> Result<AscPtr<AscBigInt>, DeterministicHostError> {
        let bits = u8::try_from(bits).map_err(|e| DeterministicHostError(e.into()))?;
        let result = self
            .ctx
            .host_exports
            .big_int_right_shift(self.asc_get(x_ptr)?, bits)?;
        self.asc_new(&result)
    }

    /// function typeConversion.bytesToBase58(bytes: Bytes): string
    fn bytes_to_base58(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscString>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .bytes_to_base58(self.asc_get(bytes_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigDecimal.toString(x: BigDecimal): string
    fn big_decimal_to_string(
        &mut self,
        big_decimal_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscString>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_to_string(self.try_asc_get(big_decimal_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigDecimal.fromString(x: string): BigDecimal
    fn big_decimal_from_string(
        &mut self,
        string_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscBigDecimal>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_from_string(self.asc_get(string_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigDecimal.plus(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_plus(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_plus(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigDecimal.minus(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_minus(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_minus(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigDecimal.times(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_times(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_times(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigDecimal.dividedBy(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_divided_by(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, DeterministicHostError> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_divided_by(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?)?;
        self.asc_new(&result)
    }

    /// function bigDecimal.equals(x: BigDecimal, y: BigDecimal): bool
    fn big_decimal_equals(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<bool, DeterministicHostError> {
        self.ctx
            .host_exports
            .big_decimal_equals(self.try_asc_get(x_ptr)?, self.try_asc_get(y_ptr)?)
    }

    /// function dataSource.create(name: string, params: Array<string>): void
    fn data_source_create(
        &mut self,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<(), HostExportError> {
        let name: String = self.asc_get(name_ptr)?;
        let params: Vec<String> = self.asc_get(params_ptr)?;
        self.ctx.host_exports.data_source_create(
            &self.ctx.logger,
            &mut self.ctx.state,
            name,
            params,
            None,
            self.ctx.block_ptr.number,
        )
    }

    /// function createWithContext(name: string, params: Array<string>, context: DataSourceContext): void
    fn data_source_create_with_context(
        &mut self,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
        context_ptr: AscPtr<AscEntity>,
    ) -> Result<(), HostExportError> {
        let name: String = self.asc_get(name_ptr)?;
        let params: Vec<String> = self.asc_get(params_ptr)?;
        let context: HashMap<_, _> = self.try_asc_get(context_ptr)?;
        self.ctx.host_exports.data_source_create(
            &self.ctx.logger,
            &mut self.ctx.state,
            name,
            params,
            Some(context.into()),
            self.ctx.block_ptr.number,
        )
    }

    /// function dataSource.address(): Bytes
    fn data_source_address(&mut self) -> Result<AscPtr<Uint8Array>, DeterministicHostError> {
        self.asc_new(&self.ctx.host_exports.data_source_address())
    }

    /// function dataSource.network(): String
    fn data_source_network(&mut self) -> Result<AscPtr<AscString>, DeterministicHostError> {
        self.asc_new(&self.ctx.host_exports.data_source_network())
    }

    /// function dataSource.context(): DataSourceContext
    fn data_source_context(&mut self) -> Result<AscPtr<AscEntity>, DeterministicHostError> {
        self.asc_new(&self.ctx.host_exports.data_source_context().sorted())
    }

    fn ens_name_by_hash(
        &mut self,
        hash_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let hash: String = self.asc_get(hash_ptr)?;
        let name = self.ctx.host_exports.ens_name_by_hash(&*hash)?;
        // map `None` to `null`, and `Some(s)` to a runtime string
        name.map(|name| self.asc_new(&*name).map_err(Into::into))
            .unwrap_or(Ok(AscPtr::null()))
    }

    fn log_log(
        &mut self,
        level: u32,
        msg: AscPtr<AscString>,
    ) -> Result<(), DeterministicHostError> {
        let level = LogLevel::from(level).into();
        let msg: String = self.asc_get(msg)?;
        self.ctx.host_exports.log_log(&self.ctx.logger, level, msg)
    }

    /// function encode(token: ethereum.Value): Bytes | null
    fn ethereum_encode(
        &mut self,
        token_ptr: AscPtr<AscEnum<EthereumValueKind>>,
    ) -> Result<AscPtr<Uint8Array>, DeterministicHostError> {
        let data = host_exports::ethereum_encode(self.asc_get(token_ptr)?);
        // return `null` if it fails
        data.map(|bytes| self.asc_new(&*bytes))
            .unwrap_or(Ok(AscPtr::null()))
    }

    /// function decode(types: String, data: Bytes): ethereum.Value | null
    fn ethereum_decode(
        &mut self,
        types_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscEnum<EthereumValueKind>>, DeterministicHostError> {
        let result =
            host_exports::ethereum_decode(self.asc_get(types_ptr)?, self.asc_get(data_ptr)?);
        // return `null` if it fails
        result
            .map(|param| self.asc_new(&param))
            .unwrap_or(Ok(AscPtr::null()))
    }

    /// function arweave.transactionData(txId: string): Bytes | null
    fn arweave_transaction_data(
        &mut self,
        tx_id: AscPtr<AscString>,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        if !self.experimental_features.allow_non_deterministic_arweave {
            return Err(HostExportError::Deterministic(anyhow!(
                "`arweave.transactionData` is deprecated. Improved support for arweave may be added in the future"
            )));
        }
        let tx_id: String = self.asc_get(tx_id)?;
        let data = self.ctx.host_exports.arweave_transaction_data(&tx_id);
        data.map(|data| self.asc_new(&*data).map_err(Into::into))
            .unwrap_or(Ok(AscPtr::null()))
    }

    /// function box.profile(address: string): JSONValue | null
    fn box_profile(
        &mut self,
        address: AscPtr<AscString>,
    ) -> Result<AscPtr<AscJson>, HostExportError> {
        // TODO: 3box data is mutable and the best solution here is probably to remove support
        if !self.experimental_features.allow_non_deterministic_3box {
            return Err(HostExportError::Deterministic(anyhow!(
                "`box.profile` is deprecated. Improved support for 3box may be added in the future"
            )));
        }
        let address: String = self.asc_get(address)?;
        let profile = self.ctx.host_exports.box_profile(&address);
        profile
            .map(|profile| self.asc_new(&profile).map_err(|e| e.into()))
            .unwrap_or(Ok(AscPtr::null()))
    }
}
