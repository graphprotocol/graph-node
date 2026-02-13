use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::Error;
use graph::futures03::future::BoxFuture;
use graph::futures03::FutureExt as _;
use graph::slog::SendSyncRefUnwindSafeKV;

use semver::Version;
use wasmtime::{AsContextMut, Linker, Store, Trap};

use graph::blockchain::{Blockchain, HostFnCtx};
use graph::data::store;
use graph::data::subgraph::schema::SubgraphError;
use graph::data_source::{MappingTrigger, TriggerWithHandler};
use graph::prelude::*;
use graph::runtime::{
    asc_new,
    gas::{Gas, GasCounter, SaturatingInto},
    HostExportError,
};
use graph::{components::subgraph::MappingError, runtime::AscPtr};

use super::IntoWasmRet;
use super::{IntoTrap, WasmInstanceContext};
use crate::error::DeterminismLevel;
use crate::mapping::MappingContext;
use crate::mapping::ValidModule;
use crate::module::WasmInstanceData;
use crate::ExperimentalFeatures;

use super::{is_trap_deterministic, AscHeapCtx, ToAscPtr};

/// Handle to a WASM instance, which is terminated if and only if this is dropped.
pub struct WasmInstance {
    pub instance: wasmtime::Instance,
    pub store: wasmtime::Store<WasmInstanceData>,

    // A reference to the gas counter used for reporting the gas used.
    pub gas: GasCounter,
}

#[cfg(debug_assertions)]
mod impl_for_tests {
    use graph::runtime::{
        asc_new, AscIndexId, AscPtr, AscType, DeterministicHostError, FromAscObj, HostExportError,
        ToAscObj,
    };

    use crate::module::{asc_get, WasmInstanceContext};

    impl super::WasmInstance {
        pub fn asc_get<T, P>(&mut self, asc_ptr: AscPtr<P>) -> Result<T, DeterministicHostError>
        where
            P: AscType + AscIndexId,
            T: FromAscObj<P>,
        {
            let ctx = WasmInstanceContext::new(&mut self.store);
            asc_get(&ctx, asc_ptr, &self.gas)
        }

        pub async fn asc_new<P, T>(&mut self, rust_obj: &T) -> Result<AscPtr<P>, HostExportError>
        where
            P: AscType + AscIndexId,
            T: ToAscObj<P> + ?Sized,
        {
            let mut ctx = WasmInstanceContext::new(&mut self.store);
            asc_new(&mut ctx, rust_obj, &self.gas).await
        }
    }
}

impl WasmInstance {
    pub(crate) async fn handle_json_callback(
        mut self,
        handler_name: &str,
        value: &serde_json::Value,
        user_data: &store::Value,
    ) -> Result<BlockState, anyhow::Error> {
        let gas_metrics = self.store.data().host_metrics.gas_metrics.clone();
        let gas = GasCounter::new(gas_metrics);
        let mut ctx = self.instance_ctx();
        let (value, user_data) = {
            let value = asc_new(&mut ctx, value, &gas).await;

            let user_data = asc_new(&mut ctx, user_data, &gas).await;

            (value, user_data)
        };

        self.instance_ctx().as_mut().ctx.state.enter_handler();

        // Invoke the callback
        self.instance
            .get_func(self.store.as_context_mut(), handler_name)
            .with_context(|| format!("function {} not found", handler_name))?
            .typed::<(u32, u32), ()>(self.store.as_context_mut())?
            .call_async(
                self.store.as_context_mut(),
                (value?.wasm_ptr(), user_data?.wasm_ptr()),
            )
            .await
            .with_context(|| format!("Failed to handle callback '{}'", handler_name))?;

        let mut wasm_ctx = self.store.into_data();
        wasm_ctx.ctx.state.exit_handler();

        Ok(wasm_ctx.take_state())
    }

    pub(crate) async fn handle_trigger<C: Blockchain>(
        mut self,
        trigger: TriggerWithHandler<MappingTrigger<C>>,
    ) -> Result<(BlockState, Gas), MappingError>
    where
        <C as Blockchain>::MappingTrigger: ToAscPtr,
    {
        let handler_name = trigger.handler_name().to_owned();
        let gas = self.gas.clone();
        let logging_extras = trigger.logging_extras().cheap_clone();
        let error_context = trigger.trigger.error_context();
        let mut ctx = self.instance_ctx();
        let asc_trigger = trigger.to_asc_ptr(&mut ctx, &gas).await?;

        self.invoke_handler(&handler_name, asc_trigger, logging_extras, error_context)
            .await
    }

    pub fn take_ctx(self) -> WasmInstanceData {
        self.store.into_data()
    }

    pub(crate) fn instance_ctx(&mut self) -> WasmInstanceContext<'_> {
        WasmInstanceContext::new(&mut self.store)
    }

    #[cfg(debug_assertions)]
    pub fn get_func(&mut self, func_name: &str) -> wasmtime::Func {
        self.instance
            .get_func(self.store.as_context_mut(), func_name)
            .unwrap()
    }

    #[cfg(debug_assertions)]
    pub fn gas_used(&self) -> u64 {
        self.gas.get().value()
    }

    async fn invoke_handler<T>(
        mut self,
        handler: &str,
        arg: AscPtr<T>,
        logging_extras: Arc<dyn SendSyncRefUnwindSafeKV>,
        error_context: Option<String>,
    ) -> Result<(BlockState, Gas), MappingError> {
        let func = self
            .instance
            .get_func(self.store.as_context_mut(), handler)
            .with_context(|| format!("function {} not found", handler))?;

        let func = func
            .typed(self.store.as_context_mut())
            .context("wasm function has incorrect signature")?;

        // Caution: Make sure all exit paths from this function call `exit_handler`.
        self.instance_ctx().as_mut().ctx.state.enter_handler();

        // This `match` will return early if there was a non-deterministic trap.
        let deterministic_error: Option<Error> = match func
            .call_async(self.store.as_context_mut(), arg.wasm_ptr())
            .await
        {
            Ok(()) => {
                assert!(!self.instance_ctx().as_ref().possible_reorg);
                assert!(!self.instance_ctx().as_ref().deterministic_host_trap);
                None
            }
            Err(trap) if self.instance_ctx().as_ref().possible_reorg => {
                self.instance_ctx().as_mut().ctx.state.exit_handler();
                return Err(MappingError::PossibleReorg(trap));
            }

            // Treat timeouts anywhere in the error chain as a special case to have a better error
            // message. Any `TrapCode::Interrupt` is assumed to be a timeout.
            // See also: runtime-timeouts
            Err(trap)
                if trap
                    .chain()
                    .any(|e| e.downcast_ref::<Trap>() == Some(&Trap::Interrupt)) =>
            {
                self.instance_ctx().as_mut().ctx.state.exit_handler();
                return Err(MappingError::Unknown(trap.context(format!(
                        "Handler '{}' hit the timeout of '{}' seconds",
                        handler,
                        self.instance_ctx().as_ref().valid_module.timeout.unwrap().as_secs()
                    ))));
            }
            Err(trap) => {
                let trap_is_deterministic = is_trap_deterministic(&trap)
                    || self.instance_ctx().as_ref().deterministic_host_trap;
                match trap_is_deterministic {
                    true => Some(trap),
                    false => {
                        self.instance_ctx().as_mut().ctx.state.exit_handler();
                        return Err(MappingError::Unknown(trap));
                    }
                }
            }
        };

        if let Some(deterministic_error) = deterministic_error {
            let deterministic_error = match error_context {
                Some(error_context) => deterministic_error.context(error_context),
                None => deterministic_error,
            };
            let message = format!("{:#}", deterministic_error).replace('\n', "\t");

            // Log the error and restore the updates snapshot, effectively reverting the handler.
            error!(&self.instance_ctx().as_ref().ctx.logger,
                "Handler skipped due to execution failure";
                "handler" => handler,
                "error" => &message,
                logging_extras
            );
            let subgraph_error = SubgraphError {
                subgraph_id: self
                    .instance_ctx()
                    .as_ref()
                    .ctx
                    .host_exports
                    .subgraph_id
                    .clone(),
                message,
                block_ptr: Some(self.instance_ctx().as_ref().ctx.block_ptr.cheap_clone()),
                handler: Some(handler.to_string()),
                deterministic: true,
            };
            self.instance_ctx()
                .as_mut()
                .ctx
                .state
                .exit_handler_and_discard_changes_due_to_error(subgraph_error);
        } else {
            self.instance_ctx().as_mut().ctx.state.exit_handler();
        }

        let gas = self.gas.get();
        Ok((self.take_ctx().take_state(), gas))
    }
}

/// Register a chain-specific host function dispatcher for the given import name.
/// The registered closure looks up the `HostFn` by name from `caller.data().ctx.host_fns`
/// at call time. If the module doesn't import this function, this is a no-op.
fn link_chain_host_fn(
    linker: &mut Linker<WasmInstanceData>,
    import_name_to_modules: &BTreeMap<String, Vec<String>>,
    name: &'static str,
) -> Result<(), anyhow::Error> {
    let modules = match import_name_to_modules.get(name) {
        Some(m) => m,
        None => return Ok(()),
    };

    let name_for_metrics = name.replace('.', "_");
    let section_name = format!("host_export_{}", name_for_metrics);

    for module in modules {
        let name_for_metrics = name_for_metrics.clone();
        let section_name = section_name.clone();
        linker.func_wrap_async(
            module,
            name,
            move |mut caller: wasmtime::Caller<'_, WasmInstanceData>, (call_ptr,): (u32,)| {
                let name_for_metrics = name_for_metrics.clone();
                let section_name = section_name.clone();
                Box::new(async move {
                    let host_fn = caller
                        .data()
                        .ctx
                        .host_fns
                        .iter()
                        .find(|hf| hf.name == name)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "chain host function '{}' is not available for this chain",
                                name
                            )
                        })?
                        .cheap_clone();

                    let start = Instant::now();

                    let gas = caller.data().gas.cheap_clone();
                    let host_metrics = caller.data().host_metrics.cheap_clone();
                    let stopwatch = host_metrics.stopwatch.cheap_clone();
                    let _section = stopwatch.start_section(&section_name);

                    let ctx = HostFnCtx {
                        logger: caller.data().ctx.logger.cheap_clone(),
                        block_ptr: caller.data().ctx.block_ptr.cheap_clone(),
                        gas: gas.cheap_clone(),
                        metrics: host_metrics.cheap_clone(),
                        heap: &mut WasmInstanceContext::new(&mut caller),
                    };
                    let ret = (host_fn.func)(ctx, call_ptr).await.map_err(|e| match e {
                        HostExportError::Deterministic(e) => {
                            caller.data_mut().deterministic_host_trap = true;
                            e
                        }
                        HostExportError::PossibleReorg(e) => {
                            caller.data_mut().possible_reorg = true;
                            e
                        }
                        HostExportError::Unknown(e) => e,
                    })?;
                    host_metrics.observe_host_fn_execution_time(
                        start.elapsed().as_secs_f64(),
                        &name_for_metrics,
                    );
                    Ok(ret)
                })
            },
        )?;
    }
    Ok(())
}

/// Build a pre-linked `Linker` for a WASM module. This linker can be reused across triggers by
/// calling `linker.instantiate_pre()` once and then `instance_pre.instantiate_async()` per trigger.
///
/// All host functions (builtins + chain-specific) are registered here. Chain-specific host functions
/// are dispatched generically by looking up the `HostFn` by name from `caller.data().ctx.host_fns`
/// at call time rather than capturing concrete closures at link time.
pub(crate) fn build_linker(
    engine: &wasmtime::Engine,
    import_name_to_modules: &BTreeMap<String, Vec<String>>,
) -> Result<Linker<WasmInstanceData>, anyhow::Error> {
    let mut linker: Linker<WasmInstanceData> = wasmtime::Linker::new(engine);

    // Helper to turn a parameter name into 'u32' for a tuple type
    // (param1, parma2, ..) : (u32, u32, ..)
    macro_rules! param_u32 {
        ($param:ident) => {
            u32
        };
    }

    // The difficulty with this macro is that it needs to turn a list of
    // parameter names into a tuple declaration (param1, parma2, ..) :
    // (u32, u32, ..), but also for an empty parameter list, it needs to
    // produce '(): ()'. In the first case we need a trailing comma, in
    // the second case we don't. That's why there are two separate
    // expansions, one with and one without params
    macro_rules! link {
        ($wasm_name:expr, $rust_name:ident, $($param:ident),*) => {
            link!($wasm_name, $rust_name, "host_export_other",$($param),*)
        };

        ($wasm_name:expr, $rust_name:ident, $section:expr, $($param:ident),+) => {
            let modules = import_name_to_modules
                .get($wasm_name)
                .into_iter()
                .flatten();

            // link an import with all the modules that require it.
            for module in modules {
                linker.func_wrap_async(
                    module,
                    $wasm_name,
                    move |mut caller: wasmtime::Caller<'_, WasmInstanceData>,
                          ($($param),*,) : ($(param_u32!($param)),*,)|  {
                        Box::new(async move {
                            let gas = caller.data().gas.cheap_clone();
                            let host_metrics = caller.data().host_metrics.cheap_clone();
                            let _section = host_metrics.stopwatch.start_section($section);

                            #[allow(unused_mut)]
                            let mut ctx = std::pin::pin!(WasmInstanceContext::new(&mut caller));
                            let result = ctx.$rust_name(
                                &gas,
                                $($param.into()),*
                            ).await;
                            let ctx = ctx.get_mut();
                            match result {
                                Ok(result) => Ok(result.into_wasm_ret()),
                                Err(e) => {
                                    match IntoTrap::determinism_level(&e) {
                                        DeterminismLevel::Deterministic => {
                                            ctx.as_mut().deterministic_host_trap = true;
                                        }
                                        DeterminismLevel::PossibleReorg => {
                                            ctx.as_mut().possible_reorg = true;
                                        }
                                        DeterminismLevel::Unimplemented
                                        | DeterminismLevel::NonDeterministic => {}
                                    }

                                    Err(e.into())
                                }
                            }
                    }) },
                )?;
            }
        };

        ($wasm_name:expr, $rust_name:ident, $section:expr,) => {
            let modules = import_name_to_modules
                .get($wasm_name)
                .into_iter()
                .flatten();

            // link an import with all the modules that require it.
            for module in modules {
                linker.func_wrap_async(
                    module,
                    $wasm_name,
                    move |mut caller: wasmtime::Caller<'_, WasmInstanceData>,
                          _ : ()|  {
                        Box::new(async move {
                            let gas = caller.data().gas.cheap_clone();
                            let host_metrics = caller.data().host_metrics.cheap_clone();
                            let _section = host_metrics.stopwatch.start_section($section);

                            #[allow(unused_mut)]
                            let mut ctx = WasmInstanceContext::new(&mut caller);
                            let result = ctx.$rust_name(&gas).await;
                            match result {
                                Ok(result) => Ok(result.into_wasm_ret()),
                                Err(e) => {
                                    match IntoTrap::determinism_level(&e) {
                                        DeterminismLevel::Deterministic => {
                                            ctx.as_mut().deterministic_host_trap = true;
                                        }
                                        DeterminismLevel::PossibleReorg => {
                                            ctx.as_mut().possible_reorg = true;
                                        }
                                        DeterminismLevel::Unimplemented
                                        | DeterminismLevel::NonDeterministic => {}
                                    }

                                    Err(e.into())
                                }
                            }
                    }) },
                )?;
            }
        };
    }

    // Chain-specific host functions. Each is registered explicitly rather than
    // discovered dynamically from imports.
    link_chain_host_fn(&mut linker, import_name_to_modules, "ethereum.call")?;
    link_chain_host_fn(&mut linker, import_name_to_modules, "ethereum.getBalance")?;
    link_chain_host_fn(&mut linker, import_name_to_modules, "ethereum.hasCode")?;

    link!("ethereum.encode", ethereum_encode, params_ptr);
    link!("ethereum.decode", ethereum_decode, params_ptr, data_ptr);

    link!("abort", abort, message_ptr, file_name_ptr, line, column);

    link!("store.get", store_get, "host_export_store_get", entity, id);
    link!(
        "store.loadRelated",
        store_load_related,
        "host_export_store_load_related",
        entity,
        id,
        field
    );
    link!(
        "store.get_in_block",
        store_get_in_block,
        "host_export_store_get_in_block",
        entity,
        id
    );
    link!(
        "store.set",
        store_set,
        "host_export_store_set",
        entity,
        id,
        data
    );

    // All IPFS-related functions exported by the host WASM runtime should be listed in the
    // graph::data::subgraph::features::IPFS_ON_ETHEREUM_CONTRACTS_FUNCTION_NAMES array for
    // automatic feature detection to work.
    //
    // For reference, search this codebase for: ff652476-e6ad-40e4-85b8-e815d6c6e5e2
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
    // ipfs.getBlock checks the experimental_features flag internally via
    // caller.data().experimental_features, so it can be linked unconditionally.
    link!(
        "ipfs.getBlock",
        ipfs_get_block,
        "host_export_ipfs_get_block",
        hash_ptr
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

    link!("yaml.fromBytes", yaml_from_bytes, ptr);
    link!("yaml.try_fromBytes", yaml_try_from_bytes, ptr);

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

    // `arweave` and `box` functionality was removed. The implementations return deterministic
    // errors. Linked unconditionally since import_name_to_modules ensures they're only
    // registered if the module actually imports them.
    link!("arweave.transactionData", arweave_transaction_data, ptr);
    link!("box.profile", box_profile, ptr);

    // link the `gas` function
    // See also e3f03e62-40e4-4f8c-b4a1-d0375cca0b76
    linker.func_wrap(
        "gas",
        "gas",
        |mut caller: wasmtime::Caller<'_, WasmInstanceData>, gas_used: u32| -> anyhow::Result<()> {
            // Gas metering has a relevant execution cost cost, being called tens of thousands
            // of times per handler, but it's not worth having a stopwatch section here because
            // the cost of measuring would be greater than the cost of `consume_host_fn`. Last
            // time this was benchmarked it took < 100ns to run.
            if let Err(e) = caller
                .data()
                .gas
                .consume_host_fn_with_metrics(gas_used.saturating_into(), "gas")
            {
                caller.data_mut().deterministic_host_trap = true;
                return Err(e.into());
            }

            Ok(())
        },
    )?;

    Ok(linker)
}

impl WasmInstance {
    /// Instantiates the module and sets it to be interrupted after `timeout`.
    pub async fn from_valid_module_with_ctx(
        valid_module: Arc<ValidModule>,
        ctx: MappingContext,
        host_metrics: Arc<HostMetrics>,
        experimental_features: ExperimentalFeatures,
    ) -> Result<WasmInstance, anyhow::Error> {
        let engine = valid_module.module.engine();
        let api_version = ctx.host_exports.data_source.api_version.clone();

        let gas = GasCounter::new(host_metrics.gas_metrics.clone());

        let wasm_ctx = WasmInstanceData::from_instance(
            ctx,
            valid_module.cheap_clone(),
            host_metrics.cheap_clone(),
            gas.cheap_clone(),
            experimental_features,
        );
        let mut store = Store::new(engine, wasm_ctx);

        // The epoch on the engine will only ever be incremeted if increment_epoch() is explicitly
        // called, we only do so if a timeout has been set, it will run forever. When a timeout is
        // set, the timeout duration is used as the duration of one epoch.
        //
        // Therefore, the setting of 2 here means that if a `timeout` is provided, then this
        // interrupt will be triggered between a duration of `timeout` and `timeout * 2`.
        //
        // See also: runtime-timeouts
        store.set_epoch_deadline(2);

        let instance = valid_module
            .instance_pre
            .instantiate_async(store.as_context_mut())
            .await?;

        let asc_heap = AscHeapCtx::new(
            &instance,
            &mut WasmInstanceContext::new(&mut store),
            api_version.clone(),
        )?;
        store.data_mut().set_asc_heap(asc_heap);

        // See start_function comment for more information
        // TL;DR; we need the wasmtime::Instance to create the heap, therefore
        // we cannot execute anything that requires access to the heap before it's created.
        if let Some(start_func) = valid_module.start_function.as_ref() {
            instance
                .get_func(store.as_context_mut(), start_func)
                .context(format!("`{start_func}` function not found"))?
                .typed::<(), ()>(store.as_context_mut())?
                .call_async(store.as_context_mut(), ())
                .await?;
        }

        match api_version {
            version if version <= Version::new(0, 0, 4) => {}
            _ => {
                instance
                    .get_func(store.as_context_mut(), "_start")
                    .context("`_start` function not found")?
                    .typed::<(), ()>(store.as_context_mut())?
                    .call_async(store.as_context_mut(), ())
                    .await?;
            }
        }

        Ok(WasmInstance {
            instance,
            gas,
            store,
        })
    }

    /// Similar to `from_valid_module_with_ctx` but returns a boxed future.
    /// This is needed to allow mutually recursive calls of futures, e.g.,
    /// in `ipfs_map` as that is a host function that calls back into WASM
    /// code which in turn might call back into host functions.
    pub fn from_valid_module_with_ctx_boxed(
        valid_module: Arc<ValidModule>,
        ctx: MappingContext,
        host_metrics: Arc<HostMetrics>,
        experimental_features: ExperimentalFeatures,
    ) -> BoxFuture<'static, Result<WasmInstance, anyhow::Error>> {
        async move {
            WasmInstance::from_valid_module_with_ctx(
                valid_module,
                ctx,
                host_metrics,
                experimental_features,
            )
            .await
        }
        .boxed()
    }
}
