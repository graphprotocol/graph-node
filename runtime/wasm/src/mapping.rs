use crate::gas_rules::GasRules;
use crate::module::{ExperimentalFeatures, ToAscPtr, WasmInstance, WasmInstanceData};
use graph::blockchain::{BlockTime, Blockchain, HostFn};
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::data_source::{MappingTrigger, TriggerWithHandler};
use graph::futures01::sync::mpsc;
use graph::futures01::{Future as _, Stream as _};
use graph::futures03::channel::oneshot::Sender;
use graph::parking_lot::RwLock;
use graph::prelude::*;
use graph::runtime::gas::Gas;
use graph::runtime::IndexForAscTypeId;
use parity_wasm::elements::ExportEntry;
use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::{panic, thread};

/// Spawn a wasm module in its own thread.
pub fn spawn_module<C: Blockchain>(
    raw_module: &[u8],
    logger: Logger,
    subgraph_id: DeploymentHash,
    host_metrics: Arc<HostMetrics>,
    runtime: tokio::runtime::Handle,
    timeout: Option<Duration>,
    experimental_features: ExperimentalFeatures,
) -> Result<mpsc::Sender<WasmRequest<C>>, anyhow::Error>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    static THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

    let valid_module = Arc::new(ValidModule::new(&logger, raw_module, timeout)?);

    // Create channel for event handling requests
    let (mapping_request_sender, mapping_request_receiver) = mpsc::channel(100);

    // It used to be that we had to create a dedicated thread since wasmtime
    // instances were not `Send` and could therefore not be scheduled by the
    // regular tokio executor. This isn't an issue anymore, but we still
    // spawn a dedicated thread since running WASM code async can block and
    // lock up the executor. See [the wasmtime
    // docs](https://docs.rs/wasmtime/latest/wasmtime/struct.Config.html#execution-in-poll)
    // on how this should be handled properly. As that is a fairly large
    // change to how we use wasmtime, we keep the threading model for now.
    // Once we are confident that things are working that way, we should
    // revisit this and remove the dedicated thread.
    //
    // In case of failure, this thread may panic or simply terminate,
    // dropping the `mapping_request_receiver` which ultimately causes the
    // subgraph to fail the next time it tries to handle an event.
    let next_id = THREAD_COUNT.fetch_add(1, Ordering::SeqCst);
    let conf = thread::Builder::new().name(format!("mapping-{}-{:0>4}", &subgraph_id, next_id));
    conf.spawn(move || {
        let _runtime_guard = runtime.enter();

        // Pass incoming triggers to the WASM module and return entity changes;
        // Stop when canceled because all RuntimeHosts and their senders were dropped.
        match mapping_request_receiver
            .map_err(|()| unreachable!())
            .for_each(move |request| {
                let WasmRequest {
                    ctx,
                    inner,
                    result_sender,
                } = request;
                let logger = ctx.logger.clone();

                let handle_fut = async {
                    let result = instantiate_module::<C>(
                        valid_module.cheap_clone(),
                        ctx,
                        host_metrics.cheap_clone(),
                        experimental_features,
                    )
                    .await;
                    match result {
                        Ok(module) => match inner {
                            WasmRequestInner::TriggerRequest(trigger) => {
                                handle_trigger(&logger, module, trigger, host_metrics.cheap_clone())
                                    .await
                            }
                        },
                        Err(e) => Err(MappingError::Unknown(e)),
                    }
                };
                let result = panic::catch_unwind(AssertUnwindSafe(|| graph::block_on(handle_fut)));

                let result = match result {
                    Ok(result) => result,
                    Err(panic_info) => {
                        let err_msg = if let Some(payload) = panic_info
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or(panic_info.downcast_ref::<&str>().copied())
                        {
                            anyhow!("Subgraph panicked with message: {}", payload)
                        } else {
                            anyhow!("Subgraph panicked with an unknown payload.")
                        };
                        Err(MappingError::Unknown(err_msg))
                    }
                };

                result_sender
                    .send(result)
                    .map_err(|_| anyhow::anyhow!("WASM module result receiver dropped."))
            })
            .wait()
        {
            Ok(()) => debug!(logger, "Subgraph stopped, WASM runtime thread terminated"),
            Err(e) => debug!(logger, "WASM runtime thread terminated abnormally";
                                    "error" => e.to_string()),
        }
    })
    .map(|_| ())
    .context("Spawning WASM runtime thread failed")?;

    Ok(mapping_request_sender)
}

async fn instantiate_module<C: Blockchain>(
    valid_module: Arc<ValidModule>,
    ctx: MappingContext,
    host_metrics: Arc<HostMetrics>,
    experimental_features: ExperimentalFeatures,
) -> Result<WasmInstance, anyhow::Error>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    // Start the WASM module runtime.
    let _section = host_metrics.stopwatch.start_section("module_init");
    WasmInstance::from_valid_module_with_ctx(
        valid_module,
        ctx,
        host_metrics.cheap_clone(),
        experimental_features,
    )
    .await
    .context("module instantiation failed")
}

async fn handle_trigger<C: Blockchain>(
    logger: &Logger,
    module: WasmInstance,
    trigger: TriggerWithHandler<MappingTrigger<C>>,
    host_metrics: Arc<HostMetrics>,
) -> Result<(BlockState, Gas), MappingError>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    let logger = logger.cheap_clone();

    let _section = host_metrics.stopwatch.start_section("run_handler");
    if ENV_VARS.log_trigger_data {
        debug!(logger, "trigger data: {:?}", trigger);
    }
    module.handle_trigger(trigger).await
}

pub struct WasmRequest<C: Blockchain> {
    pub(crate) ctx: MappingContext,
    pub(crate) inner: WasmRequestInner<C>,
    pub(crate) result_sender: Sender<Result<(BlockState, Gas), MappingError>>,
}

impl<C: Blockchain> WasmRequest<C> {
    pub(crate) fn new_trigger(
        ctx: MappingContext,
        trigger: TriggerWithHandler<MappingTrigger<C>>,
        result_sender: Sender<Result<(BlockState, Gas), MappingError>>,
    ) -> Self {
        WasmRequest {
            ctx,
            inner: WasmRequestInner::TriggerRequest(trigger),
            result_sender,
        }
    }
}

pub enum WasmRequestInner<C: Blockchain> {
    TriggerRequest(TriggerWithHandler<MappingTrigger<C>>),
}

pub struct MappingContext {
    pub logger: Logger,
    pub host_exports: Arc<crate::host_exports::HostExports>,
    pub block_ptr: BlockPtr,
    pub timestamp: BlockTime,
    pub state: BlockState,
    pub proof_of_indexing: SharedProofOfIndexing,
    pub host_fns: Arc<Vec<HostFn>>,
    pub debug_fork: Option<Arc<dyn SubgraphFork>>,
    /// Logger for messages coming from mappings
    pub mapping_logger: Logger,
    /// Whether to log details about host fn execution
    pub instrument: bool,
}

impl MappingContext {
    pub fn derive_with_empty_block_state(&self) -> Self {
        MappingContext {
            logger: self.logger.cheap_clone(),
            host_exports: self.host_exports.cheap_clone(),
            block_ptr: self.block_ptr.cheap_clone(),
            timestamp: self.timestamp,
            state: BlockState::new(self.state.entity_cache.store.clone(), Default::default()),
            proof_of_indexing: self.proof_of_indexing.cheap_clone(),
            host_fns: self.host_fns.cheap_clone(),
            debug_fork: self.debug_fork.cheap_clone(),
            mapping_logger: Logger::new(&self.logger, o!("component" => "UserMapping")),
            instrument: self.instrument,
        }
    }
}

// See the start_index comment below for more information.
const GN_START_FUNCTION_NAME: &str = "gn::start";

/// Ensure the epoch counter task is running. The first call with a timeout
/// starts the task; subsequent calls are no-ops (the interval is locked in
/// by the first caller).
fn ensure_epoch_counter(engine: &wasmtime::Engine, timeout: Duration) {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let engine = engine.clone();
        graph::spawn(async move {
            loop {
                tokio::time::sleep(timeout).await;
                engine.increment_epoch();
            }
        });
    });
}

/// Returns a shared wasmtime Engine used by all ValidModules. The engine is
/// created once on first access and reused for the lifetime of the process.
/// Sharing the engine means all modules share a single pooling allocator
/// pool and a single epoch counter task.
fn shared_engine() -> &'static wasmtime::Engine {
    static ENGINE: OnceLock<wasmtime::Engine> = OnceLock::new();

    ENGINE.get_or_init(|| {
        let opt_level = match ENV_VARS.mappings.wasm_opt_level {
            graph::env::WasmOptLevel::None => wasmtime::OptLevel::None,
            graph::env::WasmOptLevel::Speed => wasmtime::OptLevel::Speed,
            graph::env::WasmOptLevel::SpeedAndSize => wasmtime::OptLevel::SpeedAndSize,
        };

        let mut config = wasmtime::Config::new();
        config.strategy(wasmtime::Strategy::Cranelift);
        config.epoch_interruption(true);
        config.cranelift_nan_canonicalization(true); // For NaN determinism.
        config.cranelift_opt_level(opt_level);
        config.max_wasm_stack(ENV_VARS.mappings.max_stack_size);
        config.async_support(true);

        // Use the pooling allocator to reuse pre-allocated instance slots
        // instead of mmap/munmap per trigger. This significantly reduces
        // per-trigger instantiation cost.
        let pool_size = ENV_VARS.mappings.wasm_instance_pool_size;
        let mut pool = wasmtime::PoolingAllocationConfig::new();
        pool.total_core_instances(pool_size);
        pool.total_memories(pool_size);
        pool.total_tables(pool_size);
        pool.total_stacks(pool_size);
        config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(pool));

        let engine = wasmtime::Engine::new(&config).expect("failed to create wasmtime engine");

        // Start the global epoch counter task if a timeout is configured.
        // The epoch is incremented at the configured timeout interval;
        // Stores set their deadline to 2 epochs so effective timeout is
        // between 1x and 2x the interval. See also: runtime-timeouts
        if let Some(timeout) = ENV_VARS.mappings.timeout {
            ensure_epoch_counter(&engine, timeout);
        }

        engine
    })
}

/// A pre-processed and valid WASM module, ready to be started as a WasmModule.
pub struct ValidModule {
    pub module: wasmtime::Module,

    /// Pre-linked instance template. Created once at module validation time and reused for every
    /// trigger instantiation, avoiding the cost of rebuilding the linker (~60 host function
    /// registrations) and resolving imports on each trigger.
    pub instance_pre: wasmtime::InstancePre<WasmInstanceData>,

    // Due to our internal architecture we don't want to run the start function at instantiation time,
    // so we track it separately so that we can run it at an appropriate time.
    // Since the start function is not an export, we will also create an export for it.
    // It's an option because start might not be present.
    pub start_function: Option<String>,

    // A wasm import consists of a `module` and a `name`. AS will generate imports such that they
    // have `module` set to the name of the file it is imported from and `name` set to the imported
    // function name or `namespace.function` if inside a namespace. We'd rather not specify names of
    // source files, so we consider that the import `name` uniquely identifies an import. Still we
    // need to know the `module` to properly link it, so here we map import names to modules.
    //
    // AS now has an `@external("module", "name")` decorator which would make things cleaner, but
    // the ship has sailed.
    pub import_name_to_modules: BTreeMap<String, Vec<String>>,

    // The timeout for the module.
    pub timeout: Option<Duration>,

    /// Cache for asc_type_id results. Maps IndexForAscTypeId to their WASM runtime
    /// type IDs. Populated lazily on first use; deterministic per compiled module.
    asc_type_id_cache: RwLock<HashMap<IndexForAscTypeId, u32>>,
}

impl ValidModule {
    /// Pre-process and validate the module.
    pub fn new(
        logger: &Logger,
        raw_module: &[u8],
        timeout: Option<Duration>,
    ) -> Result<Self, anyhow::Error> {
        // Add the gas calls here. Module name "gas" must match. See also
        // e3f03e62-40e4-4f8c-b4a1-d0375cca0b76. We do this by round-tripping the module through
        // parity - injecting gas then serializing again.
        let parity_module = parity_wasm::elements::Module::from_bytes(raw_module)?;
        let mut parity_module = match parity_module.parse_names() {
            Ok(module) => module,
            Err((errs, module)) => {
                for (index, err) in errs {
                    warn!(
                        logger,
                        "unable to parse function name for index {}: {}",
                        index,
                        err.to_string()
                    );
                }

                module
            }
        };

        let start_function = parity_module.start_section().map(|index| {
            let name = GN_START_FUNCTION_NAME.to_string();

            parity_module.clear_start_section();
            parity_module
                .export_section_mut()
                .unwrap()
                .entries_mut()
                .push(ExportEntry::new(
                    name.clone(),
                    parity_wasm::elements::Internal::Function(index),
                ));

            name
        });
        let parity_module = wasm_instrument::gas_metering::inject(parity_module, &GasRules, "gas")
            .map_err(|_| anyhow!("Failed to inject gas counter"))?;
        let raw_module = parity_module.into_bytes()?;

        let engine = shared_engine();
        if let Some(timeout) = timeout {
            ensure_epoch_counter(engine, timeout);
        }
        let module = wasmtime::Module::from_binary(engine, &raw_module)?;

        let mut import_name_to_modules: BTreeMap<String, Vec<String>> = BTreeMap::new();

        // Unwrap: Module linking is disabled.
        for (name, module) in module
            .imports()
            .map(|import| (import.name(), import.module()))
        {
            import_name_to_modules
                .entry(name.to_string())
                .or_default()
                .push(module.to_string());
        }

        let linker = crate::module::build_linker(engine, &import_name_to_modules)?;
        let instance_pre = linker.instantiate_pre(&module)?;

        Ok(ValidModule {
            module,
            instance_pre,
            import_name_to_modules,
            start_function,
            timeout,
            asc_type_id_cache: RwLock::new(HashMap::new()),
        })
    }

    pub fn get_cached_type_id(&self, idx: IndexForAscTypeId) -> Option<u32> {
        self.asc_type_id_cache.read().get(&idx).copied()
    }

    pub fn cache_type_id(&self, idx: IndexForAscTypeId, type_id: u32) {
        self.asc_type_id_cache.write().insert(idx, type_id);
    }
}
