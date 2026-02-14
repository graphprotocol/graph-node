use crate::gas_rules::GasRules;
use crate::module::WasmInstanceData;
use graph::blockchain::{BlockTime, HostFn};
use graph::components::store::SubgraphFork;
use graph::components::subgraph::SharedProofOfIndexing;
use graph::parking_lot::RwLock;
use graph::prelude::*;
use graph::runtime::IndexForAscTypeId;
use parity_wasm::elements::ExportEntry;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

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
