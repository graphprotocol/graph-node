use crate::module::{ExperimentalFeatures, WasmInstance};
use futures::sync::mpsc;
use futures03::channel::oneshot::Sender;
use graph::blockchain::{Blockchain, HostFn};
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

lazy_static! {
    /// Verbose logging of mapping inputs
    pub static ref LOG_TRIGGER_DATA: bool = std::env::var("GRAPH_LOG_TRIGGER_DATA").is_ok();
}

/// Spawn a wasm module in its own thread.
pub fn spawn_module<C: Blockchain>(
    raw_module: Vec<u8>,
    logger: Logger,
    subgraph_id: DeploymentHash,
    host_metrics: Arc<HostMetrics>,
    runtime: tokio::runtime::Handle,
    timeout: Option<Duration>,
    experimental_features: ExperimentalFeatures,
) -> Result<mpsc::Sender<MappingRequest<C>>, anyhow::Error> {
    let valid_module = Arc::new(ValidModule::new(&raw_module)?);

    // Create channel for event handling requests
    let (mapping_request_sender, mapping_request_receiver) = mpsc::channel(100);

    // wasmtime instances are not `Send` therefore they cannot be scheduled by
    // the regular tokio executor, so we create a dedicated thread.
    //
    // In case of failure, this thread may panic or simply terminate,
    // dropping the `mapping_request_receiver` which ultimately causes the
    // subgraph to fail the next time it tries to handle an event.
    let conf =
        thread::Builder::new().name(format!("mapping-{}-{}", &subgraph_id, uuid::Uuid::new_v4()));
    conf.spawn(move || {
        runtime.enter(|| {
            // Pass incoming triggers to the WASM module and return entity changes;
            // Stop when canceled because all RuntimeHosts and their senders were dropped.
            match mapping_request_receiver
                .map_err(|()| unreachable!())
                .for_each(move |request| {
                    let MappingRequest {
                        ctx,
                        trigger,
                        result_sender,
                    } = request;
                    let logger = ctx.logger.cheap_clone();

                    // Start the WASM module runtime.
                    let section = host_metrics.stopwatch.start_section("module_init");
                    let module = WasmInstance::from_valid_module_with_ctx(
                        valid_module.cheap_clone(),
                        ctx,
                        host_metrics.cheap_clone(),
                        timeout,
                        experimental_features.clone(),
                    )?;
                    section.end();

                    let section = host_metrics.stopwatch.start_section("run_handler");
                    if *LOG_TRIGGER_DATA {
                        debug!(logger, "trigger data: {:?}", trigger);
                    }
                    let result = module.handle_trigger(trigger);
                    section.end();

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
    })
    .map(|_| ())
    .context("Spawning WASM runtime thread failed")?;

    Ok(mapping_request_sender)
}

pub struct MappingRequest<C: Blockchain> {
    pub(crate) ctx: MappingContext<C>,
    pub(crate) trigger: C::MappingTrigger,
    pub(crate) result_sender: Sender<Result<BlockState<C>, MappingError>>,
}

pub struct MappingContext<C: Blockchain> {
    pub logger: Logger,
    pub host_exports: Arc<crate::host_exports::HostExports<C>>,
    pub block_ptr: BlockPtr,
    pub state: BlockState<C>,
    pub proof_of_indexing: SharedProofOfIndexing,
    pub host_fns: Arc<Vec<HostFn>>,
}

impl<C: Blockchain> MappingContext<C> {
    pub fn derive_with_empty_block_state(&self) -> Self {
        MappingContext {
            logger: self.logger.cheap_clone(),
            host_exports: self.host_exports.cheap_clone(),
            block_ptr: self.block_ptr.cheap_clone(),
            state: BlockState::new(self.state.entity_cache.store.clone(), Default::default()),
            proof_of_indexing: self.proof_of_indexing.cheap_clone(),
            host_fns: self.host_fns.cheap_clone(),
        }
    }
}

/// A pre-processed and valid WASM module, ready to be started as a WasmModule.
pub struct ValidModule {
    pub(super) module: wasmtime::Module,

    // A wasm import consists of a `module` and a `name`. AS will generate imports such that they
    // have `module` set to the name of the file it is imported from and `name` set to the imported
    // function name or `namespace.function` if inside a namespace. We'd rather not specify names of
    // source files, so we consider that the import `name` uniquely identifies an import. Still we
    // need to know the `module` to properly link it, so here we map import names to modules.
    //
    // AS now has an `@external("module", "name")` decorator which would make things cleaner, but
    // the ship has sailed.
    pub(super) import_name_to_modules: BTreeMap<String, Vec<String>>,
}

impl ValidModule {
    /// Pre-process and validate the module.
    pub fn new(raw_module: &[u8]) -> Result<Self, anyhow::Error> {
        // We currently use Cranelift as a compilation engine. Cranelift is an optimizing compiler,
        // but that should not cause determinism issues since it adheres to the Wasm spec. Still we
        // turn off optional optimizations to be conservative.
        let mut config = wasmtime::Config::new();
        config.strategy(wasmtime::Strategy::Cranelift).unwrap();
        config.interruptable(true); // For timeouts.
        config.cranelift_nan_canonicalization(true); // For NaN determinism.
        config.cranelift_opt_level(wasmtime::OptLevel::None);
        let engine = &wasmtime::Engine::new(&config)?;
        let module = wasmtime::Module::from_binary(&engine, raw_module)?;

        let mut import_name_to_modules: BTreeMap<String, Vec<String>> = BTreeMap::new();

        // Unwrap: Module linking is disabled.
        for (name, module) in module
            .imports()
            .map(|import| (import.name().unwrap(), import.module()))
        {
            import_name_to_modules
                .entry(name.to_string())
                .or_default()
                .push(module.to_string());
        }

        Ok(ValidModule {
            module,
            import_name_to_modules,
        })
    }
}
