use crate::module::WasmiModule;
use ethabi::LogParam;
use futures::sync::mpsc;
use futures::sync::oneshot;
use graph::components::ethereum::*;
use graph::prelude::*;
use std::thread;
use std::time::Instant;
use web3::types::{Log, Transaction};

/// The first channel is for mapping requests, the second is a cancelation guard.
pub(crate) fn handle<E, L, S>(
    parsed_module: parity_wasm::elements::Module,
    logger: Logger,
    thread_name: String,
) -> Result<(mpsc::Sender<MappingRequest<E, L, S>>, oneshot::Sender<()>), Error>
where
    E: EthereumAdapter,
    L: LinkResolver,
    S: Store + SubgraphDeploymentStore + EthereumCallCache,
{
    let valid_module = Arc::new(ValidModule::new(parsed_module)?);

    // Create channel for canceling the module
    let (cancel_sender, cancel_receiver) = oneshot::channel();

    // Create channel for event handling requests
    let (mapping_request_sender, mapping_request_receiver) = mpsc::channel(100);

    // wasmi modules are not `Send` therefore they cannot be scheduled by
    // the regular tokio executor, so we create a dedicated thread.
    //
    // This thread can spawn tasks on the runtime by sending them to
    // `task_receiver`.
    let (task_sender, task_receiver) = mpsc::channel(100);
    tokio::spawn(task_receiver.for_each(tokio::spawn));

    // Spawn a dedicated thread for the runtime.
    //
    // In case of failure, this thread may panic or simply terminate,
    // dropping the `mapping_request_receiver` which ultimately causes the
    // subgraph to fail the next time it tries to handle an event.
    let conf = thread::Builder::new().name(thread_name);
    conf.spawn(move || {
        // Pass incoming triggers to the WASM module and return entity changes;
        // Stop when cancelled.
        let canceler = cancel_receiver.into_stream();
        mapping_request_receiver
            .select(
                canceler
                    .map(|_| panic!("WASM module thread cancelled"))
                    .map_err(|_| ()),
            )
            .map_err(|()| err_msg("Cancelled"))
            .for_each(move |request| -> Result<(), Error> {
                let MappingRequest {
                    ctx,
                    trigger,
                    result_sender,
                } = request;

                // Start the WASMI module runtime.
                let module = WasmiModule::from_valid_module_with_ctx(
                    valid_module.clone(),
                    ctx,
                    task_sender.clone(),
                )?;

                let result = match trigger {
                    MappingTrigger::Log {
                        transaction,
                        log,
                        params,
                        handler,
                    } => module.handle_ethereum_log(
                        handler.handler.as_str(),
                        transaction,
                        log,
                        params,
                    ),
                    MappingTrigger::Call {
                        transaction,
                        call,
                        inputs,
                        outputs,
                        handler,
                    } => module.handle_ethereum_call(
                        handler.handler.as_str(),
                        transaction,
                        call,
                        inputs,
                        outputs,
                    ),
                    MappingTrigger::Block { handler } => {
                        module.handle_ethereum_block(handler.handler.as_str())
                    }
                };

                result_sender
                    .send((result, future::ok(Instant::now())))
                    .map_err(|_| err_msg("WASM module result receiver dropped."))
            })
            .wait()
            .unwrap_or_else(|e| {
                debug!(logger, "WASM runtime thread terminating";
                           "reason" => e.to_string())
            });
    })
    .map(|_| ())
    .map_err(|e| format_err!("Spawning WASM runtime thread failed: {}", e))?;

    Ok((mapping_request_sender, cancel_sender))
}

#[derive(Debug)]
pub(crate) enum MappingTrigger {
    Log {
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
        handler: MappingEventHandler,
    },
    Call {
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
        handler: MappingCallHandler,
    },
    Block {
        handler: MappingBlockHandler,
    },
}

type MappingResponse = (Result<BlockState, Error>, futures::Finished<Instant, Error>);

pub(crate) struct MappingRequest<E, L, S> {
    pub(crate) ctx: MappingContext<E, L, S>,
    pub(crate) trigger: MappingTrigger,
    pub(crate) result_sender: oneshot::Sender<MappingResponse>,
}

pub(crate) struct MappingContext<E, L, S> {
    pub(crate) logger: Logger,
    pub(crate) host_exports: Arc<crate::host_exports::HostExports<E, L, S>>,
    pub(crate) block: Arc<EthereumBlock>,
    pub(crate) state: BlockState,
}

/// Cloning an `MappingContext` clones all its fields,
/// except the `state_operations`, since they are an output
/// accumulator and are therefore initialized with an empty state.
impl<E, L, S> Clone for MappingContext<E, L, S> {
    fn clone(&self) -> Self {
        MappingContext {
            logger: self.logger.clone(),
            host_exports: self.host_exports.clone(),
            block: self.block.clone(),
            state: BlockState::default(),
        }
    }
}

/// A pre-processed and valid WASM module, ready to be started as a WasmiModule.
pub(crate) struct ValidModule {
    pub(super) module: wasmi::Module,
    pub(super) user_module: Option<String>,
}

impl ValidModule {
    /// Pre-process and validate the module.
    pub fn new(parsed_module: parity_wasm::elements::Module) -> Result<Self, Error> {
        // Inject metering calls, which are used for checking timeouts.
        let parsed_module = pwasm_utils::inject_gas_counter(parsed_module, &Default::default())
            .map_err(|_| err_msg("failed to inject gas counter"))?;

        // `inject_gas_counter` injects an import so the section must exist.
        let import_section = parsed_module.import_section().unwrap().clone();

        // Hack: AS currently puts all user imports in one module, in addition
        // to the built-in "env" module. The name of that module is not fixed,
        // to able able to infer the name we allow only one module with imports,
        // with "env" being optional.
        let mut user_modules: Vec<_> = import_section
            .entries()
            .into_iter()
            .map(|import| import.module().to_owned())
            .filter(|module| module != "env")
            .collect();
        user_modules.dedup();
        let user_module = match user_modules.len() {
            0 => None,
            1 => Some(user_modules.into_iter().next().unwrap()),
            _ => return Err(err_msg("WASM module has multiple import sections")),
        };

        let module = wasmi::Module::from_parity_wasm_module(parsed_module).map_err(|e| {
            format_err!(
                "Invalid module `{}`: {}",
                user_module.as_ref().unwrap_or(&String::new()),
                e
            )
        })?;

        Ok(ValidModule {
            module,
            user_module,
        })
    }
}
