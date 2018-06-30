use futures::prelude::*;
use futures::sync::mpsc::Sender;
use parity_wasm;
use slog::Logger;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;
use wasmi::{
    Error, Externals, FuncInstance, FuncRef, ImportsBuilder, MemoryRef, Module,
    ModuleImportResolver, ModuleInstance, ModuleRef, NopExternals, RuntimeArgs, RuntimeValue,
    Signature, Trap, ValueType,
};

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::ethereum::*;
use thegraph::components::store::StoreKey;
use thegraph::prelude::*;

use asc_abi::asc_ptr::*;
use asc_abi::class::*;
use asc_abi::*;

/// AssemblyScript-compatible WASM memory heap.
#[derive(Clone)]
struct WasmiAscHeap {
    module: ModuleRef,
    memory: MemoryRef,
}

impl WasmiAscHeap {
    pub fn new(module: ModuleRef, memory: MemoryRef) -> Self {
        WasmiAscHeap { module, memory }
    }
}

impl AscHeap for WasmiAscHeap {
    fn raw_new(&self, bytes: &[u8]) -> Result<u32, Error> {
        let address = self.module
            .invoke_export(
                "allocate_memory",
                &[RuntimeValue::I32(bytes.len() as i32)],
                &mut NopExternals,
            )
            .expect("Failed to invoke memory allocation function")
            .expect("Function did not return a value")
            .try_into::<u32>()
            .expect("Function did not return u32");

        self.memory.set(address, bytes)?;

        Ok(address)
    }

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, Error> {
        self.memory.get(offset, size as usize)
    }
}

// Indexes for exported host functions
const ABORT_FUNC_INDEX: usize = 0;
const DATABASE_CREATE_FUNC_INDEX: usize = 1;
const DATABASE_UPDATE_FUNC_INDEX: usize = 2;
const DATABASE_REMOVE_FUNC_INDEX: usize = 3;
const ETHEREUM_CALL_FUNC_INDEX: usize = 4;

pub struct WasmiModuleConfig<T> {
    pub data_source_id: String,
    pub runtime: Handle,
    pub event_sink: Sender<RuntimeHostEvent>,
    pub ethereum_adapter: Arc<Mutex<T>>,
}

/// A WASM module based on wasmi that powers a data source runtime.
pub struct WasmiModule<T> {
    pub logger: Logger,
    pub module: ModuleRef,
    externals: HostExternals<T>,
    heap: WasmiAscHeap,
}

impl<T> WasmiModule<T>
where
    T: EthereumAdapter,
{
    /// Creates a new wasmi module
    pub fn new(path: PathBuf, logger: &Logger, config: WasmiModuleConfig<T>) -> Self {
        let logger = logger.new(o!("component" => "WasmiModule"));

        let module = parity_wasm::deserialize_file(&path)
            .expect(format!("Failed to deserialize WASM: {}", path.to_string_lossy()).as_str());
        let module = Module::from_parity_wasm_module(module).expect(
            format!(
                "Wasmi could not interpret module loaded from file: {}",
                path.to_string_lossy()
            ).as_str(),
        );

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &EnvModuleResolver);
        imports.push_resolver("database", &StoreModuleResolver);
        imports.push_resolver("ethereum", &EthereumModuleResolver);

        // Instantiate the runtime module using hosted functions and import resolver
        let module =
            ModuleInstance::new(&module, &imports).expect("Failed to instantiate WASM module");

        // Provide access to the WASM runtime linear memory
        let not_started_module = module.not_started_instance().clone();
        let memory = not_started_module
            .export_by_name("memory")
            .expect("Failed to find memory export in the WASM module")
            .as_memory()
            .expect("Export \"memory\" has an invalid type")
            .clone();

        // Create a AssemblyScript-compatible WASM memory heap
        let heap = WasmiAscHeap::new(not_started_module, memory);

        // Create new instance of externally hosted functions invoker
        let mut externals = HostExternals {
            data_source_id: config.data_source_id.clone(),
            logger: logger.clone(),
            runtime: config.runtime.clone(),
            event_sink: config.event_sink.clone(),
            heap: heap.clone(),
            _ethereum_adapter: config.ethereum_adapter.clone(),
        };

        let module = module
            .run_start(&mut externals)
            .expect("Failed to start WASM module instance");

        WasmiModule {
            logger,
            module,
            externals,
            heap,
        }
    }

    pub fn handle_ethereum_event(&mut self, handler_name: &str, event: EthereumEvent) {
        self.module
            .invoke_export(
                handler_name,
                &[RuntimeValue::from(self.heap.asc_new(&event))],
                &mut self.externals,
            )
            .expect(
                format!(
                    "Failed to invoke call Ethereum event handler: {}",
                    handler_name
                ).as_str(),
            );
    }
}

// Placeholder for deserializer module
pub struct WasmConverter {}

impl WasmConverter {
    // Put StoreKey into linear memory and return a u32 pointer
    pub fn _store_key_to_memory(_key: StoreKey) -> RuntimeValue {
        unimplemented!();
    }
    // Get StoreKey from .wasm pointer
    pub fn store_key_from_wasm(_pointer: u32) -> StoreKey {
        unimplemented!();
    }
    // Get Entity from .wasm pointer
    pub fn entity_from_wasm(_pointer: u32) -> Entity {
        unimplemented!();
    }
}

/// Hosted functions for external use by wasm module
pub struct HostExternals<T> {
    logger: Logger,
    runtime: Handle,
    data_source_id: String,
    event_sink: Sender<RuntimeHostEvent>,
    heap: WasmiAscHeap,
    _ethereum_adapter: Arc<Mutex<T>>,
}

impl<T> HostExternals<T>
where
    T: EthereumAdapter,
{
    fn database_create(
        &self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscTypedMap<AscString, AscEnum<StoreValueKind>>>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let entity: String = self.heap.asc_get(entity_ptr);
        let id: String = self.heap.asc_get(id_ptr);
        let data: HashMap<String, Value> = self.heap.asc_get(data_ptr);

        let store_key = StoreKey {
            entity: entity,
            id: id.clone(),
        };

        let entity_data = Entity::from(data);

        // Send an entity created event
        let logger = self.logger.clone();
        self.runtime.spawn(
            self.event_sink
                .clone()
                .send(RuntimeHostEvent::EntityCreated(
                    self.data_source_id.clone(),
                    store_key,
                    entity_data,
                ))
                .map_err(move |e| {
                    error!(logger, "Failed to forward runtime host event";
                                        "error" => format!("{}", e));
                })
                .and_then(|_| Ok(())),
        );

        Ok(None)
    }

    fn database_update(
        &self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscTypedMap<AscString, AscEnum<StoreValueKind>>>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let entity: String = self.heap.asc_get(entity_ptr);
        let id: String = self.heap.asc_get(id_ptr);
        let data: HashMap<String, Value> = self.heap.asc_get(data_ptr);

        let store_key = StoreKey {
            entity: entity,
            id: id.clone(),
        };

        let entity_data = Entity::from(data);

        // Send an entity changed event
        let logger = self.logger.clone();
        self.runtime.spawn(
            self.event_sink
                .clone()
                .send(RuntimeHostEvent::EntityChanged(
                    self.data_source_id.clone(),
                    store_key,
                    entity_data,
                ))
                .map_err(move |e| {
                    error!(logger, "Failed to forward runtime host event";
                                        "error" => format!("{}", e));
                })
                .and_then(|_| Ok(())),
        );

        Ok(None)
    }

    fn database_remove(
        &self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let entity: String = self.heap.asc_get(entity_ptr);
        let id: String = self.heap.asc_get(id_ptr);

        let store_key = StoreKey {
            entity: entity,
            id: id,
        };

        // Send an entity removed event
        let logger = self.logger.clone();
        self.runtime.spawn(
            self.event_sink
                .clone()
                .send(RuntimeHostEvent::EntityRemoved(
                    self.data_source_id.clone(),
                    store_key,
                ))
                .map_err(move |e| {
                    error!(logger, "Failed to forward runtime host event";
                               "error" => format!("{}", e));
                })
                .and_then(|_| Ok(())),
        );

        Ok(None)
    }
}

impl<T> Externals for HostExternals<T>
where
    T: EthereumAdapter,
{
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let logger = self.logger.clone();

        match index {
            DATABASE_CREATE_FUNC_INDEX => self.database_create(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
            ),
            DATABASE_UPDATE_FUNC_INDEX => self.database_update(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
            ),
            DATABASE_REMOVE_FUNC_INDEX => {
                self.database_remove(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            ETHEREUM_CALL_FUNC_INDEX => {
                let _request_ptr: u32 = args.nth_checked(0)?;
                // TODO: self.ethereum_adapter.lock().unwrap().contract_call();
                Ok(None)
            }
            _ => panic!("Unimplemented function at {}", index),
        }
    }
}

/// Env module resolver
pub struct EnvModuleResolver;

impl ModuleImportResolver for EnvModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "abort" => FuncInstance::alloc_host(
                Signature::new(
                    &[
                        ValueType::I32,
                        ValueType::I32,
                        ValueType::I32,
                        ValueType::I32,
                    ][..],
                    None,
                ),
                ABORT_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )));
            }
        })
    }
}

/// Database module resolver
pub struct StoreModuleResolver;

impl ModuleImportResolver for StoreModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "create" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32][..], None),
                DATABASE_CREATE_FUNC_INDEX,
            ),
            "update" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32][..], None),
                DATABASE_UPDATE_FUNC_INDEX,
            ),
            "remove" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                DATABASE_REMOVE_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}

/// Ethereum module resolver
pub struct EthereumModuleResolver;

impl ModuleImportResolver for EthereumModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "call" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ETHEREUM_CALL_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use ethabi::{LogParam, Token};
    use ethereum_types::Address;
    use futures::prelude::*;
    use futures::sync::mpsc::channel;
    use slog;
    use std::collections::HashMap;
    use std::iter::FromIterator;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tokio_core;

    use thegraph::components::data_sources::*;
    use thegraph::components::ethereum::*;
    use thegraph::components::store::*;
    use thegraph::prelude::*;
    use thegraph::util;

    use super::{WasmiModule, WasmiModuleConfig};

    #[derive(Default)]
    struct MockEthereumAdapter {}

    impl EthereumAdapter for MockEthereumAdapter {
        fn contract_state(
            &mut self,
            _request: EthereumContractStateRequest,
        ) -> Result<EthereumContractState, EthereumContractStateError> {
            unimplemented!()
        }

        fn contract_call(
            &mut self,
            _request: EthereumContractCallRequest,
        ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>> {
            unimplemented!()
        }

        fn subscribe_to_event(
            &mut self,
            _subscription: EthereumEventSubscription,
        ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError>> {
            unimplemented!()
        }

        fn unsubscribe_from_event(&mut self, _subscription_id: String) -> bool {
            false
        }
    }

    #[test]
    fn call_event_handler_and_receive_database_event() {
        // Load the example_event_handler.wasm test module. All this module does
        // is implement an `handleExampleEvent` function that calls `database.create()`
        // with sample data taken from the event parameters.
        //
        // This test verifies that the event is delivered and the example data
        // is returned to the RuntimeHostEvent stream.

        // Load the module
        let logger = slog::Logger::root(slog::Discard, o!());
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let path = PathBuf::from("test/example_event_handler.wasm");
        let (sender, receiver) = channel(1);
        let mock_ethereum_adapter = Arc::new(Mutex::new(MockEthereumAdapter::default()));
        let mut module = WasmiModule::new(
            path,
            &logger,
            WasmiModuleConfig {
                data_source_id: String::from("example data source"),
                runtime: core.handle(),
                event_sink: sender,
                ethereum_adapter: mock_ethereum_adapter,
            },
        );

        // Create a mock Ethereum event
        let ethereum_event = EthereumEvent {
            address: Address::from("22843e74c59580b3eaf6c233fa67d8b7c561a835"),
            event_signature: util::ethereum::string_to_h256("ExampleEvent(string)"),
            block_hash: util::ethereum::string_to_h256("example block hash"),
            params: vec![LogParam {
                name: String::from("exampleParam"),
                value: Token::String(String::from("some data")),
            }],
        };

        // Call the event handler in the test module and pass the event to it
        module.handle_ethereum_event("handleExampleEvent", ethereum_event);

        // Expect a database create call to be made by the handler and a
        // RuntimeHostEvent::EntityCreated event to be written to the event stream
        let work = receiver.take(1).into_future();
        let database_event = core.run(work)
            .expect("No database event received from runtime")
            .0
            .expect("Database event must not be None");

        // Verify that this event matches what the test module is sending
        assert_eq!(
            database_event,
            RuntimeHostEvent::EntityCreated(
                String::from("example data source"),
                StoreKey {
                    entity: String::from("ExampleEntity"),
                    id: String::from("example id"),
                },
                Entity::from(HashMap::from_iter(
                    vec![(String::from("exampleAttribute"), Value::from("some data"))].into_iter()
                ))
            )
        );
    }
}
