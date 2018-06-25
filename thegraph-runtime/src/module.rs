use futures::prelude::*;
use futures::sync::mpsc::Sender;
use parity_wasm;
use slog::Logger;
use std::path::PathBuf;
use tokio_core::reactor::Handle;
use wasmi::{
    Error, Externals, FuncInstance, FuncRef, ImportsBuilder, Module, ModuleImportResolver,
    ModuleInstance, ModuleRef, NopExternals, RuntimeArgs, RuntimeValue, Signature, Trap, ValueType,
};

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::store::StoreKey;
use thegraph::prelude::*;

// Indexes for exported host functions
const ABORT_FUNC_INDEX: usize = 0;
const DATABASE_CREATE_FUNC_INDEX: usize = 1;
const DATABASE_UPDATE_FUNC_INDEX: usize = 2;
const DATABASE_DELETE_FUNC_INDEX: usize = 3;
const ETHEREUM_CALL_FUNC_INDEX: usize = 4;

pub struct WasmiModuleConfig {
    pub data_source_id: String,
    pub runtime: Handle,
    pub event_sink: Sender<RuntimeHostEvent>,
}

/// Wasm runtime module
pub struct WasmiModule {
    _logger: Logger,
    _config: WasmiModuleConfig,
    pub module: ModuleRef,
}

impl WasmiModule {
    /// Creates a new wasmi module
    pub fn new(path: PathBuf, logger: &Logger, config: WasmiModuleConfig) -> Self {
        let logger = logger.new(o!("component" => "WasmiModule"));

        let module = parity_wasm::deserialize_file(&path).expect("Failed to deserialize WASM file");
        let loaded_module = Module::from_parity_wasm_module(module)
            .expect("Invalid parity_wasm module; Wasmi could not interpret");

        // Create new instance of externally hosted functions invoker
        let mut external_functions = HostExternals {
            data_source_id: config.data_source_id.clone(),
            logger: logger.clone(),
            runtime: config.runtime.clone(),
            event_sink: config.event_sink.clone(),
        };

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &EnvModuleResolver);
        imports.push_resolver("database", &DatabaseModuleResolver);
        imports.push_resolver("ethereum", &EthereumModuleResolver);

        // Instantiate the runtime module using hosted functions and import resolver
        let module = ModuleInstance::new(&loaded_module, &imports)
            .expect("Failed to instantiate WASM module")
            .run_start(&mut external_functions)
            .expect("Failed to start WASM module instance");

        WasmiModule {
            _logger: logger,
            _config: config,
            module,
        }
    }

    // Expose the allocate memory function exported from .wasm for memory management
    pub fn _allocate_memory(&self, size: i32) -> i32 {
        self.module
            .invoke_export("malloc", &[RuntimeValue::I32(size)], &mut NopExternals)
            .expect("Failed to invoke memory allocation function")
            .expect("Function did not return a value")
            .try_into::<i32>()
            .expect("Function return value was not expected type, u32")
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
pub struct HostExternals {
    logger: Logger,
    runtime: Handle,
    data_source_id: String,
    event_sink: Sender<RuntimeHostEvent>,
}

impl Externals for HostExternals {
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let logger = self.logger.clone();

        match index {
            DATABASE_CREATE_FUNC_INDEX => {
                println!("DATABASE_CREATE");

                let store_key_ptr: u32 = args.nth_checked(0)?;
                let store_key = WasmConverter::store_key_from_wasm(store_key_ptr);
                let entity_ptr: u32 = args.nth_checked(1)?;
                let entity = WasmConverter::entity_from_wasm(entity_ptr);

                self.runtime.spawn(
                    self.event_sink
                        .clone()
                        .send(RuntimeHostEvent::EntityCreated(
                            self.data_source_id.clone(),
                            store_key,
                            entity,
                        ))
                        .map_err(move |e| {
                            error!(logger, "Failed to forward runtime host event";
                                        "error" => format!("{}", e));
                        })
                        .and_then(|_| Ok(())),
                );

                Ok(None)
            }
            DATABASE_UPDATE_FUNC_INDEX => {
                println!("DATABASE_UPDATE");

                let store_key_ptr: u32 = args.nth_checked(0)?;
                let store_key = WasmConverter::store_key_from_wasm(store_key_ptr);
                let entity_ptr: u32 = args.nth_checked(1)?;
                let entity = WasmConverter::entity_from_wasm(entity_ptr);

                self.runtime.spawn(
                    self.event_sink
                        .clone()
                        .send(RuntimeHostEvent::EntityChanged(
                            self.data_source_id.clone(),
                            store_key,
                            entity,
                        ))
                        .map_err(move |e| {
                            error!(logger, "Failed to forward runtime host event";
                                   "error" => format!("{}", e));
                        })
                        .and_then(|_| Ok(())),
                );

                Ok(None)
            }
            DATABASE_DELETE_FUNC_INDEX => {
                println!("DATABASE_DELETE");

                let store_key_ptr: u32 = args.nth_checked(0)?;
                let store_key = WasmConverter::store_key_from_wasm(store_key_ptr);

                // Send a delete entity event
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
pub struct DatabaseModuleResolver;

impl ModuleImportResolver for DatabaseModuleResolver {
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
            "delete" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                DATABASE_DELETE_FUNC_INDEX,
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
    use super::WasmiModule;
    use futures::prelude::*;
    use futures::sync::mpsc::channel;
    use module;
    use slog;
    use thegraph::components::data_sources::RuntimeHostEvent;
    use thegraph::components::store::StoreKey;
    use thegraph::data::store::{Entity, Value};
    use wasmi::{NopExternals, RuntimeValue};

    #[test]
    fn run_simple_function_exported_from_wasm_and_return_result() {
        let logger = slog::Logger::root(slog::Discard, o!());
        let wasm_location = "test/add_fn.wasm";
        let (sender, _receiver) = channel(10);

        debug!(logger, "Instantiate wasm module from file";
               "file_location" => format!("{:?}", wasm_location));
        let main = WasmiModule::new(String::from(wasm_location), sender);

        debug!(
            logger,
            "Invoke exported function, find the sum of two integers."
        );
        let sum = main.module
            .invoke_export(
                "add",
                &[RuntimeValue::I32(8 as i32), RuntimeValue::I32(3 as i32)],
                &mut NopExternals,
            )
            .expect("Failed to invoke memory allocation function.")
            .expect("Function did not return a value.")
            .try_into::<i32>()
            .expect("Function return value was not expected type, u32.");

        assert_eq!(11 as i32, sum);
    }

    #[test]
    fn exported_function_create_entity_method_emits_an_entity_created_event() {
        let (sender, receiver) = channel(10);

        // Build event data to send: data_source, StoreKey, Entity
        let data_source = "memefactory".to_string();
        let key = StoreKey {
            entity: "test_type".to_string(),
            id: 1.to_string(),
        };

        let mut entity = Entity::new();
        entity.insert("Name".to_string(), Value::String("John".to_string()));

        // Create EntityCreated event and send to channel
        module::Db::create_entity(sender.clone(), data_source, key, entity);

        // Consume receiver
        let result = receiver
            .into_future()
            .wait()
            .unwrap()
            .0
            .expect("No event found in receiver");

        // Confirm receiver contains EntityCreated event with correct data_source and StoreKey
        match result {
            RuntimeHostEvent::EntityCreated(rec_data_source, rec_key, _rec_entity) => {
                assert_eq!("memefactory".to_string(), rec_data_source);
                assert_eq!(
                    StoreKey {
                        entity: "test_type".to_string(),
                        id: 1.to_string(),
                    },
                    rec_key
                );
            }
            _ => panic!("Unexpected Event recieved, expected RuntimeHostEvent::EntityCreated."),
        }
    }
}
