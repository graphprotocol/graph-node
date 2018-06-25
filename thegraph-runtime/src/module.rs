use futures::prelude::*;
use futures::sync::mpsc::Sender;
use parity_wasm;
use std::path::PathBuf;
use wasmi::{
    Error, Externals, FuncInstance, FuncRef, ImportsBuilder, Module, ModuleImportResolver,
    ModuleInstance, ModuleRef, NopExternals, RuntimeArgs, RuntimeValue, Signature, Trap, ValueType,
};

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::store::StoreKey;
use thegraph::prelude::*;

// Set indexes for Store functions that will be exported with module
const CREATE_FUNC_INDEX: usize = 0;
const UPDATE_FUNC_INDEX: usize = 1;
const DELETE_FUNC_INDEX: usize = 2;

/// Wasm runtime module
pub struct WasmiModule {
    pub module: ModuleRef,
}

impl WasmiModule {
    /// Creates a new wasmi module
    pub fn new(wasm_location: String, event_sink: Sender<RuntimeHostEvent>) -> Self {
        WasmiModule {
            module: WasmiModule::instantiate_wasmi_module(wasm_location, event_sink),
        }
    }

    /// Create wasmi module instance using wasm, external functions and dependency resolver
    pub fn instantiate_wasmi_module(
        wasm_location: String,
        event_sink: Sender<RuntimeHostEvent>,
    ) -> ModuleRef {
        let wasm_buffer = PathBuf::from(wasm_location);
        let module =
            parity_wasm::deserialize_file(&wasm_buffer).expect("Failed to deserialize wasm file.");
        let loaded_module = Module::from_parity_wasm_module(module)
            .expect("Invalid parity_wasm module; Wasmi could not interpret.");

        // Create new instance of externally hosted functions invoker
        let mut external_functions = HostExternals { event_sink };

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &RuntimeModuleImportResolver);

        // Instantiate the runtime module using hosted functions and import resolver
        ModuleInstance::new(&loaded_module, &imports)
            .expect("Failed to instantiate module")
            .run_start(&mut external_functions)
            .expect("Failed to start module instance")
    }

    // Expose the allocate memory function exported from .wasm for memory management
    pub fn _allocate_memory(&self, size: i32) -> i32 {
        self.module
            .invoke_export("malloc", &[RuntimeValue::I32(size)], &mut NopExternals)
            .expect("Failed to invoke memory allocation function.")
            .expect("Function did not return a value.")
            .try_into::<i32>()
            .expect("Function return value was not expected type, u32.")
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

/// Store event senders that will be exposed to the wasm module
pub struct Db {}

impl Db {
    /// Send Entity Created Event
    pub fn create_entity(
        sender: Sender<RuntimeHostEvent>,
        data_source: String,
        key: StoreKey,
        entity: Entity,
    ) -> i32 {
        let id: i32 = key.id.parse().unwrap();
        sender
            .send(RuntimeHostEvent::EntityCreated(data_source, key, entity))
            .wait()
            .expect("Failed to forward runtime host event");
        id
    }

    /// Send Entity Updated Event
    pub fn update_entity(
        sender: Sender<RuntimeHostEvent>,
        data_source: String,
        key: StoreKey,
        entity: Entity,
    ) -> i32 {
        let id: i32 = key.id.parse().unwrap();
        sender
            .send(RuntimeHostEvent::EntityChanged(data_source, key, entity))
            .wait()
            .expect("Failed to forward runtime host event");
        id
    }

    /// Send Entity Removed Event
    pub fn remove_entity(
        sender: Sender<RuntimeHostEvent>,
        data_source: String,
        key: StoreKey,
    ) -> i32 {
        let id: i32 = key.id.parse().unwrap();
        sender
            .send(RuntimeHostEvent::EntityRemoved(data_source, key))
            .wait()
            .expect("Failed to forward runtime host event");
        id
    }
}

/// Hosted functions for external use by wasm module
pub struct HostExternals {
    event_sink: Sender<RuntimeHostEvent>,
}

impl Externals for HostExternals {
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        match index {
            CREATE_FUNC_INDEX => {
                // Input: StoreKey and Entity
                let store_key_ptr: u32 = args.nth_checked(0)?;
                let store_key = WasmConverter::store_key_from_wasm(store_key_ptr);
                let entity_ptr: u32 = args.nth_checked(1)?;
                let entity = WasmConverter::entity_from_wasm(entity_ptr);

                // Send an add entity event
                let id =
                    Db::create_entity(self.event_sink.clone(), "".to_string(), store_key, entity);

                Ok(Some(RuntimeValue::I32(id)))
            }
            UPDATE_FUNC_INDEX => {
                // Input: StoreKey and Entity
                let store_key_ptr: u32 = args.nth_checked(0)?;
                let store_key = WasmConverter::store_key_from_wasm(store_key_ptr);
                let entity_ptr: u32 = args.nth_checked(1)?;
                let entity = WasmConverter::entity_from_wasm(entity_ptr);

                // Send an update entity event
                let id =
                    Db::update_entity(self.event_sink.clone(), "".to_string(), store_key, entity);

                Ok(Some(RuntimeValue::I32(id)))
            }
            DELETE_FUNC_INDEX => {
                // Input: StoreKey
                let store_key_ptr: u32 = args.nth_checked(0)?;
                let store_key = WasmConverter::store_key_from_wasm(store_key_ptr);

                // Send a delete entity event
                let id = Db::remove_entity(self.event_sink.clone(), "".to_string(), store_key);

                Ok(Some(RuntimeValue::I32(id)))
            }
            _ => panic!("Unimplemented function at {}", index),
        }
    }
}

/// Resolver of the modules dependencies
pub struct RuntimeModuleImportResolver;

impl ModuleImportResolver for RuntimeModuleImportResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        let func_ref = match field_name {
            "create" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                CREATE_FUNC_INDEX,
            ),
            "update" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                UPDATE_FUNC_INDEX,
            ),
            "delete" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                DELETE_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export {} not found",
                    field_name
                )))
            }
        };
        Ok(func_ref)
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
