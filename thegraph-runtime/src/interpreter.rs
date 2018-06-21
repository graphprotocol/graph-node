use parity_wasm;
use std::env::current_dir;
use thegraph::components::store as StoreComponents;
use thegraph::data::store as StoreData;
use wasmi::{Error, Externals, FuncInstance, FuncRef, ImportsBuilder, Module, ModuleImportResolver,
            ModuleInstance, ModuleRef, NopExternals, RuntimeArgs, RuntimeValue, Signature, Trap,
            ValueType};

use futures::prelude::*;
use futures::sync::mpsc::Sender;
use thegraph::components::data_sources::RuntimeAdapterEvent;

// Set indexes for Store functions that will be exported with module
const CREATE_FUNC_INDEX: usize = 0;
const UPDATE_FUNC_INDEX: usize = 1;
const DELETE_FUNC_INDEX: usize = 2;

/// Wasm runtime module
pub struct WasmiModule {
    pub module: ModuleRef,
}

impl WasmiModule {
    pub fn new(wasm_location: &str, event_sink: Sender<RuntimeAdapterEvent>) -> Self {
        WasmiModule {
            module: WasmiModule::instantiate_wasmi_module(wasm_location, event_sink),
        }
    }
    pub fn instantiate_wasmi_module(
        wasm_location: &str,
        event_sink: Sender<RuntimeAdapterEvent>,
    ) -> ModuleRef {
        // Load .wasm file into Wasmi interpreter
        let wasm_buffer = current_dir().unwrap().join(wasm_location);
        let module = parity_wasm::deserialize_file(&wasm_buffer).expect("File to be deserialized");
        let loaded_module = Module::from_parity_wasm_module(module).expect("Module to be valid");

        // Create new instance of externally hosted functions invoker
        let mut external_functions = HostExternals { event_sink };

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &RuntimeModuleImportResolver);

        // Instantiate the runtime module using hosted functions and import resolver
        ModuleInstance::new(&loaded_module, &imports)
            .expect("Failed to instantiate module")
            .run_start(&mut external_functions)
            .expect("Failed to start moduleinstance")
    }

    // Expose the allocate memory function exported from .wasm for memory management
    pub fn _allocate_memory(&self, size: i32) -> i32 {
        self.module
            .invoke_export(
                "allocate_memory",
                &[RuntimeValue::I32(size)],
                &mut NopExternals,
            )
            .expect("call failed")
            .expect("call returned nothing")
            .try_into::<i32>()
            .expect("call did not return u32")
    }
}

// Placeholder for deserializer module
pub struct WasmConverter {}

impl WasmConverter {
    // Put StoreKey into linear memory and return a u32 pointer
    pub fn _storekey_to_memory(_key: StoreComponents::StoreKey) -> RuntimeValue {
        unimplemented!();
    }
    // Get StoreKey from .wasm pointer
    pub fn storekey_from_wasm(_pointer: u32) -> StoreComponents::StoreKey {
        unimplemented!();
    }
    // Get Entity from .wasm pointer
    pub fn entity_from_wasm(_pointer: u32) -> StoreData::Entity {
        unimplemented!();
    }
}

// Expose Store events to the wasm module
pub struct Db {}

impl Db {
    //
    pub fn create_entity(
        sender: Sender<RuntimeAdapterEvent>,
        datasource: String,
        key: StoreComponents::StoreKey,
        entity: StoreData::Entity,
    ) -> i32 {
        let id: i32 = key.id.parse().unwrap();
        sender
            .clone()
            .send(RuntimeAdapterEvent::EntityAdded(datasource, key, entity))
            .wait()
            .expect("Failed to forward runtime adapter event");
        id
    }
    pub fn update_entity(
        sender: Sender<RuntimeAdapterEvent>,
        datasource: String,
        key: StoreComponents::StoreKey,
        entity: StoreData::Entity,
    ) -> i32 {
        let id: i32 = key.id.parse().unwrap();
        sender
            .clone()
            .send(RuntimeAdapterEvent::EntityChanged(datasource, key, entity))
            .wait()
            .expect("Failed to forward runtime adapter event");
        id
    }
    pub fn remove_entity(
        sender: Sender<RuntimeAdapterEvent>,
        datasource: String,
        key: StoreComponents::StoreKey,
    ) -> i32 {
        let id: i32 = key.id.parse().unwrap();
        sender
            .clone()
            .send(RuntimeAdapterEvent::EntityRemoved(datasource, key))
            .wait()
            .expect("Failed to forward runtime adapter event");
        id
    }
}

pub struct HostExternals {
    event_sink: Sender<RuntimeAdapterEvent>,
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
                let storekey_ptr: u32 = args.nth_checked(0)?;
                let storekey: StoreComponents::StoreKey =
                    WasmConverter::storekey_from_wasm(storekey_ptr);
                let entity_ptr: u32 = args.nth_checked(1)?;
                let entity: StoreData::Entity = WasmConverter::entity_from_wasm(entity_ptr);

                // Create add entity store and add to sink
                let id = Db::create_entity(
                    self.event_sink.clone(),
                    "memefactory".to_string(),
                    storekey,
                    entity,
                );

                Ok(Some(RuntimeValue::I32(id)))
            }
            UPDATE_FUNC_INDEX => {
                // Input: StoreKey and Entity
                let storekey_ptr: u32 = args.nth_checked(0)?;
                let storekey: StoreComponents::StoreKey =
                    WasmConverter::storekey_from_wasm(storekey_ptr);
                let entity_ptr: u32 = args.nth_checked(1)?;
                let entity: StoreData::Entity = WasmConverter::entity_from_wasm(entity_ptr);

                // Create update entity store event and add to sink
                let id = Db::update_entity(
                    self.event_sink.clone(),
                    "memefactory".to_string(),
                    storekey,
                    entity,
                );

                Ok(Some(RuntimeValue::I32(id)))
            }
            DELETE_FUNC_INDEX => {
                // Input: StoreKey
                let storekey_ptr: u32 = args.nth_checked(0)?;
                let storekey: StoreComponents::StoreKey =
                    WasmConverter::storekey_from_wasm(storekey_ptr);

                // Create delete entity store event and add to sink
                let id =
                    Db::remove_entity(self.event_sink.clone(), "memefactory".to_string(), storekey);

                Ok(Some(RuntimeValue::I32(id)))
            }
            _ => panic!("Unimplemented function at {}", index),
        }
    }
}

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
    use std::sync::{Arc, Mutex};

    #[test]
    fn runs_a_function_exported_from_wasm() {
        let wasm_location = "test/allocate_add.wasm";
        let event_sink_raw = Arc::new(Mutex::new(None));
        let event_sink = event_sink_raw
            .lock()
            .unwrap()
            .clone()
            .expect("Runtime started without event sink");

        let main = WasmiModule::new(wasm_location, event_sink);
        let allocated_memory = main.allocate_memory(2 as i32);
    }
}
