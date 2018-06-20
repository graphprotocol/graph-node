use parity_wasm;
use std::env::current_dir;
use thegraph::components::store as StoreComponents;
use thegraph::data::store as StoreData;
use wasmi::{Error, Externals, FuncInstance, FuncRef, ImportsBuilder,
            Module, ModuleImportResolver, ModuleInstance, ModuleRef, RuntimeArgs, RuntimeValue, Signature, Trap, ValueType};

// Set indexes for Store functions that will be exported with module
const ADD_FUNC_INDEX: usize = 0;
const UPDATE_FUNC_INDEX: usize = 1;
const DELETE_FUNC_INDEX: usize = 2;

pub struct HostExternals {}

// Put StoreKey into linear memory and return a u32 describing location (pointer)
pub fn _storekey_to_memory(_key: StoreComponents::StoreKey) -> RuntimeValue {
    unimplemented!();
}

// Get StoreKey from wasm pointer
pub fn storekey_from_wasm(_pointer: u32) -> StoreComponents::StoreKey {
    unimplemented!();
}

// Get Entity from wasm pointer
pub fn entity_from_wasm(_pointer: u32) -> StoreData::Entity {
    unimplemented!();
}

// pub fn runtime_event_sender() -

impl Externals for HostExternals {
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        match index {
            ADD_FUNC_INDEX => {
                // Input: StoreKey and Entity
                let storekey_ptr: u32 = args.nth_checked(0)?;
                let _storekey: StoreComponents::StoreKey = storekey_from_wasm(storekey_ptr);

                let entity_ptr: u32 = args.nth_checked(1)?;
                let _entity: StoreData::Entity = entity_from_wasm(entity_ptr);

                // Create add entity store and add to sink

                // Placeholder for function
                let result = storekey_ptr;

                Ok(Some(RuntimeValue::I32(result as i32)))
            }
            UPDATE_FUNC_INDEX => {
                // Input: StoreKey and Entity
                let storekey_ptr: u32 = args.nth_checked(0)?;
                let _storekey: StoreComponents::StoreKey = storekey_from_wasm(storekey_ptr);

                let entity_ptr: u32 = args.nth_checked(1)?;
                let _entity: StoreData::Entity = entity_from_wasm(entity_ptr);

                // Create update entity store event and add to sink

                // Placeholder for function
                let result = storekey_ptr;

                Ok(Some(RuntimeValue::I32(result as i32)))
            }
            DELETE_FUNC_INDEX => {
                // Input: StoreKey
                let storekey_ptr: u32 = args.nth_checked(0)?;
                let _storekey: StoreComponents::StoreKey = storekey_from_wasm(storekey_ptr);

                // Create delete entity store event and add to sink

                // Placeholder for function
                let result = storekey_ptr;

                Ok(Some(RuntimeValue::I32(result as i32)))
            }
            _ => panic!("Unimplemented function at {}", index),
        }
    }
}

impl HostExternals {
    fn _check_signature(&self, index: usize, signature: &Signature) -> bool {
        let (params, ret_ty): (&[ValueType], Option<ValueType>) = match index {
            ADD_FUNC_INDEX => (&[ValueType::I32, ValueType::I32], Some(ValueType::I32)),
            UPDATE_FUNC_INDEX => (&[ValueType::I32, ValueType::I32], Some(ValueType::I32)),
            DELETE_FUNC_INDEX => (&[ValueType::I32], Some(ValueType::I32)),
            _ => return false,
        };
        signature.params() == params && signature.return_type() == ret_ty
    }
}

pub struct RuntimeModuleImportResolver;

impl ModuleImportResolver for RuntimeModuleImportResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        let func_ref = match field_name {
            "add" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                ADD_FUNC_INDEX,
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

pub struct WasmiModule {
    pub module: ModuleRef,
}

impl WasmiModule {
    pub fn new(wasm_location: &str) -> Self {
        WasmiModule {
            module: WasmiModule::instantiate_wasmi_module(wasm_location),
        }
    }
    pub fn instantiate_wasmi_module(wasm_location: &str) -> ModuleRef {
        // Load .wasm file into Wasmi interpreter
        let wasm_buffer = current_dir().unwrap().join(wasm_location);
        let module = parity_wasm::deserialize_file(&wasm_buffer).expect("File to be deserialized");
        let loaded_module = Module::from_parity_wasm_module(module).expect("Module to be valid");

        // Create new instance of externally hosted functions invoker
        let mut external_functions = HostExternals {};

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &RuntimeModuleImportResolver);

        ModuleInstance::new(&loaded_module, &imports)
            .expect("Failed to instantiate module")
            .run_start(&mut external_functions)
            .expect("Failed to start moduleinstance")
    }
}
