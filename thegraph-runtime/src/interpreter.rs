use parity_wasm;
use std::env::current_dir;
use wasmi::{Error, Externals, FuncRef, ImportsBuilder,
            Module, ModuleImportResolver, ModuleInstance, ModuleRef, RuntimeArgs, RuntimeValue, Signature, Trap, ValueType};

// Set indexes for Store functions that will be exported with module

pub struct HostExternals {}

// pub fn runtime_event_sender() -

impl Externals for HostExternals {
    fn invoke_index(
        &mut self,
        index: usize,
        _args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        match index {
            _ => panic!("Unimplemented function at {}", index),
        }
    }
}

impl HostExternals {
    fn _check_signature(&self, index: usize, signature: &Signature) -> bool {
        let (params, ret_ty): (&[ValueType], Option<ValueType>) = match index {
            _ => return false,
        };
        signature.params() == params && signature.return_type() == ret_ty
    }
}

pub struct RuntimeModuleImportResolver;

impl ModuleImportResolver for RuntimeModuleImportResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        let func_ref = match field_name {
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
