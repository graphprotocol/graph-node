use super::class::ArrayBuffer;
use super::{AscHeap, AscPtr};
use ethereum_types::H160;
use parity_wasm;
use wasmi::{self, ImportsBuilder, MemoryRef, ModuleImportResolver, ModuleInstance, ModuleRef,
            NopExternals, RuntimeValue, Signature};

struct TestModule {
    module: ModuleRef,
    memory: MemoryRef,
}

struct DummyImportResolver;

impl ModuleImportResolver for DummyImportResolver {
    fn resolve_func(
        &self,
        field_name: &str,
        _signature: &Signature,
    ) -> Result<wasmi::FuncRef, wasmi::Error> {
        Ok(match field_name {
            "abort" => wasmi::FuncInstance::alloc_host(
                Signature::new(
                    &[
                        wasmi::ValueType::I32,
                        wasmi::ValueType::I32,
                        wasmi::ValueType::I32,
                        wasmi::ValueType::I32,
                    ][..],
                    None,
                ),
                0,
            ),
            _ => panic!("requested non-existing import {}", field_name),
        })
    }
}

impl TestModule {
    fn new(path: &str) -> Self {
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &DummyImportResolver);
        // Load .wasm file into Wasmi interpreter
        let module = &wasmi::Module::from_parity_wasm_module(
            parity_wasm::deserialize_file(path).expect("Failed to deserialize wasm"),
        ).expect("Invalid module");

        let module_instance = ModuleInstance::new(module, &imports)
            .expect("Failed to instantiate module")
            .run_start(&mut NopExternals)
            .expect("Failed to start module");

        // Access the wasm runtime linear memory
        let memory = module_instance
            .export_by_name("memory")
            .expect("Failed to find memory export in the wasm module")
            .as_memory()
            .expect("Extern value is not Memory")
            .clone();

        Self {
            module: module_instance,
            memory,
        }
    }
}

impl AscHeap for TestModule {
    fn raw_new(&self, bytes: &[u8]) -> Result<u32, wasmi::Error> {
        let addr = self.module
            .invoke_export(
                "allocate_memory",
                &[RuntimeValue::I32(bytes.len() as i32)],
                &mut NopExternals,
            )
            .expect("call failed")
            .expect("call returned nothing")
            .try_into::<u32>()
            .expect("call did not return u32");

        self.memory.set(addr, bytes)?;
        Ok(addr)
    }

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, wasmi::Error> {
        self.memory.get(offset, size as usize)
    }
}

#[test]
fn abi_h160() {
    // Load .wasm file into Wasmi interpreter
    let module = TestModule::new("wasm_test/abi_classes.wasm");
    let address = H160::zero();

    let new_address_obj: AscPtr<ArrayBuffer<u8>> = module
        .module
        .invoke_export(
            "test_address",
            &[RuntimeValue::from(module.asc_new(&address))],
            &mut NopExternals,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return u32");

    // This should have 1 added to the first and last byte.
    let new_address: H160 = module.asc_get(new_address_obj);

    assert_eq!(
        *new_address,
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
    )
}
