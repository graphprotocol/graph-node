use super::class::*;
use super::{AscHeap, AscPtr};
use ethereum_types::{H160, U256};
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

    fn takes_ptr_returns_ptr<T, U>(&self, fn_name: &str, arg: AscPtr<T>) -> AscPtr<U> {
        self.module
            .invoke_export(fn_name, &[RuntimeValue::from(arg)], &mut NopExternals)
            .expect("call failed")
            .expect("call returned nothing")
            .try_into()
            .expect("call did not return pointer")
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
    let module = TestModule::new("wasm_test/abi_classes.wasm");
    let address = H160::zero();

    let new_address_obj: AscPtr<ArrayBuffer<u8>> =
        module.takes_ptr_returns_ptr("test_address", module.asc_new(&address));

    // This should have 1 added to the first and last byte.
    let new_address: H160 = module.asc_get(new_address_obj);

    assert_eq!(
        new_address,
        H160([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
    )
}

#[test]
fn string() {
    let module = TestModule::new("wasm_test/abi_classes.wasm");
    let string = "    漢字Double_Me🇧🇷  ";
    let trimmed_string_obj: AscPtr<AscString> =
        module.takes_ptr_returns_ptr("repeat_twice", module.asc_new(string));
    let doubled_string: String = module.asc_get(trimmed_string_obj);
    assert_eq!(doubled_string, string.repeat(2))
}

#[test]
fn abi_u256() {
    let module = TestModule::new("wasm_test/abi_classes.wasm");
    let address = U256::zero();

    let new_uint_obj: AscPtr<ArrayBuffer<u64>> =
        module.takes_ptr_returns_ptr("test_uint", module.asc_new(&address));

    // This should have 1 added to the first and last `u64`s.
    let new_uint: U256 = module.asc_get(new_uint_obj);

    assert_eq!(new_uint, U256([1, 0, 0, 1]))
}
