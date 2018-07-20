use futures::prelude::*;
use futures::sync::mpsc::Sender;
use slog::Logger;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;
use wasmi::{
    Error, Externals, FuncInstance, FuncRef, HostError, ImportsBuilder, MemoryRef, Module,
    ModuleImportResolver, ModuleInstance, ModuleRef, NopExternals, RuntimeArgs, RuntimeValue,
    Signature, Trap, TrapKind, ValueType,
};
use web3::types::BlockId;

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::ethereum::*;
use thegraph::components::store::StoreKey;
use thegraph::data::data_sources::DataSet;
use thegraph::prelude::*;

use super::UnresolvedContractCall;
use asc_abi::asc_ptr::*;
use asc_abi::class::*;
use asc_abi::*;
use hex;

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
const TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX: usize = 5;
const TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX: usize = 6;
const TYPE_CONVERSION_U64_ARRAY_TO_HEX_FUNC_INDEX: usize = 7;

pub struct WasmiModuleConfig<T> {
    pub data_source: DataSourceDefinition,
    pub data_set: DataSet,
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
    pub fn new(logger: &Logger, config: WasmiModuleConfig<T>) -> Self {
        let logger = logger.new(o!("component" => "WasmiModule"));

        let module = Module::from_parity_wasm_module(config.data_set.mapping.runtime.clone())
            .expect(
                format!(
                    "Wasmi could not interpret module of data set: {}",
                    config.data_set.data.name
                ).as_str(),
            );

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &EnvModuleResolver);
        imports.push_resolver("database", &StoreModuleResolver);
        imports.push_resolver("ethereum", &EthereumModuleResolver);
        imports.push_resolver("typeConversion", &TypeConversionModuleResolver);

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
            data_source: config.data_source,
            data_set: config.data_set,
            logger: logger.clone(),
            runtime: config.runtime.clone(),
            event_sink: config.event_sink.clone(),
            heap: heap.clone(),
            ethereum_adapter: config.ethereum_adapter.clone(),
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
            .map_err(|e| {
                warn!(self.logger, "Failed to handle Ethereum event";
                      "handler" => &handler_name,
                      "error" => format!("{}", e))
            })
            .unwrap_or_default();
    }
}

/// Error raised in host functions.
#[derive(Debug)]
struct HostExternalsError(String);

impl HostError for HostExternalsError {}

impl fmt::Display for HostExternalsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Hosted functions for external use by wasm module
pub struct HostExternals<T> {
    logger: Logger,
    runtime: Handle,
    data_source: DataSourceDefinition,
    data_set: DataSet,
    event_sink: Sender<RuntimeHostEvent>,
    heap: WasmiAscHeap,
    ethereum_adapter: Arc<Mutex<T>>,
}

impl<T> HostExternals<T>
where
    T: EthereumAdapter,
{
    /// function database.create(entity: string, id: string, data: Entity): void
    fn database_create(
        &self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let entity: String = self.heap.asc_get(entity_ptr);
        let id: String = self.heap.asc_get(id_ptr);
        let data: HashMap<String, Value> = self.heap.asc_get(data_ptr);

        let store_key = StoreKey { entity, id };

        let entity_data = Entity::from(data);

        // Send an entity created event
        let logger = self.logger.clone();
        self.runtime.spawn(
            self.event_sink
                .clone()
                .send(RuntimeHostEvent::EntityCreated(
                    self.data_source.id.clone(),
                    store_key,
                    entity_data,
                ))
                .map_err(move |e| {
                    error!(logger, "Failed to forward runtime host event";
                           "error" => format!("{}", e));
                })
                .map(|_| ()),
        );

        Ok(None)
    }

    /// function database.update(entity: string, id: string, data: Entity): void
    fn database_update(
        &self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
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
                    self.data_source.id.clone(),
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

    /// function database.remove(entity: string, id: string): void
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
                    self.data_source.id.clone(),
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

    /// function ethereum.call(call: SmartContractCall): Array<Token>
    fn ethereum_call(
        &self,
        call_ptr: AscPtr<AscUnresolvedContractCall>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let unresolved_call: UnresolvedContractCall = self.heap.asc_get(call_ptr);

        info!(self.logger, "Call smart contract";
              "contract" => &unresolved_call.contract_name,
              "function" => &unresolved_call.function_name);

        // Obtain the path to the contract ABI
        let contract = self.data_set
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == unresolved_call.contract_name)
            .ok_or(Trap::new(TrapKind::Host(Box::new(HostExternalsError(
                format!(
                    "Unknown contract \"{}\" called from WASM runtime",
                    unresolved_call.contract_name
                ),
            )))))?
            .contract
            .clone();

        let function = contract
            .function(unresolved_call.function_name.as_str())
            .map_err(|e| {
                Trap::new(TrapKind::Host(Box::new(HostExternalsError(format!(
                    "Unknown function \"{}::{}\" called from WASM runtime: {}",
                    unresolved_call.contract_name, unresolved_call.function_name, e
                )))))
            })?;

        let call = EthereumContractCall {
            address: unresolved_call.contract_address.clone(),
            block_id: BlockId::Hash(unresolved_call.block_hash.clone()),
            function: function.clone(),
            args: unresolved_call.function_args.clone(),
        };

        self.ethereum_adapter
            .lock()
            .unwrap()
            .contract_call(call)
            .wait()
            .map(|result| Some(RuntimeValue::from(self.heap.asc_new(&*result))))
            .map_err(|e| {
                Trap::new(TrapKind::Host(Box::new(HostExternalsError(format!(
                    "Failed to call function \"{}\" of contract \"{}\": {}",
                    unresolved_call.contract_name, unresolved_call.function_name, e
                )))))
            })
    }

    /// function typeConversions.bytesToString(bytes: Bytes): string
    fn convert_bytes_to_string(
        &self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.heap.asc_get(bytes_ptr);
        let s = String::from_utf8_lossy(&*bytes);
        // The string may have been encoded in a fixed length
        // buffer and padded with null characters, so trim
        // trailing nulls.
        let trimmed_s = s.trim_right_matches('\u{0000}');
        Ok(Some(RuntimeValue::from(self.heap.asc_new(trimmed_s))))
    }

    /// function typeConversions.u64ArrayToHex(u64_array: Uint64Array): string
    fn u64_array_to_hex(
        &self,
        u64_array_ptr: AscPtr<Uint64Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let u64_array: Vec<u64> = self.heap.asc_get(u64_array_ptr);
        let mut bytes: Vec<u8> = Vec::new();
        for x in u64_array {
            // This is just `x.to_bytes()` which is unstable.
            let x_bytes: [u8; 8] = unsafe { ::std::mem::transmute(x) };
            bytes.extend(x_bytes.iter());
        }

        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let hex_string = format!("0x{}", hex::encode(bytes));
        let hex_string_obj = self.heap.asc_new(hex_string.as_str());
        Ok(Some(RuntimeValue::from(hex_string_obj)))
    }

    /// Converts bytes to a hex string.
    /// References:
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    /// https://github.com/ethereum/web3.js/blob/f98fe1462625a6c865125fecc9cb6b414f0a5e83/packages/web3-utils/src/utils.js#L283
    ///
    /// function typeConversions.bytesToHex(bytes: Bytes): string
    fn bytes_to_hex(&self, bytes_ptr: AscPtr<Uint8Array>) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.heap.asc_get(bytes_ptr);

        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let hex_string = format!("0x{}", hex::encode(bytes));
        let hex_string_obj = self.heap.asc_new(hex_string.as_str());

        Ok(Some(RuntimeValue::from(hex_string_obj)))
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
            ETHEREUM_CALL_FUNC_INDEX => self.ethereum_call(args.nth_checked(0)?),
            TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX => {
                self.convert_bytes_to_string(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX => self.bytes_to_hex(args.nth_checked(0)?),
            TYPE_CONVERSION_U64_ARRAY_TO_HEX_FUNC_INDEX => {
                self.u64_array_to_hex(args.nth_checked(0)?)
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

/// Types conversion module resolver
pub struct TypeConversionModuleResolver;

impl ModuleImportResolver for TypeConversionModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "bytesToString" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX,
            ),
            "bytesToHex" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX,
            ),
            "u64ArrayToHex" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U64_ARRAY_TO_HEX_FUNC_INDEX,
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
    extern crate graphql_parser;
    extern crate parity_wasm;

    use self::graphql_parser::schema::Document;
    use ethabi::{LogParam, Token};
    use ethereum_types::Address;
    use futures::prelude::*;
    use futures::sync::mpsc::channel;
    use slog;
    use std::collections::HashMap;
    use std::iter::FromIterator;
    use std::sync::{Arc, Mutex};
    use tokio_core;

    use thegraph::components::data_sources::*;
    use thegraph::components::ethereum::*;
    use thegraph::components::store::*;
    use thegraph::data::data_sources::*;
    use thegraph::prelude::*;
    use thegraph::util;

    use super::{WasmiModule, WasmiModuleConfig};

    #[derive(Default)]
    struct MockEthereumAdapter {}

    impl EthereumAdapter for MockEthereumAdapter {
        fn contract_call(
            &mut self,
            _call: EthereumContractCall,
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
        let runtime = parity_wasm::deserialize_file("test/example_event_handler.wasm")
            .expect("Failed to deserialize wasm");
        let (sender, receiver) = channel(1);
        let mock_ethereum_adapter = Arc::new(Mutex::new(MockEthereumAdapter::default()));
        let mut module = WasmiModule::new(
            &logger,
            WasmiModuleConfig {
                data_source: DataSourceDefinition {
                    id: String::from("example data source"),
                    location: String::from("/path/to/example-data-source.yaml"),
                    spec_version: String::from("0.1.0"),
                    schema: Schema {
                        id: String::from("exampled id"),
                        document: Document {
                            definitions: vec![],
                        },
                    },
                    datasets: vec![],
                },
                data_set: DataSet {
                    data: Data {
                        kind: String::from("ethereum/contract"),
                        name: String::from("example data set"),
                        address: String::from("0123123123"),
                        structure: DataStructure {
                            abi: String::from("123123"),
                        },
                    },
                    mapping: Mapping {
                        kind: String::from("ethereum/events"),
                        api_version: String::from("0.1.0"),
                        language: String::from("wasm/assemblyscript"),
                        entities: vec![],
                        abis: vec![],
                        event_handlers: vec![],
                        runtime,
                    },
                },
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
