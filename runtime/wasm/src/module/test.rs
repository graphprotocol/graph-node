extern crate graph_mock;
extern crate ipfs_api;

use self::graph_mock::FakeStore;
use crate::failure::Error;
use ethabi::Token;
use futures::sync::mpsc::{channel, Sender};
use graph::components::ethereum::*;
use graph::components::store::*;
use graph::data::store::scalar;
use graph::data::subgraph::*;
use graph::web3::types::{Address, Block, Transaction, H160, H256};
use hex;
use std::io::Cursor;
use std::str::FromStr;
use wasmi::nan_preserving_float::F64;

use std::env;

use super::*;

mod abi;

#[derive(Default)]
struct MockEthereumAdapter {}

impl EthereumAdapter for MockEthereumAdapter {
    fn net_identifiers(
        &self,
        _: &Logger,
    ) -> Box<Future<Item = EthereumNetworkIdentifier, Error = Error> + Send> {
        unimplemented!();
    }

    fn latest_block(
        &self,
        _: &Logger,
    ) -> Box<Future<Item = Block<Transaction>, Error = EthereumAdapterError> + Send> {
        unimplemented!();
    }

    fn block_by_hash(
        &self,
        _: &Logger,
        _: H256,
    ) -> Box<Future<Item = Option<Block<Transaction>>, Error = Error> + Send> {
        unimplemented!();
    }

    fn load_full_block(
        &self,
        _: &Logger,
        _: Block<Transaction>,
    ) -> Box<Future<Item = EthereumBlock, Error = EthereumAdapterError> + Send> {
        unimplemented!();
    }

    fn block_hash_by_block_number(
        &self,
        _: &Logger,
        _: u64,
    ) -> Box<Future<Item = Option<H256>, Error = Error> + Send> {
        unimplemented!();
    }

    fn is_on_main_chain(
        &self,
        _: &Logger,
        _: EthereumBlockPointer,
    ) -> Box<Future<Item = bool, Error = Error> + Send> {
        unimplemented!();
    }

    fn find_first_blocks_with_logs(
        &self,
        _: &Logger,
        _: u64,
        _: u64,
        _: EthereumLogFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        unimplemented!();
    }

    fn contract_call(
        &self,
        _: &Logger,
        _: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError> + Send> {
        unimplemented!();
    }
}

fn test_valid_module(
    data_source: DataSource,
) -> ValidModule<
    MockEthereumAdapter,
    ipfs_api::IpfsClient,
    FakeStore,
    Sender<Box<Future<Item = (), Error = ()> + Send>>,
> {
    let logger = Logger::root(slog::Discard, o!());
    let mock_ethereum_adapter = Arc::new(MockEthereumAdapter::default());
    let (task_sender, task_receiver) = channel(100);
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(task_receiver.for_each(tokio::spawn));
    ::std::mem::forget(runtime);
    ValidModule::new(
        &logger,
        WasmiModuleConfig {
            subgraph_id: SubgraphDeploymentId::new("wasmModuleTest").unwrap(),
            data_source,
            ethereum_adapter: mock_ethereum_adapter,
            link_resolver: Arc::new(ipfs_api::IpfsClient::default()),
            store: Arc::new(FakeStore),
        },
        task_sender,
    )
    .unwrap()
}

fn mock_data_source(path: &str) -> DataSource {
    let runtime = parity_wasm::deserialize_file(path).expect("Failed to deserialize wasm");

    DataSource {
        kind: String::from("ethereum/contract"),
        name: String::from("example data source"),
        network: Some(String::from("mainnet")),
        source: Source {
            address: Address::from_str("0123123123012312312301231231230123123123").unwrap(),
            abi: String::from("123123"),
        },
        mapping: Mapping {
            kind: String::from("ethereum/events"),
            api_version: String::from("0.1.0"),
            language: String::from("wasm/assemblyscript"),
            entities: vec![],
            abis: vec![],
            event_handlers: vec![],
            link: Link {
                link: "link".to_owned(),
            },
            runtime,
        },
    }
}

fn mock_context() -> EventHandlerContext {
    EventHandlerContext {
        logger: Logger::root(slog::Discard, o!()),
        block: Default::default(),
        transaction: Default::default(),
        entity_operations: Vec::new(),
    }
}

impl<'a, T, L, S, U> WasmiModule<'a, T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + 'static,
{
    fn takes_val_returns_ptr<P>(&mut self, fn_name: &str, val: RuntimeValue) -> AscPtr<P> {
        self.module
            .clone()
            .invoke_export(fn_name, &[val], self)
            .expect("call failed")
            .expect("call returned nothing")
            .try_into()
            .expect("call did not return pointer")
    }

    fn takes_ptr_returns_ptr<P, Q>(&mut self, fn_name: &str, arg: AscPtr<P>) -> AscPtr<Q> {
        self.module
            .clone()
            .invoke_export(fn_name, &[RuntimeValue::from(arg)], self)
            .expect("call failed")
            .expect("call returned nothing")
            .try_into()
            .expect("call did not return pointer")
    }

    fn takes_ptr_ptr_returns_ptr<P, Q, R>(
        &mut self,
        fn_name: &str,
        arg1: AscPtr<P>,
        arg2: AscPtr<Q>,
    ) -> AscPtr<R> {
        self.module
            .clone()
            .invoke_export(
                fn_name,
                &[RuntimeValue::from(arg1), RuntimeValue::from(arg2)],
                self,
            )
            .expect("call failed")
            .expect("call returned nothing")
            .try_into()
            .expect("call did not return pointer")
    }
}

#[test]
fn json_conversions() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/string_to_number.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();

    // test u64 conversion
    let number = 9223372036850770800;
    let number_ptr = module.asc_new(&number.to_string());
    let converted: u64 = module
        .module
        .clone()
        .invoke_export("testToU64", &[RuntimeValue::from(number_ptr)], &mut module)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return I64");
    assert_eq!(number, converted);

    // test i64 conversion
    let number = -9223372036850770800;
    let number_ptr = module.asc_new(&number.to_string());
    let converted: i64 = module
        .module
        .clone()
        .invoke_export("testToI64", &[RuntimeValue::from(number_ptr)], &mut module)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return I64");
    assert_eq!(number, converted);

    // test f64 conversion
    let number = F64::from(-9223372036850770.92345034);
    let number_ptr = module.asc_new(&number.to_float().to_string());
    let converted: F64 = module
        .module
        .clone()
        .invoke_export("testToF64", &[RuntimeValue::from(number_ptr)], &mut module)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return F64");
    assert_eq!(number, converted);

    // test BigInt conversion
    let number = "-922337203685077092345034";
    let number_ptr = module.asc_new(number);
    let big_int_obj: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export(
            "testToBigInt",
            &[RuntimeValue::from(number_ptr)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let bytes: Vec<u8> = module.asc_get(big_int_obj);
    assert_eq!(
        scalar::BigInt::from_str(number).unwrap(),
        scalar::BigInt::from_signed_bytes_le(&bytes)
    );
}

#[test]
fn ipfs_cat() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/ipfs_cat.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();
    let ipfs = Arc::new(ipfs_api::IpfsClient::default());

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let hash = runtime.block_on(ipfs.add(Cursor::new("42"))).unwrap().hash;
    let converted: AscPtr<AscString> = module
        .module
        .clone()
        .invoke_export(
            "ipfsCatString",
            &[RuntimeValue::from(module.asc_new(&hash))],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let data: String = module.asc_get(converted);
    assert_eq!(data, "42");
}

#[test]
fn ipfs_fail() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/ipfs_cat.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();

    let hash = module.asc_new("invalid hash");
    assert!(module
        .takes_ptr_returns_ptr::<_, AscString>("ipfsCat", hash,)
        .is_null());
}

#[test]
fn crypto_keccak256() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/crypto.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();
    let input: &[u8] = "eth".as_ref();
    let input: AscPtr<Uint8Array> = module.asc_new(input);

    let hash: AscPtr<Uint8Array> = module
        .module
        .clone()
        .invoke_export("hash", &[RuntimeValue::from(input)], &mut module)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let hash: Vec<u8> = module.asc_get(hash);
    assert_eq!(
        hex::encode(hash),
        "4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0"
    );
}

#[test]
fn token_numeric_conversion() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/token_to_numeric.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();

    // Convert numeric to token and back.
    let num = i32::min_value();
    let token_ptr: AscPtr<AscEnum<EthereumValueKind>> =
        module.takes_val_returns_ptr("token_from_i32", RuntimeValue::from(num));
    let num_return = module
        .module
        .clone()
        .invoke_export(
            "token_to_i32",
            &[RuntimeValue::from(token_ptr)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into::<i32>()
        .expect("call did not return i32");
    assert_eq!(num, num_return);
}

#[test]
fn big_int_to_from_i32() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/big_int_to_from_i32.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();

    // Convert i32 to BigInt
    let input: i32 = -157;
    let output_ptr: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export("i32_to_big_int", &[RuntimeValue::from(input)], &mut module)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let output: BigInt = module.asc_get(output_ptr);
    assert_eq!(output, BigInt::from(-157 as i32));

    // Convert BigInt to i32
    let input = BigInt::from(-50 as i32);
    let input_ptr: AscPtr<AscBigInt> = module.asc_new(&input);
    let output: i32 = module
        .module
        .clone()
        .invoke_export(
            "big_int_to_i32",
            &[RuntimeValue::from(input_ptr)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    assert_eq!(output, -50 as i32);
}

#[test]
fn big_int_to_hex() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/big_int_to_hex.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();

    // Convert zero to hex
    let zero = BigInt::from_unsigned_u256(&U256::zero());
    let zero: AscPtr<AscBigInt> = module.asc_new(&zero);
    let zero_hex_ptr: AscPtr<AscString> = module
        .module
        .clone()
        .invoke_export("big_int_to_hex", &[RuntimeValue::from(zero)], &mut module)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let zero_hex_str: String = module.asc_get(zero_hex_ptr);
    assert_eq!(zero_hex_str, "0x0");

    // Convert 1 to hex
    let one = BigInt::from_unsigned_u256(&U256::one());
    let one: AscPtr<AscBigInt> = module.asc_new(&one);
    let one_hex_ptr: AscPtr<AscString> = module
        .module
        .clone()
        .invoke_export("big_int_to_hex", &[RuntimeValue::from(one)], &mut module)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let one_hex_str: String = module.asc_get(one_hex_ptr);
    assert_eq!(one_hex_str, "0x1");

    // Convert U256::max_value() to hex
    let u256_max = BigInt::from_unsigned_u256(&U256::max_value());
    let u256_max: AscPtr<AscBigInt> = module.asc_new(&u256_max);
    let u256_max_hex_ptr: AscPtr<AscString> = module
        .module
        .clone()
        .invoke_export(
            "big_int_to_hex",
            &[RuntimeValue::from(u256_max)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let u256_max_hex_str: String = module.asc_get(u256_max_hex_ptr);
    assert_eq!(
        u256_max_hex_str,
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    );
}

#[test]
fn big_int_arithmetic() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/big_int_arithmetic.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();

    // 0 + 1 = 1
    let zero = BigInt::from(0);
    let zero: AscPtr<AscBigInt> = module.asc_new(&zero);
    let one = BigInt::from(1);
    let one: AscPtr<AscBigInt> = module.asc_new(&one);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export(
            "plus",
            &[RuntimeValue::from(zero), RuntimeValue::from(one)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(1));

    // 127 + 1 = 128
    let zero = BigInt::from(127);
    let zero: AscPtr<AscBigInt> = module.asc_new(&zero);
    let one = BigInt::from(1);
    let one: AscPtr<AscBigInt> = module.asc_new(&one);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export(
            "plus",
            &[RuntimeValue::from(zero), RuntimeValue::from(one)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(128));

    // 5 - 10 = -5
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let ten = BigInt::from(10);
    let ten: AscPtr<AscBigInt> = module.asc_new(&ten);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export(
            "minus",
            &[RuntimeValue::from(five), RuntimeValue::from(ten)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(-5));

    // -20 * 5 = -100
    let minus_twenty = BigInt::from(-20);
    let minus_twenty: AscPtr<AscBigInt> = module.asc_new(&minus_twenty);
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export(
            "times",
            &[RuntimeValue::from(minus_twenty), RuntimeValue::from(five)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(-100));

    // 5 / 2 = 2
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = module.asc_new(&two);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export(
            "dividedBy",
            &[RuntimeValue::from(five), RuntimeValue::from(two)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(2));

    // 5 % 2 = 1
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = module.asc_new(&two);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .clone()
        .invoke_export(
            "mod",
            &[RuntimeValue::from(five), RuntimeValue::from(two)],
            &mut module,
        )
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(1));
}

#[test]
fn abort() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/abort.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();
    let err = module
        .module
        .clone()
        .invoke_export("abort", &[], &mut module)
        .unwrap_err();
    assert_eq!(err.to_string(), "Trap: Trap { kind: Host(HostExportError(\"Mapping aborted at abort.ts, line 6, column 2, with message: not true\")) }");
}

#[test]
fn bytes_to_base58() {
    let valid_module = test_valid_module(mock_data_source("wasm_test/bytes_to_base58.wasm"));
    let mut module =
        WasmiModule::from_valid_module_with_ctx(&valid_module, mock_context()).unwrap();
    let bytes = hex::decode("12207D5A99F603F231D53A4F39D1521F98D2E8BB279CF29BEBFD0687DC98458E7F89")
        .unwrap();
    let bytes_ptr = module.asc_new(bytes.as_slice());
    let result_ptr: AscPtr<AscString> = module.takes_ptr_returns_ptr("bytes_to_base58", bytes_ptr);
    let base58: String = module.asc_get(result_ptr);
    assert_eq!(base58, "QmWmyoMoctfbAaiEs2G46gpeUmhqFRDW6KWo64y5r581Vz");
}
