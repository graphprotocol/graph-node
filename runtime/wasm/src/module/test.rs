extern crate graph_mock;
extern crate ipfs_api;
extern crate parity_wasm;

use self::graph_mock::FakeStore;
use ethabi::Token;
use failure::Error;
use futures::sync::mpsc::{channel, Sender};
use graph::components::ethereum::*;
use graph::components::store::*;
use graph::data::store::scalar;
use graph::data::subgraph::*;
use graph::web3::types::*;
use hex;
use std::io::Cursor;
use std::str::FromStr;

use super::*;

#[derive(Default)]
struct MockEthereumAdapter {}

impl EthereumAdapter for MockEthereumAdapter {
    fn net_identifiers(
        &self,
        _: &Logger,
    ) -> Box<Future<Item = EthereumNetworkIdentifier, Error = Error> + Send> {
        unimplemented!();
    }

    fn block_by_hash(
        &self,
        _: &Logger,
        _: H256,
    ) -> Box<Future<Item = Option<EthereumBlock>, Error = Error> + Send> {
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

fn test_module(
    data_source: DataSource,
) -> (WasmiModule<
    MockEthereumAdapter,
    ipfs_api::IpfsClient,
    FakeStore,
    Sender<Box<Future<Item = (), Error = ()> + Send>>,
>) {
    let logger = Logger::root(slog::Discard, o!());
    let mock_ethereum_adapter = Arc::new(MockEthereumAdapter::default());
    let (task_sender, task_receiver) = channel(100);
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.spawn(task_receiver.for_each(tokio::spawn));
    ::std::mem::forget(runtime);
    WasmiModule::new(
        &logger,
        WasmiModuleConfig {
            subgraph_id: SubgraphId::new("testsubgraph").unwrap(),
            data_source,
            ethereum_adapter: mock_ethereum_adapter,
            link_resolver: Arc::new(ipfs_api::IpfsClient::default()),
            store: Arc::new(FakeStore),
        },
        task_sender,
    ).unwrap()
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

impl<T, L, S, U> WasmiModule<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + 'static,
{
    fn takes_val_returns_ptr<P>(&mut self, fn_name: &str, val: RuntimeValue) -> AscPtr<P> {
        self.module
            .invoke_export(fn_name, &[val], &mut self.externals)
            .expect("call failed")
            .expect("call returned nothing")
            .try_into()
            .expect("call did not return pointer")
    }
}

#[test]
fn json_conversions() {
    let mut module = test_module(mock_data_source("wasm_test/string_to_number.wasm"));

    // test u64 conversion
    let number = 9223372036850770800;
    let converted: u64 = module
        .module
        .invoke_export(
            "testToU64",
            &[RuntimeValue::from(module.heap.asc_new(&number.to_string()))],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return I64");
    assert_eq!(number, converted);

    // test i64 conversion
    let number = -9223372036850770800;
    let converted: i64 = module
        .module
        .invoke_export(
            "testToI64",
            &[RuntimeValue::from(module.heap.asc_new(&number.to_string()))],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return I64");
    assert_eq!(number, converted);

    // test f64 conversion
    let number = F64::from(-9223372036850770.92345034);
    let converted: F64 = module
        .module
        .invoke_export(
            "testToF64",
            &[RuntimeValue::from(
                module.heap.asc_new(&number.to_float().to_string()),
            )],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return F64");
    assert_eq!(number, converted);

    // test BigInt conversion
    let number = "-922337203685077092345034";
    let big_int_obj: AscPtr<AscBigInt> = module
        .module
        .invoke_export(
            "testToBigInt",
            &[RuntimeValue::from(module.heap.asc_new(number))],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let bytes: Vec<u8> = module.heap.asc_get(big_int_obj);
    assert_eq!(
        scalar::BigInt::from_str(number).unwrap(),
        scalar::BigInt::from_signed_bytes_le(&bytes)
    );
}

#[test]
fn ipfs_cat() {
    let mut module = test_module(mock_data_source("wasm_test/ipfs_cat.wasm"));
    let ipfs = Arc::new(ipfs_api::IpfsClient::default());

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let hash = runtime.block_on(ipfs.add(Cursor::new("42"))).unwrap().hash;
    let converted: AscPtr<AscString> = module
        .module
        .invoke_export(
            "ipfsCat",
            &[RuntimeValue::from(module.heap.asc_new(&hash))],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let data: String = module.heap.asc_get(converted);
    assert_eq!(data, "42");
}

#[test]
fn crypto_keccak256() {
    let mut module = test_module(mock_data_source("wasm_test/crypto.wasm"));
    let input: &[u8] = "eth".as_ref();
    let input: AscPtr<Uint8Array> = module.heap.asc_new(input);

    let hash: AscPtr<Uint8Array> = module
        .module
        .invoke_export("hash", &[RuntimeValue::from(input)], &mut module.externals)
        .expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let hash: Vec<u8> = module.heap.asc_get(hash);
    assert_eq!(
        hex::encode(hash),
        "4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0"
    );
}

#[test]
fn token_numeric_conversion() {
    let mut module = test_module(mock_data_source("wasm_test/token_to_numeric.wasm"));

    // Convert numeric to token and back.
    let num = i32::min_value();
    let token_ptr: AscPtr<AscEnum<EthereumValueKind>> =
        module.takes_val_returns_ptr("token_from_i32", RuntimeValue::from(num));
    let num_return = module
        .module
        .invoke_export(
            "token_to_i32",
            &[RuntimeValue::from(token_ptr)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into::<i32>()
        .expect("call did not return i32");
    assert_eq!(num, num_return);
}

#[test]
fn big_int_to_from_i32() {
    let mut module = test_module(mock_data_source("wasm_test/big_int_to_from_i32.wasm"));

    // Convert i32 to BigInt
    let input: i32 = -157;
    let output_ptr: AscPtr<AscBigInt> = module
        .module
        .invoke_export(
            "i32_to_big_int",
            &[RuntimeValue::from(input)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let output: BigInt = module.heap.asc_get(output_ptr);
    assert_eq!(output, BigInt::from(-157 as i32));

    // Convert BigInt to i32
    let input = BigInt::from(-50 as i32);
    let input_ptr: AscPtr<AscBigInt> = module.heap.asc_new(&input);
    let output: i32 = module
        .module
        .invoke_export(
            "big_int_to_i32",
            &[RuntimeValue::from(input_ptr)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    assert_eq!(output, -50 as i32);
}

#[test]
fn big_int_to_hex() {
    let mut module = test_module(mock_data_source("wasm_test/big_int_to_hex.wasm"));

    // Convert zero to hex
    let zero = BigInt::from_unsigned_u256(&U256::zero());
    let zero: AscPtr<AscBigInt> = module.heap.asc_new(&zero);
    let zero_hex_ptr: AscPtr<AscString> = module
        .module
        .invoke_export(
            "big_int_to_hex",
            &[RuntimeValue::from(zero)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let zero_hex_str: String = module.heap.asc_get(zero_hex_ptr);
    assert_eq!(zero_hex_str, "0x0");

    // Convert 1 to hex
    let one = BigInt::from_unsigned_u256(&U256::one());
    let one: AscPtr<AscBigInt> = module.heap.asc_new(&one);
    let one_hex_ptr: AscPtr<AscString> = module
        .module
        .invoke_export(
            "big_int_to_hex",
            &[RuntimeValue::from(one)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let one_hex_str: String = module.heap.asc_get(one_hex_ptr);
    assert_eq!(one_hex_str, "0x1");

    // Convert U256::max_value() to hex
    let u256_max = BigInt::from_unsigned_u256(&U256::max_value());
    let u256_max: AscPtr<AscBigInt> = module.heap.asc_new(&u256_max);
    let u256_max_hex_ptr: AscPtr<AscString> = module
        .module
        .invoke_export(
            "big_int_to_hex",
            &[RuntimeValue::from(u256_max)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let u256_max_hex_str: String = module.heap.asc_get(u256_max_hex_ptr);
    assert_eq!(
        u256_max_hex_str,
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    );
}

#[test]
fn big_int_arithmetic() {
    let mut module = test_module(mock_data_source("wasm_test/big_int_arithmetic.wasm"));

    // 0 + 1 = 1
    let zero = BigInt::from(0);
    let zero: AscPtr<AscBigInt> = module.heap.asc_new(&zero);
    let one = BigInt::from(1);
    let one: AscPtr<AscBigInt> = module.heap.asc_new(&one);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .invoke_export(
            "plus",
            &[RuntimeValue::from(zero), RuntimeValue::from(one)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.heap.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(1));

    // 5 - 10 = -5
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.heap.asc_new(&five);
    let ten = BigInt::from(10);
    let ten: AscPtr<AscBigInt> = module.heap.asc_new(&ten);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .invoke_export(
            "minus",
            &[RuntimeValue::from(five), RuntimeValue::from(ten)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.heap.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(-5));

    // -20 * 5 = -100
    let minus_twenty = BigInt::from(-20);
    let minus_twenty: AscPtr<AscBigInt> = module.heap.asc_new(&minus_twenty);
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.heap.asc_new(&five);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .invoke_export(
            "times",
            &[RuntimeValue::from(minus_twenty), RuntimeValue::from(five)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.heap.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(-100));

    // 5 / 2 = 2
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.heap.asc_new(&five);
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = module.heap.asc_new(&two);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .invoke_export(
            "dividedBy",
            &[RuntimeValue::from(five), RuntimeValue::from(two)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.heap.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(2));

    // 5 % 2 = 1
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.heap.asc_new(&five);
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = module.heap.asc_new(&two);
    let result_ptr: AscPtr<AscBigInt> = module
        .module
        .invoke_export(
            "mod",
            &[RuntimeValue::from(five), RuntimeValue::from(two)],
            &mut module.externals,
        ).expect("call failed")
        .expect("call returned nothing")
        .try_into()
        .expect("call did not return pointer");
    let result: BigInt = module.heap.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(1));
}

#[test]
fn abort() {
    let mut module = test_module(mock_data_source("wasm_test/abort.wasm"));
    let err = module
        .module
        .invoke_export("abort", &[], &mut module.externals)
        .unwrap_err();
    assert_eq!(err.to_string(), "Trap: Trap { kind: Host(HostExportError(\"Mapping aborted at abort.ts, line 7, column 2, with message: not true\")) }");
}
