use graph::data::store::scalar;
use graph::data::subgraph::*;
use graph::prelude::web3::types::U256;
use graph::prelude::*;
use graph::runtime::AscPtr;
use graph::runtime::{asc_get, asc_new, try_asc_get};
use graph::{components::store::*, ipfs_client::IpfsClient};
use graph_chain_ethereum::{Chain, DataSource};
use graph_mock::MockMetricsRegistry;
use graph_runtime_wasm::asc_abi::class::{Array, AscBigInt, AscEntity, AscString, Uint8Array};
use graph_runtime_wasm::{ExperimentalFeatures, ValidModule, WasmInstance};
use hex;
use semver::Version;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use test_store::{LOGGER, STORE};
use web3::types::H160;

use crate::common::{mock_context, mock_data_source};

mod abi;

const API_VERSION_0_0_4: Version = Version::new(0, 0, 4);
const API_VERSION_0_0_5: Version = Version::new(0, 0, 5);

fn wasm_file_path(wasm_file: &str, api_version: Version) -> String {
    format!(
        "wasm_test/api_version_{}_{}_{}/{}",
        api_version.major, api_version.minor, api_version.patch, wasm_file
    )
}

fn subgraph_id_with_api_version(subgraph_id: &str, api_version: Version) -> String {
    format!(
        "{}_{}_{}_{}",
        subgraph_id, api_version.major, api_version.minor, api_version.patch
    )
}

fn test_valid_module_and_store(
    subgraph_id: &str,
    data_source: DataSource,
    api_version: Version,
) -> (
    WasmInstance<Chain>,
    Arc<impl SubgraphStore>,
    DeploymentLocator,
) {
    test_valid_module_and_store_with_timeout(subgraph_id, data_source, api_version, None)
}

fn test_valid_module_and_store_with_timeout(
    subgraph_id: &str,
    data_source: DataSource,
    api_version: Version,
    timeout: Option<Duration>,
) -> (
    WasmInstance<Chain>,
    Arc<impl SubgraphStore>,
    DeploymentLocator,
) {
    let subgraph_id_with_api_version =
        subgraph_id_with_api_version(subgraph_id, api_version.clone());

    let store = STORE.clone();
    let metrics_registry = Arc::new(MockMetricsRegistry::new());
    let deployment_id = DeploymentHash::new(&subgraph_id_with_api_version).unwrap();
    let deployment = test_store::create_test_subgraph(
        &deployment_id,
        "type User @entity {
            id: ID!,
            name: String,
        }

        type Thing @entity {
            id: ID!,
            value: String,
            extra: String
        }",
    );
    let stopwatch_metrics = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        deployment_id.clone(),
        metrics_registry.clone(),
    );
    let host_metrics = Arc::new(HostMetrics::new(
        metrics_registry,
        deployment_id.as_str(),
        stopwatch_metrics,
    ));

    let experimental_features = ExperimentalFeatures {
        allow_non_deterministic_ipfs: true,
    };

    let module = WasmInstance::from_valid_module_with_ctx(
        Arc::new(ValidModule::new(data_source.mapping.runtime.as_ref()).unwrap()),
        mock_context(
            deployment.clone(),
            data_source,
            store.subgraph_store(),
            api_version,
        ),
        host_metrics,
        timeout,
        experimental_features,
    )
    .unwrap();

    (module, store.subgraph_store(), deployment)
}

fn test_module(
    subgraph_id: &str,
    data_source: DataSource,
    api_version: Version,
) -> WasmInstance<Chain> {
    test_valid_module_and_store(subgraph_id, data_source, api_version).0
}

trait WasmInstanceExt {
    fn invoke_export0_void(&self, f: &str);
    fn invoke_export0<R>(&self, f: &str) -> AscPtr<R>;
    fn invoke_export1<C, R>(&self, f: &str, arg: AscPtr<C>) -> AscPtr<R>;
    fn invoke_export2<C, D, R>(&self, f: &str, arg0: AscPtr<C>, arg1: AscPtr<D>) -> AscPtr<R>;
    fn invoke_export2_void<C, D>(
        &self,
        f: &str,
        arg0: AscPtr<C>,
        arg1: AscPtr<D>,
    ) -> Result<(), wasmtime::Trap>;
    fn takes_ptr_returns_val<P, V: wasmtime::WasmTy>(&mut self, fn_name: &str, v: AscPtr<P>) -> V;
    fn takes_val_returns_ptr<P>(&mut self, fn_name: &str, val: impl wasmtime::WasmTy) -> AscPtr<P>;
}

impl WasmInstanceExt for WasmInstance<Chain> {
    fn invoke_export0_void(&self, f: &str) {
        let func = self.get_func(f).typed().unwrap().clone();
        let _: () = func.call(()).unwrap();
    }

    fn invoke_export0<R>(&self, f: &str) -> AscPtr<R> {
        let func = self.get_func(f).typed().unwrap().clone();
        let ptr: u32 = func.call(()).unwrap();
        ptr.into()
    }

    fn invoke_export1<C, R>(&self, f: &str, arg: AscPtr<C>) -> AscPtr<R> {
        let func = self.get_func(f).typed().unwrap().clone();
        let ptr: u32 = func.call(arg.wasm_ptr()).unwrap();
        ptr.into()
    }

    fn invoke_export2<C, D, R>(&self, f: &str, arg0: AscPtr<C>, arg1: AscPtr<D>) -> AscPtr<R> {
        let func = self.get_func(f).typed().unwrap().clone();
        let ptr: u32 = func.call((arg0.wasm_ptr(), arg1.wasm_ptr())).unwrap();
        ptr.into()
    }

    fn invoke_export2_void<C, D>(
        &self,
        f: &str,
        arg0: AscPtr<C>,
        arg1: AscPtr<D>,
    ) -> Result<(), wasmtime::Trap> {
        let func = self.get_func(f).typed().unwrap().clone();
        func.call((arg0.wasm_ptr(), arg1.wasm_ptr()))
    }

    fn takes_ptr_returns_val<P, V: wasmtime::WasmTy>(&mut self, fn_name: &str, v: AscPtr<P>) -> V {
        let func = self.get_func(fn_name).typed().unwrap().clone();
        func.call(v.wasm_ptr()).unwrap()
    }

    fn takes_val_returns_ptr<P>(&mut self, fn_name: &str, val: impl wasmtime::WasmTy) -> AscPtr<P> {
        let func = self.get_func(fn_name).typed().unwrap().clone();
        let ptr: u32 = func.call(val).unwrap();
        ptr.into()
    }
}

fn test_json_conversions(api_version: Version, gas_used: u64) {
    let mut module = test_module(
        "jsonConversions",
        mock_data_source(
            &wasm_file_path("string_to_number.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    // test u64 conversion
    let number = 9223372036850770800;
    let number_ptr = asc_new(&mut module, &number.to_string()).unwrap();
    let converted: i64 = module.takes_ptr_returns_val("testToU64", number_ptr);
    assert_eq!(number, u64::from_le_bytes(converted.to_le_bytes()));

    // test i64 conversion
    let number = -9223372036850770800;
    let number_ptr = asc_new(&mut module, &number.to_string()).unwrap();
    let converted: i64 = module.takes_ptr_returns_val("testToI64", number_ptr);
    assert_eq!(number, converted);

    // test f64 conversion
    let number = -9223372036850770.92345034;
    let number_ptr = asc_new(&mut module, &number.to_string()).unwrap();
    let converted: f64 = module.takes_ptr_returns_val("testToF64", number_ptr);
    assert_eq!(number, converted);

    // test BigInt conversion
    let number = "-922337203685077092345034";
    let number_ptr = asc_new(&mut module, number).unwrap();
    let big_int_obj: AscPtr<AscBigInt> = module.invoke_export1("testToBigInt", number_ptr);
    let bytes: Vec<u8> = asc_get(&module, big_int_obj).unwrap();

    assert_eq!(
        scalar::BigInt::from_str(number).unwrap(),
        scalar::BigInt::from_signed_bytes_le(&bytes)
    );

    assert_eq!(module.gas_used(), gas_used);
}

#[tokio::test]
async fn json_conversions_v0_0_4() {
    test_json_conversions(API_VERSION_0_0_4, 51937534);
}

#[tokio::test]
async fn json_conversions_v0_0_5() {
    test_json_conversions(API_VERSION_0_0_5, 912148);
}

fn test_json_parsing(api_version: Version, gas_used: u64) {
    let mut module = test_module(
        "jsonParsing",
        mock_data_source(
            &wasm_file_path("json_parsing.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    // Parse invalid JSON and handle the error gracefully
    let s = "foo"; // Invalid because there are no quotes around `foo`
    let bytes: &[u8] = s.as_ref();
    let bytes_ptr = asc_new(&mut module, bytes).unwrap();
    let return_value: AscPtr<AscString> = module.invoke_export1("handleJsonError", bytes_ptr);
    let output: String = asc_get(&module, return_value).unwrap();
    assert_eq!(output, "ERROR: true");

    // Parse valid JSON and get it back
    let s = "\"foo\""; // Valid because there are quotes around `foo`
    let bytes: &[u8] = s.as_ref();
    let bytes_ptr = asc_new(&mut module, bytes).unwrap();
    let return_value: AscPtr<AscString> = module.invoke_export1("handleJsonError", bytes_ptr);

    let output: String = asc_get(&module, return_value).unwrap();
    assert_eq!(output, "OK: foo, ERROR: false");
    assert_eq!(module.gas_used(), gas_used);
}

#[tokio::test]
async fn json_parsing_v0_0_4() {
    test_json_parsing(API_VERSION_0_0_4, 2062683);
}

#[tokio::test]
async fn json_parsing_v0_0_5() {
    test_json_parsing(API_VERSION_0_0_5, 2693805);
}

async fn test_ipfs_cat(api_version: Version) {
    let ipfs = IpfsClient::localhost();
    let hash = ipfs.add("42".into()).await.unwrap().hash;

    // Ipfs host functions use `block_on` which must be called from a sync context,
    // so we replicate what we do `spawn_module`.
    let runtime = tokio::runtime::Handle::current();
    std::thread::spawn(move || {
        let _runtime_guard = runtime.enter();

        let mut module = test_module(
            "ipfsCat",
            mock_data_source(
                &wasm_file_path("ipfs_cat.wasm", api_version.clone()),
                api_version.clone(),
            ),
            api_version,
        );
        let arg = asc_new(&mut module, &hash).unwrap();
        let converted: AscPtr<AscString> = module.invoke_export1("ipfsCatString", arg);
        let data: String = asc_get(&module, converted).unwrap();
        assert_eq!(data, "42");
    })
    .join()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ipfs_cat_v0_0_4() {
    test_ipfs_cat(API_VERSION_0_0_4).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ipfs_cat_v0_0_5() {
    test_ipfs_cat(API_VERSION_0_0_5).await;
}

// The user_data value we use with calls to ipfs_map
const USER_DATA: &str = "user_data";

fn make_thing(
    subgraph_id: &str,
    id: &str,
    value: &str,
    api_version: Version,
) -> (String, EntityModification) {
    let subgraph_id_with_api_version = subgraph_id_with_api_version(subgraph_id, api_version);

    let mut data = Entity::new();
    data.set("id", id);
    data.set("value", value);
    data.set("extra", USER_DATA);
    let key = EntityKey::data(
        DeploymentHash::new(&subgraph_id_with_api_version).unwrap(),
        "Thing".to_string(),
        id.to_string(),
    );
    (
        format!("{{ \"id\": \"{}\", \"value\": \"{}\"}}", id, value),
        EntityModification::Insert { key, data },
    )
}

const BAD_IPFS_HASH: &str = "bad-ipfs-hash";

async fn run_ipfs_map(
    ipfs: IpfsClient,
    subgraph_id: &'static str,
    json_string: String,
    api_version: Version,
) -> Result<Vec<EntityModification>, anyhow::Error> {
    let hash = if json_string == BAD_IPFS_HASH {
        "Qm".to_string()
    } else {
        ipfs.add(json_string.into()).await.unwrap().hash
    };

    // Ipfs host functions use `block_on` which must be called from a sync context,
    // so we replicate what we do `spawn_module`.
    let runtime = tokio::runtime::Handle::current();
    std::thread::spawn(move || {
        let _runtime_guard = runtime.enter();

        let (mut module, _, _) = test_valid_module_and_store(
            &subgraph_id,
            mock_data_source(
                &wasm_file_path("ipfs_map.wasm", api_version.clone()),
                api_version.clone(),
            ),
            api_version,
        );
        let value = asc_new(&mut module, &hash).unwrap();
        let user_data = asc_new(&mut module, USER_DATA).unwrap();

        // Invoke the callback
        let func = module.get_func("ipfsMap").typed().unwrap().clone();
        let _: () = func.call((value.wasm_ptr(), user_data.wasm_ptr()))?;
        let mut mods = module
            .take_ctx()
            .ctx
            .state
            .entity_cache
            .as_modifications()?
            .modifications;

        // Bring the modifications into a predictable order (by entity_id)
        mods.sort_by(|a, b| {
            a.entity_key()
                .entity_id
                .partial_cmp(&b.entity_key().entity_id)
                .unwrap()
        });
        Ok(mods)
    })
    .join()
    .unwrap()
}

async fn test_ipfs_map(api_version: Version, json_error_msg: &str) {
    let ipfs = IpfsClient::localhost();
    let subgraph_id = "ipfsMap";

    // Try it with two valid objects
    let (str1, thing1) = make_thing(&subgraph_id, "one", "eins", api_version.clone());
    let (str2, thing2) = make_thing(&subgraph_id, "two", "zwei", api_version.clone());
    let ops = run_ipfs_map(
        ipfs.clone(),
        subgraph_id,
        format!("{}\n{}", str1, str2),
        api_version.clone(),
    )
    .await
    .expect("call failed");
    let expected = vec![thing1, thing2];
    assert_eq!(expected, ops);

    // Valid JSON, but not what the callback expected; it will
    // fail on an assertion
    let err = run_ipfs_map(
        ipfs.clone(),
        subgraph_id,
        format!("{}\n[1,2]", str1),
        api_version.clone(),
    )
    .await
    .unwrap_err();
    assert!(
        format!("{:#}", err).contains("JSON value is not an object."),
        "{:#}",
        err
    );

    // Malformed JSON
    let errmsg = run_ipfs_map(
        ipfs.clone(),
        subgraph_id,
        format!("{}\n[", str1),
        api_version.clone(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(errmsg.contains("EOF while parsing a list"));

    // Empty input
    let ops = run_ipfs_map(
        ipfs.clone(),
        subgraph_id,
        "".to_string(),
        api_version.clone(),
    )
    .await
    .expect("call failed for emoty string");
    assert_eq!(0, ops.len());

    // Missing entry in the JSON object
    let errmsg = format!(
        "{:#}",
        run_ipfs_map(
            ipfs.clone(),
            subgraph_id,
            "{\"value\": \"drei\"}".to_string(),
            api_version.clone(),
        )
        .await
        .unwrap_err()
    );
    assert!(errmsg.contains(json_error_msg));

    // Bad IPFS hash.
    let errmsg = run_ipfs_map(
        ipfs.clone(),
        subgraph_id,
        BAD_IPFS_HASH.to_string(),
        api_version.clone(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(errmsg.contains("500 Internal Server Error"));
}

#[tokio::test(flavor = "multi_thread")]
async fn ipfs_map_v0_0_4() {
    test_ipfs_map(API_VERSION_0_0_4, "JSON value is not a string.").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ipfs_map_v0_0_5() {
    test_ipfs_map(API_VERSION_0_0_5, "'id' should not be null").await;
}

async fn test_ipfs_fail(api_version: Version) {
    let runtime = tokio::runtime::Handle::current();

    // Ipfs host functions use `block_on` which must be called from a sync context,
    // so we replicate what we do `spawn_module`.
    std::thread::spawn(move || {
        let _runtime_guard = runtime.enter();
        let mut module = test_module(
            "ipfsFail",
            mock_data_source(
                &wasm_file_path("ipfs_cat.wasm", api_version.clone()),
                api_version.clone(),
            ),
            api_version,
        );

        let hash = asc_new(&mut module, "invalid hash").unwrap();
        assert!(module
            .invoke_export1::<_, AscString>("ipfsCat", hash)
            .is_null());
    })
    .join()
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ipfs_fail_v0_0_4() {
    test_ipfs_fail(API_VERSION_0_0_4).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ipfs_fail_v0_0_5() {
    test_ipfs_fail(API_VERSION_0_0_5).await;
}

fn test_crypto_keccak256(api_version: Version) {
    let mut module = test_module(
        "cryptoKeccak256",
        mock_data_source(
            &wasm_file_path("crypto.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );
    let input: &[u8] = "eth".as_ref();
    let input: AscPtr<Uint8Array> = asc_new(&mut module, input).unwrap();

    let hash: AscPtr<Uint8Array> = module.invoke_export1("hash", input);
    let hash: Vec<u8> = asc_get(&module, hash).unwrap();
    assert_eq!(
        hex::encode(hash),
        "4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0"
    );
}

#[tokio::test]
async fn crypto_keccak256_v0_0_4() {
    test_crypto_keccak256(API_VERSION_0_0_4);
}

#[tokio::test]
async fn crypto_keccak256_v0_0_5() {
    test_crypto_keccak256(API_VERSION_0_0_5);
}

fn test_big_int_to_hex(api_version: Version, gas_used: u64) {
    let mut module = test_module(
        "BigIntToHex",
        mock_data_source(
            &wasm_file_path("big_int_to_hex.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    // Convert zero to hex
    let zero = BigInt::from_unsigned_u256(&U256::zero());
    let zero: AscPtr<AscBigInt> = asc_new(&mut module, &zero).unwrap();
    let zero_hex_ptr: AscPtr<AscString> = module.invoke_export1("big_int_to_hex", zero);
    let zero_hex_str: String = asc_get(&module, zero_hex_ptr).unwrap();
    assert_eq!(zero_hex_str, "0x0");

    // Convert 1 to hex
    let one = BigInt::from_unsigned_u256(&U256::one());
    let one: AscPtr<AscBigInt> = asc_new(&mut module, &one).unwrap();
    let one_hex_ptr: AscPtr<AscString> = module.invoke_export1("big_int_to_hex", one);
    let one_hex_str: String = asc_get(&module, one_hex_ptr).unwrap();
    assert_eq!(one_hex_str, "0x1");

    // Convert U256::max_value() to hex
    let u256_max = BigInt::from_unsigned_u256(&U256::max_value());
    let u256_max: AscPtr<AscBigInt> = asc_new(&mut module, &u256_max).unwrap();
    let u256_max_hex_ptr: AscPtr<AscString> = module.invoke_export1("big_int_to_hex", u256_max);
    let u256_max_hex_str: String = asc_get(&module, u256_max_hex_ptr).unwrap();
    assert_eq!(
        u256_max_hex_str,
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    );

    assert_eq!(module.gas_used(), gas_used);
}

#[tokio::test]
async fn big_int_to_hex_v0_0_4() {
    test_big_int_to_hex(API_VERSION_0_0_4, 51770565);
}

#[tokio::test]
async fn big_int_to_hex_v0_0_5() {
    test_big_int_to_hex(API_VERSION_0_0_5, 962685);
}

fn test_big_int_arithmetic(api_version: Version, gas_used: u64) {
    let mut module = test_module(
        "BigIntArithmetic",
        mock_data_source(
            &wasm_file_path("big_int_arithmetic.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    // 0 + 1 = 1
    let zero = BigInt::from(0);
    let zero: AscPtr<AscBigInt> = asc_new(&mut module, &zero).unwrap();
    let one = BigInt::from(1);
    let one: AscPtr<AscBigInt> = asc_new(&mut module, &one).unwrap();
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("plus", zero, one);
    let result: BigInt = asc_get(&module, result_ptr).unwrap();
    assert_eq!(result, BigInt::from(1));

    // 127 + 1 = 128
    let zero = BigInt::from(127);
    let zero: AscPtr<AscBigInt> = asc_new(&mut module, &zero).unwrap();
    let one = BigInt::from(1);
    let one: AscPtr<AscBigInt> = asc_new(&mut module, &one).unwrap();
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("plus", zero, one);
    let result: BigInt = asc_get(&module, result_ptr).unwrap();
    assert_eq!(result, BigInt::from(128));

    // 5 - 10 = -5
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = asc_new(&mut module, &five).unwrap();
    let ten = BigInt::from(10);
    let ten: AscPtr<AscBigInt> = asc_new(&mut module, &ten).unwrap();
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("minus", five, ten);
    let result: BigInt = asc_get(&module, result_ptr).unwrap();
    assert_eq!(result, BigInt::from(-5));

    // -20 * 5 = -100
    let minus_twenty = BigInt::from(-20);
    let minus_twenty: AscPtr<AscBigInt> = asc_new(&mut module, &minus_twenty).unwrap();
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = asc_new(&mut module, &five).unwrap();
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("times", minus_twenty, five);
    let result: BigInt = asc_get(&module, result_ptr).unwrap();
    assert_eq!(result, BigInt::from(-100));

    // 5 / 2 = 2
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = asc_new(&mut module, &five).unwrap();
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = asc_new(&mut module, &two).unwrap();
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("dividedBy", five, two);
    let result: BigInt = asc_get(&module, result_ptr).unwrap();
    assert_eq!(result, BigInt::from(2));

    // 5 % 2 = 1
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = asc_new(&mut module, &five).unwrap();
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = asc_new(&mut module, &two).unwrap();
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("mod", five, two);
    let result: BigInt = asc_get(&module, result_ptr).unwrap();
    assert_eq!(result, BigInt::from(1));

    assert_eq!(module.gas_used(), gas_used);
}

#[tokio::test]
async fn big_int_arithmetic_v0_0_4() {
    test_big_int_arithmetic(API_VERSION_0_0_4, 52099180);
}

#[tokio::test]
async fn big_int_arithmetic_v0_0_5() {
    test_big_int_arithmetic(API_VERSION_0_0_5, 3035221);
}

fn test_abort(api_version: Version, error_msg: &str) {
    let module = test_module(
        "abort",
        mock_data_source(
            &wasm_file_path("abort.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );
    let res: Result<(), _> = module.get_func("abort").typed().unwrap().call(());
    assert!(res.unwrap_err().to_string().contains(error_msg));
}

#[tokio::test]
async fn abort_v0_0_4() {
    test_abort(
        API_VERSION_0_0_4,
        "line 6, column 2, with message: not true",
    );
}

#[tokio::test]
async fn abort_v0_0_5() {
    test_abort(
        API_VERSION_0_0_5,
        "line 4, column 3, with message: not true",
    );
}

fn test_bytes_to_base58(api_version: Version, gas_used: u64) {
    let mut module = test_module(
        "bytesToBase58",
        mock_data_source(
            &wasm_file_path("bytes_to_base58.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );
    let bytes = hex::decode("12207D5A99F603F231D53A4F39D1521F98D2E8BB279CF29BEBFD0687DC98458E7F89")
        .unwrap();
    let bytes_ptr = asc_new(&mut module, bytes.as_slice()).unwrap();
    let result_ptr: AscPtr<AscString> = module.invoke_export1("bytes_to_base58", bytes_ptr);
    let base58: String = asc_get(&module, result_ptr).unwrap();

    assert_eq!(base58, "QmWmyoMoctfbAaiEs2G46gpeUmhqFRDW6KWo64y5r581Vz");
    assert_eq!(module.gas_used(), gas_used);
}

#[tokio::test]
async fn bytes_to_base58_v0_0_4() {
    test_bytes_to_base58(API_VERSION_0_0_4, 51577627);
}

#[tokio::test]
async fn bytes_to_base58_v0_0_5() {
    test_bytes_to_base58(API_VERSION_0_0_5, 477157);
}

fn test_data_source_create(api_version: Version, gas_used: u64) {
    let run_data_source_create =
        move |name: String,
              params: Vec<String>|
              -> Result<Vec<DataSourceTemplateInfo<Chain>>, wasmtime::Trap> {
            let mut module = test_module(
                "DataSourceCreate",
                mock_data_source(
                    &wasm_file_path("data_source_create.wasm", api_version.clone()),
                    api_version.clone(),
                ),
                api_version.clone(),
            );

            let name = asc_new(&mut module, &name).unwrap();
            let params = asc_new(&mut module, params.as_slice()).unwrap();
            module.instance_ctx_mut().ctx.state.enter_handler();
            module.invoke_export2_void("dataSourceCreate", name, params)?;
            module.instance_ctx_mut().ctx.state.exit_handler();

            assert_eq!(module.gas_used(), gas_used);

            Ok(module.take_ctx().ctx.state.drain_created_data_sources())
        };

    // Test with a valid template
    let template = String::from("example template");
    let params = vec![String::from("0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95")];
    let result = run_data_source_create(template.clone(), params.clone())
        .expect("unexpected error returned from dataSourceCreate");
    assert_eq!(result[0].params, params.clone());
    assert_eq!(result[0].template.name, template);

    // Test with a template that doesn't exist
    let template = String::from("nonexistent template");
    let params = vec![String::from("0xc000000000000000000000000000000000000000")];
    match run_data_source_create(template.clone(), params.clone()) {
        Ok(_) => panic!("expected an error because the template does not exist"),
        Err(e) => assert!(e.to_string().contains(
            "Failed to create data source from name `nonexistent template`: \
             No template with this name in parent data source `example data source`. \
             Available names: example template."
        )),
    };
}

#[tokio::test]
async fn data_source_create_v0_0_4() {
    test_data_source_create(API_VERSION_0_0_4, 151393645);
}

#[tokio::test]
async fn data_source_create_v0_0_5() {
    test_data_source_create(API_VERSION_0_0_5, 100440279);
}

fn test_ens_name_by_hash(api_version: Version) {
    let mut module = test_module(
        "EnsNameByHash",
        mock_data_source(
            &wasm_file_path("ens_name_by_hash.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    let hash = "0x7f0c1b04d1a4926f9c635a030eeb611d4c26e5e73291b32a1c7a4ac56935b5b3";
    let name = "dealdrafts";
    test_store::insert_ens_name(hash, name);
    let val = asc_new(&mut module, hash).unwrap();
    let converted: AscPtr<AscString> = module.invoke_export1("nameByHash", val);
    let data: String = asc_get(&module, converted).unwrap();
    assert_eq!(data, name);

    let hash = asc_new(&mut module, "impossible keccak hash").unwrap();
    assert!(module
        .invoke_export1::<_, AscString>("nameByHash", hash)
        .is_null());
}

#[tokio::test]
async fn ens_name_by_hash_v0_0_4() {
    test_ens_name_by_hash(API_VERSION_0_0_4);
}

#[tokio::test]
async fn ens_name_by_hash_v0_0_5() {
    test_ens_name_by_hash(API_VERSION_0_0_5);
}

fn test_entity_store(api_version: Version) {
    let (mut module, store, deployment) = test_valid_module_and_store(
        "entityStore",
        mock_data_source(
            &wasm_file_path("store.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    let mut alex = Entity::new();
    alex.set("id", "alex");
    alex.set("name", "Alex");
    let mut steve = Entity::new();
    steve.set("id", "steve");
    steve.set("name", "Steve");
    let user_type = EntityType::from("User");
    test_store::insert_entities(
        &deployment,
        vec![(user_type.clone(), alex), (user_type, steve)],
    )
    .unwrap();

    let get_user = move |module: &mut WasmInstance<Chain>, id: &str| -> Option<Entity> {
        let id = asc_new(module, id).unwrap();
        let entity_ptr: AscPtr<AscEntity> = module.invoke_export1("getUser", id);
        if entity_ptr.is_null() {
            None
        } else {
            Some(Entity::from(
                try_asc_get::<HashMap<String, Value>, _, _>(module, entity_ptr).unwrap(),
            ))
        }
    };

    let load_and_set_user_name = |module: &mut WasmInstance<Chain>, id: &str, name: &str| {
        let id_ptr = asc_new(module, id).unwrap();
        let name_ptr = asc_new(module, name).unwrap();
        module
            .invoke_export2_void("loadAndSetUserName", id_ptr, name_ptr)
            .unwrap();
    };

    // store.get of a nonexistent user
    assert_eq!(None, get_user(&mut module, "herobrine"));
    // store.get of an existing user
    let steve = get_user(&mut module, "steve").unwrap();
    assert_eq!(Some(&Value::from("Steve")), steve.get("name"));

    // Load, set, save cycle for an existing entity
    load_and_set_user_name(&mut module, "steve", "Steve-O");

    // We need to empty the cache for the next test
    let writable =
        futures03::executor::block_on(store.writable(LOGGER.clone(), deployment.id)).unwrap();
    let cache = std::mem::replace(
        &mut module.instance_ctx_mut().ctx.state.entity_cache,
        EntityCache::new(writable.clone()),
    );
    let mut mods = cache.as_modifications().unwrap().modifications;
    assert_eq!(1, mods.len());
    match mods.pop().unwrap() {
        EntityModification::Overwrite { data, .. } => {
            assert_eq!(Some(&Value::from("steve")), data.get("id"));
            assert_eq!(Some(&Value::from("Steve-O")), data.get("name"));
        }
        _ => assert!(false, "expected Overwrite modification"),
    }

    // Load, set, save cycle for a new entity with fulltext API
    load_and_set_user_name(&mut module, "herobrine", "Brine-O");
    let mut fulltext_entities = BTreeMap::new();
    let mut fulltext_fields = BTreeMap::new();
    fulltext_fields.insert("name".to_string(), vec!["search".to_string()]);
    fulltext_entities.insert("User".to_string(), fulltext_fields);
    let mut mods = module
        .take_ctx()
        .ctx
        .state
        .entity_cache
        .as_modifications()
        .unwrap()
        .modifications;
    assert_eq!(1, mods.len());
    match mods.pop().unwrap() {
        EntityModification::Insert { data, .. } => {
            assert_eq!(Some(&Value::from("herobrine")), data.get("id"));
            assert_eq!(Some(&Value::from("Brine-O")), data.get("name"));
        }
        _ => assert!(false, "expected Insert modification"),
    };
}

#[tokio::test]
async fn entity_store_v0_0_4() {
    test_entity_store(API_VERSION_0_0_4);
}

#[tokio::test]
async fn entity_store_v0_0_5() {
    test_entity_store(API_VERSION_0_0_5);
}

fn test_detect_contract_calls(api_version: Version) {
    let data_source_without_calls = mock_data_source(
        &wasm_file_path("abi_store_value.wasm", api_version.clone()),
        api_version.clone(),
    );
    assert_eq!(
        data_source_without_calls
            .mapping
            .requires_archive()
            .unwrap(),
        false
    );

    let data_source_with_calls = mock_data_source(
        &wasm_file_path("contract_calls.wasm", api_version.clone()),
        api_version,
    );
    assert_eq!(
        data_source_with_calls.mapping.requires_archive().unwrap(),
        true
    );
}

#[tokio::test]
async fn detect_contract_calls_v0_0_4() {
    test_detect_contract_calls(API_VERSION_0_0_4);
}

#[tokio::test]
async fn detect_contract_calls_v0_0_5() {
    test_detect_contract_calls(API_VERSION_0_0_5);
}

fn test_allocate_global(api_version: Version) {
    let module = test_module(
        "AllocateGlobal",
        mock_data_source(
            &wasm_file_path("allocate_global.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    // Assert globals can be allocated and don't break the heap
    module.invoke_export0_void("assert_global_works");
}

#[tokio::test]
async fn allocate_global_v0_0_5() {
    // Only in apiVersion v0.0.5 because there's no issue in older versions.
    // The problem with the new one is related to the AS stub runtime `offset`
    // variable not being initialized (lazy) before we use it so this test checks
    // that it works (at the moment using __alloc call to force offset to be eagerly
    // evaluated).
    test_allocate_global(API_VERSION_0_0_5);
}

fn test_null_ptr_read(api_version: Version) {
    let module = test_module(
        "NullPtrRead",
        mock_data_source(
            &wasm_file_path("null_ptr_read.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    module.invoke_export0_void("nullPtrRead");
}

#[tokio::test]
#[should_panic(expected = "Tried to read AssemblyScript value that is 'null'")]
async fn null_ptr_read_0_0_5() {
    test_null_ptr_read(API_VERSION_0_0_5);
}

fn test_safe_null_ptr_read(api_version: Version) {
    let module = test_module(
        "SafeNullPtrRead",
        mock_data_source(
            &wasm_file_path("null_ptr_read.wasm", api_version.clone()),
            api_version.clone(),
        ),
        api_version,
    );

    module.invoke_export0_void("safeNullPtrRead");
}

#[tokio::test]
#[should_panic(expected = "Failed to sum BigInts because left hand side is 'null'")]
async fn safe_null_ptr_read_0_0_5() {
    test_safe_null_ptr_read(API_VERSION_0_0_5);
}
