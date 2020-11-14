use ethabi::Token;
use hex;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::str::FromStr;

use crate::host_exports::HostExports;
use graph::components::store::*;
use graph::data::store::scalar;
use graph::data::subgraph::*;
use graph::mock::MockEthereumAdapter;
use graph::prelude::im;
use graph_chain_arweave::adapter::ArweaveAdapter;
use graph_core;
use graph_core::three_box::ThreeBoxAdapter;
use graph_mock::MockMetricsRegistry;
use test_store::STORE;

use web3::types::{Address, H160};

use super::*;

mod abi;

fn test_valid_module_and_store(
    subgraph_id: &str,
    data_source: DataSource,
) -> (
    WasmInstance,
    Arc<impl Store + SubgraphDeploymentStore + EthereumCallCache>,
) {
    test_valid_module_and_store_with_timeout(subgraph_id, data_source, None)
}

fn test_valid_module_and_store_with_timeout(
    subgraph_id: &str,
    data_source: DataSource,
    timeout: Option<Duration>,
) -> (
    WasmInstance,
    Arc<impl Store + SubgraphDeploymentStore + EthereumCallCache>,
) {
    let store = STORE.clone();
    let metrics_registry = Arc::new(MockMetricsRegistry::new());
    let deployment_id = SubgraphDeploymentId::new(subgraph_id).unwrap();
    test_store::create_test_subgraph(
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

    let module = WasmInstance::from_valid_module_with_ctx(
        Arc::new(ValidModule::new(data_source.mapping.runtime.as_ref()).unwrap()),
        mock_context(deployment_id, data_source, store.clone()),
        host_metrics,
        timeout,
        true,
    )
    .unwrap();

    (module, store)
}

fn test_module(subgraph_id: &str, data_source: DataSource) -> WasmInstance {
    test_valid_module_and_store(subgraph_id, data_source).0
}

fn mock_data_source(path: &str) -> DataSource {
    let runtime = std::fs::read(path).unwrap();

    DataSource {
        kind: String::from("ethereum/contract"),
        name: String::from("example data source"),
        network: Some(String::from("mainnet")),
        source: Source {
            address: Some(Address::from_str("0123123123012312312301231231230123123123").unwrap()),
            abi: String::from("123123"),
            start_block: 0,
        },
        mapping: Mapping {
            kind: String::from("ethereum/events"),
            api_version: String::from("0.1.0"),
            language: String::from("wasm/assemblyscript"),
            entities: vec![],
            abis: vec![],
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
            link: Link {
                link: "link".to_owned(),
            },
            runtime: Arc::new(runtime.clone()),
        },
        templates: vec![DataSourceTemplate {
            kind: String::from("ethereum/contract"),
            name: String::from("example template"),
            network: Some(String::from("mainnet")),
            source: TemplateSource {
                abi: String::from("foo"),
            },
            mapping: Mapping {
                kind: String::from("ethereum/events"),
                api_version: String::from("0.1.0"),
                language: String::from("wasm/assemblyscript"),
                entities: vec![],
                abis: vec![],
                event_handlers: vec![],
                call_handlers: vec![],
                block_handlers: vec![],
                link: Link {
                    link: "link".to_owned(),
                },
                runtime: Arc::new(runtime),
            },
        }],
        context: None,
    }
}

fn mock_host_exports(
    subgraph_id: SubgraphDeploymentId,
    data_source: DataSource,
    store: Arc<impl Store + SubgraphDeploymentStore + EthereumCallCache>,
) -> HostExports {
    let mock_ethereum_adapter = Arc::new(MockEthereumAdapter::default());
    let arweave_adapter = Arc::new(ArweaveAdapter::new("https://arweave.net".to_string()));
    let three_box_adapter = Arc::new(ThreeBoxAdapter::new("https://ipfs.3box.io/".to_string()));

    HostExports::new(
        subgraph_id,
        Version::parse(&data_source.mapping.api_version).unwrap(),
        data_source.name,
        data_source.source.address,
        data_source.network.unwrap(),
        data_source.context,
        Arc::new(data_source.templates),
        data_source.mapping.abis,
        mock_ethereum_adapter,
        Arc::new(graph_core::LinkResolver::from(
            ipfs_api::IpfsClient::default(),
        )),
        store.clone(),
        store,
        arweave_adapter,
        three_box_adapter,
    )
}

fn mock_context(
    subgraph_id: SubgraphDeploymentId,
    data_source: DataSource,
    store: Arc<impl Store + SubgraphDeploymentStore + EthereumCallCache>,
) -> MappingContext {
    MappingContext {
        logger: test_store::LOGGER.clone(),
        block: Default::default(),
        host_exports: Arc::new(mock_host_exports(subgraph_id, data_source, store.clone())),
        state: BlockState::new(store, Default::default()),
        proof_of_indexing: None,
    }
}

impl WasmInstance {
    fn invoke_export<C, R>(&self, f: &str, arg: AscPtr<C>) -> AscPtr<R> {
        let func = self.get_func(f).get1().unwrap();
        let ptr: u32 = func(arg.wasm_ptr()).unwrap();
        ptr.into()
    }

    fn invoke_export2<C, D, R>(&self, f: &str, arg0: AscPtr<C>, arg1: AscPtr<D>) -> AscPtr<R> {
        let func = self.get_func(f).get2().unwrap();
        let ptr: u32 = func(arg0.wasm_ptr(), arg1.wasm_ptr()).unwrap();
        ptr.into()
    }

    fn invoke_export2_void<C, D>(
        &self,
        f: &str,
        arg0: AscPtr<C>,
        arg1: AscPtr<D>,
    ) -> Result<(), wasmtime::Trap> {
        let func = self.get_func(f).get2().unwrap();
        func(arg0.wasm_ptr(), arg1.wasm_ptr())
    }

    fn takes_ptr_returns_val<P, V: wasmtime::WasmTy>(&mut self, fn_name: &str, v: AscPtr<P>) -> V {
        let func = self.get_func(fn_name).get1().unwrap();
        func(v.wasm_ptr()).unwrap()
    }

    fn takes_val_returns_ptr<P>(&mut self, fn_name: &str, val: impl wasmtime::WasmTy) -> AscPtr<P> {
        let func = self.get_func(fn_name).get1().unwrap();
        let ptr: u32 = func(val).unwrap();
        ptr.into()
    }
}

#[tokio::test]
async fn json_conversions() {
    let mut module = test_module(
        "jsonConversions",
        mock_data_source("wasm_test/string_to_number.wasm"),
    );

    // test u64 conversion
    let number = 9223372036850770800;
    let number_ptr = module.asc_new(&number.to_string());
    let converted: i64 = module.takes_ptr_returns_val("testToU64", number_ptr);
    assert_eq!(number, u64::from_le_bytes(converted.to_le_bytes()));

    // test i64 conversion
    let number = -9223372036850770800;
    let number_ptr = module.asc_new(&number.to_string());
    let converted: i64 = module.takes_ptr_returns_val("testToI64", number_ptr);
    assert_eq!(number, converted);

    // test f64 conversion
    let number = -9223372036850770.92345034;
    let number_ptr = module.asc_new(&number.to_string());
    let converted: f64 = module.takes_ptr_returns_val("testToF64", number_ptr);
    assert_eq!(number, converted);

    // test BigInt conversion
    let number = "-922337203685077092345034";
    let number_ptr = module.asc_new(number);
    let big_int_obj: AscPtr<AscBigInt> = module.invoke_export("testToBigInt", number_ptr);
    let bytes: Vec<u8> = module.asc_get(big_int_obj);
    assert_eq!(
        scalar::BigInt::from_str(number).unwrap(),
        scalar::BigInt::from_signed_bytes_le(&bytes)
    );
}

#[tokio::test]
async fn json_parsing() {
    let mut module = test_module(
        "jsonParsing",
        mock_data_source("wasm_test/json_parsing.wasm"),
    );

    // Parse invalid JSON and handle the error gracefully
    let s = "foo"; // Invalid because there are no quotes around `foo`
    let bytes: &[u8] = s.as_ref();
    let bytes_ptr = module.asc_new(bytes);
    let return_value: AscPtr<AscString> = module.invoke_export("handleJsonError", bytes_ptr);
    let output: String = module.asc_get(return_value);
    assert_eq!(output, "ERROR: true");

    // Parse valid JSON and get it back
    let s = "\"foo\""; // Valid because there are quotes around `foo`
    let bytes: &[u8] = s.as_ref();
    let bytes_ptr = module.asc_new(bytes);
    let return_value: AscPtr<AscString> = module.invoke_export("handleJsonError", bytes_ptr);
    let output: String = module.asc_get(return_value);
    assert_eq!(output, "OK: foo");
}

#[tokio::test(threaded_scheduler)]
async fn ipfs_cat() {
    let ipfs = Arc::new(ipfs_api::IpfsClient::default());
    let hash = ipfs.add(Cursor::new("42")).await.unwrap().hash;

    // Ipfs host functions use `block_on` which must be called from a sync context,
    // so we replicate what we do `spawn_module`.
    let runtime = tokio::runtime::Handle::current();
    std::thread::spawn(move || {
        runtime.enter(|| {
            let mut module = test_module("ipfsCat", mock_data_source("wasm_test/ipfs_cat.wasm"));
            let arg = module.asc_new(&hash);
            let converted: AscPtr<AscString> = module.invoke_export("ipfsCatString", arg);
            let data: String = module.instance_ctx().asc_get(converted);
            assert_eq!(data, "42");
        })
    })
    .join()
    .unwrap()
}

// The user_data value we use with calls to ipfs_map
const USER_DATA: &str = "user_data";

fn make_thing(subgraph_id: &str, id: &str, value: &str) -> (String, EntityModification) {
    let mut data = Entity::new();
    data.set("id", id);
    data.set("value", value);
    data.set("extra", USER_DATA);
    let key = EntityKey {
        subgraph_id: SubgraphDeploymentId::new(subgraph_id).unwrap(),
        entity_type: "Thing".to_string(),
        entity_id: id.to_string(),
    };
    (
        format!("{{ \"id\": \"{}\", \"value\": \"{}\"}}", id, value),
        EntityModification::Insert { key, data },
    )
}

#[tokio::test(threaded_scheduler)]
async fn ipfs_map() {
    const BAD_IPFS_HASH: &str = "bad-ipfs-hash";

    let ipfs = Arc::new(ipfs_api::IpfsClient::default());
    let subgraph_id = "ipfsMap";

    async fn run_ipfs_map(
        ipfs: Arc<ipfs_api::IpfsClient>,
        subgraph_id: &'static str,
        json_string: String,
    ) -> Result<Vec<EntityModification>, anyhow::Error> {
        let hash = if json_string == BAD_IPFS_HASH {
            "Qm".to_string()
        } else {
            ipfs.add(Cursor::new(json_string)).await.unwrap().hash
        };

        // Ipfs host functions use `block_on` which must be called from a sync context,
        // so we replicate what we do `spawn_module`.
        let runtime = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            runtime.enter(|| {
                let (mut module, store) = test_valid_module_and_store(
                    subgraph_id,
                    mock_data_source("wasm_test/ipfs_map.wasm"),
                );
                let value = module.asc_new(&hash);
                let user_data = module.asc_new(USER_DATA);

                // Invoke the callback
                let func = module.get_func("ipfsMap").get2().unwrap();
                let _: () = func(value.wasm_ptr(), user_data.wasm_ptr())?;
                let mut mods = module
                    .take_ctx()
                    .ctx
                    .state
                    .entity_cache
                    .as_modifications(store.as_ref())?
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
        })
        .join()
        .unwrap()
    };

    // Try it with two valid objects
    let (str1, thing1) = make_thing(subgraph_id, "one", "eins");
    let (str2, thing2) = make_thing(subgraph_id, "two", "zwei");
    let ops = run_ipfs_map(ipfs.clone(), subgraph_id, format!("{}\n{}", str1, str2))
        .await
        .expect("call failed");
    let expected = vec![thing1, thing2];
    assert_eq!(expected, ops);

    // Valid JSON, but not what the callback expected; it will
    // fail on an assertion
    let err = run_ipfs_map(ipfs.clone(), subgraph_id, format!("{}\n[1,2]", str1))
        .await
        .unwrap_err();
    assert!(
        format!("{:#}", err).contains("JSON value is not an object."),
        format!("{:#}", err)
    );

    // Malformed JSON
    let errmsg = run_ipfs_map(ipfs.clone(), subgraph_id, format!("{}\n[", str1))
        .await
        .unwrap_err()
        .to_string();
    assert!(errmsg.contains("EOF while parsing a list"));

    // Empty input
    let ops = run_ipfs_map(ipfs.clone(), subgraph_id, "".to_string())
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
        )
        .await
        .unwrap_err()
    );
    assert!(errmsg.contains("JSON value is not a string."));

    // Bad IPFS hash.
    let errmsg = run_ipfs_map(ipfs.clone(), subgraph_id, BAD_IPFS_HASH.to_string())
        .await
        .unwrap_err()
        .to_string();
    assert!(errmsg.contains("ApiError"));
}

#[tokio::test(threaded_scheduler)]
async fn ipfs_fail() {
    let runtime = tokio::runtime::Handle::current();

    // Ipfs host functions use `block_on` which must be called from a sync context,
    // so we replicate what we do `spawn_module`.
    std::thread::spawn(move || {
        runtime.enter(|| {
            let mut module = test_module("ipfsFail", mock_data_source("wasm_test/ipfs_cat.wasm"));

            let hash = module.asc_new("invalid hash");
            assert!(module
                .invoke_export::<_, AscString>("ipfsCat", hash,)
                .is_null());
        })
    })
    .join()
    .unwrap()
}

#[tokio::test]
async fn crypto_keccak256() {
    let mut module = test_module("cryptoKeccak256", mock_data_source("wasm_test/crypto.wasm"));
    let input: &[u8] = "eth".as_ref();
    let input: AscPtr<Uint8Array> = module.asc_new(input);

    let hash: AscPtr<Uint8Array> = module.invoke_export("hash", input);
    let hash: Vec<u8> = module.asc_get(hash);
    assert_eq!(
        hex::encode(hash),
        "4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0"
    );
}

#[tokio::test]
async fn big_int_to_hex() {
    let mut module = test_module(
        "BigIntToHex",
        mock_data_source("wasm_test/big_int_to_hex.wasm"),
    );

    // Convert zero to hex
    let zero = BigInt::from_unsigned_u256(&U256::zero());
    let zero: AscPtr<AscBigInt> = module.asc_new(&zero);
    let zero_hex_ptr: AscPtr<AscString> = module.invoke_export("big_int_to_hex", zero);
    let zero_hex_str: String = module.asc_get(zero_hex_ptr);
    assert_eq!(zero_hex_str, "0x0");

    // Convert 1 to hex
    let one = BigInt::from_unsigned_u256(&U256::one());
    let one: AscPtr<AscBigInt> = module.asc_new(&one);
    let one_hex_ptr: AscPtr<AscString> = module.invoke_export("big_int_to_hex", one);
    let one_hex_str: String = module.asc_get(one_hex_ptr);
    assert_eq!(one_hex_str, "0x1");

    // Convert U256::max_value() to hex
    let u256_max = BigInt::from_unsigned_u256(&U256::max_value());
    let u256_max: AscPtr<AscBigInt> = module.asc_new(&u256_max);
    let u256_max_hex_ptr: AscPtr<AscString> = module.invoke_export("big_int_to_hex", u256_max);
    let u256_max_hex_str: String = module.asc_get(u256_max_hex_ptr);
    assert_eq!(
        u256_max_hex_str,
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    );
}

#[tokio::test]
async fn big_int_arithmetic() {
    let mut module = test_module(
        "BigIntArithmetic",
        mock_data_source("wasm_test/big_int_arithmetic.wasm"),
    );

    // 0 + 1 = 1
    let zero = BigInt::from(0);
    let zero: AscPtr<AscBigInt> = module.asc_new(&zero);
    let one = BigInt::from(1);
    let one: AscPtr<AscBigInt> = module.asc_new(&one);
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("plus", zero, one);
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(1));

    // 127 + 1 = 128
    let zero = BigInt::from(127);
    let zero: AscPtr<AscBigInt> = module.asc_new(&zero);
    let one = BigInt::from(1);
    let one: AscPtr<AscBigInt> = module.asc_new(&one);
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("plus", zero, one);
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(128));

    // 5 - 10 = -5
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let ten = BigInt::from(10);
    let ten: AscPtr<AscBigInt> = module.asc_new(&ten);
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("minus", five, ten);
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(-5));

    // -20 * 5 = -100
    let minus_twenty = BigInt::from(-20);
    let minus_twenty: AscPtr<AscBigInt> = module.asc_new(&minus_twenty);
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("times", minus_twenty, five);
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(-100));

    // 5 / 2 = 2
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = module.asc_new(&two);
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("dividedBy", five, two);
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(2));

    // 5 % 2 = 1
    let five = BigInt::from(5);
    let five: AscPtr<AscBigInt> = module.asc_new(&five);
    let two = BigInt::from(2);
    let two: AscPtr<AscBigInt> = module.asc_new(&two);
    let result_ptr: AscPtr<AscBigInt> = module.invoke_export2("mod", five, two);
    let result: BigInt = module.asc_get(result_ptr);
    assert_eq!(result, BigInt::from(1));
}

#[tokio::test]
async fn abort() {
    let module = test_module("abort", mock_data_source("wasm_test/abort.wasm"));
    let func = module.get_func("abort").get0().unwrap();
    let res: Result<(), _> = func();
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("line 6, column 2, with message: not true"));
}

#[tokio::test]
async fn bytes_to_base58() {
    let mut module = test_module(
        "bytesToBase58",
        mock_data_source("wasm_test/bytes_to_base58.wasm"),
    );
    let bytes = hex::decode("12207D5A99F603F231D53A4F39D1521F98D2E8BB279CF29BEBFD0687DC98458E7F89")
        .unwrap();
    let bytes_ptr = module.asc_new(bytes.as_slice());
    let result_ptr: AscPtr<AscString> = module.invoke_export("bytes_to_base58", bytes_ptr);
    let base58: String = module.asc_get(result_ptr);
    assert_eq!(base58, "QmWmyoMoctfbAaiEs2G46gpeUmhqFRDW6KWo64y5r581Vz");
}

#[tokio::test]
async fn data_source_create() {
    let run_data_source_create =
        move |name: String,
              params: Vec<String>|
              -> Result<im::Vector<DataSourceTemplateInfo>, wasmtime::Trap> {
            let mut module = test_module(
                "DataSourceCreate",
                mock_data_source("wasm_test/data_source_create.wasm"),
            );

            let name = module.asc_new(&name);
            let params = module.asc_new(&*params);
            module.invoke_export2_void("dataSourceCreate", name, params)?;
            Ok(module.take_ctx().ctx.state.created_data_sources)
        };

    // Test with a valid template
    let data_source = String::from("example data source");
    let template = String::from("example template");
    let params = vec![String::from("0xc0a47dFe034B400B47bDaD5FecDa2621de6c4d95")];
    let result = run_data_source_create(template.clone(), params.clone())
        .expect("unexpected error returned from dataSourceCreate");
    assert_eq!(result[0].data_source, data_source);
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
async fn ens_name_by_hash() {
    let mut module = test_module(
        "EnsNameByHash",
        mock_data_source("wasm_test/ens_name_by_hash.wasm"),
    );

    let hash = "0x7f0c1b04d1a4926f9c635a030eeb611d4c26e5e73291b32a1c7a4ac56935b5b3";
    let name = "dealdrafts";
    test_store::insert_ens_name(hash, name);
    let val = module.asc_new(hash);
    let converted: AscPtr<AscString> = module.invoke_export("nameByHash", val);
    let data: String = module.asc_get(converted);
    assert_eq!(data, name);

    let hash = module.asc_new("impossible keccak hash");
    assert!(module
        .invoke_export::<_, AscString>("nameByHash", hash)
        .is_null());
}

#[tokio::test]
async fn entity_store() {
    let (mut module, store) =
        test_valid_module_and_store("entityStore", mock_data_source("wasm_test/store.wasm"));

    let mut alex = Entity::new();
    alex.set("id", "alex");
    alex.set("name", "Alex");
    let mut steve = Entity::new();
    steve.set("id", "steve");
    steve.set("name", "Steve");
    let subgraph_id = SubgraphDeploymentId::new("entityStore").unwrap();
    test_store::insert_entities(subgraph_id, vec![("User", alex), ("User", steve)]).unwrap();

    let get_user = move |module: &mut WasmInstance, id: &str| -> Option<Entity> {
        let id = module.asc_new(id);
        let entity_ptr: AscPtr<AscEntity> = module.invoke_export("getUser", id);
        if entity_ptr.is_null() {
            None
        } else {
            Some(Entity::from(
                module
                    .try_asc_get::<HashMap<String, Value>, _>(entity_ptr)
                    .unwrap(),
            ))
        }
    };

    let load_and_set_user_name = |module: &mut WasmInstance, id: &str, name: &str| {
        let id_ptr = module.asc_new(id);
        let name_ptr = module.asc_new(name);
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
    let cache = std::mem::replace(
        &mut module.instance_ctx_mut().ctx.state.entity_cache,
        EntityCache::new(store.clone()),
    );
    let mut mods = cache
        .as_modifications(store.as_ref())
        .unwrap()
        .modifications;
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
        .as_modifications(store.as_ref())
        .unwrap()
        .modifications;
    assert_eq!(1, mods.len());
    match mods.pop().unwrap() {
        EntityModification::Insert { data, .. } => {
            assert_eq!(Some(&Value::from("herobrine")), data.get("id"));
            assert_eq!(Some(&Value::from("Brine-O")), data.get("name"));
        }
        _ => assert!(false, "expected Insert modification"),
    }
}

#[tokio::test]
async fn detect_contract_calls() {
    let data_source_without_calls = mock_data_source("wasm_test/abi_store_value.wasm");
    assert_eq!(
        data_source_without_calls
            .mapping
            .calls_host_fn("ethereum.call"),
        false
    );

    let data_source_with_calls = mock_data_source("wasm_test/contract_calls.wasm");
    assert_eq!(
        data_source_with_calls
            .mapping
            .calls_host_fn("ethereum.call"),
        true
    );
}
