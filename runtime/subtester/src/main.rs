use ethabi::Contract;
use graph::components::store::CallCache;
use graph::data::subgraph::*;
use graph::{
    blockchain::BlockPtr,
    components::store::DeploymentLocator,
    data::subgraph::{Mapping, Source, TemplateSource},
    ipfs_client::IpfsClient,
    prelude::{
        o, slog, BlockState, DataSourceTemplate, DeploymentHash, EthereumCallCache, HostMetrics,
        Link, Logger, StopwatchMetrics, SubgraphStore,
    },
    semver::Version,
};
use graph_chain_arweave::adapter::ArweaveAdapter;
use graph_chain_ethereum::DataSource;
use graph_chain_ethereum::MockEthereumAdapter;
use graph_core;
use graph_core::three_box::ThreeBoxAdapter;
use graph_mock::MockMetricsRegistry;
use graph_runtime_wasm::mapping::ValidModule;
use graph_runtime_wasm::module::WasmInstance;
use graph_runtime_wasm::module::MOCK_STORE;
use graph_runtime_wasm::module::SNAPSHOT;
use graph_runtime_wasm::{
    host_exports::HostExports, mapping::MappingContext, module::ExperimentalFeatures,
};
use slog::*;
use slog_term;
use std::str::FromStr;
use std::sync::Arc;
use test_store::{STORE};
use web3::types::Address;

fn mock_host_exports(
    subgraph_id: DeploymentHash,
    data_source: DataSource,
    store: Arc<impl SubgraphStore>,
    call_cache: Arc<impl EthereumCallCache>,
) -> HostExports {
    let mock_ethereum_adapter = Arc::new(MockEthereumAdapter::default());
    let arweave_adapter = Arc::new(ArweaveAdapter::new("https://arweave.net".to_string()));
    let three_box_adapter = Arc::new(ThreeBoxAdapter::new("https://ipfs.3box.io/".to_string()));

    let templates = vec![DataSourceTemplate {
        kind: String::from("ethereum/contract"),
        name: String::from("example template"),
        network: Some(String::from("mainnet")),
        source: TemplateSource {
            abi: String::from("foo"),
        },
        mapping: Mapping {
            kind: String::from("ethereum/events"),
            api_version: Version::parse("0.1.0").unwrap(),
            language: String::from("wasm/assemblyscript"),
            entities: vec![],
            abis: vec![],
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
            link: Link {
                link: "link".to_owned(),
            },
            runtime: Arc::new(vec![]),
        },
    }];

    let network = data_source.network.clone().unwrap();
    HostExports::new(
        subgraph_id,
        &data_source,
        network,
        Arc::new(templates),
        mock_ethereum_adapter,
        Arc::new(graph_core::LinkResolver::from(IpfsClient::localhost())),
        store,
        call_cache,
        arweave_adapter,
        three_box_adapter,
    )
}

fn mock_context(
    deployment: DeploymentLocator,
    data_source: DataSource,
    store: Arc<impl SubgraphStore>,
    call_cache: Arc<impl EthereumCallCache>,
) -> MappingContext {
    MappingContext {
        logger: test_store::LOGGER.clone(),
        block_ptr: BlockPtr {
            hash: Default::default(),
            number: 0,
        },
        host_exports: Arc::new(mock_host_exports(
            deployment.hash.clone(),
            data_source,
            store.clone(),
            call_cache,
        )),
        state: BlockState::new(store.writable(&deployment).unwrap(), Default::default()),
        proof_of_indexing: None,
    }
}

fn mock_abi() -> MappingABI {
    MappingABI {
        name: "mock_abi".to_string(),
        contract: Contract::load(
            r#"[
            {
                "inputs": [
                    {
                        "name": "a",
                        "type": "address"
                    }
                ],
                "type": "constructor"
            }
        ]"#
            .as_bytes(),
        )
        .unwrap(),
    }
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
            api_version: Version::parse("0.1.0").unwrap(),
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
        context: Default::default(),
        creation_block: None,
        contract_abi: Arc::new(mock_abi()),
    }
}

pub fn main() -> () {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let logger = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

    let args: Vec<String> = std::env::args().collect();

    if args.len() == 1 {
        panic!("Must provide path to wasm file.")
    }

    let path_to_wasm = &args[1];

    let subgraph_id = "ipfsMap";
    let deployment_id = DeploymentHash::new(subgraph_id).unwrap();

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

    let data_source = mock_data_source(path_to_wasm);
    
    let store = STORE.clone();

    pub const NETWORK_NAME: &str = "fake_network";
    let call_cache = store
        .block_store()
        .ethereum_call_cache(NETWORK_NAME)
        .expect("call cache for test network");

    let metrics_registry = Arc::new(MockMetricsRegistry::new());

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
        allow_non_deterministic_arweave: true,
        allow_non_deterministic_3box: true,
    };

    let valid_module = Arc::new(ValidModule::new(data_source.mapping.runtime.as_ref()).unwrap());

    let module = WasmInstance::from_valid_module_with_ctx(
        valid_module,
        mock_context(
            deployment.clone(),
            data_source,
            store.subgraph_store(),
            call_cache,
        ),
        host_metrics,
        None,
        experimental_features,
    )
    .unwrap();

    let fire_events = module.get_func("fireEvents");
    fire_events
        .call(&[])
        .expect("Couldn't call wasm function 'fireEvents'.");

    let get_store_snapshot = module.get_func("assertStoreEq");
    get_store_snapshot
        .call(&[])
        .expect("Couldn't call wasm function 'assertStoreEq'.");

    let snapshot = unsafe { SNAPSHOT.get() };
    info!(logger, "Store snapshot {:?}", snapshot);

    let mock_store = unsafe { MOCK_STORE.get() };
    info!(logger, "Mock store {:?}", mock_store.replace("\\", ""));

    info!(
        logger,
        "Mock state equal to given snapshot: {:?}",
        mock_store.replace("\\", "").eq(snapshot)
    );
}
