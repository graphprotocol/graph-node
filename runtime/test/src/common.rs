use ethabi::Contract;
use graph::blockchain::BlockTime;
use graph::components::store::DeploymentLocator;
use graph::components::subgraph::SharedProofOfIndexing;
use graph::data::subgraph::*;
use graph::data_source;
use graph::data_source::common::MappingABI;
use graph::env::EnvVars;
use graph::ipfs::IpfsRpcClient;
use graph::ipfs::ServerAddress;
use graph::log;
use graph::prelude::*;
use graph_chain_ethereum::{Chain, DataSource, DataSourceTemplate, Mapping, TemplateSource};
use graph_runtime_wasm::host_exports::DataSourceDetails;
use graph_runtime_wasm::{HostExports, MappingContext};
use semver::Version;
use std::env;
use std::str::FromStr;
use web3::types::Address;

lazy_static! {
    pub static ref LOGGER: Logger = match env::var_os("GRAPH_LOG") {
        Some(_) => log::logger(false),
        None => Logger::root(slog::Discard, o!()),
    };
}

fn mock_host_exports(
    subgraph_id: DeploymentHash,
    data_source: DataSource,
    store: Arc<impl SubgraphStore>,
    api_version: Version,
) -> HostExports {
    let templates = vec![data_source::DataSourceTemplate::Onchain::<Chain>(
        DataSourceTemplate {
            kind: String::from("ethereum/contract"),
            name: String::from("example template"),
            manifest_idx: 0,
            network: Some(String::from("mainnet")),
            source: TemplateSource {
                abi: String::from("foo"),
            },
            mapping: Mapping {
                kind: String::from("ethereum/events"),
                api_version,
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
        },
    )];

    let network = data_source.network.clone().unwrap();
    let ens_lookup = store.ens_lookup();

    let ds_details = DataSourceDetails::from_data_source(
        &graph::data_source::DataSource::Onchain::<Chain>(data_source),
        Arc::new(templates.iter().map(|t| t.into()).collect()),
    );

    let client = IpfsRpcClient::new_unchecked(ServerAddress::local_rpc_api(), &LOGGER).unwrap();

    HostExports::new(
        subgraph_id,
        network,
        ds_details,
        Arc::new(IpfsResolver::new(
            Arc::new(client),
            Arc::new(EnvVars::default()),
        )),
        ens_lookup,
    )
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

pub fn mock_context(
    deployment: DeploymentLocator,
    data_source: DataSource,
    store: Arc<impl SubgraphStore>,
    api_version: Version,
) -> MappingContext {
    MappingContext {
        logger: Logger::root(slog::Discard, o!()),
        block_ptr: BlockPtr {
            hash: Default::default(),
            number: 0,
        },
        timestamp: BlockTime::NONE,
        host_exports: Arc::new(mock_host_exports(
            deployment.hash.clone(),
            data_source,
            store.clone(),
            api_version,
        )),
        state: BlockState::new(
            graph::futures03::executor::block_on(store.writable(
                LOGGER.clone(),
                deployment.id,
                Arc::new(Vec::new()),
            ))
            .unwrap(),
            Default::default(),
        ),
        proof_of_indexing: SharedProofOfIndexing::ignored(),
        host_fns: Arc::new(Vec::new()),
        debug_fork: None,
        mapping_logger: Logger::root(slog::Discard, o!()),
        instrument: false,
    }
}

pub fn mock_data_source(path: &str, api_version: Version) -> DataSource {
    let runtime = std::fs::read(path).unwrap();

    DataSource {
        kind: String::from("ethereum/contract"),
        name: String::from("example data source"),
        manifest_idx: 0,
        network: Some(String::from("mainnet")),
        address: Some(Address::from_str("0123123123012312312301231231230123123123").unwrap()),
        start_block: 0,
        end_block: None,
        mapping: Mapping {
            kind: String::from("ethereum/events"),
            api_version,
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
        context: Default::default(),
        creation_block: None,
        contract_abi: Arc::new(mock_abi()),
    }
}
