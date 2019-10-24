extern crate graph;
extern crate graph_core;
extern crate graph_mock;
extern crate ipfs_api;
extern crate semver;
extern crate walkdir;

use ipfs_api::IpfsClient;
use walkdir::WalkDir;

use std::collections::HashMap;
use std::fs::read_to_string;
use std::io::Cursor;
use std::time::Duration;
use std::time::Instant;

use graph::prelude::*;

use graph_core::LinkResolver;
use graph_mock::{MockEthereumAdapter, MockStore};

use crate::tokio::timer::Delay;

/// Adds subgraph located in `test/subgraphs/`, replacing "link to" placeholders
/// in the subgraph manifest with links to files just added into a local IPFS
/// daemon on port 5001.
fn add_subgraph_to_ipfs(
    client: Arc<IpfsClient>,
    subgraph: &str,
) -> impl Future<Item = String, Error = Error> {
    /// Adds string to IPFS and returns link of the form `/ipfs/`.
    fn add(client: &IpfsClient, data: String) -> impl Future<Item = String, Error = Error> {
        client
            .add(Cursor::new(data))
            .map(|res| format!("/ipfs/{}", res.hash))
            .map_err(|err| format_err!("error adding to IPFS {}", err))
    }

    let dir = format!("tests/subgraphs/{}", subgraph);
    let subgraph_string = std::fs::read_to_string(format!("{}/{}.yaml", dir, subgraph)).unwrap();
    let mut ipfs_upload = Box::new(future::ok(subgraph_string.clone()))
        as Box<dyn Future<Item = String, Error = Error> + Send>;
    // Search for files linked by the subgraph, upload and update the sugraph
    // with their link.
    for file in WalkDir::new(&dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| {
            subgraph_string.contains(&format!("link to {}", entry.file_name().to_str().unwrap()))
        })
    {
        let client = client.clone();
        ipfs_upload = Box::new(ipfs_upload.and_then(move |subgraph_string| {
            add(&client, read_to_string(file.path()).unwrap()).map(move |link| {
                subgraph_string.replace(
                    &format!("link to {}", file.file_name().to_str().unwrap()),
                    &format!("/ipfs/{}", link),
                )
            })
        }))
    }
    let add_client = client.clone();
    ipfs_upload.and_then(move |subgraph_string| add(&add_client, subgraph_string))
}

#[ignore]
#[test]
#[cfg(any())]
fn multiple_data_sources_per_subgraph() {
    #[derive(Debug)]
    struct MockRuntimeHost {}

    impl RuntimeHost for MockRuntimeHost {
        fn matches_log(&self, _: &Log) -> bool {
            true
        }

        fn matches_call(&self, _call: &EthereumCall) -> bool {
            true
        }

        fn matches_block(&self, _call: EthereumBlockTriggerType, _block_number: u64) -> bool {
            true
        }

        fn process_log(
            &self,
            _: Logger,
            _: Arc<LightEthereumBlock>,
            _: Arc<Transaction>,
            _: Arc<Log>,
            _: BlockState,
        ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
            unimplemented!();
        }

        fn process_call(
            &self,
            _logger: Logger,
            _block: Arc<LightEthereumBlock>,
            _transaction: Arc<Transaction>,
            _call: Arc<EthereumCall>,
            _state: BlockState,
        ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
            unimplemented!();
        }

        fn process_block(
            &self,
            _logger: Logger,
            _block: Arc<LightEthereumBlock>,
            _trigger_type: EthereumBlockTriggerType,
            _state: BlockState,
        ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send> {
            unimplemented!();
        }
    }

    #[derive(Debug, Default)]
    struct MockRuntimeHostBuilder {
        data_sources_received: Arc<Mutex<Vec<DataSource>>>,
    }

    impl MockRuntimeHostBuilder {
        fn new() -> Self {
            Self::default()
        }
    }

    impl Clone for MockRuntimeHostBuilder {
        fn clone(&self) -> Self {
            Self {
                data_sources_received: self.data_sources_received.clone(),
            }
        }
    }

    impl RuntimeHostBuilder for MockRuntimeHostBuilder {
        type Host = MockRuntimeHost;

        fn build(
            &self,
            _: &Logger,
            _: String,
            _: SubgraphDeploymentId,
            data_source: DataSource,
            _: Vec<DataSourceTemplate>,
            _: Arc<HostMetrics>,
        ) -> Result<Self::Host, Error> {
            self.data_sources_received.lock().unwrap().push(data_source);

            Ok(MockRuntimeHost {})
        }
    }

    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let subgraph_link = runtime
        .block_on(future::lazy(move || {
            add_subgraph_to_ipfs(Arc::new(IpfsClient::default()), "two-datasources")
        }))
        .unwrap();

    runtime
        .block_on(future::lazy(|| {
            let resolver = Arc::new(LinkResolver::from(IpfsClient::default()));
            let logger = Logger::root(slog::Discard, o!());
            let logger_factory = LoggerFactory::new(logger.clone(), None);
            let mut stores = HashMap::new();
            stores.insert("mainnet".to_string(), Arc::new(FakeStore));
            let host_builder = MockRuntimeHostBuilder::new();
            let block_stream_builder = MockBlockStreamBuilder::new();
            let metrics_registry = Arc::new(MockMetricsRegistry::new());

            let manager = SubgraphInstanceManager::new(
                &logger_factory,
                stores,
                host_builder.clone(),
                block_stream_builder.clone(),
                metrics_registry,
            );

            // Load a subgraph with two data sources
            SubgraphManifest::resolve(
                Link {
                    link: subgraph_link,
                },
                resolver,
                logger,
            )
            .map_err(|e| panic!("subgraph resolve error {:?}", e))
            .and_then(move |subgraph| {
                // Send the new subgraph to the manager.
                manager
                    .event_sink()
                    .send(SubgraphAssignmentProviderEvent::SubgraphStart(subgraph))
            })
            .and_then(move |_| {
                // If we created a RuntimeHost for each data source,
                // then we're handling multiple data sets.
                // Wait for thirty seconds for that to happen, otherwise fail the test.
                let start_time = Instant::now();
                let max_wait = Duration::from_secs(30);
                loop {
                    let data_sources_received = host_builder.data_sources_received.lock().unwrap();
                    let data_source_names = data_sources_received
                        .iter()
                        .map(|data_source| data_source.name.as_str())
                        .collect::<HashSet<&str>>();
                    let expected_data_source_names =
                        HashSet::from_iter(vec!["ExampleDataSource", "ExampleDataSource2"]);

                    if data_source_names == expected_data_source_names {
                        break;
                    }
                    if Instant::now().duration_since(start_time) > max_wait {
                        panic!(
                            "Test failed, runtime hosts created for data sources: {:?}",
                            data_source_names
                        )
                    }
                    ::std::thread::yield_now();
                }
                Ok(())
            })
        }))
        .unwrap();
}

fn added_subgraph_id_eq(
    event: &SubgraphAssignmentProviderEvent,
    id: &SubgraphDeploymentId,
) -> bool {
    match event {
        SubgraphAssignmentProviderEvent::SubgraphStart(manifest) => &manifest.id == id,
        _ => false,
    }
}

#[test]
fn subgraph_provider_events() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(future::lazy(|| {
            let logger = Logger::root(slog::Discard, o!());
            let logger_factory = LoggerFactory::new(logger.clone(), None);
            let ipfs = Arc::new(IpfsClient::default());
            let resolver = Arc::new(LinkResolver::from(IpfsClient::default()));
            let store = Arc::new(MockStore::new(vec![]));
            let stores: HashMap<String, Arc<MockStore>> = vec![store.clone()]
                .into_iter()
                .map(|s| ("mainnet".to_string(), s))
                .collect();
            let mock_ethereum_adapter =
                Arc::new(MockEthereumAdapter::default()) as Arc<dyn EthereumAdapter>;
            let ethereum_adapters: HashMap<String, Arc<dyn EthereumAdapter>> =
                vec![mock_ethereum_adapter]
                    .into_iter()
                    .map(|e| ("mainnet".to_string(), e))
                    .collect();
            let graphql_runner = Arc::new(graph_core::GraphQlRunner::new(&logger, store.clone()));
            let mut provider = graph_core::SubgraphAssignmentProvider::new(
                &logger_factory,
                resolver.clone(),
                store.clone(),
                graphql_runner.clone(),
            );
            let provider_events = provider.take_event_stream().unwrap();
            let node_id = NodeId::new("test").unwrap();

            let registrar = graph_core::SubgraphRegistrar::new(
                &logger_factory,
                resolver.clone(),
                Arc::new(provider),
                store.clone(),
                stores,
                ethereum_adapters,
                node_id.clone(),
                SubgraphVersionSwitchingMode::Instant,
            );
            registrar
                .start()
                .and_then(move |_| {
                    add_subgraph_to_ipfs(ipfs.clone(), "two-datasources")
                        .join(add_subgraph_to_ipfs(ipfs, "dummy"))
                })
                .and_then(move |(subgraph1_link, subgraph2_link)| {
                    let registrar = Arc::new(registrar);
                    let subgraph1_id =
                        SubgraphDeploymentId::new(subgraph1_link.trim_start_matches("/ipfs/"))
                            .unwrap();
                    let subgraph2_id =
                        SubgraphDeploymentId::new(subgraph2_link.trim_start_matches("/ipfs/"))
                            .unwrap();
                    let subgraph_name = SubgraphName::new("subgraph").unwrap();

                    // Prepare the clones
                    let registrar_clone1 = registrar;
                    let registrar_clone2 = registrar_clone1.clone();
                    let registrar_clone3 = registrar_clone1.clone();
                    let registrar_clone4 = registrar_clone1.clone();
                    let registrar_clone5 = registrar_clone1.clone();
                    let registrar_clone6 = registrar_clone1.clone();
                    let subgraph1_id_clone1 = subgraph1_id;
                    let subgraph1_id_clone2 = subgraph1_id_clone1.clone();
                    let subgraph2_id_clone1 = subgraph2_id;
                    let subgraph2_id_clone2 = subgraph2_id_clone1.clone();
                    let subgraph_name_clone1 = subgraph_name;
                    let subgraph_name_clone2 = subgraph_name_clone1.clone();
                    let subgraph_name_clone3 = subgraph_name_clone1.clone();
                    let subgraph_name_clone4 = subgraph_name_clone1.clone();
                    let subgraph_name_clone5 = subgraph_name_clone1.clone();
                    let node_id_clone1 = node_id;
                    let node_id_clone2 = node_id_clone1.clone();

                    // Deploying to non-existant subgraph is an error.
                    registrar_clone1
                        .create_subgraph_version(
                            subgraph_name_clone1.clone(),
                            subgraph1_id_clone1.clone(),
                            node_id_clone1.clone(),
                        )
                        .then(move |result| {
                            assert!(result.is_err());

                            // Create subgraph
                            registrar_clone1.create_subgraph(subgraph_name_clone1.clone())
                        })
                        .and_then(move |_| {
                            // Deploy
                            registrar_clone2.create_subgraph_version(
                                subgraph_name_clone2.clone(),
                                subgraph1_id_clone1.clone(),
                                node_id_clone1.clone(),
                            )
                        })
                        .and_then(move |()| {
                            // Give some time for event to be picked up.
                            Delay::new(Instant::now() + Duration::from_secs(2))
                                .map_err(|_| panic!("time error"))
                        })
                        .and_then(move |()| {
                            // Update
                            registrar_clone3.create_subgraph_version(
                                subgraph_name_clone3,
                                subgraph2_id_clone1,
                                node_id_clone2,
                            )
                        })
                        .and_then(move |()| {
                            // Give some time for event to be picked up.
                            Delay::new(Instant::now() + Duration::from_secs(2))
                                .map_err(|_| panic!("time error"))
                        })
                        .and_then(move |()| {
                            // Remove
                            registrar_clone4.remove_subgraph(subgraph_name_clone4)
                        })
                        .and_then(move |()| {
                            // Give some time for event to be picked up.
                            Delay::new(Instant::now() + Duration::from_secs(2))
                                .map_err(|_| panic!("time error"))
                        })
                        .and_then(move |()| {
                            // Removing a subgraph that is not deployed is an error.
                            registrar_clone5.remove_subgraph(subgraph_name_clone5)
                        })
                        .then(move |result| {
                            assert!(result.is_err());

                            provider_events
                                .take(4)
                                .collect()
                                .then(|result| Ok(result.unwrap()))
                        })
                        .and_then(move |provider_events| -> Result<(), Error> {
                            // Keep named provider alive until after events have been collected
                            let _ = registrar_clone6;

                            // Assert that the expected events were sent.
                            assert_eq!(provider_events.len(), 4);
                            assert!(provider_events
                                .iter()
                                .any(|event| added_subgraph_id_eq(event, &subgraph1_id_clone2)));
                            assert!(provider_events
                                .iter()
                                .any(|event| added_subgraph_id_eq(event, &subgraph2_id_clone2)));
                            assert!(provider_events.iter().any(|event| event
                                == &SubgraphAssignmentProviderEvent::SubgraphStop(
                                    subgraph1_id_clone2.clone()
                                )));
                            assert!(provider_events.iter().any(|event| event
                                == &SubgraphAssignmentProviderEvent::SubgraphStop(
                                    subgraph2_id_clone2.clone()
                                )));
                            Ok(())
                        })
                })
                .then(|result| -> Result<(), ()> { Ok(result.unwrap()) })
        }))
        .unwrap();
}
