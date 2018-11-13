extern crate graph;
extern crate graph_core;
extern crate graph_mock;
extern crate graph_runtime_wasm;
extern crate ipfs_api;
extern crate walkdir;

use ipfs_api::IpfsClient;
use walkdir::WalkDir;

use std::collections::HashSet;
use std::fs::read_to_string;
use std::io::Cursor;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use graph::components::ethereum::*;
use graph::prelude::*;
use graph::web3::types::*;
use graph_core::SubgraphInstanceManager;
use graph_mock::{FakeStore, MockBlockStreamBuilder, MockStore};

/// Adds subgraph located in `test/subgraphs/`, replacing "link to" placeholders
/// in the subgraph manifest with links to files just added into a local IPFS
/// daemon on port 5001.
fn add_subgraph_to_ipfs(
    client: Arc<IpfsClient>,
    subgraph: &str,
) -> impl Future<Item = String, Error = ()> {
    /// Adds string to IPFS and returns link of the form `/ipfs/`.
    fn add(client: &IpfsClient, data: String) -> impl Future<Item = String, Error = ()> {
        client
            .add(Cursor::new(data))
            .map(|res| format!("/ipfs/{}", res.hash))
            .map_err(|err| eprintln!("error adding to IPFS {}", err))
    }

    let dir = format!("tests/subgraphs/{}", subgraph);
    let subgraph_string = std::fs::read_to_string(format!("{}/{}.yaml", dir, subgraph)).unwrap();
    let mut ipfs_upload = Box::new(future::ok(subgraph_string.clone()))
        as Box<Future<Item = String, Error = ()> + Send>;
    // Search for files linked by the subgraph, upload and update the sugraph
    // with their link.
    for file in WalkDir::new(&dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| {
            subgraph_string.contains(&format!("link to {}", entry.file_name().to_str().unwrap()))
        }) {
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

#[test]
fn multiple_data_sources_per_subgraph() {
    #[derive(Debug)]
    struct MockRuntimeHost {}

    impl RuntimeHost for MockRuntimeHost {
        fn matches_log(&self, _: &Log) -> bool {
            true
        }

        fn process_log(
            &self,
            _: &Logger,
            _: Arc<EthereumBlock>,
            _: Arc<Transaction>,
            _: Arc<Log>,
            _: Vec<EntityOperation>,
        ) -> Box<Future<Item = Vec<EntityOperation>, Error = Error> + Send> {
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
            _: SubgraphId,
            data_source: DataSource,
        ) -> Result<Self::Host, Error> {
            self.data_sources_received.lock().unwrap().push(data_source);

            Ok(MockRuntimeHost {})
        }
    }

    let mut runtime = tokio::runtime::Runtime::new().unwrap();

    let subgraph_link = runtime
        .block_on(future::lazy(move || {
            add_subgraph_to_ipfs(Arc::new(IpfsClient::default()), "two-datasources")
        })).unwrap();

    runtime
        .block_on(future::lazy(|| {
            let resolver = Arc::new(IpfsClient::default());
            let logger = Logger::root(slog::Discard, o!());
            let store = Arc::new(FakeStore);
            let host_builder = MockRuntimeHostBuilder::new();
            let block_stream_builder = MockBlockStreamBuilder::new();
            let manager = SubgraphInstanceManager::new(
                &logger,
                store,
                host_builder.clone(),
                block_stream_builder,
                None,
            );

            // Load a subgraph with two data sources
            SubgraphManifest::resolve(
                Link {
                    link: subgraph_link,
                },
                resolver,
            ).map_err(|e| panic!("subgraph resolve error {:?}", e))
            .and_then(move |subgraph| {
                // Send the new subgraph to the manager.
                manager
                    .event_sink()
                    .send(SubgraphProviderEvent::SubgraphStart(subgraph))
            }).and_then(move |_| {
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
                    use std::iter::FromIterator;
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
        })).unwrap();
}

fn added_subgraph_id(event: &SubgraphProviderEvent) -> &str {
    match event {
        SubgraphProviderEvent::SubgraphStart(manifest) => &manifest.id,
        _ => panic!("not `SubgraphStart`"),
    }
}

fn added_schema_id(event: &SchemaEvent) -> &str {
    match event {
        SchemaEvent::SchemaAdded(schema) => &schema.id,
        _ => panic!("not `SchemaAdded`"),
    }
}

#[test]
fn subgraph_provider_events() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let logger = Logger::root(slog::Discard, o!());
    let mut provider = graph_core::SubgraphProvider::new(
        logger.clone(),
        Arc::new(IpfsClient::default()),
        Arc::new(MockStore::new()),
    );
    let provider_events = provider.take_event_stream().unwrap();
    let schema_events = provider.take_event_stream().unwrap();
    let named_provider = runtime
        .block_on(graph_core::SubgraphProviderWithNames::init(
            logger.clone(),
            Arc::new(provider),
            Arc::new(MockStore::new()),
        )).unwrap();

    let (subgraph1_link, subgraph2_link) = runtime
        .block_on(future::lazy(|| {
            let resolver = Arc::new(IpfsClient::default());
            add_subgraph_to_ipfs(resolver.clone(), "two-datasources")
                .join(add_subgraph_to_ipfs(resolver, "dummy"))
        })).unwrap();
    let subgraph1_id = subgraph1_link.trim_left_matches("/ipfs/");
    let subgraph2_id = subgraph2_link.trim_left_matches("/ipfs/");

    // Deploy
    runtime
        .block_on(named_provider.deploy("subgraph".to_owned(), subgraph1_id.to_owned()))
        .unwrap();

    // Update
    runtime
        .block_on(named_provider.deploy("subgraph".to_owned(), subgraph2_id.to_owned()))
        .unwrap();

    // Remove
    runtime
        .block_on(named_provider.remove("subgraph".to_owned()))
        .unwrap();

    // Removing a subgraph that is not deployed is an error.
    assert!(
        runtime
            .block_on(named_provider.remove("subgraph".to_owned()))
            .is_err()
    );

    // Finish the event streams.
    drop(named_provider);

    // Assert that the expected events were sent.
    let provider_events = runtime.block_on(provider_events.collect()).unwrap();
    assert_eq!(provider_events.len(), 4);
    assert_eq!(added_subgraph_id(&provider_events[0]), subgraph1_id);
    match &provider_events[1] {
        SubgraphProviderEvent::SubgraphStop(id) => assert_eq!(id, subgraph1_id),
        _ => panic!("wrong event sent"),
    }
    assert_eq!(added_subgraph_id(&provider_events[2]), subgraph2_id);
    match &provider_events[3] {
        SubgraphProviderEvent::SubgraphStop(id) => assert_eq!(id, subgraph2_id),
        _ => panic!("wrong event sent"),
    }

    let schema_events = runtime.block_on(schema_events.collect()).unwrap();
    assert_eq!(schema_events.len(), 4);
    assert_eq!(added_schema_id(&schema_events[0]), subgraph1_id);
    assert_eq!(
        schema_events[1],
        SchemaEvent::SchemaRemoved(subgraph1_id.to_owned())
    );
    assert_eq!(added_schema_id(&schema_events[2]), subgraph2_id);
    assert_eq!(
        schema_events[3],
        SchemaEvent::SchemaRemoved(subgraph2_id.to_owned())
    );
}

#[test]
fn subgraph_list() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let logger = Logger::root(slog::Discard, o!());
    let provider = graph_core::SubgraphProvider::new(
        logger.clone(),
        Arc::new(IpfsClient::default()),
        Arc::new(MockStore::new()),
    );
    let named_provider = runtime
        .block_on(graph_core::SubgraphProviderWithNames::init(
            logger.clone(),
            Arc::new(provider),
            Arc::new(MockStore::new()),
        )).unwrap();

    let (subgraph1_link, subgraph2_link) = runtime
        .block_on(future::lazy(|| {
            let resolver = Arc::new(IpfsClient::default());
            add_subgraph_to_ipfs(resolver.clone(), "two-datasources")
                .join(add_subgraph_to_ipfs(resolver, "dummy"))
        })).unwrap();
    let subgraph1_id = subgraph1_link.trim_left_matches("/ipfs/").to_owned();
    let subgraph2_id = subgraph2_link.trim_left_matches("/ipfs/").to_owned();

    assert!(named_provider.list().unwrap().is_empty());
    runtime
        .block_on(named_provider.deploy("subgraph1".to_owned(), subgraph1_id.clone()))
        .unwrap();
    runtime
        .block_on(named_provider.deploy("subgraph2".to_owned(), subgraph2_id.clone()))
        .unwrap();
    assert_eq!(
        named_provider
            .list()
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        vec![
            ("subgraph1".to_owned(), Some(subgraph1_id)),
            ("subgraph2".to_owned(), Some(subgraph2_id.clone()))
        ].into_iter()
        .collect::<HashSet<_>>()
    );
    runtime
        .block_on(named_provider.remove("subgraph1".to_owned()))
        .unwrap();
    assert_eq!(
        named_provider
            .list()
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>(),
        vec![("subgraph2".to_owned(), Some(subgraph2_id))]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    runtime
        .block_on(named_provider.remove("subgraph2".to_owned()))
        .unwrap();
    assert!(named_provider.list().unwrap().is_empty());
}
