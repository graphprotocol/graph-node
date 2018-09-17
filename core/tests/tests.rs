extern crate ethabi;
extern crate graph;
extern crate graph_core;
extern crate graph_mock;
extern crate graph_runtime_wasm;
extern crate ipfs_api;

use ipfs_api::IpfsClient;
use std::fs::read_to_string;
use std::io::Cursor;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use graph::components::ethereum::*;
use graph::prelude::*;
use graph_core::RuntimeManager;
use graph_mock::FakeStore;
use graph_runtime_wasm::RuntimeHostBuilder;

#[test]
fn multiple_data_sources_per_subgraph() {
    struct MockEthereumAdapter {
        received_subscriptions: Vec<String>,
    }

    impl EthereumAdapter for MockEthereumAdapter {
        fn contract_call(
            &mut self,
            _request: EthereumContractCall,
        ) -> Box<Future<Item = Vec<ethabi::Token>, Error = EthereumContractCallError>> {
            unimplemented!()
        }

        fn subscribe_to_event(
            &mut self,
            subscription: EthereumEventSubscription,
        ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError>> {
            self.received_subscriptions.push(subscription.event.name);
            Box::new(stream::iter_ok(vec![]))
        }

        fn unsubscribe_from_event(&mut self, _subscription_id: String) -> bool {
            unimplemented!()
        }
    }

    /// Adds string to IPFS and returns link of the form `/ipfs/`.
    fn add(client: &IpfsClient, data: String) -> impl Future<Item = String, Error = ()> {
        client
            .add(Cursor::new(data))
            .map(|res| format!("/ipfs/{}", res.hash))
            .map_err(|err| eprintln!("error adding to IPFS {}", err))
    }

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(future::lazy(|| {
            // Replace "link to" placeholders in the subgraph manifest with hashes
            // of files just added into a local IPFS daemon on port 5001.
            let resolver = Arc::new(IpfsClient::default());
            let subgraph_string =
                std::fs::read_to_string("tests/subgraph-two-datasources/two-datasources.yaml")
                    .unwrap();
            let mut ipfs_upload = Box::new(future::ok(subgraph_string))
                as Box<Future<Item = String, Error = ()> + Send>;
            for file in &[
                "abis/ExampleContract.json",
                "abis/ExampleContract2.json",
                "empty.wasm",
                "schema.graphql",
            ] {
                let resolver = resolver.clone();
                ipfs_upload = Box::new(ipfs_upload.and_then(move |subgraph_string| {
                    add(
                        &resolver,
                        read_to_string(format!("tests/subgraph-two-datasources/{}", file)).unwrap(),
                    ).map(move |link| {
                        subgraph_string
                            .replace(&format!("link to {}", file), &format!("/ipfs/{}", link))
                    })
                }))
            }
            let add_resolver = resolver.clone();
            ipfs_upload
                .and_then(move |subgraph_string| add(&add_resolver, subgraph_string))
                .and_then(|subgraph_link| {
                    let logger = Logger::root(slog::Discard, o!());
                    let eth_adapter = Arc::new(Mutex::new(MockEthereumAdapter {
                        received_subscriptions: vec![],
                    }));
                    let fake_store = Arc::new(Mutex::new(FakeStore));

                    let host_builder = RuntimeHostBuilder::new(
                        &logger,
                        eth_adapter.clone(),
                        resolver.clone(),
                        fake_store.clone(),
                    );

                    let manager = RuntimeManager::new(&logger, fake_store, host_builder);

                    // Load a subgraph with two data sets, one listening for `ExampleEvent`
                    // and the other for `ExampleEvent2`.
                    SubgraphManifest::resolve(
                        "example_subgraph".to_owned(),
                        Link {
                            link: subgraph_link,
                        },
                        resolver,
                    ).map_err(|e| panic!("subgraph resolve error {:?}", e))
                    .and_then(move |subgraph| {
                        // Send the new subgraph to the manager.
                        manager
                            .event_sink()
                            .send(SubgraphProviderEvent::SubgraphAdded(subgraph))
                    }).and_then(move |_| {
                        // If we subscribed to both events, then we're handling multiple data sets.
                        // Wait for thirty seconds for that to happen, otherwise fail the test.
                        let start_time = Instant::now();
                        let max_wait = Duration::from_secs(30);
                        loop {
                            let subscriptions = &eth_adapter.lock().unwrap().received_subscriptions;
                            if subscriptions.contains(&"ExampleEvent".to_owned())
                                && subscriptions.contains(&"ExampleEvent2".to_owned())
                            {
                                break;
                            }
                            if Instant::now().duration_since(start_time) > max_wait {
                                panic!("Test failed, events subscribed to: {:?}", subscriptions)
                            }
                            ::std::thread::yield_now();
                        }
                        Ok(())
                    })
                })
        })).unwrap();
}
