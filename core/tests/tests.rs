extern crate ethabi;
extern crate futures;
extern crate graph;
extern crate graph_core;
extern crate graph_mock;
extern crate graph_runtime_wasm;
extern crate ipfs_api;
extern crate slog;

use futures::{Future, Sink};
use graph::components::ethereum::*;
use graph::prelude::*;
use graph::util::log::logger;
use graph_core::RuntimeManager;
use graph_mock::FakeStore;
use graph_runtime_wasm::RuntimeHostBuilder;
use ipfs_api::IpfsClient;
use std::fs::read_to_string;
use std::io::Cursor;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[test]
#[ignore]
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

    // Replace "link to" placeholders in the subgraph manifest with hashes
    // of files just added into a local IPFS daemon on port 5001.
    let resolver = Arc::new(IpfsClient::default());
    let mut subgraph_string =
        std::fs::read_to_string("tests/subgraph-two-datasources/two-datasources.yaml").unwrap();
    for file in &[
        "abis/ExampleContract.json",
        "abis/ExampleContract2.json",
        "empty.wasm",
        "schema.graphql",
    ] {
        let link = add(
            &resolver,
            read_to_string(format!("tests/subgraph-two-datasources/{}", file)).unwrap(),
        ).wait()
            .unwrap();
        subgraph_string =
            subgraph_string.replace(&format!("link to {}", file), &format!("/ipfs/{}", link));
    }
    let subgraph_link = add(&resolver, subgraph_string).wait().unwrap();

    let logger = logger();
    let eth_adapter = Arc::new(Mutex::new(MockEthereumAdapter {
        received_subscriptions: vec![],
    }));
    let host_builder = RuntimeHostBuilder::new(&logger, eth_adapter.clone(), resolver.clone());

    let fake_store = Arc::new(Mutex::new(FakeStore));
    let manager = RuntimeManager::new(&logger, fake_store, host_builder);

    // Load a subgraph with two data sets, one listening for `ExampleEvent`
    // and the other for `ExampleEvent2`.
    let subgraph = SubgraphManifest::resolve(
        Link {
            link: subgraph_link,
        },
        resolver,
    ).wait()
        .expect("failed to load subgraph");

    // Send the new subgraph to the manager.
    manager
        .event_sink()
        .send(SubgraphProviderEvent::SubgraphAdded(subgraph))
        .wait()
        .unwrap();

    // If we subscribed to both events, then we're handling multiple data sets.
    // Wait for one second for that to happen, otherwise fail the test.
    let start_time = Instant::now();
    let max_wait = Duration::from_secs(1);
    while eth_adapter.lock().unwrap().received_subscriptions != ["ExampleEvent", "ExampleEvent2"] {
        ::std::thread::yield_now();
        if Instant::now().duration_since(start_time) > max_wait {
            panic!("Test failed, events not subscribed to.")
        }
    }
}
