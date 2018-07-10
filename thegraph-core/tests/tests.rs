extern crate ethabi;
extern crate futures;
extern crate ipfs_api;
extern crate slog;
extern crate thegraph;
extern crate thegraph_core;
extern crate thegraph_mock;
extern crate thegraph_runtime;
extern crate tokio_core;

use futures::stream::{self, Stream};
use futures::{Future, Sink};
use ipfs_api::IpfsClient;
use std::fs::read_to_string;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use thegraph::components::ethereum::*;
use thegraph::prelude::{DataSourceDefinitionLoader as LoaderTrait, *};
use thegraph::util::log::logger;
use thegraph_core::{DataSourceDefinitionLoader, RuntimeManager};
use thegraph_mock::FakeStore;
use thegraph_runtime::RuntimeHostBuilder;
use tokio_core::reactor::Core;

#[test]
#[ignore]
fn multiple_data_sets_per_data_source() {
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

    let mut core = Core::new().unwrap();

    // Replace "link to" placeholders in the data source definition with hashes
    // of files just added into a local IPFS daemon on port 5001.
    let ipfs_client = IpfsClient::default();
    let mut data_source_string =
        std::fs::read_to_string("tests/datasource-two-datasets/two-datasets.yaml").unwrap();
    for file in &[
        "abis/ExampleContract.json",
        "abis/ExampleContract2.json",
        "empty.wasm",
        "schema.graphql",
    ] {
        let link = core.run(add(
            &ipfs_client,
            read_to_string(format!("tests/datasource-two-datasets/{}", file)).unwrap(),
        )).unwrap();
        data_source_string =
            data_source_string.replace(&format!("link to {}", file), &format!("/ipfs/{}", link));
    }
    let data_source_link = core.run(add(&ipfs_client, data_source_string)).unwrap();

    let logger = logger();
    let eth_adapter = Arc::new(Mutex::new(MockEthereumAdapter {
        received_subscriptions: vec![],
    }));
    let host_builder = RuntimeHostBuilder::new(&logger, core.handle(), eth_adapter.clone());

    let fake_store = Arc::new(Mutex::new(FakeStore));
    let manager = RuntimeManager::new(&logger, core.handle(), fake_store, host_builder);

    // Load a data source with two data sets, one listening for `ExampleEvent`
    // and the other for `ExampleEvent2`.
    let data_source = core.run(
        DataSourceDefinitionLoader.load_from_ipfs(&data_source_link, &ipfs_client),
    ).expect("failed to load data source");

    // Send the new data source to the manager.
    manager
        .event_sink()
        .send(DataSourceProviderEvent::DataSourceAdded(data_source))
        .wait()
        .unwrap();

    // If we subscribed to both events, then we're handling multiple data sets.
    // Wait for one second for that to happen, otherwise fail the test.
    let start_time = Instant::now();
    let max_wait = Duration::from_secs(1);
    while eth_adapter.lock().unwrap().received_subscriptions != ["ExampleEvent", "ExampleEvent2"] {
        core.turn(Some(max_wait));
        if Instant::now().duration_since(start_time) > max_wait {
            panic!("Test failed, events not subscribed to.")
        }
    }
}
