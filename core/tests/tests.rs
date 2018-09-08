extern crate ethabi;
extern crate graph;
extern crate graph_core;
extern crate graph_mock;
extern crate graph_runtime_wasm;
extern crate ipfs_api;
extern crate web3;

use ethabi::Token;
use ipfs_api::IpfsClient;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::read_to_string;
use std::io::Cursor;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use web3::types::Block;
use web3::types::H256;
use web3::types::Transaction;

use graph::components::ethereum::*;
use graph::components::store::*;
use graph::util::ethereum::string_to_h256;
use graph::prelude::*;
use graph_core::RuntimeManager;
use graph_mock::FakeStore;
use graph_runtime_wasm::RuntimeHostBuilder;

#[test]
fn multiple_data_sources_per_subgraph() {
    struct MockEthereumAdapter {
        event_sigs: RefCell<HashSet<H256>>,
    }

    impl EthereumAdapter for MockEthereumAdapter {
        fn block_by_hash(
            &self,
            _: H256,
        ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send> {
            unimplemented!()
        }

        fn block_by_number(
            &self,
            _: u64,
        ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send> {
            unimplemented!()
        }

        fn is_on_main_chain(
            &self,
            _: EthereumBlockPointer,
        ) -> Box<Future<Item = bool, Error = Error> + Send> {
            unimplemented!()
        }

        fn find_first_blocks_with_events(
            &self,
            _: u64,
            _: u64,
            event_filter: EthereumEventFilter,
        ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
            // Record what events were asked for
            for events_by_sig in event_filter.event_types_by_contract_address_and_sig.values() {
                for sig in events_by_sig.keys() {
                    self.event_sigs.borrow_mut().insert(sig.clone());
                }
            }

            Box::new(future::ok(vec![]))
        }

        fn get_events_in_block(
            &self,
            _: Block<Transaction>,
            event_filter: EthereumEventFilter,
        ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError>> {
            // Record what events were asked for
            for events_by_sig in event_filter.event_types_by_contract_address_and_sig.values() {
                for sig in events_by_sig.keys() {
                    self.event_sigs.borrow_mut().insert(sig.clone());
                }
            }

            Box::new(stream::empty())
        }

        fn contract_call(
            &mut self,
            _: EthereumContractCall,
        ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>> {
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
            let subgraph_string = std::fs::read_to_string(
                "tests/subgraph-two-datasources/two-datasources.yaml",
            ).unwrap();
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
                    let log_drain = Mutex::new(slog_term::CompactFormat::new(slog_term::TermDecorator::new().build()).build()).fuse();
                    let logger = Logger::root(log_drain, o!());
                    let eth_adapter = Arc::new(Mutex::new(MockEthereumAdapter {
                        event_sigs: RefCell::new(HashSet::new()),
                    }));
                    let host_builder =
                        RuntimeHostBuilder::new(&logger, eth_adapter.clone(), resolver.clone());

                    let fake_store = Arc::new(Mutex::new(FakeStore::new()));
                    let manager =
                        RuntimeManager::new(&logger, fake_store, eth_adapter.clone(), host_builder);

                    let event_sig1 = string_to_h256("ExampleEvent(string)");
                    let event_sig2 = string_to_h256("ExampleEvent2(string)");

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
                        })
                        .and_then(move |_| {
                            // If we subscribed to both events, then we're handling multiple data sets.
                            // Wait for thirty seconds for that to happen, otherwise fail the test.
                            let start_time = Instant::now();
                            let max_wait = Duration::from_secs(30);
                            loop {
                                let eth_adapter = eth_adapter.lock().unwrap();
                                let event_sigs = eth_adapter.event_sigs.borrow();
                                if event_sigs.contains(&event_sig1) && event_sigs.contains(&event_sig2) {
                                    break;
                                }
                                if Instant::now().duration_since(start_time) > max_wait {
                                    panic!("Test failed, events subscribed to: {:?}", event_sigs)
                                }
                                ::std::thread::yield_now();
                            }
                            Ok(())
                        })
                })
        }))
        .unwrap();
}
