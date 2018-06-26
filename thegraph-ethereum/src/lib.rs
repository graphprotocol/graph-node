extern crate ethabi;
extern crate futures;
extern crate serde_json;
extern crate thegraph;
extern crate tokio_core;
extern crate web3;

mod ethereum_adapter;

pub use self::ethereum_adapter::EthereumAdapter;
pub use self::ethereum_adapter::EthereumAdapterConfig;
pub use web3::transports;

#[cfg(test)]
mod tests {
    use ethabi::{Event, EventParam, ParamType};
    use ethereum_adapter::{EthereumAdapter, EthereumAdapterConfig};
    use thegraph::components::ethereum::{BlockNumberRange, EthereumEventSubscription};
    use thegraph::prelude::EthereumAdapter as EthereumAdapterTrait;
    use tokio_core::reactor::Core;
    use web3;
    use web3::futures::future;
    use web3::futures::{Future, Stream};
    use web3::transports;
    use web3::types::*;

    #[test]
    fn new_ethereum_ipc_adapter() {
        let mut core = Core::new().unwrap();
        transports::ipc::Ipc::with_event_loop(&"INSERT_IPC_PATH"[..], &core.handle())
            .map(|transport| {
                EthereumAdapter::new(
                    EthereumAdapterConfig {
                        transport: transport,
                    },
                    core.handle(),
                )
            })
            .map(|eth_adapter| eth_adapter.block_number())
            .and_then(|work| core.run(work));
    }

    #[test]
    fn new_ethereum_rpc_adapter() {
        let mut core = Core::new().unwrap();
        let result = transports::http::Http::new(&"http://127.0.0.1:8545"[..])
            // eloop needs to be threaded through, because if the handle is dropped
            // then rust-web3 futures will stop working.
            .map(|(eloop, transport)| {
                (
                    eloop,
                    EthereumAdapter::new(
                        EthereumAdapterConfig {
                            transport: transport,
                        },
                        core.handle(),
                    ),
                )
            })
            .and_then(|(eloop, eth_adapter)| {
                (eth_adapter.block_number().wait().map(|res| (println!("rpc block number {:?}", res))))
            });
    }

    #[test]
    fn event_logs() {
        let mut core = Core::new().unwrap();
        let tranport_result = transports::ipc::Ipc::with_event_loop(
            &"/Users/aklempner/Library/Application Support/io.parity.ethereum/jsonrpc.ipc"[..],
            &core.handle(),
        );
        let transport = tranport_result.unwrap();
        let mut adapter = EthereumAdapter::new(
            EthereumAdapterConfig {
                transport: transport,
            },
            core.handle(),
        );
        let work = adapter
            .sha3("Transfer(address,address,uint256)")
            .join(adapter.block_number())
            .and_then(|(transfer_topic, block_number)| {
                let sub = EthereumEventSubscription {
                    subscription_id: String::from("1"),
                    address: Address::zero(),
                    event_signature: transfer_topic,
                    range: BlockNumberRange {
                        from: BlockNumber::Number(block_number.as_u64()),
                        to: BlockNumber::Latest,
                    },
                    event: Event {
                        name: "Transfer".to_owned(),
                        inputs: vec![
                            EventParam {
                                name: "_from".to_owned(),
                                kind: ParamType::Address,
                                indexed: true,
                            },
                            EventParam {
                                name: "_to".to_owned(),
                                kind: ParamType::Address,
                                indexed: true,
                            },
                            EventParam {
                                name: "_value".to_owned(),
                                kind: ParamType::Uint(256),
                                indexed: false,
                            },
                        ],
                        anonymous: false,
                    },
                };
                let event_signature = sub.event_signature.clone();
                let params = sub.event.inputs.clone();
                adapter
                    .subscribe_to_event(sub)
                    .take(3)
                    .for_each(move |log| {
                        println!("{:?}", log);
                        assert_eq!(log.event_signature, event_signature);
                        let zipper: Vec<_> = log.params.iter().zip(params.clone()).collect();
                        assert_eq!(zipper[0].0.name, zipper[0].1.name);
                        assert_eq!(zipper[1].0.name, zipper[1].1.name);
                        assert_eq!(zipper[2].0.name, zipper[2].1.name);
                        future::ok::<(), web3::error::Error>(())
                    })
            });
        core.run(work);
    }
}
