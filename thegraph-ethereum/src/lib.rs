extern crate ethereum_types;
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
    use ethereum_adapter::{EthereumAdapter, EthereumAdapterConfig};
    use tokio_core::reactor::Core;
    use web3::futures::Future;
    use web3::transports;

    #[test]
    fn new_ethereum_adapter() {
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
}
