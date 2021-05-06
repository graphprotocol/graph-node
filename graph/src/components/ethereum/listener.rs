use futures::Stream;

/// The updates have no payload, receivers should call `Store::chain_head_ptr`
/// to check what the latest block is.
pub type ChainHeadUpdateStream = Box<dyn Stream<Item = (), Error = ()> + Send>;

pub trait ChainHeadUpdateListener: Send + Sync + 'static {
    // Subscribe to chain head updates for the given network.
    fn subscribe(&self, network: String) -> ChainHeadUpdateStream;
}
