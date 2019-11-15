use crate::prelude::Future;

/// A component that indexes network data for a blockchain.
///
/// Until we have multi-blockchain support, this is tied to `EthereumAdapter`.
/// Note the terminology is _blockchain_ or _chain_ for the general blockchain
/// technology, such as Ethereum, and _network_ for specific instances of such
/// a chain (e.g. Mainnet, Kovan, POA).
pub trait NetworkIndexer {
  fn into_polling_stream(self) -> Box<dyn Future<Item = (), Error = ()> + Send>;
}
