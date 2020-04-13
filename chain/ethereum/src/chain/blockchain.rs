use async_trait::async_trait;

use web3::types::H256;

use graph::data::store::scalar::Bytes;
use graph::prelude::{
  web3, BlockPointer, Blockchain, Error, Future01CompatExt, Logger, MetricsRegistry, Store,
  ToBlockPointer,
};

use super::Chain;

#[async_trait]
impl<MR, S> Blockchain for Chain<MR, S>
where
  MR: MetricsRegistry,
  S: Store,
{
  async fn latest_block_pointer(&self, logger: &Logger) -> Result<BlockPointer, Error> {
    let block = self.adapter.latest_block(logger).compat().await?;
    Ok(block.to_block_pointer())
  }

  async fn block_pointer_by_number(
    &self,
    logger: &Logger,
    n: u64,
  ) -> Result<Option<BlockPointer>, Error> {
    self
      .adapter
      .block_by_number(logger, n)
      .compat()
      .await
      .map(|block_opt| block_opt.map(|block| block.to_block_pointer()))
  }

  async fn block_pointer_by_hash(
    &self,
    _logger: &Logger,
    _hash: Bytes,
  ) -> Result<Option<BlockPointer>, Error> {
    todo!();
  }

  async fn parent_block_pointer(
    &self,
    logger: &Logger,
    ptr: &BlockPointer,
  ) -> Result<Option<BlockPointer>, Error> {
    let block = self
      .adapter
      .block_by_hash(logger, H256::from_slice(ptr.hash.as_slice()))
      .compat()
      .await?;

    Ok(block.and_then(
      |block| match (block.number.unwrap().as_u64(), block.parent_hash) {
        (n, parent_hash) if n >= 1 => Some(BlockPointer {
          number: n - 1,
          hash: parent_hash.as_bytes().into(),
        }),
        _ => None,
      },
    ))
  }
}
