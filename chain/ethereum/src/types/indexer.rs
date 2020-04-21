use async_trait::async_trait;

use crate::prelude::{futures03, Error, SubgraphManifest};
use futures03::future::AbortHandle;

/// A component that loads and indexes subgraph instances.
#[async_trait]
pub trait SubgraphIndexer: Send + Sync + 'static {
  async fn start(&self, manifest: SubgraphManifest) -> Result<AbortHandle, Error>;
}
