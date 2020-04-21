use async_trait::async_trait;

use futures03::future::AbortHandle;
use graph::prelude::{futures03, Error, SubgraphManifest};

/// A component that loads and indexes subgraph instances.
#[async_trait]
pub trait SubgraphIndexer: Send + Sync + 'static {
  async fn start(&self, manifest: SubgraphManifest) -> Result<AbortHandle, Error>;
}
