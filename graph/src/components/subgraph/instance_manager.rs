use async_trait::async_trait;

use crate::prelude::{futures03, Error, SubgraphManifest};
use futures03::future::AbortHandle;

/// A `SubgraphInstanceManager` loads and runs subgraph instances.
#[async_trait]
pub trait SubgraphInstanceManager: Send + Sync + 'static {
  async fn start(&self, manifest: SubgraphManifest) -> Result<AbortHandle, Error>;
}
