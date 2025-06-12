mod arweave;
mod ipfs;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use slog::Logger;

use crate::cheap_clone::CheapClone;
use crate::data::subgraph::{DeploymentHash, Link};
use crate::derive::CheapClone;
use crate::prelude::Error;

pub use arweave::*;
pub use ipfs::*;

/// Resolves links to subgraph manifests and resources referenced by them.
#[async_trait]
pub trait LinkResolver: Send + Sync + 'static + Debug {
    /// Updates the timeout used by the resolver.
    fn with_timeout(&self, timeout: Duration) -> Box<dyn LinkResolver>;

    /// Enables infinite retries.
    fn with_retries(&self) -> Box<dyn LinkResolver>;

    /// Fetches the link contents as bytes.
    async fn cat(&self, ctx: LinkResolverContext, link: &Link) -> Result<Vec<u8>, Error>;

    /// Fetches the IPLD block contents as bytes.
    async fn get_block(&self, ctx: LinkResolverContext, link: &Link) -> Result<Vec<u8>, Error>;

    /// Read the contents of `link` and deserialize them into a stream of JSON
    /// values. The values must each be on a single line; newlines are significant
    /// as they are used to split the file contents and each line is deserialized
    /// separately.
    async fn json_stream(
        &self,
        ctx: LinkResolverContext,
        link: &Link,
    ) -> Result<JsonValueStream, Error>;
}

#[derive(Clone, Debug, CheapClone)]
pub struct LinkResolverContext {
    pub deployment_hash: Arc<str>,
    pub logger: Logger,
}

impl LinkResolverContext {
    pub fn new(deployment_hash: &DeploymentHash, logger: &Logger) -> Self {
        Self {
            deployment_hash: deployment_hash.as_str().into(),
            logger: logger.cheap_clone(),
        }
    }
}

#[cfg(debug_assertions)]
impl Default for LinkResolverContext {
    fn default() -> Self {
        Self {
            deployment_hash: "test".into(),
            logger: crate::log::discard(),
        }
    }
}
