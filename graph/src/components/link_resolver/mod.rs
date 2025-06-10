use std::time::Duration;

use slog::Logger;

use crate::data::subgraph::Link;
use crate::prelude::Error;
use std::fmt::Debug;

mod arweave;
mod file;
mod ipfs;

pub use arweave::*;
use async_trait::async_trait;
pub use file::*;
pub use ipfs::*;

/// Resolves links to subgraph manifests and resources referenced by them.
#[async_trait]
pub trait LinkResolver: Send + Sync + 'static + Debug {
    /// Updates the timeout used by the resolver.
    fn with_timeout(&self, timeout: Duration) -> Box<dyn LinkResolver>;

    /// Enables infinite retries.
    fn with_retries(&self) -> Box<dyn LinkResolver>;

    /// Fetches the link contents as bytes.
    async fn cat(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error>;

    /// Fetches the IPLD block contents as bytes.
    async fn get_block(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error>;

    /// Creates a new resolver scoped to a specific subgraph manifest.
    ///
    /// For FileLinkResolver, this sets the base directory to the manifest's parent directory.
    /// Note the manifest here is the manifest in the build directory, not the manifest in the source directory
    /// to properly resolve relative paths referenced in the manifest (schema, mappings, etc.).
    /// For other resolvers (IPFS/Arweave), this simply returns a clone since they use
    /// absolute content identifiers.
    ///
    /// The `manifest_path` parameter can be a filesystem path or an alias. Aliases are used
    /// in development environments (via `gnd --sources`) to map user-defined
    /// aliases to actual subgraph paths, enabling local development with file-based
    /// subgraphs that reference each other.
    fn for_manifest(&self, manifest_path: &str) -> Result<Box<dyn LinkResolver>, Error>;

    /// Read the contents of `link` and deserialize them into a stream of JSON
    /// values. The values must each be on a single line; newlines are significant
    /// as they are used to split the file contents and each line is deserialized
    /// separately.
    async fn json_stream(&self, logger: &Logger, link: &Link) -> Result<JsonValueStream, Error>;
}
