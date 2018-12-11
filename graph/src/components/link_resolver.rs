use data::subgraph::Link;
use failure;
use ipfs_api;
use tokio::prelude::*;

use std::time::Duration;

/// Resolves links to subgraph manifests and resources referenced by them.
pub trait LinkResolver: Send + Sync + 'static {
    /// Fetches the link contents as bytes.
    fn cat(&self, link: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send>;
}

impl LinkResolver for ipfs_api::IpfsClient {
    /// Currently supports only links of the form `/ipfs/ipfs_hash`
    fn cat(&self, link: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send> {
        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_left_matches("/ipfs/");

        Box::new(
            self.cat(path)
                .concat2()
                // Guard against IPFS unresponsiveness.
                .timeout(Duration::from_secs(10))
                .map(|x| x.to_vec())
                .map_err(|e| failure::err_msg(e.to_string())),
        )
    }
}
