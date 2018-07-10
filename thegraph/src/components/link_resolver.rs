use failure;
use futures::prelude::*;
use ipfs_api;
use std::error::Error;

/// Resolves links to data source definitions and resources referenced by them.
pub trait LinkResolver {
    /// Fetches the link contents as bytes.
    fn cat(&self, link: &str) -> Box<Future<Item = Vec<u8>, Error = Box<Error>>>;
}

impl LinkResolver for ipfs_api::IpfsClient {
    /// Currently supports only links of the form `/ipfs/ipfs_hash`
    fn cat(&self, link: &str) -> Box<Future<Item = Vec<u8>, Error = Box<Error>>> {
        // Verify that the link is in the expected form `/ipfs/hash`.
        if !link.starts_with("/ipfs/") {
            return Box::new(
                Err(
                    Box::new(failure::err_msg(format!("Invalid link {}", link)).compat())
                        as Box<Error>,
                ).into_future(),
            );
        }

        // Discard the `/ipfs/` prefix to get the hash.
        let path = link.trim_left_matches("/ipfs/");

        Box::new(
            self.cat(path)
                .concat2()
                .map(|x| x.to_vec())
                .map_err(From::from),
        )
    }
}
