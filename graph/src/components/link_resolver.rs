use data::subgraph::Link;
use failure;
use ipfs_api;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tokio::prelude::*;

const MAX_IPFS_FILE_BYTES_ENV_VAR: &str = "GRAPH_MAX_IPFS_FILE_BYTES";

/// Resolves links to subgraph manifests and resources referenced by them.
pub trait LinkResolver: Send + Sync + 'static {
    /// Fetches the link contents as bytes.
    fn cat(&self, link: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send>;
}

impl LinkResolver for ipfs_api::IpfsClient {
    /// Supports links of the form `/ipfs/ipfs_hash` or just `ipfs_hash`.
    fn cat(&self, link: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send> {
        // Grab env vars.
        let max_file_bytes = env::var(MAX_IPFS_FILE_BYTES_ENV_VAR).ok().map(|s| {
            u64::from_str(&s).unwrap_or_else(|_| {
                panic!("failed to parse env var {}", MAX_IPFS_FILE_BYTES_ENV_VAR)
            })
        });
        let ipfs_timeout = env::var("GRAPH_IPFS_TIMEOUT").ok().map(|s| {
            u64::from_str(&s)
                .unwrap_or_else(|_| panic!("failed to parse env var GRAPH_IPFS_TIMEOUT"))
        });

        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_left_matches("/ipfs/").to_owned();

        let ipfs_timeout = Duration::from_secs(ipfs_timeout.unwrap_or(30));
        let cat = self
            .cat(&path)
            .concat2()
            .timeout(ipfs_timeout)
            .map(|x| x.to_vec())
            .map_err(|e| failure::err_msg(e.to_string()));

        match max_file_bytes {
            Some(max_bytes) => Box::new(
                self.object_stat(&path)
                    .timeout(ipfs_timeout)
                    .map_err(|e| failure::err_msg(e.to_string()))
                    .and_then(move |stat| match stat.cumulative_size > max_bytes {
                        false => Ok(()),
                        true => Err(format_err!("IPFS file {} is too large", path)),
                    })
                    .and_then(|()| cat),
            ),
            None => Box::new(cat),
        }
    }
}

#[test]
fn max_file_size() {
    env::set_var(MAX_IPFS_FILE_BYTES_ENV_VAR, "200");
    let file: &[u8] = &[0u8; 201];
    let client = ipfs_api::IpfsClient::default();

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let link = runtime.block_on(client.add(file)).unwrap().hash;
    let err = runtime
        .block_on(LinkResolver::cat(&client, &Link { link: link.clone() }))
        .unwrap_err();
    env::remove_var(MAX_IPFS_FILE_BYTES_ENV_VAR);
    assert_eq!(err.to_string(), format!("IPFS file {} is too large", link));
}
