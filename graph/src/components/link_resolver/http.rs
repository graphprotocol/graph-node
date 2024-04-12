use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use lru_time_cache::LruCache;
use regex::Regex;
use slog::{debug, Logger};

use crate::cheap_clone::CheapClone;
// use crate::data_source::offchain::Base64;
use crate::env::EnvVars;
use crate::http_client::{FileSizeLimit, HttpClient, HttpClientError};
use crate::prelude::Error;
use crate::prelude::Link;
use crate::prelude::*;
use crate::util::futures::RetryConfigNoTimeout;
use std::fmt::Debug;

/// The values that `json_stream` returns. The struct contains the deserialized
/// JSON value from the input stream, together with the line number from which
/// the value was read.

fn retry_policy<I: Send + Sync>(
    always_retry: bool,
    op: &'static str,
    logger: &Logger,
) -> RetryConfigNoTimeout<I, HttpClientError> {
    // Even if retries were not requested, networking errors are still retried until we either get
    // a valid HTTP response or a timeout.
    if always_retry {
        retry(op, logger).no_limit()
    } else {
        retry(op, logger)
            .no_limit()
            .when(|res: &Result<_, HttpClientError>| match res {
                Ok(_) => false,
                Err(HttpClientError::FileTooLarge(..)) => false,
                Err(e) => !(e.is_status() || e.is_timeout()),
            })
    }
    .no_timeout() // The timeout should be set in the internal future.
}

#[derive(Clone)]
pub struct HttpResolver {
    client: HttpClient,
    cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    timeout: Duration,
    retry: bool,
    env_vars: Arc<EnvVars>,
}

// impl HttpResolver {
//     pub fn new(client: HttpClient, env_vars: Arc<EnvVars>) -> Self {
//         Self {
//             client: client.clone(),
//             cache: Arc::new(Mutex::new(LruCache::with_capacity(
//                 env_vars.mappings.max_ipfs_cache_size as usize,
//             ))),
//             timeout: env_vars.mappings.ipfs_timeout,
//             retry: false,
//             env_vars,
//         }
//     }
// }

impl Debug for HttpResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkResolver")
            .field("timeout", &self.timeout)
            .field("retry", &self.retry)
            .field("env_vars", &self.env_vars)
            .finish()
    }
}

impl CheapClone for HttpResolver {
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

#[async_trait]
pub trait HttpResolverTrait: Send + Sync + 'static + Debug {
    /// Updates the timeout used by the resolver.
    fn with_timeout(&self, timeout: Duration) -> Box<dyn HttpResolverTrait>;

    /// Enables infinite retries.
    fn with_retries(&self) -> Box<dyn HttpResolverTrait>;

    /// Fetches the link contents as bytes.
    async fn cat(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error>;

    /// Fetches the IPLD block contents as bytes.
    async fn get_block(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error>;
}

const DATA_REGEX: &str = r"^data:(.*?);(.*?),(.*)$";

#[async_trait]
impl HttpResolverTrait for HttpResolver {
    fn with_timeout(&self, timeout: Duration) -> Box<dyn HttpResolverTrait> {
        let mut s = self.cheap_clone();
        s.timeout = timeout;
        Box::new(s)
    }

    fn with_retries(&self) -> Box<dyn HttpResolverTrait> {
        let mut s = self.cheap_clone();
        s.retry = true;
        Box::new(s)
    }

    /// Supports links of the form `https?://`
    async fn cat(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error> {
        let path = link.link.to_owned();

        if path.starts_with("data:") {
            // data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABgAAAAYCAYAAADgdz34AAABjElEQVRIS+2VwQ3CMAxFc
            let regex = Regex::new(DATA_REGEX).unwrap();
            let parts = regex.captures(&path.as_str()).unwrap();
            let encoding = parts.get(2).unwrap().as_str();
            if encoding.ends_with("base64") {
                return Ok(general_purpose::STANDARD
                    .decode(parts.get(3).unwrap().as_str())
                    .unwrap());
            }
            return Ok(Vec::from(parts.get(3).unwrap().as_str()));
        }

        if let Some(data) = self.cache.lock().unwrap().get(&path) {
            trace!(logger, "URL cache hit"; "hash" => &path);
            return Ok(data.clone());
        }
        trace!(logger, "URL cache miss"; "hash" => &path);

        let max_cache_file_size = self.env_vars.mappings.max_ipfs_cache_file_size;
        let max_file_size: u64 = self
            .env_vars
            .mappings
            .max_ipfs_file_bytes
            .try_into()
            .unwrap();

        let req_path = link.clone();
        let client = self.client.clone();
        let data = retry_policy(self.retry, "http.cat", logger)
            .run(move || {
                let path = req_path.clone();
                let client = client.clone();
                async move {
                    Ok(client
                        .get_with_limit(&path, &FileSizeLimit::MaxBytes(max_file_size))
                        .await?
                        .to_vec())
                }
            })
            .await?;

        // Only cache files if they are not too large
        if data.len() <= max_cache_file_size {
            let mut cache = self.cache.lock().unwrap();
            if !cache.contains_key(&path) {
                cache.insert(path.clone(), data.clone());
            }
        } else {
            debug!(logger, "File too large for cache";
                        "path" => path,
                        "size" => data.len()
            );
        }

        Ok(data)
    }

    async fn get_block(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error> {
        trace!(logger, "URL block get"; "hash" => &link.link);

        // Note: The IPFS protocol limits the size of blocks to 1MB, so we don't need to enforce size
        // limits here.
        let link = link.clone();
        let client = self.client.clone();
        let data = retry_policy(self.retry, "ipfs.getBlock", logger)
            .run(move || {
                let link = link.clone();
                let client = client.clone();
                async move {
                    let data = client.get(&link).await?.to_vec();
                    Result::<Vec<u8>, _>::Ok(data)
                }
            })
            .await?;

        Ok(data)
    }
}

#[cfg(test)]
mod test {
    use serde_derive::Deserialize;
    use serde_json::{Map, Value};
    use slog::debug;

    use crate::{data_source::offchain::Base64, http_client::HttpClient, prelude::Link};

    // This test ensures that passing txid/filename works when the txid refers to manifest.
    // the actual data seems to have some binary header and footer so these ranges were found
    // by inspecting the data with hexdump.
    #[tokio::test]
    async fn fetch_bundler_url() {
        let url = Link { link: "https://api.universalprofile.cloud/ipfs/QmaMY4aex2v9QPkxAN1mXQzoNzAY5EyLbpEegN6Mrx5bAa".to_string() };

        let client = HttpClient::default();
        let no_header = &client.get(&url).await.unwrap();
        let content: Map<String, Value> = serde_json::from_slice(no_header).unwrap();

        let name = content
            .get("LSP4Metadata")
            .unwrap()
            .get("name")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(name, "HexGenzo (Gen0)");
    }
}
