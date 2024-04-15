use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::env::EnvVars;
use crate::futures01::{stream::poll_fn, try_ready};
use crate::futures01::{Async, Poll};
use crate::ipfs_client::IpfsError;
use crate::util::futures::RetryConfigNoTimeout;
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BytesMut;
use futures03::compat::Stream01CompatExt;
use futures03::future::TryFutureExt;
use futures03::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use lru_time_cache::LruCache;
use serde_json::Value;

use crate::{
    cheap_clone::CheapClone,
    derive::CheapClone,
    futures01::stream::Stream,
    ipfs_client::IpfsClient,
    prelude::{LinkResolver as LinkResolverTrait, *},
};

fn retry_policy<I: Send + Sync>(
    always_retry: bool,
    op: &'static str,
    logger: &Logger,
) -> RetryConfigNoTimeout<I, IpfsError> {
    // Even if retries were not requested, networking errors are still retried until we either get
    // a valid HTTP response or a timeout.
    if always_retry {
        retry(op, logger).no_limit()
    } else {
        retry(op, logger)
            .no_limit()
            .when(|res: &Result<_, IpfsError>| match res {
                Ok(_) => false,
                Err(IpfsError::FileTooLarge(..)) => false,
                Err(e) => !(e.is_status() || e.is_timeout()),
            })
    }
    .no_timeout() // The timeout should be set in the internal future.
}

/// The IPFS APIs don't have a quick "do you have the file" function. Instead, we
/// just rely on whether an API times out. That makes sense for IPFS, but not for
/// our application. We want to be able to quickly select from a potential list
/// of clients where hopefully one already has the file, and just get the file
/// from that.
///
/// The strategy here then is to cat a single byte as a proxy for "do you have the
/// file". Whichever client has or gets the file first wins. This API is a good
/// choice, because it doesn't involve us actually starting to download the file
/// from each client, which would be wasteful of bandwidth and memory in the
/// case multiple clients respond in a timely manner.
async fn select_fastest_client(
    clients: Arc<Vec<IpfsClient>>,
    logger: Logger,
    path: String,
    timeout: Duration,
    do_retry: bool,
) -> Result<IpfsClient, Error> {
    if clients.len() == 1 {
        return Ok(clients[0].cheap_clone());
    }

    let mut err: Option<Error> = None;

    let mut exists: FuturesUnordered<_> = clients
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let c = c.cheap_clone();
            let path = path.clone();
            retry_policy(do_retry, "IPFS exists", &logger).run(move || {
                let path = path.clone();
                let c = c.cheap_clone();
                async move { c.exists(&path, Some(timeout)).map_ok(|()| i).await }
            })
        })
        .collect();

    while let Some(result) = exists.next().await {
        match result {
            Ok(index) => {
                return Ok(clients[index].cheap_clone());
            }
            Err(e) => err = Some(e.into()),
        }
    }

    Err(err.unwrap_or_else(|| {
        anyhow!(
            "No IPFS clients were supplied to handle the call. File: {}",
            path
        )
    }))
}

#[derive(Clone, CheapClone)]
pub struct IpfsResolver {
    clients: Arc<Vec<IpfsClient>>,
    cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    timeout: Duration,
    retry: bool,
    env_vars: Arc<EnvVars>,
}

impl IpfsResolver {
    pub fn new(clients: Vec<IpfsClient>, env_vars: Arc<EnvVars>) -> Self {
        Self {
            clients: Arc::new(clients.into_iter().collect()),
            cache: Arc::new(Mutex::new(LruCache::with_capacity(
                env_vars.mappings.max_ipfs_cache_size as usize,
            ))),
            timeout: env_vars.mappings.ipfs_timeout,
            retry: false,
            env_vars,
        }
    }
}

impl Debug for IpfsResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkResolver")
            .field("timeout", &self.timeout)
            .field("retry", &self.retry)
            .field("env_vars", &self.env_vars)
            .finish()
    }
}

#[async_trait]
impl LinkResolverTrait for IpfsResolver {
    fn with_timeout(&self, timeout: Duration) -> Box<dyn LinkResolverTrait> {
        let mut s = self.cheap_clone();
        s.timeout = timeout;
        Box::new(s)
    }

    fn with_retries(&self) -> Box<dyn LinkResolverTrait> {
        let mut s = self.cheap_clone();
        s.retry = true;
        Box::new(s)
    }

    /// Supports links of the form `/ipfs/ipfs_hash` or just `ipfs_hash`.
    async fn cat(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error> {
        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_start_matches("/ipfs/").to_owned();

        if let Some(data) = self.cache.lock().unwrap().get(&path) {
            trace!(logger, "IPFS cache hit"; "hash" => &path);
            return Ok(data.clone());
        }
        trace!(logger, "IPFS cache miss"; "hash" => &path);

        let client = select_fastest_client(
            self.clients.cheap_clone(),
            logger.cheap_clone(),
            path.clone(),
            self.timeout,
            self.retry,
        )
        .await?;

        let max_cache_file_size = self.env_vars.mappings.max_ipfs_cache_file_size;
        let max_file_size = self.env_vars.mappings.max_ipfs_file_bytes;

        let req_path = path.clone();
        let timeout = self.timeout;
        let data = retry_policy(self.retry, "ipfs.cat", logger)
            .run(move || {
                let path = req_path.clone();
                let client = client.clone();
                async move {
                    Ok(client
                        .cat_all(&path, Some(timeout), max_file_size)
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
        trace!(logger, "IPFS block get"; "hash" => &link.link);
        let client = select_fastest_client(
            self.clients.cheap_clone(),
            logger.cheap_clone(),
            link.link.clone(),
            self.timeout,
            self.retry,
        )
        .await?;

        // Note: The IPFS protocol limits the size of blocks to 1MB, so we don't need to enforce size
        // limits here.
        let link = link.link.clone();
        let data = retry_policy(self.retry, "ipfs.getBlock", logger)
            .run(move || {
                let link = link.clone();
                let client = client.clone();
                async move {
                    let data = client.get_block(link.clone()).await?.to_vec();
                    Result::<Vec<u8>, _>::Ok(data)
                }
            })
            .await?;

        Ok(data)
    }

    async fn json_stream(&self, logger: &Logger, link: &Link) -> Result<JsonValueStream, Error> {
        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_start_matches("/ipfs/").to_string();

        let client = select_fastest_client(
            self.clients.cheap_clone(),
            logger.cheap_clone(),
            path.to_string(),
            self.timeout,
            self.retry,
        )
        .await?;

        let max_file_size = self.env_vars.mappings.max_ipfs_map_file_size;
        let mut cummulative_file_size = 0;

        let mut stream = client
            .cat_stream(&path, None)
            .await?
            .fuse()
            .boxed()
            .compat();

        let mut buf = BytesMut::with_capacity(1024);

        // Count the number of lines we've already successfully deserialized.
        // We need that to adjust the line number in error messages from serde_json
        // to translate from line numbers in the snippet we are deserializing
        // to the line number in the overall file
        let mut count = 0;

        let stream: JsonValueStream = Box::pin(
            poll_fn(move || -> Poll<Option<JsonStreamValue>, Error> {
                loop {
                    cummulative_file_size += buf.len();

                    if cummulative_file_size > max_file_size {
                        return Err(anyhow!(
                            "IPFS file {} is too large. It can be at most {} bytes",
                            path,
                            max_file_size,
                        ));
                    }

                    if let Some(offset) = buf.iter().position(|b| *b == b'\n') {
                        let line_bytes = buf.split_to(offset + 1);
                        count += 1;
                        if line_bytes.len() > 1 {
                            let line = std::str::from_utf8(&line_bytes)?;
                            let res = match serde_json::from_str::<Value>(line) {
                                Ok(v) => Ok(Async::Ready(Some(JsonStreamValue {
                                    value: v,
                                    line: count,
                                }))),
                                Err(e) => {
                                    // Adjust the line number in the serde error. This
                                    // is fun because we can only get at the full error
                                    // message, and not the error message without line number
                                    let msg = e.to_string();
                                    let msg = msg.split(" at line ").next().unwrap();
                                    Err(anyhow!(
                                        "{} at line {} column {}: '{}'",
                                        msg,
                                        e.line() + count - 1,
                                        e.column(),
                                        line
                                    ))
                                }
                            };
                            return res;
                        }
                    } else {
                        // We only get here if there is no complete line in buf, and
                        // it is therefore ok to immediately pass an Async::NotReady
                        // from stream through.
                        // If we get a None from poll, but still have something in buf,
                        // that means the input was not terminated with a newline. We
                        // add that so that the last line gets picked up in the next
                        // run through the loop.
                        match try_ready!(stream.poll().map_err(|e| anyhow::anyhow!("{}", e))) {
                            Some(b) => buf.extend_from_slice(&b),
                            None if !buf.is_empty() => buf.extend_from_slice(&[b'\n']),
                            None => return Ok(Async::Ready(None)),
                        }
                    }
                }
            })
            .compat(),
        );

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::EnvVars;
    use serde_json::json;

    #[tokio::test]
    async fn max_file_size() {
        let mut env_vars = EnvVars::default();
        env_vars.mappings.max_ipfs_file_bytes = 200;

        let file: &[u8] = &[0u8; 201];
        let client = IpfsClient::localhost();
        let resolver = super::IpfsResolver::new(vec![client.clone()], Arc::new(env_vars));

        let logger = Logger::root(slog::Discard, o!());

        let link = client.add(file.into()).await.unwrap().hash;
        let err = IpfsResolver::cat(&resolver, &logger, &Link { link: link.clone() })
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "IPFS file {} is too large. It can be at most 200 bytes",
                link
            )
        );
    }

    async fn json_round_trip(text: &'static str, env_vars: EnvVars) -> Result<Vec<Value>, Error> {
        let client = IpfsClient::localhost();
        let resolver = super::IpfsResolver::new(vec![client.clone()], Arc::new(env_vars));

        let logger = Logger::root(slog::Discard, o!());
        let link = client.add(text.as_bytes().into()).await.unwrap().hash;

        let stream = IpfsResolver::json_stream(&resolver, &logger, &Link { link }).await?;
        stream.map_ok(|sv| sv.value).try_collect().await
    }

    #[tokio::test]
    async fn read_json_stream() {
        let values = json_round_trip("\"with newline\"\n", EnvVars::default()).await;
        assert_eq!(vec![json!("with newline")], values.unwrap());

        let values = json_round_trip("\"without newline\"", EnvVars::default()).await;
        assert_eq!(vec![json!("without newline")], values.unwrap());

        let values = json_round_trip("\"two\" \n \"things\"", EnvVars::default()).await;
        assert_eq!(vec![json!("two"), json!("things")], values.unwrap());

        let values = json_round_trip(
            "\"one\"\n  \"two\" \n [\"bad\" \n \"split\"]",
            EnvVars::default(),
        )
        .await;
        assert_eq!(
            "EOF while parsing a list at line 4 column 0: ' [\"bad\" \n'",
            values.unwrap_err().to_string()
        );
    }

    #[tokio::test]
    async fn ipfs_map_file_size() {
        let file = "\"small test string that trips the size restriction\"";
        let mut env_vars = EnvVars::default();
        env_vars.mappings.max_ipfs_map_file_size = file.len() - 1;

        let err = json_round_trip(file, env_vars).await.unwrap_err();

        assert!(err.to_string().contains(" is too large"));

        env_vars = EnvVars::default();
        let values = json_round_trip(file, env_vars).await;
        assert_eq!(
            vec!["small test string that trips the size restriction"],
            values.unwrap()
        );
    }
}
