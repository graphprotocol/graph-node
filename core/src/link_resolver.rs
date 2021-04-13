use std::env;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BytesMut;
use futures01::{stream::poll_fn, try_ready};
use futures03::stream::FuturesUnordered;
use lazy_static::lazy_static;
use lru_time_cache::LruCache;
use serde_json::Value;

use graph::{
    ipfs_client::{IpfsClient, ObjectStatResponse},
    prelude::{LinkResolver as LinkResolverTrait, *},
};

/// Environment variable for limiting the `ipfs.map` file size limit.
const MAX_IPFS_MAP_FILE_SIZE_VAR: &'static str = "GRAPH_MAX_IPFS_MAP_FILE_SIZE";

/// The default file size limit for `ipfs.map` is 256MiB.
const DEFAULT_MAX_IPFS_MAP_FILE_SIZE: u64 = 256 * 1024 * 1024;

/// Environment variable for limiting the `ipfs.cat` file size limit.
const MAX_IPFS_FILE_SIZE_VAR: &'static str = "GRAPH_MAX_IPFS_FILE_BYTES";

lazy_static! {
    /// The default file size limit for the IPFS cache is 1MiB.
    static ref MAX_IPFS_CACHE_FILE_SIZE: u64 = read_u64_from_env("GRAPH_MAX_IPFS_CACHE_FILE_SIZE")
        .unwrap_or(1024 * 1024);

    /// The default size limit for the IPFS cache is 50 items.
    static ref MAX_IPFS_CACHE_SIZE: u64 = read_u64_from_env("GRAPH_MAX_IPFS_CACHE_SIZE")
        .unwrap_or(50);

    /// The timeout for IPFS requests in seconds
    static ref IPFS_TIMEOUT: Duration = Duration::from_secs(
        read_u64_from_env("GRAPH_IPFS_TIMEOUT").unwrap_or(60)
    );
}

fn read_u64_from_env(name: &str) -> Option<u64> {
    env::var(name).ok().map(|s| {
        u64::from_str(&s).unwrap_or_else(|_| {
            panic!(
                "expected env var {} to contain a number (unsigned 64-bit integer), but got '{}'",
                name, s
            )
        })
    })
}

/// The IPFS APIs don't have a quick "do you have the file" function. Instead, we
/// just rely on whether an API times out. That makes sense for IPFS, but not for
/// our application. We want to be able to quickly select from a potential list
/// of clients where hopefully one already has the file, and just get the file
/// from that.
///
/// The strategy here then is to use the object_stat API as a proxy for "do you
/// have the file". Whichever client has or gets the file first wins. This API is
/// a good choice, because it doesn't involve us actually starting to download
/// the file from each client, which would be wasteful of bandwidth and memory in
/// the case multiple clients respond in a timely manner. In addition, we may
/// make good use of the stat returned.
async fn select_fastest_client_with_stat(
    clients: Arc<Vec<Arc<IpfsClient>>>,
    logger: Logger,
    path: String,
    timeout: Duration,
    do_retry: bool,
) -> Result<(ObjectStatResponse, Arc<IpfsClient>), Error> {
    let mut err: Option<Error> = None;

    let mut stats: FuturesUnordered<_> = clients
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let c = c.cheap_clone();
            let retry_fut = if do_retry {
                retry("object.stat", &logger).no_limit()
            } else {
                retry("object.stat", &logger).limit(1)
            }
            .timeout(timeout);

            let path = path.clone();
            retry_fut
                .run(move || {
                    let path = path.clone();
                    let c = c.cheap_clone();
                    async move { c.object_stat(path).map_ok(move |s| (s, i)).await }
                        .boxed()
                        .compat()
                })
                .compat()
        })
        .collect();

    while let Some(result) = stats.next().await {
        match result {
            Ok((stat, index)) => {
                return Ok((stat, clients[index].cheap_clone()));
            }
            Err(e) => err = Some(e.into()),
        }
    }

    Err(err.unwrap_or_else(|| {
        anyhow!(
            "No IPFS clients were supplied to handle the call to object.stat. File: {}",
            path
        )
    }))
}

// Returns an error if the stat is bigger than `max_file_bytes`
fn restrict_file_size(
    path: &str,
    stat: &ObjectStatResponse,
    max_file_bytes: &Option<u64>,
) -> Result<(), Error> {
    if let Some(max_file_bytes) = max_file_bytes {
        if stat.cumulative_size > *max_file_bytes {
            return Err(anyhow!(
                "IPFS file {} is too large. It can be at most {} bytes but is {} bytes",
                path,
                max_file_bytes,
                stat.cumulative_size
            ));
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct LinkResolver {
    clients: Arc<Vec<Arc<IpfsClient>>>,
    cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    timeout: Duration,
    retry: bool,
}

impl CheapClone for LinkResolver {
    fn cheap_clone(&self) -> Self {
        LinkResolver {
            clients: self.clients.cheap_clone(),
            cache: self.cache.cheap_clone(),
            timeout: self.timeout,
            retry: self.retry,
        }
    }
}

impl From<IpfsClient> for LinkResolver {
    fn from(client: IpfsClient) -> Self {
        vec![client].into()
    }
}

impl From<Vec<IpfsClient>> for LinkResolver {
    fn from(clients: Vec<IpfsClient>) -> Self {
        Self {
            clients: Arc::new(clients.into_iter().map(Arc::new).collect()),
            cache: Arc::new(Mutex::new(LruCache::with_capacity(
                *MAX_IPFS_CACHE_SIZE as usize,
            ))),
            timeout: *IPFS_TIMEOUT,
            retry: false,
        }
    }
}

#[async_trait]
impl LinkResolverTrait for LinkResolver {
    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn with_retries(mut self) -> Self {
        self.retry = true;
        self
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

        let (stat, client) = select_fastest_client_with_stat(
            self.clients.cheap_clone(),
            logger.cheap_clone(),
            path.clone(),
            self.timeout,
            self.retry,
        )
        .await?;

        // FIXME: Having an env variable here is a problem for consensus.
        // Index Nodes should not disagree on whether the file should be read.
        let max_file_size: Option<u64> = read_u64_from_env(MAX_IPFS_FILE_SIZE_VAR);
        restrict_file_size(&path, &stat, &max_file_size)?;

        let path = path.clone();
        let retry_fut = if self.retry {
            retry("ipfs.cat", &logger).no_limit()
        } else {
            retry("ipfs.cat", &logger).limit(1)
        }
        .timeout(self.timeout);

        let this = self.clone();
        let logger = logger.clone();
        let data = retry_fut
            .run(move || {
                let path = path.clone();
                let client = client.clone();
                let this = this.clone();
                let logger = logger.clone();
                async move {
                    let data = client
                        .cat_all(path.clone())
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e))?
                        .to_vec();

                    // Only cache files if they are not too large
                    if data.len() <= *MAX_IPFS_CACHE_FILE_SIZE as usize {
                        let mut cache = this.cache.lock().unwrap();
                        if !cache.contains_key(&path) {
                            cache.insert(path.to_owned(), data.clone());
                        }
                    } else {
                        debug!(logger, "File too large for cache";
                                    "path" => path,
                                    "size" => data.len()
                        );
                    }
                    Result::<Vec<u8>, Error>::Ok(data)
                }
                .boxed()
                .compat()
            })
            .compat()
            .await?;

        Ok(data)
    }

    async fn json_stream(&self, logger: &Logger, link: &Link) -> Result<JsonValueStream, Error> {
        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_start_matches("/ipfs/");

        let (stat, client) = select_fastest_client_with_stat(
            self.clients.cheap_clone(),
            logger.cheap_clone(),
            path.to_string(),
            self.timeout,
            self.retry,
        )
        .await?;

        let max_file_size =
            read_u64_from_env(MAX_IPFS_MAP_FILE_SIZE_VAR).or(Some(DEFAULT_MAX_IPFS_MAP_FILE_SIZE));
        restrict_file_size(path, &stat, &max_file_size)?;

        let mut stream = client.cat(path.to_string()).await?.fuse().boxed().compat();

        let mut buf = BytesMut::with_capacity(1024);

        // Count the number of lines we've already successfully deserialized.
        // We need that to adjust the line number in error messages from serde_json
        // to translate from line numbers in the snippet we are deserializing
        // to the line number in the overall file
        let mut count = 0;

        let stream: JsonValueStream = Box::pin(
            poll_fn(move || -> Poll<Option<JsonStreamValue>, Error> {
                loop {
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
                            None if buf.len() > 0 => buf.extend_from_slice(&[b'\n']),
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
    use serde_json::json;

    #[tokio::test]
    async fn max_file_size() {
        env::set_var(MAX_IPFS_FILE_SIZE_VAR, "200");
        let file: &[u8] = &[0u8; 201];
        let client = IpfsClient::localhost();
        let resolver = super::LinkResolver::from(client.clone());

        let logger = Logger::root(slog::Discard, o!());

        let link = client.add(file.into()).await.unwrap().hash;
        let err = LinkResolver::cat(&resolver, &logger, &Link { link: link.clone() })
            .await
            .unwrap_err();
        env::remove_var(MAX_IPFS_FILE_SIZE_VAR);
        assert_eq!(
            err.to_string(),
            format!(
                "IPFS file {} is too large. It can be at most 200 bytes but is 212 bytes",
                link
            )
        );
    }

    async fn json_round_trip(text: &'static str) -> Result<Vec<Value>, Error> {
        let client = IpfsClient::localhost();
        let resolver = super::LinkResolver::from(client.clone());

        let logger = Logger::root(slog::Discard, o!());
        let link = client.add(text.as_bytes().into()).await.unwrap().hash;

        let stream = LinkResolver::json_stream(&resolver, &logger, &Link { link }).await?;
        stream.map_ok(|sv| sv.value).try_collect().await
    }

    #[tokio::test]
    async fn read_json_stream() {
        let values = json_round_trip("\"with newline\"\n").await;
        assert_eq!(vec![json!("with newline")], values.unwrap());

        let values = json_round_trip("\"without newline\"").await;
        assert_eq!(vec![json!("without newline")], values.unwrap());

        let values = json_round_trip("\"two\" \n \"things\"").await;
        assert_eq!(vec![json!("two"), json!("things")], values.unwrap());

        let values = json_round_trip("\"one\"\n  \"two\" \n [\"bad\" \n \"split\"]").await;
        assert_eq!(
            "EOF while parsing a list at line 4 column 0: ' [\"bad\" \n'",
            values.unwrap_err().to_string()
        );
    }

    #[tokio::test]
    async fn ipfs_map_file_size() {
        let file = "\"small test string that trips the size restriction\"";
        env::set_var(MAX_IPFS_MAP_FILE_SIZE_VAR, (file.len() - 1).to_string());

        let err = json_round_trip(file).await.unwrap_err();
        env::remove_var(MAX_IPFS_MAP_FILE_SIZE_VAR);

        assert!(err.to_string().contains(" is too large"));

        let values = json_round_trip(file).await;
        assert_eq!(
            vec!["small test string that trips the size restriction"],
            values.unwrap()
        );
    }
}
