use bytes::BytesMut;
use futures::{stream::poll_fn, try_ready};
use ipfs_api;
use lazy_static::lazy_static;
use lru_time_cache::LruCache;
use std::env;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use graph::prelude::{LinkResolver as LinkResolverTrait, *};
use serde_json::Value;

// Environment variable for limiting the `ipfs.map` file size limit.
const MAX_IPFS_MAP_FILE_SIZE_VAR: &'static str = "GRAPH_MAX_IPFS_MAP_FILE_SIZE";

// The default file size limit for `ipfs.map` is 256MiB.
const DEFAULT_MAX_IPFS_MAP_FILE_SIZE: u64 = 256 * 1024 * 1024;

// Environment variable for limiting the `ipfs.cat` file size limit.
const MAX_IPFS_FILE_SIZE_VAR: &'static str = "GRAPH_MAX_IPFS_FILE_BYTES";

lazy_static! {
    // The default file size limit for the IPFS cahce is 1MiB.
    static ref MAX_IPFS_CACHE_FILE_SIZE: u64 = read_u64_from_env("GRAPH_MAX_IPFS_CACHE_FILE_SIZE")
        .unwrap_or(1024 * 1024);

    // The default size limit for the IPFS cache is 50 items.
    static ref MAX_IPFS_CACHE_SIZE: u64 = read_u64_from_env("GRAPH_MAX_IPFS_CACHE_SIZE")
        .unwrap_or(50);

    // The timeout for IPFS requests in seconds
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

/// Wrap the future `fut` into another future that only resolves successfully
/// if the IPFS file at `path` is no bigger than `max_file_bytes`.
/// If `max_file_bytes` is `None`, do not restrict the size of the file
fn restrict_file_size<T>(
    client: &ipfs_api::IpfsClient,
    path: String,
    timeout: Duration,
    max_file_bytes: Option<u64>,
    fut: Box<dyn Future<Item = T, Error = failure::Error> + Send>,
) -> Box<dyn Future<Item = T, Error = failure::Error> + Send>
where
    T: Send + 'static,
{
    match max_file_bytes {
        Some(max_bytes) => Box::new(
            client
                .object_stat(&path)
                .timeout(timeout)
                .map_err(|e| failure::err_msg(e.to_string()))
                .and_then(move |stat| match stat.cumulative_size > max_bytes {
                    false => Ok(()),
                    true => Err(format_err!(
                        "IPFS file {} is too large. It can be at most {} bytes but is {} bytes",
                        path,
                        max_bytes,
                        stat.cumulative_size
                    )),
                })
                .and_then(|()| fut),
        ),
        None => fut,
    }
}

#[derive(Clone)]
pub struct LinkResolver {
    client: ipfs_api::IpfsClient,
    cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
    timeout: Duration,
    retry: bool,
}

impl From<ipfs_api::IpfsClient> for LinkResolver {
    fn from(client: ipfs_api::IpfsClient) -> Self {
        Self {
            client,
            cache: Arc::new(Mutex::new(LruCache::with_capacity(
                *MAX_IPFS_CACHE_SIZE as usize,
            ))),
            timeout: *IPFS_TIMEOUT,
            retry: false,
        }
    }
}

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
    fn cat(
        &self,
        logger: &Logger,
        link: &Link,
    ) -> Box<dyn Future<Item = Vec<u8>, Error = failure::Error> + Send> {
        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_start_matches("/ipfs/").to_owned();
        let path_for_error = path.clone();

        if let Some(data) = self.cache.lock().unwrap().get(&path) {
            trace!(logger, "IPFS cache hit"; "hash" => &path);
            return Box::new(future::ok(data.clone()));
        } else {
            trace!(logger, "IPFS cache miss"; "hash" => &path);
        }

        let client_for_cat = self.client.clone();
        let client_for_file_size = self.client.clone();
        let cache_for_writing = self.cache.clone();

        let max_file_size: Option<u64> = read_u64_from_env(MAX_IPFS_FILE_SIZE_VAR);
        let timeout_for_file_size = self.timeout.clone();

        let retry_fut = if self.retry {
            retry("ipfs.cat", &logger).no_limit()
        } else {
            retry("ipfs.cat", &logger).limit(1)
        };

        Box::new(
            retry_fut
                .timeout(self.timeout)
                .run(move || {
                    let cache_for_writing = cache_for_writing.clone();
                    let path = path.clone();

                    let cat = client_for_cat
                        .cat(&path)
                        .concat2()
                        .map(|x| x.to_vec())
                        .map_err(|e| failure::err_msg(e.to_string()));

                    restrict_file_size(
                        &client_for_file_size,
                        path.clone(),
                        timeout_for_file_size,
                        max_file_size,
                        Box::new(cat),
                    )
                    .map(move |data| {
                        // Only cache files if they are not too large
                        if data.len() <= *MAX_IPFS_CACHE_FILE_SIZE as usize {
                            let mut cache = cache_for_writing.lock().unwrap();
                            if !cache.contains_key(&path) {
                                cache.insert(path, data.clone());
                            }
                        }
                        data
                    })
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or(format_err!(
                        "ipfs.cat took too long or failed to load `{}`",
                        path_for_error,
                    ))
                }),
        )
    }

    fn json_stream(
        &self,
        link: &Link,
    ) -> Box<dyn Future<Item = JsonValueStream, Error = failure::Error> + Send + 'static> {
        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_start_matches("/ipfs/").to_owned();
        let mut stream = self.client.cat(&path).fuse();
        let mut buf = BytesMut::with_capacity(1024);
        // Count the number of lines we've already successfully deserialized.
        // We need that to adjust the line number in error messages from serde_json
        // to translate from line numbers in the snippet we are deserializing
        // to the line number in the overall file
        let mut count = 0;

        let stream: JsonValueStream = Box::new(poll_fn(
            move || -> Poll<Option<JsonStreamValue>, failure::Error> {
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
                                    Err(format_err!(
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
                        match try_ready!(stream.poll()) {
                            Some(b) => buf.extend_from_slice(&b),
                            None if buf.len() > 0 => buf.extend_from_slice(&[b'\n']),
                            None => return Ok(Async::Ready(None)),
                        }
                    }
                }
            },
        ));

        let max_file_size =
            read_u64_from_env(MAX_IPFS_MAP_FILE_SIZE_VAR).unwrap_or(DEFAULT_MAX_IPFS_MAP_FILE_SIZE);

        restrict_file_size(
            &self.client,
            path,
            self.timeout,
            Some(max_file_size),
            Box::new(future::ok(stream)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn max_file_size() {
        env::set_var(MAX_IPFS_FILE_SIZE_VAR, "200");
        let file: &[u8] = &[0u8; 201];
        let client = ipfs_api::IpfsClient::default();
        let resolver = super::LinkResolver::from(client.clone());

        let logger = Logger::root(slog::Discard, o!());

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let link = runtime.block_on(client.add(file)).unwrap().hash;
        let err = runtime
            .block_on(LinkResolver::cat(
                &resolver,
                &logger,
                &Link { link: link.clone() },
            ))
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

    fn json_round_trip(text: &'static str) -> Result<Vec<Value>, failure::Error> {
        let client = ipfs_api::IpfsClient::default();
        let resolver = super::LinkResolver::from(client.clone());

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let link = runtime.block_on(client.add(text.as_bytes())).unwrap().hash;
        runtime.block_on(
            LinkResolver::json_stream(&resolver, &Link { link: link.clone() })
                .and_then(|stream| stream.map(|sv| sv.value).collect()),
        )
    }

    #[test]
    fn read_json_stream() {
        let values = json_round_trip("\"with newline\"\n");
        assert_eq!(vec![json!("with newline")], values.unwrap());

        let values = json_round_trip("\"without newline\"");
        assert_eq!(vec![json!("without newline")], values.unwrap());

        let values = json_round_trip("\"two\" \n \"things\"");
        assert_eq!(vec![json!("two"), json!("things")], values.unwrap());

        let values = json_round_trip("\"one\"\n  \"two\" \n [\"bad\" \n \"split\"]");
        assert_eq!(
            "EOF while parsing a list at line 4 column 0: ' [\"bad\" \n'",
            values.unwrap_err().to_string()
        );
    }

    #[test]
    fn ipfs_map_file_size() {
        let file = "\"small test string that trips the size restriction\"";
        env::set_var(MAX_IPFS_MAP_FILE_SIZE_VAR, (file.len() - 1).to_string());

        let err = json_round_trip(file).unwrap_err();
        env::remove_var(MAX_IPFS_MAP_FILE_SIZE_VAR);

        assert!(err.to_string().contains(" is too large"));

        let values = json_round_trip(file);
        assert_eq!(
            vec!["small test string that trips the size restriction"],
            values.unwrap()
        );
    }
}
