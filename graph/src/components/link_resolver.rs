use crate::data::subgraph::Link;
use bytes::BytesMut;
use failure;
use futures::{stream::poll_fn, try_ready};
use ipfs_api;
use serde_json::Value;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tokio::prelude::*;

const MAX_IPFS_FILE_BYTES_ENV_VAR: &str = "GRAPH_MAX_IPFS_FILE_BYTES";

const MAX_IPFS_MAP_FILE_SIZE_ENV_VAR: &str = "GRAPH_MAX_IPFS_MAP_FILE_SIZE";

// Default size limitation for streaming files through ipfs_map is
// 256MB
const MAX_IPFS_MAP_FILE_SIZE: u64 = 256 * 1024 * 1024;

type ValueStream = Box<Stream<Item = Value, Error = failure::Error> + Send + 'static>;

/// Resolves links to subgraph manifests and resources referenced by them.
pub trait LinkResolver: Send + Sync + 'static {
    /// Fetches the link contents as bytes.
    fn cat(&self, link: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send>;

    /// Read the contents of `link` and deserialize them into a stream of JSON
    /// values. The values must each be on a single line; newlines are significant
    /// as they are used to split the file contents and each line is deserialized
    /// separately.
    fn json_stream(
        &self,
        link: &Link,
    ) -> Box<Future<Item = ValueStream, Error = failure::Error> + Send + 'static>;
}

// The timeout for IPFS requests in seconds
fn ipfs_timeout() -> Duration {
    let timeout = env::var("GRAPH_IPFS_TIMEOUT").ok().map(|s| {
        u64::from_str(&s).unwrap_or_else(|_| panic!("failed to parse env var GRAPH_IPFS_TIMEOUT"))
    });
    Duration::from_secs(timeout.unwrap_or(30))
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
    max_file_bytes: Option<u64>,
    fut: Box<Future<Item = T, Error = failure::Error> + Send>,
) -> Box<Future<Item = T, Error = failure::Error> + Send>
where
    T: Send + 'static,
{
    match max_file_bytes {
        Some(max_bytes) => Box::new(
            client
                .object_stat(&path)
                .timeout(ipfs_timeout())
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

impl LinkResolver for ipfs_api::IpfsClient {
    /// Supports links of the form `/ipfs/ipfs_hash` or just `ipfs_hash`.
    fn cat(&self, link: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send> {
        // Grab env vars.
        let max_file_bytes = read_u64_env_var(MAX_IPFS_FILE_BYTES_ENV_VAR);

        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_start_matches("/ipfs/").to_owned();

        let ipfs_timeout = ipfs_timeout();
        let cat = self
            .cat(&path)
            .concat2()
            .timeout(ipfs_timeout)
            .map(|x| x.to_vec())
            .map_err(|e| failure::err_msg(e.to_string()));

        restrict_file_size(&self, path, max_file_bytes, Box::new(cat))
    }

    fn json_stream(
        &self,
        link: &Link,
    ) -> Box<Future<Item = ValueStream, Error = failure::Error> + Send + 'static> {
        // Discard the `/ipfs/` prefix (if present) to get the hash.
        let path = link.link.trim_start_matches("/ipfs/").to_owned();
        let mut stream = self.cat(&path).fuse();
        let mut buf = BytesMut::with_capacity(1024);
        // Count the number of lines we've already successfully deserialized.
        // We need that to adjust the line number in error messages from serde_json
        // to translate from line numbers in the snippet we are deserializing
        // to the line number in the overall file
        let mut count = 0;

        let stream: ValueStream =
            Box::new(poll_fn(move || -> Poll<Option<Value>, failure::Error> {
                loop {
                    if let Some(offset) = buf.iter().position(|b| *b == b'\n') {
                        let line_bytes = buf.split_to(offset + 1);
                        count += 1;
                        if line_bytes.len() > 1 {
                            let line = std::str::from_utf8(&line_bytes)?;
                            let res = match serde_json::from_str::<Value>(line) {
                                Ok(v) => Ok(Async::Ready(Some(v))),
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
            }));
        // Check the size of the file
        let max_file_bytes =
            read_u64_from_env(MAX_IPFS_MAP_FILE_SIZE_ENV_VAR).unwrap_or(MAX_IPFS_MAP_FILE_SIZE);

        restrict_file_size(
            &self,
            path,
            Some(max_file_bytes),
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
        env::set_var(MAX_IPFS_FILE_BYTES_ENV_VAR, "200");
        let file: &[u8] = &[0u8; 201];
        let client = ipfs_api::IpfsClient::default();

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let link = runtime.block_on(client.add(file)).unwrap().hash;
        let err = runtime
            .block_on(LinkResolver::cat(&client, &Link { link: link.clone() }))
            .unwrap_err();
        env::remove_var(MAX_IPFS_FILE_BYTES_ENV_VAR);
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

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let link = runtime.block_on(client.add(text.as_bytes())).unwrap().hash;
        runtime.block_on(
            LinkResolver::json_stream(&client, &Link { link: link.clone() })
                .and_then(|stream| stream.collect()),
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
        env::set_var(MAX_IPFS_MAP_FILE_SIZE_ENV_VAR, (file.len() - 1).to_string());

        let err = json_round_trip(file).unwrap_err();
        env::remove_var(MAX_IPFS_MAP_FILE_SIZE_ENV_VAR);

        assert!(err.to_string().contains(" is too large"));

        let values = json_round_trip(file);
        assert_eq!(
            vec!["small test string that trips the size restriction"],
            values.unwrap()
        );
    }
}
