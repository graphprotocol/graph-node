use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::BytesMut;
use derivative::Derivative;
use futures03::compat::Stream01CompatExt;
use futures03::stream::StreamExt;
use futures03::stream::TryStreamExt;
use lru_time_cache::LruCache;
use serde_json::Value;

use crate::derive::CheapClone;
use crate::env::EnvVars;
use crate::futures01::stream::poll_fn;
use crate::futures01::stream::Stream;
use crate::futures01::try_ready;
use crate::futures01::Async;
use crate::futures01::Poll;
use crate::ipfs::ContentPath;
use crate::ipfs::IpfsClient;
use crate::prelude::{LinkResolver as LinkResolverTrait, *};

#[derive(Clone, CheapClone, Derivative)]
#[derivative(Debug)]
pub struct IpfsResolver {
    #[derivative(Debug = "ignore")]
    client: Arc<dyn IpfsClient>,

    #[derivative(Debug = "ignore")]
    cache: Arc<Mutex<LruCache<ContentPath, Vec<u8>>>>,

    timeout: Duration,
    max_file_size: usize,
    max_map_file_size: usize,
    max_cache_file_size: usize,
}

impl IpfsResolver {
    pub fn new(client: Arc<dyn IpfsClient>, env_vars: Arc<EnvVars>) -> Self {
        let env = &env_vars.mappings;

        Self {
            client,
            cache: Arc::new(Mutex::new(LruCache::with_capacity(
                env.max_ipfs_cache_size as usize,
            ))),
            timeout: env.ipfs_timeout,
            max_file_size: env.max_ipfs_file_bytes,
            max_map_file_size: env.max_ipfs_map_file_size,
            max_cache_file_size: env.max_ipfs_cache_file_size,
        }
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
        // IPFS clients have internal retries enabled by default.
        Box::new(self.cheap_clone())
    }

    async fn cat(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error> {
        let path = ContentPath::new(&link.link)?;
        let timeout = self.timeout;
        let max_file_size = self.max_file_size;
        let max_cache_file_size = self.max_cache_file_size;

        if let Some(data) = self.cache.lock().unwrap().get(&path) {
            trace!(logger, "IPFS cat cache hit"; "hash" => path.to_string());
            return Ok(data.to_owned());
        }

        trace!(logger, "IPFS cat cache miss"; "hash" => path.to_string());

        let data = self
            .client
            .cat(&path, max_file_size, Some(timeout))
            .await?
            .to_vec();

        if data.len() <= max_cache_file_size {
            let mut cache = self.cache.lock().unwrap();

            if !cache.contains_key(&path) {
                cache.insert(path.clone(), data.clone());
            }
        } else {
            debug!(
                logger,
                "IPFS file too large for cache";
                "path" => path.to_string(),
                "size" => data.len(),
            );
        }

        Ok(data)
    }

    async fn get_block(&self, logger: &Logger, link: &Link) -> Result<Vec<u8>, Error> {
        let path = ContentPath::new(&link.link)?;
        let timeout = self.timeout;

        trace!(logger, "IPFS block get"; "hash" => path.to_string());

        let data = self.client.get_block(&path, Some(timeout)).await?.to_vec();

        Ok(data)
    }

    async fn json_stream(&self, logger: &Logger, link: &Link) -> Result<JsonValueStream, Error> {
        let path = ContentPath::new(&link.link)?;
        let max_map_file_size = self.max_map_file_size;

        trace!(logger, "IPFS JSON stream"; "hash" => path.to_string());

        let mut stream = self
            .client
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

        let mut cumulative_file_size = 0;

        let stream: JsonValueStream = Box::pin(
            poll_fn(move || -> Poll<Option<JsonStreamValue>, Error> {
                loop {
                    cumulative_file_size += buf.len();

                    if cumulative_file_size > max_map_file_size {
                        return Err(anyhow!(
                            "IPFS file {} is too large. It can be at most {} bytes",
                            path,
                            max_map_file_size,
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
    use serde_json::json;

    use super::*;
    use crate::env::EnvVars;
    use crate::ipfs::test_utils::add_files_to_local_ipfs_node_for_testing;
    use crate::ipfs::IpfsRpcClient;
    use crate::ipfs::ServerAddress;

    #[tokio::test]
    async fn max_file_size() {
        let mut env_vars = EnvVars::default();
        env_vars.mappings.max_ipfs_file_bytes = 200;

        let file: &[u8] = &[0u8; 201];

        let cid = add_files_to_local_ipfs_node_for_testing([file.to_vec()])
            .await
            .unwrap()[0]
            .hash
            .to_owned();

        let logger = crate::log::discard();

        let client = IpfsRpcClient::new_unchecked(ServerAddress::local_rpc_api(), &logger)
            .unwrap()
            .into_boxed();

        let resolver = IpfsResolver::new(client.into(), Arc::new(env_vars));

        let err = IpfsResolver::cat(&resolver, &logger, &Link { link: cid.clone() })
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            format!("IPFS content from '{cid}' exceeds the 200 bytes limit")
        );
    }

    async fn json_round_trip(text: &'static str, env_vars: EnvVars) -> Result<Vec<Value>, Error> {
        let cid = add_files_to_local_ipfs_node_for_testing([text.as_bytes().to_vec()]).await?[0]
            .hash
            .to_owned();

        let logger = crate::log::discard();
        let client =
            IpfsRpcClient::new_unchecked(ServerAddress::local_rpc_api(), &logger)?.into_boxed();
        let resolver = IpfsResolver::new(client.into(), Arc::new(env_vars));

        let stream = IpfsResolver::json_stream(&resolver, &logger, &Link { link: cid }).await?;
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
