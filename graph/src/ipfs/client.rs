use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use futures03::stream::BoxStream;
use futures03::StreamExt;
use futures03::TryStreamExt;
use slog::Logger;

use crate::cheap_clone::CheapClone as _;
use crate::data::subgraph::DeploymentHash;
use crate::derive::CheapClone;
use crate::ipfs::{ContentPath, IpfsError, IpfsMetrics, IpfsResult, RetryPolicy};

/// A read-only connection to an IPFS server.
#[async_trait]
pub trait IpfsClient: Send + Sync + 'static {
    /// Returns the metrics associated with the IPFS client.
    fn metrics(&self) -> &IpfsMetrics;

    /// Sends a request to the IPFS server and returns a raw response.
    async fn call(self: Arc<Self>, req: IpfsRequest) -> IpfsResult<IpfsResponse>;

    /// Streams data from the specified content path.
    ///
    /// If a timeout is specified, the execution will be aborted if the IPFS server
    /// does not return a response within the specified amount of time.
    ///
    /// The timeout is not propagated to the resulting stream.
    async fn cat_stream(
        self: Arc<Self>,
        ctx: &IpfsContext,
        path: &ContentPath,
        timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> IpfsResult<BoxStream<'static, IpfsResult<Bytes>>> {
        let fut = retry_policy
            .create("IPFS.cat_stream", &ctx.logger(path))
            .no_timeout()
            .run({
                let path = path.cheap_clone();
                let deployment_hash = ctx.deployment_hash();

                move || {
                    let client = self.cheap_clone();
                    let metrics = self.metrics().cheap_clone();
                    let deployment_hash = deployment_hash.cheap_clone();
                    let path = path.cheap_clone();

                    async move {
                        run_with_metrics(
                            client.call(IpfsRequest::Cat(path)),
                            deployment_hash,
                            metrics,
                        )
                        .await
                    }
                }
            });

        let resp = run_with_optional_timeout(path, fut, timeout).await?;

        Ok(resp.bytes_stream())
    }

    /// Downloads data from the specified content path.
    ///
    /// If a timeout is specified, the execution will be aborted if the IPFS server
    /// does not return a response within the specified amount of time.
    async fn cat(
        self: Arc<Self>,
        ctx: &IpfsContext,
        path: &ContentPath,
        max_size: usize,
        timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> IpfsResult<Bytes> {
        let fut = retry_policy
            .create("IPFS.cat", &ctx.logger(path))
            .no_timeout()
            .run({
                let path = path.cheap_clone();
                let deployment_hash = ctx.deployment_hash();

                move || {
                    let client = self.cheap_clone();
                    let metrics = self.metrics().cheap_clone();
                    let deployment_hash = deployment_hash.cheap_clone();
                    let path = path.cheap_clone();

                    async move {
                        run_with_metrics(
                            client.call(IpfsRequest::Cat(path)),
                            deployment_hash,
                            metrics,
                        )
                        .await?
                        .bytes(Some(max_size))
                        .await
                    }
                }
            });

        run_with_optional_timeout(path, fut, timeout).await
    }

    /// Downloads an IPFS block in raw format.
    ///
    /// If a timeout is specified, the execution will be aborted if the IPFS server
    /// does not return a response within the specified amount of time.
    async fn get_block(
        self: Arc<Self>,
        ctx: &IpfsContext,
        path: &ContentPath,
        timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> IpfsResult<Bytes> {
        let fut = retry_policy
            .create("IPFS.get_block", &ctx.logger(path))
            .no_timeout()
            .run({
                let path = path.cheap_clone();
                let deployment_hash = ctx.deployment_hash();

                move || {
                    let client = self.cheap_clone();
                    let metrics = self.metrics().cheap_clone();
                    let deployment_hash = deployment_hash.cheap_clone();
                    let path = path.cheap_clone();

                    async move {
                        run_with_metrics(
                            client.call(IpfsRequest::GetBlock(path)),
                            deployment_hash,
                            metrics,
                        )
                        .await?
                        .bytes(None)
                        .await
                    }
                }
            });

        run_with_optional_timeout(path, fut, timeout).await
    }
}

#[derive(Clone, Debug, CheapClone)]
pub struct IpfsContext {
    pub deployment_hash: Arc<str>,
    pub logger: Logger,
}

impl IpfsContext {
    pub fn new(deployment_hash: &DeploymentHash, logger: &Logger) -> Self {
        Self {
            deployment_hash: deployment_hash.as_str().into(),
            logger: logger.cheap_clone(),
        }
    }

    pub(super) fn deployment_hash(&self) -> Arc<str> {
        self.deployment_hash.cheap_clone()
    }

    pub(super) fn logger(&self, path: &ContentPath) -> Logger {
        self.logger.new(
            slog::o!("deployment" => self.deployment_hash.to_string(), "path" => path.to_string()),
        )
    }
}

#[cfg(debug_assertions)]
impl Default for IpfsContext {
    fn default() -> Self {
        Self {
            deployment_hash: "test".into(),
            logger: crate::log::discard(),
        }
    }
}

/// Describes a request to an IPFS server.
#[derive(Clone, Debug)]
pub enum IpfsRequest {
    Cat(ContentPath),
    GetBlock(ContentPath),
}

/// Contains a raw, successful IPFS response.
#[derive(Debug)]
pub struct IpfsResponse {
    pub(super) path: ContentPath,
    pub(super) response: reqwest::Response,
}

impl IpfsResponse {
    /// Reads and returns the response body.
    ///
    /// If the max size is specified and the response body is larger than the max size,
    /// execution will result in an error.
    pub async fn bytes(self, max_size: Option<usize>) -> IpfsResult<Bytes> {
        let Some(max_size) = max_size else {
            return self.response.bytes().await.map_err(Into::into);
        };

        let bytes = self
            .response
            .bytes_stream()
            .err_into()
            .try_fold(BytesMut::new(), |mut acc, chunk| async {
                acc.extend(chunk);

                if acc.len() > max_size {
                    return Err(IpfsError::ContentTooLarge {
                        path: self.path.clone(),
                        max_size,
                    });
                }

                Ok(acc)
            })
            .await?;

        Ok(bytes.into())
    }

    /// Converts the response into a stream of bytes from the body.
    pub fn bytes_stream(self) -> BoxStream<'static, IpfsResult<Bytes>> {
        self.response.bytes_stream().err_into().boxed()
    }
}

async fn run_with_optional_timeout<F, O>(
    path: &ContentPath,
    fut: F,
    timeout: Option<Duration>,
) -> IpfsResult<O>
where
    F: Future<Output = IpfsResult<O>>,
{
    match timeout {
        Some(timeout) => {
            tokio::time::timeout(timeout, fut)
                .await
                .map_err(|_| IpfsError::RequestTimeout {
                    path: path.to_owned(),
                })?
        }
        None => fut.await,
    }
}

async fn run_with_metrics<F, O>(
    fut: F,
    deployment_hash: Arc<str>,
    metrics: IpfsMetrics,
) -> IpfsResult<O>
where
    F: Future<Output = IpfsResult<O>>,
{
    let timer = Instant::now();
    metrics.add_request(&deployment_hash);

    fut.await
        .inspect(|_resp| {
            metrics.observe_request_duration(&deployment_hash, timer.elapsed().as_secs_f64())
        })
        .inspect_err(|err| {
            if err.is_timeout() {
                metrics.add_not_found(&deployment_hash)
            } else {
                metrics.add_error(&deployment_hash)
            }
        })
}
