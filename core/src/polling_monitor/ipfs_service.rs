use anyhow::{anyhow, Error};
use bytes::Bytes;
use cid::Cid;
use futures::{Future, FutureExt};
use graph::{
    cheap_clone::CheapClone,
    ipfs_client::{IpfsClient, StatApi},
    tokio::sync::Semaphore,
};
use std::{pin::Pin, sync::Arc, task::Poll, time::Duration};
use tower::Service;

const CLOUDFLARE_TIMEOUT: u16 = 524;
const GATEWAY_TIMEOUT: u16 = 504;

#[derive(Clone)]
pub struct IpfsService {
    client: IpfsClient,
    max_file_size: u64,
    timeout: Duration,
    concurrency_limiter: Arc<Semaphore>,
}

impl CheapClone for IpfsService {
    fn cheap_clone(&self) -> Self {
        Self {
            client: self.client.cheap_clone(),
            max_file_size: self.max_file_size,
            timeout: self.timeout,
            concurrency_limiter: self.concurrency_limiter.cheap_clone(),
        }
    }
}

impl IpfsService {
    #[allow(dead_code)]
    pub fn new(
        client: IpfsClient,
        max_file_size: u64,
        timeout: Duration,
        concurrency_limit: u16,
    ) -> Self {
        Self {
            client,
            max_file_size,
            timeout,
            concurrency_limiter: Arc::new(Semaphore::new(concurrency_limit as usize)),
        }
    }

    async fn call(&self, cid: Cid) -> Result<Option<Bytes>, Error> {
        let cid_str = cid.to_string();
        let size = match self
            .client
            .stat_size(StatApi::Files, cid_str, self.timeout)
            .await
        {
            Ok(size) => size,
            Err(e) => match e.status().map(|e| e.as_u16()) {
                Some(GATEWAY_TIMEOUT) | Some(CLOUDFLARE_TIMEOUT) => return Ok(None),
                _ if e.is_timeout() => return Ok(None),
                _ => return Err(e.into()),
            },
        };

        if size > self.max_file_size {
            return Err(anyhow!(
                "IPFS file {} is too large. It can be at most {} bytes but is {} bytes",
                cid.to_string(),
                self.max_file_size,
                size
            ));
        }

        Ok(self
            .client
            .cat_all(&cid.to_string(), self.timeout)
            .await
            .map(Some)?)
    }
}

impl Service<Cid> for IpfsService {
    type Response = (Cid, Option<Bytes>);
    type Error = (Cid, Error);
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The permit is acquired and immediately dropped, as tower does not yet allow returning it.
        // So this is only indicative of capacity being available.
        Pin::new(&mut self.concurrency_limiter.acquire().boxed())
            .poll(cx)
            .map_ok(|_| ())
            .map_err(|_| unreachable!("semaphore is never closed"))
    }

    fn call(&mut self, cid: Cid) -> Self::Future {
        let this = self.cheap_clone();
        async move {
            let _permit = this.concurrency_limiter.acquire().await;
            this.call(cid).await.map(|x| (cid, x)).map_err(|e| (cid, e))
        }
        .boxed()
    }
}
