//! Mock IPFS client for `gnd test`.
//!
//! Replaces the real `IpfsRpcClient` with a map of pre-loaded CID → bytes.
//! Any CID not found in the map is sent to the `unresolved_tx` channel and
//! an error is returned so the `OffchainMonitor` retries with backoff.
//! After sync, the runner drains the channel and reports missing CIDs.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use graph::bytes::Bytes;
use graph::ipfs::{
    ContentPath, IpfsClient, IpfsError, IpfsMetrics, IpfsRequest, IpfsResponse, IpfsResult,
};
use tokio::sync::mpsc::UnboundedSender;

pub struct MockIpfsClient {
    pub files: HashMap<ContentPath, Bytes>,
    pub metrics: IpfsMetrics,
    pub unresolved_tx: UnboundedSender<ContentPath>,
}

#[async_trait]
impl IpfsClient for MockIpfsClient {
    fn metrics(&self) -> &IpfsMetrics {
        &self.metrics
    }

    async fn call(self: Arc<Self>, req: IpfsRequest) -> IpfsResult<IpfsResponse> {
        let path = match req {
            IpfsRequest::Cat(p) | IpfsRequest::GetBlock(p) => p,
        };

        match self.files.get(&path) {
            Some(bytes) => Ok(IpfsResponse::for_test(path, bytes.clone())),
            None => {
                let _ = self.unresolved_tx.send(path.clone());
                Err(IpfsError::ContentNotAvailable {
                    path,
                    reason: anyhow::anyhow!("CID not found in mock 'files'"),
                })
            }
        }
    }
}
