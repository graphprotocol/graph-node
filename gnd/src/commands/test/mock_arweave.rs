//! Mock Arweave resolver for `gnd test`.
//!
//! Replaces the real `ArweaveClient` with a map of pre-loaded txId → bytes.
//! Any txId not found in the map is sent to the `unresolved_tx` channel and
//! `ServerUnavailable` is returned, which causes the `PollingMonitor` to retry
//! with backoff. After sync, the runner drains the channel and reports missing
//! tx IDs as a clear test failure.

use std::collections::HashMap;

use async_trait::async_trait;
use graph::bytes::Bytes;
use graph::components::link_resolver::{ArweaveClientError, ArweaveResolver, FileSizeLimit};
use graph::data_source::offchain::Base64;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub struct MockArweaveResolver {
    pub files: HashMap<String, Bytes>,
    pub unresolved_tx: UnboundedSender<String>,
}

#[async_trait]
impl ArweaveResolver for MockArweaveResolver {
    async fn get(&self, file: &Base64) -> Result<Vec<u8>, ArweaveClientError> {
        self.get_with_limit(file, &FileSizeLimit::Unlimited).await
    }

    async fn get_with_limit(
        &self,
        file: &Base64,
        _limit: &FileSizeLimit,
    ) -> Result<Vec<u8>, ArweaveClientError> {
        match self.files.get(file.as_str()) {
            Some(bytes) => Ok(bytes.to_vec()),
            None => {
                let _ = self.unresolved_tx.send(file.as_str().to_owned());
                Err(ArweaveClientError::ServerUnavailable(format!(
                    "txId '{}' not found in mock 'arweaveFiles'",
                    file.as_str()
                )))
            }
        }
    }
}
