use async_trait::async_trait;
use bytes::Bytes;
use failure::Error;

#[async_trait]
pub trait ArweaveAdapter: Send + Sync {
    async fn tx_data(&self, tx_id: &str) -> Result<Bytes, Error>;
}
