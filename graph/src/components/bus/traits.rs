use super::err::BusError;
use crate::prelude::Logger;
use crate::tokio::sync::mpsc::UnboundedReceiver;
use async_trait::async_trait;

pub struct BusMessage {
    pub subgraph_id: String,
    pub value: Vec<String>,
}

#[async_trait]
pub trait Bus: Send + Sync + 'static {
    async fn new(connection_uri: String, logger: Logger) -> Self;

    fn get_name(&self) -> &str;

    async fn send_plain_text(&self, value: BusMessage) -> Result<(), BusError>;

    async fn start(&self, mut receiver: UnboundedReceiver<BusMessage>) -> ();
}
