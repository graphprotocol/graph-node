use crate::adapter::NearAdapter as NearAdapterTrait;
use graph::prelude::{async_trait, CheapClone, Logger};

#[derive(Clone)]
pub struct NearAdapter {
    logger: Logger,
    provider: String,
}

impl CheapClone for NearAdapter {
    fn cheap_clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            provider: self.provider.clone(),
        }
    }
}

impl NearAdapter {
    pub async fn new(logger: Logger, provider: String) -> Self {
        NearAdapter { logger, provider }
    }
}

#[async_trait]
impl NearAdapterTrait for NearAdapter {
    fn provider(&self) -> &str {
        &self.provider
    }
}
