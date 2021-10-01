use crate::adapter::TendermintAdapter as TendermintAdapterTrait;
use graph::prelude::{async_trait, CheapClone, Logger};

#[derive(Clone)]
pub struct TendermintAdapter {
    logger: Logger,
    provider: String,
}

impl CheapClone for TendermintAdapter {
    fn cheap_clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            provider: self.provider.clone(),
        }
    }
}

impl TendermintAdapter {
    pub async fn new(logger: Logger, provider: String) -> Self {
        TendermintAdapter { logger, provider }
    }
}

#[async_trait]
impl TendermintAdapterTrait for TendermintAdapter {
    fn provider(&self) -> &str {
        &self.provider
    }
}
