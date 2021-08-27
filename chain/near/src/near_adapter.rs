use crate::{
    adapter::{NearAdapter as NearAdapterTrait, NearBlockFilter},
    trigger::NearTrigger,
};
use graph::{
    components::near::NearBlock,
    prelude::{async_trait, CheapClone, Logger},
};

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

pub(crate) fn parse_block_triggers(
    block_filter: NearBlockFilter,
    block: &NearBlock,
) -> Vec<NearTrigger> {
    // FIXME (NEAR): Re-enable when compilation final work
    vec![]
    // let block_ptr = BlockPtr::from(&block.ethereum_block);
    // let trigger_every_block = block_filter.trigger_every_block;
    // let call_filter = EthereumCallFilter::from(block_filter);
    // let block_ptr2 = block_ptr.cheap_clone();
    // let mut triggers = match &block.calls {
    //     Some(calls) => calls
    //         .iter()
    //         .filter(move |call| call_filter.matches(call))
    //         .map(move |call| {
    //             NearTrigger::Block(
    //                 block_ptr2.clone(),
    //                 NearBlockTriggerType::WithCallTo(call.to),
    //             )
    //         })
    //         .collect::<Vec<NearTrigger>>(),
    //     None => vec![],
    // };
    // if trigger_every_block {
    //     triggers.push(NearTrigger::Block(
    //         block_ptr,
    //         NearBlockTriggerType::Every,
    //     ));
    // }
    // triggers
}
