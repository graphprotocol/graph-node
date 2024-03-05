use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use graph::{
    components::store::EthereumCallCache,
    prelude::{ethabi, BlockPtr, CachedEthereumCall},
};

/// A wrapper around an Ethereum call cache that buffers call results in
/// memory for the duration of a block. If `get_call` or `set_call` are
/// called with a different block pointer than the one used in the previous
/// call, the buffer is cleared.
pub struct BufferedCallCache {
    call_cache: Arc<dyn EthereumCallCache>,
    buffer: Arc<Mutex<HashMap<(ethabi::Address, Vec<u8>), Vec<u8>>>>,
    block: Arc<Mutex<Option<BlockPtr>>>,
}

impl BufferedCallCache {
    pub fn new(call_cache: Arc<dyn EthereumCallCache>) -> Self {
        Self {
            call_cache,
            buffer: Arc::new(Mutex::new(HashMap::new())),
            block: Arc::new(Mutex::new(None)),
        }
    }

    fn check_block(&self, block: &BlockPtr) {
        let mut self_block = self.block.lock().unwrap();
        if self_block.as_ref() != Some(block) {
            *self_block = Some(block.clone());
            self.buffer.lock().unwrap().clear();
        }
    }
}

impl EthereumCallCache for BufferedCallCache {
    fn get_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: BlockPtr,
    ) -> Result<Option<Vec<u8>>, graph::prelude::Error> {
        self.check_block(&block);

        {
            let buffer = self.buffer.lock().unwrap();
            if let Some(result) = buffer.get(&(contract_address, encoded_call.to_vec())) {
                return Ok(Some(result.clone()));
            }
        }

        let result = self
            .call_cache
            .get_call(contract_address, encoded_call, block)?;

        if let Some(result) = &result {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.insert((contract_address, encoded_call.to_vec()), result.clone());
        }
        Ok(result)
    }

    fn get_calls_in_block(
        &self,
        block: BlockPtr,
    ) -> Result<Vec<CachedEthereumCall>, graph::prelude::Error> {
        self.call_cache.get_calls_in_block(block)
    }

    fn set_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: BlockPtr,
        return_value: &[u8],
    ) -> Result<(), graph::prelude::Error> {
        self.check_block(&block);

        self.call_cache
            .set_call(contract_address, encoded_call, block, return_value)?;

        let mut buffer = self.buffer.lock().unwrap();
        buffer.insert(
            (contract_address, encoded_call.to_vec()),
            return_value.to_vec(),
        );
        Ok(())
    }
}
