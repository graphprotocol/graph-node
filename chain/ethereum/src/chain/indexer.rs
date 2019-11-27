use graph::prelude::chains::*;

pub struct EthereumIndexer {}

impl NetworkIndexer for EthereumIndexer {}

impl EventProducer<BlockPointer> 