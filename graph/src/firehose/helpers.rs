use std::sync::Arc;

use crate::blockchain::{Block as BlockchainBlock, BlockHash, BlockPtr};
use crate::{bail, firehose};
use anyhow::{anyhow, Error};
use base58::FromBase58;
use base64::Engine as _;

use super::InfoResponse;

pub fn decode_firehose_block<M>(
    block_response: &firehose::Response,
) -> Result<Arc<dyn BlockchainBlock>, Error>
where
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    let any_block = block_response
        .block
        .as_ref()
        .expect("block payload information should always be present");

    Ok(Arc::new(M::decode(any_block.value.as_ref())?))
}

impl TryFrom<InfoResponse> for BlockPtr {
    type Error = anyhow::Error;

    fn try_from(rsp: InfoResponse) -> Result<Self, Self::Error> {
        let block_hash_bytes: Vec<u8> = match rsp.block_id_encoding() {
            firehose::info_response::BlockIdEncoding::Unset => bail!("unset block id encoding"),
            firehose::info_response::BlockIdEncoding::Hex => {
                hex::decode(rsp.first_streamable_block_id)?
            }
            firehose::info_response::BlockIdEncoding::BlockIdEncoding0xHex => {
                hex::decode(rsp.first_streamable_block_id)?
            }
            firehose::info_response::BlockIdEncoding::Base58 => {
                FromBase58::from_base58(rsp.first_streamable_block_id.as_str())
                    .map_err(|e| anyhow!("{:?}", e))?
            }
            firehose::info_response::BlockIdEncoding::Base64 => {
                base64::engine::general_purpose::STANDARD.decode(rsp.first_streamable_block_id)?
            }
        };

        Ok(BlockPtr {
            hash: BlockHash::from(block_hash_bytes),
            number: rsp.first_streamable_block_num.try_into()?,
        })
    }
}
