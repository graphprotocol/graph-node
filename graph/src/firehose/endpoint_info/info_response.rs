use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;

use crate::blockchain::BlockHash;
use crate::blockchain::BlockPtr;
use crate::components::network_provider::ChainName;
use crate::firehose::codec;

#[derive(Clone, Debug)]
pub struct InfoResponse {
    pub chain_name: ChainName,
    pub block_features: Vec<String>,

    first_streamable_block_num: u64,
    first_streamable_block_hash: BlockHash,
}

impl InfoResponse {
    /// Returns the ptr of the genesis block from the perspective of the Firehose.
    /// It is not guaranteed to be the genesis block ptr of the chain.
    ///
    /// There is currently no better way to get the genesis block ptr from Firehose.
    pub fn genesis_block_ptr(&self) -> Result<BlockPtr> {
        let hash = self.first_streamable_block_hash.clone();
        let number = self.first_streamable_block_num;

        Ok(BlockPtr {
            hash,
            number: number
                .try_into()
                .with_context(|| format!("'{number}' is not a valid `BlockNumber`"))?,
        })
    }
}

impl TryFrom<codec::InfoResponse> for InfoResponse {
    type Error = anyhow::Error;

    fn try_from(resp: codec::InfoResponse) -> Result<Self> {
        let codec::InfoResponse {
            chain_name,
            chain_name_aliases: _,
            first_streamable_block_num,
            first_streamable_block_id,
            block_id_encoding,
            block_features,
        } = resp;

        let encoding = codec::info_response::BlockIdEncoding::try_from(block_id_encoding)?;

        Ok(Self {
            chain_name: chain_name_checked(chain_name)?,
            block_features: block_features_checked(block_features)?,
            first_streamable_block_num,
            first_streamable_block_hash: parse_block_hash(first_streamable_block_id, encoding)?,
        })
    }
}

fn chain_name_checked(chain_name: String) -> Result<ChainName> {
    if chain_name.is_empty() {
        return Err(anyhow!("`chain_name` is empty"));
    }

    Ok(chain_name.into())
}

fn block_features_checked(block_features: Vec<String>) -> Result<Vec<String>> {
    if block_features.iter().any(|x| x.is_empty()) {
        return Err(anyhow!("`block_features` contains empty features"));
    }

    Ok(block_features)
}

fn parse_block_hash(
    s: String,
    encoding: codec::info_response::BlockIdEncoding,
) -> Result<BlockHash> {
    use base64::engine::general_purpose::STANDARD;
    use base64::engine::general_purpose::URL_SAFE;
    use base64::Engine;
    use codec::info_response::BlockIdEncoding::*;

    let block_hash = match encoding {
        Unset => return Err(anyhow!("`block_id_encoding` is not set")),
        Hex => hex::decode(s)?.into(),
        BlockIdEncoding0xHex => hex::decode(s.trim_start_matches("0x"))?.into(),
        Base58 => bs58::decode(s).into_vec()?.into(),
        Base64 => STANDARD.decode(s)?.into(),
        Base64url => URL_SAFE.decode(s)?.into(),
    };

    Ok(block_hash)
}
