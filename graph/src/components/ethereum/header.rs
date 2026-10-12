use alloy::consensus::BlockHeader;
use alloy::network::AnyHeader;
use alloy::primitives::{Address, B64, B256, BlockNumber, Bloom, Bytes, U256};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::ops::{Deref, DerefMut};

/// Alloy's [`AnyHeader`], except that a missing `sha3Uncles` deserializes as
/// the zero hash, as it did with graph-node's rust-web3 fork.
/// The zero hash is kept for compatibility: mappings can read it as
/// `ethereum.Block.unclesHash`. Serializes exactly like `AnyHeader`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct LenientHeader(pub AnyHeader);

impl<'de> Deserialize<'de> for LenientHeader {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut header = Value::deserialize(deserializer)?;
        if let Value::Object(fields) = &mut header
            && !fields.contains_key("sha3Uncles")
        {
            fields.insert("sha3Uncles".to_string(), B256::ZERO.to_string().into());
        }
        AnyHeader::deserialize(header)
            .map(LenientHeader)
            .map_err(serde::de::Error::custom)
    }
}

impl Deref for LenientHeader {
    type Target = AnyHeader;

    fn deref(&self) -> &AnyHeader {
        &self.0
    }
}

impl DerefMut for LenientHeader {
    fn deref_mut(&mut self) -> &mut AnyHeader {
        &mut self.0
    }
}

impl From<AnyHeader> for LenientHeader {
    fn from(header: AnyHeader) -> Self {
        LenientHeader(header)
    }
}

macro_rules! delegate {
    ($($method:ident -> $ret:ty),* $(,)?) => {
        $(fn $method(&self) -> $ret { self.0.$method() })*
    };
}

impl BlockHeader for LenientHeader {
    delegate! {
        parent_hash -> B256,
        ommers_hash -> B256,
        beneficiary -> Address,
        state_root -> B256,
        transactions_root -> B256,
        receipts_root -> B256,
        withdrawals_root -> Option<B256>,
        logs_bloom -> Bloom,
        difficulty -> U256,
        number -> BlockNumber,
        gas_limit -> u64,
        gas_used -> u64,
        timestamp -> u64,
        mix_hash -> Option<B256>,
        nonce -> Option<B64>,
        base_fee_per_gas -> Option<u64>,
        blob_gas_used -> Option<u64>,
        excess_blob_gas -> Option<u64>,
        parent_beacon_block_root -> Option<B256>,
        requests_hash -> Option<B256>,
        block_access_list_hash -> Option<B256>,
        slot_number -> Option<u64>,
        extra_data -> &Bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::ethereum::AnyBlock;

    /// Celo mainnet block 18946750 (before Gingerbread), as returned by
    /// forno.celo.org. It has no `sha3Uncles` field.
    const CELO_PRE_GINGERBREAD_BLOCK: &str = r#"{"baseFeePerGas":"0x12a05f200","difficulty":"0x0","epochSnarkData":null,"extraData":"0xd983010700846765746889676f312e31372e3133856c696e7578000000000000f8ccc0c080b841a2dabeefc7d980e7da2dc0279a009f071892f2da13513fad2c78564b02ac62ad1846f48b2c374cecdb3008fbe58d3381809d62d0542a8468b94f4e9b22be904300f8418e303ed3fcd8eaff6d5f7fbc5f79bcb0f84388e266f10d9cb40a8aa194fd4d496e0481f03e0f0115b090c8510253278495ea59cd861dfd688fd62a7a263d290180f8418e3fffffffffffffffffffefffffffb0450595e2afb27d47dbababe99a3f093d4c0f9e7a4b555fff11980cf43ca2a7a5583c75877c4ed993908a49a1b8d4290080","gasLimit":"0x1312d00","gasUsed":"0xd647e","hash":"0x8e4a323d009fdd25a67f58d883abbc5b5f029f213a2e64f39dd73e0efecc083a","logsBloom":"0x00800000000200000000240040020000000000000000000000000000000000000000080000000000000000002000000800000000000000000000000000200000000000100002000200000008000000000840002004000000000000000000000000000000000400000004400000008008000000000000000040910010000001000402000080000000400080000400000800000000000040800000000000008100000000800000000000000000000010000000000000000000000000000400010000000002000000020000100000002000000800000000000000000000400000000000000000000000020004020000000000000240000020000000000000000000","miner":"0x8e5db67c3ca13c3487fa291953ade27b503ed6bd","number":"0x1211abe","parentHash":"0x265cb829255b379e0812f857b162450e8330dd34cd21082af15b502afb960539","randomness":{"committed":"0x5a6dd907afeb4dbe106605e77f3eee2d8778dee1e99d9bc74705fec15540d61c","revealed":"0xf8158ddf7f63993ecefc39d4ca064aeb987c15d62b24807b079a1c8b29d3d8f6"},"receiptsRoot":"0xf06c82b478ebd9f5281756a1876d7526a6a952e7ecad6b040748c700378bfca2","size":"0x64b","stateRoot":"0x55d3594e686901a30be71777d8cc11575da5cfa2c78c57303933a3db6cf18b87","timestamp":"0x64476aa4","totalDifficulty":"0x1211abf","transactions":["0xc06b5d3f226083ea603fdfffaf910aef6794709f0b7ac2226f828ba053fbc015","0xd46049a51bbd0baf617d63cc264694d2c9ea4cda11ac348ea5bb813844611d0a","0x2422aea2e4815963e7c6d2fefa0698cb827ef180dd662364203cf416262419f8","0x973372c269483c298d40d111d5b342f6d30d66bb46cfab6cf910e5252ab9cb9e"],"transactionsRoot":"0x7fc667ab6e209550d53bd09f04fbd8fd12f277cc12733ecf4af1c95b99b51fd3"}"#;

    #[test]
    fn missing_uncles_hash_is_zero() {
        let block: AnyBlock = serde_json::from_str(CELO_PRE_GINGERBREAD_BLOCK).unwrap();
        assert_eq!(block.header.number, 18946750);
        assert_eq!(block.header.ommers_hash, B256::ZERO);
        assert_eq!(block.transactions.len(), 4);
    }

    #[test]
    fn present_uncles_hash_is_kept() {
        let uncles = "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347";
        let json =
            CELO_PRE_GINGERBREAD_BLOCK.replacen('{', &format!(r#"{{"sha3Uncles":"{uncles}","#), 1);
        let block: AnyBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(block.header.ommers_hash, uncles.parse::<B256>().unwrap());
    }

    #[test]
    fn round_trips_through_json() {
        let uncles = "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347";
        let json =
            CELO_PRE_GINGERBREAD_BLOCK.replacen('{', &format!(r#"{{"sha3Uncles":"{uncles}","#), 1);
        let block: AnyBlock = serde_json::from_str(&json).unwrap();
        let back: AnyBlock = serde_json::from_value(serde_json::to_value(&block).unwrap()).unwrap();
        assert_eq!(back.header, block.header);
    }
}
