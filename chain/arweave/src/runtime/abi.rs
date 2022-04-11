use crate::codec;
use crate::trigger::ReceiptWithOutcome;
use graph::anyhow::anyhow;
use graph::runtime::gas::GasCounter;
use graph::runtime::{asc_new, AscHeap, AscPtr, DeterministicHostError, ToAscObj};
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, EnumPayload, Uint8Array};

pub(crate) use super::generated::*;

impl ToAscObj<AscTag> for codec::Tag {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTag, DeterministicHostError> {
        Ok(AscTag {
            name: asc_new(heap, self.name.as_slice(), gas)?,
            value: asc_new(heap, self.value.as_slice(), gas)?,
        })
    }
}

impl ToAscObj<AscTagArray> for Vec<codec::Tag> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTagArray, DeterministicHostError> {
        let content = self
            .iter()
            .map(|x| asc_new(heap, x, gas))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(AscTagArray(Array::new(&*content, heap, gas)?))
    }
}

impl ToAscObj<AscProofOfAccess> for codec::ProofOfAccess {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscProofOfAccess, DeterministicHostError> {
        Ok(AscProofOfAccess {
            option: asc_new(heap, &self.option, gas)?,
            tx_path: asc_new(heap, self.tx_path.as_slice(), gas)?,
            data_path: asc_new(heap, self.data_path.as_slice(), gas)?,
            chunk: asc_new(heap, self.chunk.as_slice(), gas)?,
        })
    }
}

impl ToAscObj<AscTransaction> for codec::Transaction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTransaction, DeterministicHostError> {
        Ok(AscTransaction {
            format: self.format,
            id: asc_new(heap, &self.id, gas)?,
            last_tx: asc_new(heap, self.last_tx.as_slice(), gas)?,
            owner: asc_new(heap, self.owner.as_slice(), gas)?,
            tags: asc_new(heap, &self.tags, gas)?,
            target: asc_new(heap, self.target.as_slice(), gas)?,
            quantity: asc_new(heap, &self.quantity, gas)?,
            data: asc_new(heap, self.data.as_slice(), gas)?,
            data_size: asc_new(heap, &self.data_size, gas)?,
            data_root: asc_new(heap, self.data_root.as_slice(), gas)?,
            signature: asc_new(heap, self.signature.as_slice(), gas)?,
            reward: asc_new(heap, &self.reward, gas)?,
        })
    }
}

impl ToAscObj<AscBlock> for codec::Block {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBlock, DeterministicHostError> {
        Ok(AscBlock {
            indep_hash: self.height,
            nonce: asc_new(heap, self.nonce.as_slice(), gas)?,
            previous_block: asc_new(heap, self.previous_block.as_slice(), gas)?,
            timestamp: self.timestamp,
            last_retarget: self.last_retarget,
            diff: asc_new(heap, &self.diff, gas)?,
            height: self.height,
            hash: asc_new(heap, self.hash.as_slice(), gas)?,
            tx_root: self
                .tx_root
                .map(|tx_root| asc_new(heap, tx_root.as_slice(), gas))
                .unwrap_or(Ok(AscPtr::null()))?,
            wallet_list: asc_new(heap, self.wallet_list.as_slice(), gas)?,
            reward_addr: asc_new(heap, self.reward_addr.as_slice(), gas)?,
            tags: asc_new(heap, &self.tags, gas)?,
            reward_pool: asc_new(heap, &self.reward_pool, gas)?,
            weave_size: asc_new(heap, &self.weave_size, gas)?,
            block_size: asc_new(heap, &self.block_size, gas)?,
            cumulative_diff: self
                .cumulative_diff
                .map(|cumulative_diff| asc_new(heap, &cumulative_diff, gas))
                .unwrap_or(Ok(AscPtr::null()))?,
            hash_list_merkle: self
                .hash_list_merkle
                .map(|hash_list_merkle| asc_new(heap, hash_list_merkle.as_slice(), gas))
                .unwrap_or(Ok(AscPtr::null()))?,
            poa: self
                .poa
                .as_ref()
                .map(|poa| asc_new(heap, poa, gas))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}
