use crate::codec;
use graph::anyhow;
use graph::runtime::{asc_new, DeterministicHostError, ToAscObj};
use graph::runtime::{AscHeap, AscPtr};
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, EnumPayload, Uint8Array};

pub(crate) use super::generated::*;

impl ToAscObj<AscBlock> for codec::BlockWrapper {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlock, DeterministicHostError> {
        let block = self.block();

        Ok(AscBlock {
            author: asc_new(heap, block.author.as_str())?,
            header: asc_new(heap, self.header())?,
            chunks: asc_new(heap, &block.chunks)?,
        })
    }
}

impl ToAscObj<AscBlockHeader> for codec::BlockHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlockHeader, DeterministicHostError> {
        Ok(AscBlockHeader {
            height: self.height,
            prev_height: self.prev_height,
            epoch_id: asc_new(heap, self.epoch_id.as_ref().unwrap())?,
            next_epoch_id: asc_new(heap, self.next_epoch_id.as_ref().unwrap())?,
            hash: asc_new(heap, self.hash.as_ref().unwrap())?,
            prev_hash: asc_new(heap, self.prev_hash.as_ref().unwrap())?,
            prev_state_root: asc_new(heap, self.prev_state_root.as_ref().unwrap())?,
            chunk_receipts_root: asc_new(heap, self.chunk_receipts_root.as_ref().unwrap())?,
            chunk_headers_root: asc_new(heap, self.chunk_headers_root.as_ref().unwrap())?,
            chunk_tx_root: asc_new(heap, self.chunk_tx_root.as_ref().unwrap())?,
            outcome_root: asc_new(heap, self.outcome_root.as_ref().unwrap())?,
            chunks_included: self.chunks_included,
            challenges_root: asc_new(heap, self.challenges_root.as_ref().unwrap())?,
            timestamp_nanosec: self.timestamp_nanosec,
            random_value: asc_new(heap, self.random_value.as_ref().unwrap())?,
            validator_proposals: asc_new(heap, &self.validator_proposals)?,
            chunk_mask: AscPtr::alloc_obj(Array::new(self.chunk_mask.as_ref(), heap)?, heap)?,
            gas_price: asc_new(heap, self.gas_price.as_ref().unwrap())?,
            block_ordinal: self.block_ordinal,
            total_supply: asc_new(heap, self.total_supply.as_ref().unwrap())?,
            challenges_result: asc_new(heap, &self.challenges_result)?,
            last_final_block: asc_new(heap, self.last_final_block.as_ref().unwrap())?,
            last_ds_final_block: asc_new(heap, self.last_ds_final_block.as_ref().unwrap())?,
            next_bp_hash: asc_new(heap, self.next_bp_hash.as_ref().unwrap())?,
            block_merkle_root: asc_new(heap, self.block_merkle_root.as_ref().unwrap())?,
            epoch_sync_data_hash: asc_new(heap, &Bytes(&self.epoch_sync_data_hash))?,
            approvals: asc_new(heap, &self.approvals)?,
            // FIXME: Right now near-dm-indexer does not populate this field properly ...
            signature: AscPtr::null(),
            latest_protocol_version: self.latest_protocol_version,
        })
    }
}

impl ToAscObj<AscChunkHeader> for codec::ChunkHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscChunkHeader, DeterministicHostError> {
        Ok(AscChunkHeader {
            chunk_hash: asc_new(heap, &Bytes(&self.chunk_hash))?,
            signature: asc_new(heap, self.signature.as_ref().unwrap())?,
            prev_block_hash: asc_new(heap, &Bytes(&self.prev_block_hash))?,
            prev_state_root: asc_new(heap, &Bytes(&self.prev_state_root))?,
            encoded_merkle_root: asc_new(heap, &Bytes(&self.encoded_merkle_root))?,
            encoded_length: self.encoded_length,
            height_created: self.height_created,
            height_included: self.height_included,
            shard_id: self.shard_id,
            gas_used: self.gas_used,
            gas_limit: self.gas_limit,
            balance_burnt: asc_new(heap, self.balance_burnt.as_ref().unwrap())?,
            outgoing_receipts_root: asc_new(heap, &Bytes(&self.outgoing_receipts_root))?,
            tx_root: asc_new(heap, &Bytes(&self.tx_root))?,
            validator_proposals: asc_new(heap, &self.validator_proposals)?,
        })
    }
}

// impl ToAscObj<AscReceiptWithOutcome> for codec::BlockHeader {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//     ) -> Result<AscReceiptWithOutcome, DeterministicHostError> {
//         Ok(AscReceiptWithOutcome {
//             outcome: todo!(),
//             receipt: todo!(),
//             block: todo!(),
//         })
//     }
// }

// impl ToAscObj<AscExecutionOutcome> for codec::ExecutionOutcomeWithIdView {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//     ) -> Result<AscExecutionOutcome, DeterministicHostError> {
//         let outcome = self.outcome.as_ref().unwrap();

//         Ok(AscExecutionOutcome {
//             proof: asc_new(heap, &self.proof.as_ref().unwrap().path)?,
//             block_hash: asc_new(heap, self.block_hash.as_ref().unwrap())?,
//             id: asc_new(heap, self.id.as_ref().unwrap())?,
//             logs: AscPtr::alloc_obj(Array::new(outcome.logs.as_ref(), heap)?, heap)?,
//             receipt_ids: asc_new(heap, &outcome.receipt_ids)?,
//             gas_burnt: outcome.gas_burnt,
//             tokens_burnt: asc_new(heap, outcome.tokens_burnt.as_ref().unwrap())?,
//             executor_id: asc_new(heap, &outcome.executor_id)?,
//             status: asc_new(heap, &SuccessStatusKin(outcome.status.as_ref().unwrap()))?,
//         })
//     }
// }

// impl ToAscObj<AscSuccessStatus> for codec::execution_outcome::Status {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         heap: &mut H,
//     ) -> Result<AscSuccessStatus, DeterministicHostError> {
//         Ok(AscSuccessStatus {
//             kind: asc_new(heap, &SuccessStatusKind(self.status)),
//             data: todo!(),
//         })
//     }
// }

impl ToAscObj<AscMerklePathItem> for codec::MerklePathItem {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscMerklePathItem, DeterministicHostError> {
        Ok(AscMerklePathItem {
            hash: asc_new(heap, self.hash.as_ref().unwrap())?,
            direction: asc_new(heap, &DirectionKind(self.direction))?,
        })
    }
}

impl ToAscObj<AscMerklePathItemArray> for Vec<codec::MerklePathItem> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscMerklePathItemArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscMerklePathItemArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscSignature> for codec::Signature {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscSignature, DeterministicHostError> {
        Ok(AscSignature {
            kind: asc_new(heap, &CurveKind(self.r#type))?,
            bytes: asc_new(heap, &Bytes(&self.bytes))?,
        })
    }
}

impl ToAscObj<AscSignatureArray> for Vec<codec::Signature> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscSignatureArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscSignatureArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscPublicKey> for codec::PublicKey {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscPublicKey, DeterministicHostError> {
        Ok(AscPublicKey {
            // FIXME: Right now proto-near definitions is wrong since it's missing the `type` field.
            //        When `proto-near` has been adjusted, we will uncomment this line and remove the
            //        one below.
            // kind: asc_new(heap, &CurveKind(self.r#type))?,
            kind: asc_new(heap, &CurveKind(0))?,
            bytes: asc_new(heap, &Bytes(&self.bytes))?,
        })
    }
}

impl ToAscObj<AscChunkHeaderArray> for Vec<codec::ChunkHeader> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscChunkHeaderArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscChunkHeaderArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscValidatorStake> for codec::ValidatorStake {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscValidatorStake, DeterministicHostError> {
        Ok(AscValidatorStake {
            account_id: asc_new(heap, &self.account_id)?,
            public_key: asc_new(heap, self.public_key.as_ref().unwrap())?,
            stake: asc_new(heap, self.stake.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscValidatorStakeArray> for Vec<codec::ValidatorStake> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscValidatorStakeArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscValidatorStakeArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscSlashedValidator> for codec::SlashedValidator {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscSlashedValidator, DeterministicHostError> {
        Ok(AscSlashedValidator {
            account_id: asc_new(heap, &self.account_id)?,
            is_double_sign: self.is_double_sign,
        })
    }
}

impl ToAscObj<AscSlashedValidatorArray> for Vec<codec::SlashedValidator> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscSlashedValidatorArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscSlashedValidatorArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<Uint8Array> for codec::CryptoHash {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCryptoHash, DeterministicHostError> {
        self.bytes.to_asc_obj(heap)
    }
}

impl ToAscObj<AscCryptoHashArray> for Vec<codec::CryptoHash> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCryptoHashArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscCryptoHashArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<Uint8Array> for codec::BigInt {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCryptoHash, DeterministicHostError> {
        // FIXME (NEAR): Hopefully the bytes here fits the BigInt format expected in `graph-node`,
        //               will need to validate that in the subgraph directly.
        self.bytes.to_asc_obj(heap)
    }
}

// FIXME: Would like to get rid of this type wrapper. I do not understand why a `Vec<u8>` cannot be turned
//        into the proper object.
struct Bytes<'a>(&'a Vec<u8>);

impl ToAscObj<Uint8Array> for Bytes<'_> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCryptoHash, DeterministicHostError> {
        self.0.to_asc_obj(heap)
    }
}

struct DirectionKind(i32);

impl ToAscObj<AscDirectionEnum> for DirectionKind {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscDirectionEnum, DeterministicHostError> {
        let value = match self.0 {
            0 => AscDirection::Left,
            1 => AscDirection::Right,
            _ => {
                return Err(DeterministicHostError(anyhow::format_err!(
                    "Invalid direction value {}",
                    self.0
                )))
            }
        };

        Ok(AscDirectionEnum(AscEnum {
            _padding: 0,
            kind: value,
            payload: EnumPayload(self.0 as u64),
        }))
    }
}
struct CurveKind(i32);

impl ToAscObj<AscCurveKindEnum> for CurveKind {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscCurveKindEnum, DeterministicHostError> {
        let value = match self.0 {
            0 => AscCurveKind::Ed25519,
            1 => AscCurveKind::Secp256K1,
            _ => {
                return Err(DeterministicHostError(anyhow::format_err!(
                    "Invalid signature type value {}",
                    self.0
                )))
            }
        };

        Ok(AscCurveKindEnum(AscEnum {
            _padding: 0,
            kind: value,
            payload: EnumPayload(self.0 as u64),
        }))
    }
}

// struct SuccessStatusKind(codec::execution_outcome::Status);

// impl ToAscObj<AscSuccessStatusKindEnum> for SuccessStatusKind {
//     fn to_asc_obj<H: AscHeap + ?Sized>(
//         &self,
//         _heap: &mut H,
//     ) -> Result<AscSuccessStatusKindEnum, DeterministicHostError> {
//         let value = match self.0 {
//             codec::execution_outcome::Status::Failure(_) => todo!(),
//             codec::execution_outcome::Status::SuccessValue(_) => todo!(),
//             codec::execution_outcome::Status::SuccessReceiptId(_) => todo!(),
//             codec::execution_outcome::Status::Unknown(_) => {
//                 return Err(DeterministicHostError(anyhow::format_err!(
//                     "Invalid success status unknown",
//                 )))
//             } // 0 => AscSuccessStatusKind::Value,
//               // 1 => AscSuccessStatusKind::ReceiptId,
//               // _ => {
//               //     return Err(DeterministicHostError(anyhow::format_err!(
//               //         "Invalid success status type value {}",
//               //         self.0
//               //     )))
//               // }
//         };

//         Ok(AscSuccessStatusKindEnum(AscEnum {
//             _padding: 0,
//             kind: value,
//             payload: EnumPayload(self.0 as u64),
//         }))
//     }
// }
