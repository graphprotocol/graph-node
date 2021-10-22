use crate::codec;
use crate::trigger::ReceiptWithOutcome;
use base64;
use graph::anyhow::{anyhow, Context};
use graph::runtime::{asc_new, AscHeap, AscPtr, DeterministicHostError, ToAscObj};
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, EnumPayload, Uint8Array};

pub(crate) use super::generated::*;

impl ToAscObj<AscBlock> for codec::BlockWrapper {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlock, DeterministicHostError> {
        let block = self.block();

        Ok(AscBlock {
            author: asc_new(heap, &block.author)?,
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
        let chunk_mask = Array::new(self.chunk_mask.as_ref(), heap)?;

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
            chunk_mask: AscPtr::alloc_obj(chunk_mask, heap)?,
            gas_price: asc_new(heap, self.gas_price.as_ref().unwrap())?,
            block_ordinal: self.block_ordinal,
            total_supply: asc_new(heap, self.total_supply.as_ref().unwrap())?,
            challenges_result: asc_new(heap, &self.challenges_result)?,
            last_final_block: asc_new(heap, self.last_final_block.as_ref().unwrap())?,
            last_ds_final_block: asc_new(heap, self.last_ds_final_block.as_ref().unwrap())?,
            next_bp_hash: asc_new(heap, self.next_bp_hash.as_ref().unwrap())?,
            block_merkle_root: asc_new(heap, self.block_merkle_root.as_ref().unwrap())?,
            epoch_sync_data_hash: asc_new(heap, self.epoch_sync_data_hash.as_slice())?,
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
            chunk_hash: asc_new(heap, self.chunk_hash.as_slice())?,
            // FIXME: Right now near-dm-indexer does not populate this field properly ...
            signature: AscPtr::null(),
            prev_block_hash: asc_new(heap, self.prev_block_hash.as_slice())?,
            prev_state_root: asc_new(heap, self.prev_state_root.as_slice())?,
            encoded_merkle_root: asc_new(heap, self.encoded_merkle_root.as_slice())?,
            encoded_length: self.encoded_length,
            height_created: self.height_created,
            height_included: self.height_included,
            shard_id: self.shard_id,
            gas_used: self.gas_used,
            gas_limit: self.gas_limit,
            balance_burnt: asc_new(heap, self.balance_burnt.as_ref().unwrap())?,
            outgoing_receipts_root: asc_new(heap, self.outgoing_receipts_root.as_slice())?,
            tx_root: asc_new(heap, self.tx_root.as_slice())?,
            validator_proposals: asc_new(heap, &self.validator_proposals)?,

            _padding: 0,
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

impl ToAscObj<AscReceiptWithOutcome> for ReceiptWithOutcome {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscReceiptWithOutcome, DeterministicHostError> {
        Ok(AscReceiptWithOutcome {
            outcome: asc_new(heap, &self.outcome)?,
            receipt: asc_new(heap, &self.receipt)?,
            block: asc_new(heap, self.block.as_ref())?,
        })
    }
}

impl ToAscObj<AscActionReceipt> for codec::Receipt {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscActionReceipt, DeterministicHostError> {
        let action = match self.receipt.as_ref().unwrap() {
            codec::receipt::Receipt::Action(action) => action,
            codec::receipt::Receipt::Data(_) => {
                return Err(DeterministicHostError(anyhow!(
                    "Data receipt are now allowed"
                )));
            }
        };

        Ok(AscActionReceipt {
            id: asc_new(heap, &self.receipt_id.as_ref().unwrap())?,
            predecessor_id: asc_new(heap, &self.predecessor_id)?,
            receiver_id: asc_new(heap, &self.receiver_id)?,
            signer_id: asc_new(heap, &action.signer_id)?,
            signer_public_key: asc_new(heap, action.signer_public_key.as_ref().unwrap())?,
            gas_price: asc_new(heap, action.gas_price.as_ref().unwrap())?,
            output_data_receivers: asc_new(heap, &action.output_data_receivers)?,
            input_data_ids: asc_new(heap, &action.input_data_ids)?,
            actions: asc_new(heap, &action.actions)?,
        })
    }
}

impl ToAscObj<AscActionEnum> for codec::Action {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscActionEnum, DeterministicHostError> {
        let (kind, payload) = match self.action.as_ref().unwrap() {
            codec::action::Action::CreateAccount(action) => (
                AscActionKind::CreateAccount,
                asc_new(heap, action)?.to_payload(),
            ),
            codec::action::Action::DeployContract(action) => (
                AscActionKind::DeployContract,
                asc_new(heap, action)?.to_payload(),
            ),
            codec::action::Action::FunctionCall(action) => (
                AscActionKind::FunctionCall,
                asc_new(heap, action)?.to_payload(),
            ),
            codec::action::Action::Transfer(action) => {
                (AscActionKind::Transfer, asc_new(heap, action)?.to_payload())
            }
            codec::action::Action::Stake(action) => {
                (AscActionKind::Stake, asc_new(heap, action)?.to_payload())
            }
            codec::action::Action::AddKey(action) => {
                (AscActionKind::AddKey, asc_new(heap, action)?.to_payload())
            }
            codec::action::Action::DeleteKey(action) => (
                AscActionKind::DeleteKey,
                asc_new(heap, action)?.to_payload(),
            ),
            codec::action::Action::DeleteAccount(action) => (
                AscActionKind::DeleteAccount,
                asc_new(heap, action)?.to_payload(),
            ),
        };

        Ok(AscActionEnum(AscEnum {
            _padding: 0,
            kind,
            payload: EnumPayload(payload),
        }))
    }
}

impl ToAscObj<AscActionEnumArray> for Vec<codec::Action> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscActionEnumArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscActionEnumArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscCreateAccountAction> for codec::CreateAccountAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscCreateAccountAction, DeterministicHostError> {
        Ok(AscCreateAccountAction {})
    }
}

impl ToAscObj<AscDeployContractAction> for codec::DeployContractAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscDeployContractAction, DeterministicHostError> {
        // In next iteration of NEAR protobuf format, `self.code` will be renamed to `self.code_hash` and
        // it will a pre-decoded Vec<u8> directly.
        let code_hash = match base64::decode(&self.code)
            .with_context(|| "DeployContract code hash is not in base64 format")
        {
            Ok(bytes) => bytes,
            Err(err) => return Err(DeterministicHostError(err)),
        };

        Ok(AscDeployContractAction {
            code: asc_new(heap, code_hash.as_slice())?,
        })
    }
}

impl ToAscObj<AscFunctionCallAction> for codec::FunctionCallAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscFunctionCallAction, DeterministicHostError> {
        // In next iteration of NEAR protobuf format, `self.args` will a pre-decoded Vec<u8> directly.
        let args = match base64::decode(&self.args)
            .with_context(|| "FunctionCall args is not in base64 format")
        {
            Ok(bytes) => bytes,
            Err(err) => return Err(DeterministicHostError(err)),
        };

        Ok(AscFunctionCallAction {
            method_name: asc_new(heap, &self.method_name)?,
            args: asc_new(heap, args.as_slice())?,
            gas: self.gas,
            deposit: asc_new(heap, self.deposit.as_ref().unwrap())?,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscTransferAction> for codec::TransferAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTransferAction, DeterministicHostError> {
        Ok(AscTransferAction {
            deposit: asc_new(heap, self.deposit.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscStakeAction> for codec::StakeAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscStakeAction, DeterministicHostError> {
        Ok(AscStakeAction {
            stake: asc_new(heap, self.stake.as_ref().unwrap())?,
            public_key: asc_new(heap, self.public_key.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscAddKeyAction> for codec::AddKeyAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscAddKeyAction, DeterministicHostError> {
        Ok(AscAddKeyAction {
            public_key: asc_new(heap, self.public_key.as_ref().unwrap())?,
            access_key: asc_new(heap, self.access_key.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscAccessKey> for codec::AccessKey {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscAccessKey, DeterministicHostError> {
        Ok(AscAccessKey {
            nonce: self.nonce,
            permission: asc_new(heap, self.permission.as_ref().unwrap())?,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscAccessKeyPermissionEnum> for codec::AccessKeyPermission {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscAccessKeyPermissionEnum, DeterministicHostError> {
        let (kind, payload) = match self.permission.as_ref().unwrap() {
            codec::access_key_permission::Permission::FunctionCall(permission) => (
                AscAccessKeyPermissionKind::FunctionCall,
                asc_new(heap, permission)?.to_payload(),
            ),
            codec::access_key_permission::Permission::FullAccess(permission) => (
                AscAccessKeyPermissionKind::FullAccess,
                asc_new(heap, permission)?.to_payload(),
            ),
        };

        Ok(AscAccessKeyPermissionEnum(AscEnum {
            _padding: 0,
            kind,
            payload: EnumPayload(payload),
        }))
    }
}

impl ToAscObj<AscFunctionCallPermission> for codec::FunctionCallPermission {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscFunctionCallPermission, DeterministicHostError> {
        Ok(AscFunctionCallPermission {
            // allowance is one of the few
            allowance: asc_new(heap, self.allowance.as_ref().unwrap())?,
            receiver_id: asc_new(heap, &self.receiver_id)?,
            method_names: asc_new(heap, &self.method_names)?,
        })
    }
}

impl ToAscObj<AscFullAccessPermission> for codec::FullAccessPermission {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscFullAccessPermission, DeterministicHostError> {
        Ok(AscFullAccessPermission {})
    }
}

impl ToAscObj<AscDeleteKeyAction> for codec::DeleteKeyAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscDeleteKeyAction, DeterministicHostError> {
        Ok(AscDeleteKeyAction {
            public_key: asc_new(heap, self.public_key.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscDeleteAccountAction> for codec::DeleteAccountAction {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscDeleteAccountAction, DeterministicHostError> {
        Ok(AscDeleteAccountAction {
            beneficiary_id: asc_new(heap, &self.beneficiary_id)?,
        })
    }
}

impl ToAscObj<AscDataReceiver> for codec::DataReceiver {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscDataReceiver, DeterministicHostError> {
        Ok(AscDataReceiver {
            data_id: asc_new(heap, self.data_id.as_ref().unwrap())?,
            receiver_id: asc_new(heap, &self.receiver_id)?,
        })
    }
}

impl ToAscObj<AscDataReceiverArray> for Vec<codec::DataReceiver> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscDataReceiverArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscDataReceiverArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscExecutionOutcome> for codec::ExecutionOutcomeWithIdView {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscExecutionOutcome, DeterministicHostError> {
        let outcome = self.outcome.as_ref().unwrap();

        Ok(AscExecutionOutcome {
            proof: asc_new(heap, &self.proof.as_ref().unwrap().path)?,
            block_hash: asc_new(heap, self.block_hash.as_ref().unwrap())?,
            id: asc_new(heap, self.id.as_ref().unwrap())?,
            logs: asc_new(heap, &outcome.logs)?,
            receipt_ids: asc_new(heap, &outcome.receipt_ids)?,
            gas_burnt: outcome.gas_burnt,
            tokens_burnt: asc_new(heap, outcome.tokens_burnt.as_ref().unwrap())?,
            executor_id: asc_new(heap, &outcome.executor_id)?,
            status: asc_new(heap, outcome.status.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscSuccessStatusEnum> for codec::execution_outcome::Status {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscSuccessStatusEnum, DeterministicHostError> {
        let (kind, payload) = match self {
            codec::execution_outcome::Status::SuccessValue(value) => {
                // In next iteration of NEAR protobuf format, `value.value` will a pre-decoded Vec<u8> directly.
                let value = match base64::decode(&value.value)
                    .with_context(|| "FunctionCall args is not in base64 format")
                {
                    Ok(bytes) => bytes,
                    Err(err) => return Err(DeterministicHostError(err)),
                };

                (
                    AscSuccessStatusKind::Value,
                    asc_new(heap, value.as_slice())?.to_payload(),
                )
            }
            codec::execution_outcome::Status::SuccessReceiptId(receipt_id) => (
                AscSuccessStatusKind::ReceiptId,
                asc_new(heap, receipt_id.id.as_ref().unwrap())?.to_payload(),
            ),
            codec::execution_outcome::Status::Failure(_) => {
                return Err(DeterministicHostError(anyhow!(
                    "Failure execution status are not allowed"
                )));
            }
            codec::execution_outcome::Status::Unknown(_) => {
                return Err(DeterministicHostError(anyhow!(
                    "Unknown execution status are not allowed"
                )));
            }
        };

        Ok(AscSuccessStatusEnum(AscEnum {
            _padding: 0,
            kind,
            payload: EnumPayload(payload),
        }))
    }
}

impl ToAscObj<AscMerklePathItem> for codec::MerklePathItem {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscMerklePathItem, DeterministicHostError> {
        Ok(AscMerklePathItem {
            hash: asc_new(heap, self.hash.as_ref().unwrap())?,
            direction: match self.direction {
                0 => AscDirection::Left,
                1 => AscDirection::Right,
                x => {
                    return Err(DeterministicHostError(anyhow!(
                        "Invalid direction value {}",
                        x
                    )))
                }
            },
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
            kind: match self.r#type {
                0 => 0,
                1 => 1,
                value => {
                    return Err(DeterministicHostError(anyhow!(
                        "Invalid signature type {}",
                        value,
                    )))
                }
            },
            bytes: asc_new(heap, self.bytes.as_slice())?,
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
            // kind: match self.r#type {
            kind: match 0 {
                0 => 0,
                1 => 1,
                value => {
                    return Err(DeterministicHostError(anyhow!(
                        "Invalid public key type {}",
                        value,
                    )))
                }
            },
            bytes: asc_new(heap, self.bytes.as_slice())?,
        })
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
    ) -> Result<Uint8Array, DeterministicHostError> {
        // FIXME (NEAR): Hopefully the bytes here fits the BigInt format expected in `graph-node`,
        //               will need to validate that in the subgraph directly.
        self.bytes.to_asc_obj(heap)
    }
}
