use crate::codec;
use crate::trigger::ReceiptWithOutcome;
use graph::anyhow::anyhow;
use graph::prelude::async_trait;
use graph::runtime::gas::GasCounter;
use graph::runtime::{asc_new, AscHeap, AscPtr, DeterministicHostError, HostExportError, ToAscObj};
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, EnumPayload, Uint8Array};

pub(crate) use super::generated::*;

#[async_trait]
impl ToAscObj<AscBlock> for codec::Block {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBlock, HostExportError> {
        Ok(AscBlock {
            author: asc_new(heap, &self.author, gas).await?,
            header: asc_new(heap, self.header(), gas).await?,
            chunks: asc_new(heap, &self.chunk_headers, gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscBlockHeader> for codec::BlockHeader {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBlockHeader, HostExportError> {
        let chunk_mask = Array::new(self.chunk_mask.as_ref(), heap, gas).await?;

        Ok(AscBlockHeader {
            height: self.height,
            prev_height: self.prev_height,
            epoch_id: asc_new(heap, self.epoch_id.as_ref().unwrap(), gas).await?,
            next_epoch_id: asc_new(heap, self.next_epoch_id.as_ref().unwrap(), gas).await?,
            hash: asc_new(heap, self.hash.as_ref().unwrap(), gas).await?,
            prev_hash: asc_new(heap, self.prev_hash.as_ref().unwrap(), gas).await?,
            prev_state_root: asc_new(heap, self.prev_state_root.as_ref().unwrap(), gas).await?,
            chunk_receipts_root: asc_new(heap, self.chunk_receipts_root.as_ref().unwrap(), gas)
                .await?,
            chunk_headers_root: asc_new(heap, self.chunk_headers_root.as_ref().unwrap(), gas)
                .await?,
            chunk_tx_root: asc_new(heap, self.chunk_tx_root.as_ref().unwrap(), gas).await?,
            outcome_root: asc_new(heap, self.outcome_root.as_ref().unwrap(), gas).await?,
            chunks_included: self.chunks_included,
            challenges_root: asc_new(heap, self.challenges_root.as_ref().unwrap(), gas).await?,
            timestamp_nanosec: self.timestamp_nanosec,
            random_value: asc_new(heap, self.random_value.as_ref().unwrap(), gas).await?,
            validator_proposals: asc_new(heap, &self.validator_proposals, gas).await?,
            chunk_mask: AscPtr::alloc_obj(chunk_mask, heap, gas).await?,
            gas_price: asc_new(heap, self.gas_price.as_ref().unwrap(), gas).await?,
            block_ordinal: self.block_ordinal,
            total_supply: asc_new(heap, self.total_supply.as_ref().unwrap(), gas).await?,
            challenges_result: asc_new(heap, &self.challenges_result, gas).await?,
            last_final_block: asc_new(heap, self.last_final_block.as_ref().unwrap(), gas).await?,
            last_ds_final_block: asc_new(heap, self.last_ds_final_block.as_ref().unwrap(), gas)
                .await?,
            next_bp_hash: asc_new(heap, self.next_bp_hash.as_ref().unwrap(), gas).await?,
            block_merkle_root: asc_new(heap, self.block_merkle_root.as_ref().unwrap(), gas).await?,
            epoch_sync_data_hash: asc_new(heap, self.epoch_sync_data_hash.as_slice(), gas).await?,
            approvals: asc_new(heap, &self.approvals, gas).await?,
            signature: asc_new(heap, &self.signature.as_ref().unwrap(), gas).await?,
            latest_protocol_version: self.latest_protocol_version,
        })
    }
}

#[async_trait]
impl ToAscObj<AscChunkHeader> for codec::ChunkHeader {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscChunkHeader, HostExportError> {
        Ok(AscChunkHeader {
            chunk_hash: asc_new(heap, self.chunk_hash.as_slice(), gas).await?,
            signature: asc_new(heap, &self.signature.as_ref().unwrap(), gas).await?,
            prev_block_hash: asc_new(heap, self.prev_block_hash.as_slice(), gas).await?,
            prev_state_root: asc_new(heap, self.prev_state_root.as_slice(), gas).await?,
            encoded_merkle_root: asc_new(heap, self.encoded_merkle_root.as_slice(), gas).await?,
            encoded_length: self.encoded_length,
            height_created: self.height_created,
            height_included: self.height_included,
            shard_id: self.shard_id,
            gas_used: self.gas_used,
            gas_limit: self.gas_limit,
            balance_burnt: asc_new(heap, self.balance_burnt.as_ref().unwrap(), gas).await?,
            outgoing_receipts_root: asc_new(heap, self.outgoing_receipts_root.as_slice(), gas)
                .await?,
            tx_root: asc_new(heap, self.tx_root.as_slice(), gas).await?,
            validator_proposals: asc_new(heap, &self.validator_proposals, gas).await?,

            _padding: 0,
        })
    }
}

#[async_trait]
impl ToAscObj<AscChunkHeaderArray> for Vec<codec::ChunkHeader> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscChunkHeaderArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscChunkHeaderArray(Array::new(&content, heap, gas).await?))
    }
}

#[async_trait]
impl ToAscObj<AscReceiptWithOutcome> for ReceiptWithOutcome {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscReceiptWithOutcome, HostExportError> {
        Ok(AscReceiptWithOutcome {
            outcome: asc_new(heap, &self.outcome, gas).await?,
            receipt: asc_new(heap, &self.receipt, gas).await?,
            block: asc_new(heap, self.block.as_ref(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscActionReceipt> for codec::Receipt {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscActionReceipt, HostExportError> {
        let action = match self.receipt.as_ref().unwrap() {
            codec::receipt::Receipt::Action(action) => action,
            codec::receipt::Receipt::Data(_) => {
                return Err(
                    DeterministicHostError::from(anyhow!("Data receipt are now allowed")).into(),
                );
            }
        };

        Ok(AscActionReceipt {
            id: asc_new(heap, &self.receipt_id.as_ref().unwrap(), gas).await?,
            predecessor_id: asc_new(heap, &self.predecessor_id, gas).await?,
            receiver_id: asc_new(heap, &self.receiver_id, gas).await?,
            signer_id: asc_new(heap, &action.signer_id, gas).await?,
            signer_public_key: asc_new(heap, action.signer_public_key.as_ref().unwrap(), gas)
                .await?,
            gas_price: asc_new(heap, action.gas_price.as_ref().unwrap(), gas).await?,
            output_data_receivers: asc_new(heap, &action.output_data_receivers, gas).await?,
            input_data_ids: asc_new(heap, &action.input_data_ids, gas).await?,
            actions: asc_new(heap, &action.actions, gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscActionEnum> for codec::Action {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscActionEnum, HostExportError> {
        let (kind, payload) = match self.action.as_ref().unwrap() {
            codec::action::Action::CreateAccount(action) => (
                AscActionKind::CreateAccount,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
            codec::action::Action::DeployContract(action) => (
                AscActionKind::DeployContract,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
            codec::action::Action::FunctionCall(action) => (
                AscActionKind::FunctionCall,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
            codec::action::Action::Transfer(action) => (
                AscActionKind::Transfer,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
            codec::action::Action::Stake(action) => (
                AscActionKind::Stake,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
            codec::action::Action::AddKey(action) => (
                AscActionKind::AddKey,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
            codec::action::Action::DeleteKey(action) => (
                AscActionKind::DeleteKey,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
            codec::action::Action::DeleteAccount(action) => (
                AscActionKind::DeleteAccount,
                asc_new(heap, action, gas).await?.to_payload(),
            ),
        };

        Ok(AscActionEnum(AscEnum {
            kind,
            _padding: 0,
            payload: EnumPayload(payload),
        }))
    }
}

#[async_trait]
impl ToAscObj<AscActionEnumArray> for Vec<codec::Action> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscActionEnumArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscActionEnumArray(Array::new(&content, heap, gas).await?))
    }
}

#[async_trait]
impl ToAscObj<AscCreateAccountAction> for codec::CreateAccountAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscCreateAccountAction, HostExportError> {
        Ok(AscCreateAccountAction {})
    }
}

#[async_trait]
impl ToAscObj<AscDeployContractAction> for codec::DeployContractAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscDeployContractAction, HostExportError> {
        Ok(AscDeployContractAction {
            code: asc_new(heap, self.code.as_slice(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscFunctionCallAction> for codec::FunctionCallAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscFunctionCallAction, HostExportError> {
        Ok(AscFunctionCallAction {
            method_name: asc_new(heap, &self.method_name, gas).await?,
            args: asc_new(heap, self.args.as_slice(), gas).await?,
            gas: self.gas,
            deposit: asc_new(heap, self.deposit.as_ref().unwrap(), gas).await?,
            _padding: 0,
        })
    }
}

#[async_trait]
impl ToAscObj<AscTransferAction> for codec::TransferAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTransferAction, HostExportError> {
        Ok(AscTransferAction {
            deposit: asc_new(heap, self.deposit.as_ref().unwrap(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscStakeAction> for codec::StakeAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscStakeAction, HostExportError> {
        Ok(AscStakeAction {
            stake: asc_new(heap, self.stake.as_ref().unwrap(), gas).await?,
            public_key: asc_new(heap, self.public_key.as_ref().unwrap(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscAddKeyAction> for codec::AddKeyAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAddKeyAction, HostExportError> {
        Ok(AscAddKeyAction {
            public_key: asc_new(heap, self.public_key.as_ref().unwrap(), gas).await?,
            access_key: asc_new(heap, self.access_key.as_ref().unwrap(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscAccessKey> for codec::AccessKey {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAccessKey, HostExportError> {
        Ok(AscAccessKey {
            nonce: self.nonce,
            permission: asc_new(heap, self.permission.as_ref().unwrap(), gas).await?,
            _padding: 0,
        })
    }
}

#[async_trait]
impl ToAscObj<AscAccessKeyPermissionEnum> for codec::AccessKeyPermission {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAccessKeyPermissionEnum, HostExportError> {
        let (kind, payload) = match self.permission.as_ref().unwrap() {
            codec::access_key_permission::Permission::FunctionCall(permission) => (
                AscAccessKeyPermissionKind::FunctionCall,
                asc_new(heap, permission, gas).await?.to_payload(),
            ),
            codec::access_key_permission::Permission::FullAccess(permission) => (
                AscAccessKeyPermissionKind::FullAccess,
                asc_new(heap, permission, gas).await?.to_payload(),
            ),
        };

        Ok(AscAccessKeyPermissionEnum(AscEnum {
            _padding: 0,
            kind,
            payload: EnumPayload(payload),
        }))
    }
}

#[async_trait]
impl ToAscObj<AscFunctionCallPermission> for codec::FunctionCallPermission {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscFunctionCallPermission, HostExportError> {
        Ok(AscFunctionCallPermission {
            // The `allowance` field is one of the few fields that can actually be None for real
            allowance: match self.allowance.as_ref() {
                Some(allowance) => asc_new(heap, allowance, gas).await?,
                None => AscPtr::null(),
            },
            receiver_id: asc_new(heap, &self.receiver_id, gas).await?,
            method_names: asc_new(heap, &self.method_names, gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscFullAccessPermission> for codec::FullAccessPermission {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscFullAccessPermission, HostExportError> {
        Ok(AscFullAccessPermission {})
    }
}

#[async_trait]
impl ToAscObj<AscDeleteKeyAction> for codec::DeleteKeyAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscDeleteKeyAction, HostExportError> {
        Ok(AscDeleteKeyAction {
            public_key: asc_new(heap, self.public_key.as_ref().unwrap(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscDeleteAccountAction> for codec::DeleteAccountAction {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscDeleteAccountAction, HostExportError> {
        Ok(AscDeleteAccountAction {
            beneficiary_id: asc_new(heap, &self.beneficiary_id, gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscDataReceiver> for codec::DataReceiver {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscDataReceiver, HostExportError> {
        Ok(AscDataReceiver {
            data_id: asc_new(heap, self.data_id.as_ref().unwrap(), gas).await?,
            receiver_id: asc_new(heap, &self.receiver_id, gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscDataReceiverArray> for Vec<codec::DataReceiver> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscDataReceiverArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscDataReceiverArray(Array::new(&content, heap, gas).await?))
    }
}

#[async_trait]
impl ToAscObj<AscExecutionOutcome> for codec::ExecutionOutcomeWithId {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscExecutionOutcome, HostExportError> {
        let outcome = self.outcome.as_ref().unwrap();

        Ok(AscExecutionOutcome {
            proof: asc_new(heap, &self.proof.as_ref().unwrap().path, gas).await?,
            block_hash: asc_new(heap, self.block_hash.as_ref().unwrap(), gas).await?,
            id: asc_new(heap, self.id.as_ref().unwrap(), gas).await?,
            logs: asc_new(heap, &outcome.logs, gas).await?,
            receipt_ids: asc_new(heap, &outcome.receipt_ids, gas).await?,
            gas_burnt: outcome.gas_burnt,
            tokens_burnt: asc_new(heap, outcome.tokens_burnt.as_ref().unwrap(), gas).await?,
            executor_id: asc_new(heap, &outcome.executor_id, gas).await?,
            status: asc_new(heap, outcome.status.as_ref().unwrap(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscSuccessStatusEnum> for codec::execution_outcome::Status {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSuccessStatusEnum, HostExportError> {
        let (kind, payload) = match self {
            codec::execution_outcome::Status::SuccessValue(value) => {
                let bytes = &value.value;

                (
                    AscSuccessStatusKind::Value,
                    asc_new(heap, bytes.as_slice(), gas).await?.to_payload(),
                )
            }
            codec::execution_outcome::Status::SuccessReceiptId(receipt_id) => (
                AscSuccessStatusKind::ReceiptId,
                asc_new(heap, receipt_id.id.as_ref().unwrap(), gas)
                    .await?
                    .to_payload(),
            ),
            codec::execution_outcome::Status::Failure(_) => {
                return Err(DeterministicHostError::from(anyhow!(
                    "Failure execution status are not allowed"
                ))
                .into());
            }
            codec::execution_outcome::Status::Unknown(_) => {
                return Err(DeterministicHostError::from(anyhow!(
                    "Unknown execution status are not allowed"
                ))
                .into());
            }
        };

        Ok(AscSuccessStatusEnum(AscEnum {
            _padding: 0,
            kind,
            payload: EnumPayload(payload),
        }))
    }
}

#[async_trait]
impl ToAscObj<AscMerklePathItem> for codec::MerklePathItem {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscMerklePathItem, HostExportError> {
        Ok(AscMerklePathItem {
            hash: asc_new(heap, self.hash.as_ref().unwrap(), gas).await?,
            direction: match self.direction {
                0 => AscDirection::Left,
                1 => AscDirection::Right,
                x => {
                    return Err(DeterministicHostError::from(anyhow!(
                        "Invalid direction value {}",
                        x
                    ))
                    .into())
                }
            },
        })
    }
}

#[async_trait]
impl ToAscObj<AscMerklePathItemArray> for Vec<codec::MerklePathItem> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscMerklePathItemArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscMerklePathItemArray(
            Array::new(&content, heap, gas).await?,
        ))
    }
}

#[async_trait]
impl ToAscObj<AscSignature> for codec::Signature {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSignature, HostExportError> {
        Ok(AscSignature {
            kind: match self.r#type {
                0 => 0,
                1 => 1,
                value => {
                    return Err(DeterministicHostError::from(anyhow!(
                        "Invalid signature type {}",
                        value,
                    ))
                    .into())
                }
            },
            bytes: asc_new(heap, self.bytes.as_slice(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscSignatureArray> for Vec<codec::Signature> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSignatureArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscSignatureArray(Array::new(&content, heap, gas).await?))
    }
}

#[async_trait]
impl ToAscObj<AscPublicKey> for codec::PublicKey {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPublicKey, HostExportError> {
        Ok(AscPublicKey {
            kind: match self.r#type {
                0 => 0,
                1 => 1,
                value => {
                    return Err(DeterministicHostError::from(anyhow!(
                        "Invalid public key type {}",
                        value,
                    ))
                    .into())
                }
            },
            bytes: asc_new(heap, self.bytes.as_slice(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscValidatorStake> for codec::ValidatorStake {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorStake, HostExportError> {
        Ok(AscValidatorStake {
            account_id: asc_new(heap, &self.account_id, gas).await?,
            public_key: asc_new(heap, self.public_key.as_ref().unwrap(), gas).await?,
            stake: asc_new(heap, self.stake.as_ref().unwrap(), gas).await?,
        })
    }
}

#[async_trait]
impl ToAscObj<AscValidatorStakeArray> for Vec<codec::ValidatorStake> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorStakeArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscValidatorStakeArray(
            Array::new(&content, heap, gas).await?,
        ))
    }
}

#[async_trait]
impl ToAscObj<AscSlashedValidator> for codec::SlashedValidator {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSlashedValidator, HostExportError> {
        Ok(AscSlashedValidator {
            account_id: asc_new(heap, &self.account_id, gas).await?,
            is_double_sign: self.is_double_sign,
        })
    }
}

#[async_trait]
impl ToAscObj<AscSlashedValidatorArray> for Vec<codec::SlashedValidator> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSlashedValidatorArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscSlashedValidatorArray(
            Array::new(&content, heap, gas).await?,
        ))
    }
}

#[async_trait]
impl ToAscObj<Uint8Array> for codec::CryptoHash {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCryptoHash, HostExportError> {
        self.bytes.to_asc_obj(heap, gas).await
    }
}

#[async_trait]
impl ToAscObj<AscCryptoHashArray> for Vec<codec::CryptoHash> {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCryptoHashArray, HostExportError> {
        let mut content = Vec::new();
        for x in self {
            content.push(asc_new(heap, x, gas).await?);
        }
        Ok(AscCryptoHashArray(Array::new(&content, heap, gas).await?))
    }
}

#[async_trait]
impl ToAscObj<Uint8Array> for codec::BigInt {
    async fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, HostExportError> {
        // Bytes are reversed to align with BigInt bytes endianess
        let reversed: Vec<u8> = self.bytes.iter().rev().copied().collect();

        reversed.to_asc_obj(heap, gas).await
    }
}
